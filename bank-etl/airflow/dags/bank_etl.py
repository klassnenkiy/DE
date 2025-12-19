from datetime import datetime, timezone
import json
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_ID = "bank_etl_local"

SRC_CONN_ID = "src_postgres"
DWH_CONN_ID = "dwh_postgres"

def _ensure_meta(dwh):
    dwh.run("""
    create table if not exists public.etl_meta (
        key text primary key,
        value_bigint bigint,
        value_ts timestamptz
    )
    """)

def _get_meta_bigint(dwh, key, default=0):
    row = dwh.get_first("select value_bigint from public.etl_meta where key=%s", parameters=(key,))
    if not row or row[0] is None:
        return default
    return int(row[0])

def _get_meta_ts(dwh, key):
    row = dwh.get_first("select value_ts from public.etl_meta where key=%s", parameters=(key,))
    if not row or row[0] is None:
        return None
    return row[0]

def _set_meta_bigint(dwh, key, val):
    dwh.run("""
    insert into public.etl_meta(key, value_bigint) values (%s, %s)
    on conflict (key) do update set value_bigint=excluded.value_bigint
    """, parameters=(key, int(val)))

def _set_meta_ts(dwh, key, val):
    dwh.run("""
    insert into public.etl_meta(key, value_ts) values (%s, %s)
    on conflict (key) do update set value_ts=excluded.value_ts
    """, parameters=(key, val))

def _to_json_str(x):
    if x is None:
        return None
    if isinstance(x, (dict, list)):
        return json.dumps(x, ensure_ascii=False)
    if isinstance(x, (str, int, float, bool)):
        return x
    try:
        return json.dumps(x, default=str, ensure_ascii=False)
    except Exception:
        return str(x)

def snapshot_dims(**context):
    src = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    _ensure_meta(dwh)

    batch_dttm = context["data_interval_end"].astimezone(timezone.utc).replace(tzinfo=timezone.utc)
    batch_id = batch_dttm.isoformat()

    cust = src.get_pandas_df("select * from public.customer")
    cust["batch_id"] = batch_id
    cust["etl_upload_dttm"] = batch_dttm

    term = src.get_pandas_df("select * from public.terminal")
    term["batch_id"] = batch_id
    term["etl_upload_dttm"] = batch_dttm

    dwh.run("truncate table stg.customer_snapshot")
    dwh.run("truncate table stg.terminal_snapshot")

    eng = dwh.get_sqlalchemy_engine()
    cust.to_sql("customer_snapshot", eng, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)
    term.to_sql("terminal_snapshot", eng, schema="stg", if_exists="append", index=False, method="multi", chunksize=1000)

def increment_transactions(**context):
    src = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    _ensure_meta(dwh)

    last_ts = _get_meta_ts(dwh, "last_txn_ts")
    if last_ts is None:
        last_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)

    q = """
    select
        txn_id, card_id, terminal_id, txn_ts, amount, currency_code, txn_type, status
    from public."transaction"
    where txn_ts > %s
    order by txn_ts
    """
    df = src.get_pandas_df(q, parameters=(last_ts,))
    dwh.run("truncate table stg.transaction_increment")

    if df.empty:
        return

    load_dttm = context["data_interval_end"].astimezone(timezone.utc).replace(tzinfo=timezone.utc)
    df["etl_upload_dttm"] = load_dttm

    eng = dwh.get_sqlalchemy_engine()
    df.to_sql("transaction_increment", eng, schema="stg", if_exists="append", index=False, method="multi", chunksize=2000)

    new_max = pd.to_datetime(df["txn_ts"], utc=True).max()
    if new_max is not None and getattr(new_max, "tzinfo", None) is None:
        new_max = new_max.replace(tzinfo=timezone.utc)
    _set_meta_ts(dwh, "last_txn_ts", new_max.to_pydatetime() if hasattr(new_max, "to_pydatetime") else new_max)

def extract_cdc_events(**context):
    src = PostgresHook(postgres_conn_id=SRC_CONN_ID)
    dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    _ensure_meta(dwh)

    last_id = _get_meta_bigint(dwh, "last_cdc_event_id", default=0)

    q = """
    select event_id, table_name, op, pk, row_data, event_ts
    from public.cdc_events
    where event_id > %s
    order by event_id
    """
    df = src.get_pandas_df(q, parameters=(last_id,))

    if df.empty:
        return

    if "pk" in df.columns:
        df["pk"] = df["pk"].map(_to_json_str)
    if "row_data" in df.columns:
        df["row_data"] = df["row_data"].map(_to_json_str)

    df["loaded_at"] = context["data_interval_end"].astimezone(timezone.utc).replace(tzinfo=timezone.utc)

    eng = dwh.get_sqlalchemy_engine()
    df.to_sql("cdc_events", eng, schema="stg", if_exists="append", index=False, method="multi", chunksize=5000)

    _set_meta_bigint(dwh, "last_cdc_event_id", int(df["event_id"].max()))

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "airflow", "retries": 1},
) as dag:
    t1 = PythonOperator(task_id="snapshot_dims", python_callable=snapshot_dims)
    t2 = PythonOperator(task_id="increment_transactions", python_callable=increment_transactions)
    t3 = PythonOperator(task_id="extract_cdc_events", python_callable=extract_cdc_events)

    t4 = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt && dbt run --profiles-dir . --project-dir .",
    )

    t1 >> t2 >> t3 >> t4
