from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="hw2_fraud_dbt_dag",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run_fraud",
        bash_command=f"cd {DBT_DIR} && dbt run --select cdm_fraud_cases dm_fraud_daily",
    )

    dbt_run
