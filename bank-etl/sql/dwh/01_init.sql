create schema if not exists stg;
create schema if not exists raw;
create schema if not exists ods;

create table if not exists stg.customer_snapshot (
    customer_id bigint,
    customer_uuid uuid,
    full_name text,
    birth_date date,
    email text,
    phone text,
    city text,
    created_at timestamptz,
    updated_at timestamptz,
    is_deleted boolean,
    batch_id text not null,
    etl_upload_dttm timestamptz not null
);

create table if not exists stg.terminal_snapshot (
    terminal_id bigint,
    terminal_code text,
    city text,
    country text,
    latitude numeric,
    longitude numeric,
    created_at timestamptz,
    updated_at timestamptz,
    is_working boolean,
    is_deleted boolean,
    batch_id text not null,
    etl_upload_dttm timestamptz not null
);

create table if not exists stg.transaction_increment (
    txn_id bigint,
    card_id bigint,
    terminal_id bigint,
    txn_ts timestamptz,
    amount numeric,
    currency_code text,
    txn_type text,
    status text,
    etl_upload_dttm timestamptz not null
);

create table if not exists stg.cdc_events (
    event_id bigint,
    table_name text,
    op char(1),
    pk jsonb,
    row_data jsonb,
    event_ts timestamptz,
    loaded_at timestamptz
);

create index if not exists ix_stg_cdc_events_event_id on stg.cdc_events(event_id);
create index if not exists ix_stg_cdc_events_table on stg.cdc_events(table_name);
