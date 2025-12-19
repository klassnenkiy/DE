create extension if not exists pgcrypto;

create table if not exists public.customer (
    customer_id bigint generated always as identity primary key,
    customer_uuid uuid not null default gen_random_uuid(),
    full_name text,
    birth_date date,
    email text,
    phone text,
    city text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    is_deleted boolean not null default false
);

create table if not exists public.account (
    account_id bigint generated always as identity primary key,
    account_number text,
    customer_id bigint not null references public.customer(customer_id),
    currency_code text,
    opened_at timestamptz,
    closed_at timestamptz,
    status text,
    daily_transfer_limit numeric,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    is_deleted boolean not null default false
);

create table if not exists public.card (
    card_id bigint generated always as identity primary key,
    card_number text,
    account_id bigint not null references public.account(account_id),
    status text,
    opened_at timestamptz,
    closed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    is_deleted boolean not null default false
);

create table if not exists public.terminal (
    terminal_id bigint generated always as identity primary key,
    terminal_code text,
    city text,
    country text,
    latitude numeric,
    longitude numeric,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    is_working boolean not null default true,
    is_deleted boolean not null default false
);

create table if not exists public.transaction (
    txn_id bigint generated always as identity primary key,
    card_id bigint not null references public.card(card_id),
    terminal_id bigint not null references public.terminal(terminal_id),
    txn_ts timestamptz not null default now(),
    amount numeric not null,
    currency_code text,
    txn_type text,
    status text
);

create table if not exists public.cdc_events (
    event_id bigint generated always as identity primary key,
    table_name text not null,
    op char(1) not null,
    pk jsonb not null,
    row_data jsonb,
    event_ts timestamptz not null default now()
);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_customer_updated_at on public.customer;
create trigger trg_customer_updated_at
before update on public.customer
for each row execute function public.set_updated_at();

drop trigger if exists trg_account_updated_at on public.account;
create trigger trg_account_updated_at
before update on public.account
for each row execute function public.set_updated_at();

drop trigger if exists trg_card_updated_at on public.card;
create trigger trg_card_updated_at
before update on public.card
for each row execute function public.set_updated_at();

drop trigger if exists trg_terminal_updated_at on public.terminal;
create trigger trg_terminal_updated_at
before update on public.terminal
for each row execute function public.set_updated_at();

create or replace function public.emit_cdc_event()
returns trigger
language plpgsql
as $$
declare
  tname text;
  op char(1);
  pk jsonb;
  payload jsonb;
begin
  tname = tg_table_name;

  if tg_op = 'INSERT' then
    op = 'c';
    payload = to_jsonb(new);
  elsif tg_op = 'UPDATE' then
    op = 'u';
    payload = to_jsonb(new);
  else
    op = 'd';
    payload = to_jsonb(old);
  end if;

  if tname = 'account' then
    pk = jsonb_build_object('account_id', coalesce((payload->>'account_id')::bigint, 0));
  elsif tname = 'card' then
    pk = jsonb_build_object('card_id', coalesce((payload->>'card_id')::bigint, 0));
  else
    pk = jsonb_build_object('id', 0);
  end if;

  insert into public.cdc_events(table_name, op, pk, row_data, event_ts)
  values (tname, op, pk, payload, now());

  if tg_op = 'DELETE' then
    return old;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_account_cdc on public.account;
create trigger trg_account_cdc
after insert or update or delete on public.account
for each row execute function public.emit_cdc_event();

drop trigger if exists trg_card_cdc on public.card;
create trigger trg_card_cdc
after insert or update or delete on public.card
for each row execute function public.emit_cdc_event();

insert into public.customer(full_name, birth_date, email, phone, city)
select
  'Customer ' || gs::text,
  date '1980-01-01' + (gs % 100) * interval '100 days',
  'c' || gs::text || '@mail.com',
  '+7000000' || lpad(gs::text, 4, '0'),
  case when gs % 3 = 0 then 'Moscow' when gs % 3 = 1 then 'Berlin' else 'Paris' end
from generate_series(1, 50) gs
on conflict do nothing;

insert into public.terminal(terminal_code, city, country, latitude, longitude, is_working)
select
  'T' || gs::text,
  case when gs % 3 = 0 then 'Moscow' when gs % 3 = 1 then 'Berlin' else 'Paris' end,
  case when gs % 3 = 0 then 'RU' when gs % 3 = 1 then 'DE' else 'FR' end,
  55.0 + (random() * 10.0),
  37.0 + (random() * 10.0),
  true
from generate_series(1, 10) gs
on conflict do nothing;

insert into public.account(account_number, customer_id, currency_code, opened_at, status, daily_transfer_limit)
select
  'ACC' || gs::text,
  ((gs - 1) % 50) + 1,
  case when gs % 2 = 0 then 'RUB' else 'EUR' end,
  now() - (gs % 30) * interval '1 day',
  case when gs % 4 = 0 then 'blocked' else 'active' end,
  100000 + (gs % 10) * 5000
from generate_series(1, 80) gs
on conflict do nothing;

insert into public.card(card_number, account_id, status, opened_at)
select
  'CARD' || gs::text,
  ((gs - 1) % 80) + 1,
  case when gs % 5 = 0 then 'inactive' else 'active' end,
  now() - (gs % 30) * interval '1 day'
from generate_series(1, 160) gs
on conflict do nothing;

insert into public.transaction(card_id, terminal_id, txn_ts, amount, currency_code, txn_type, status)
select
  ((gs - 1) % 160) + 1,
  ((gs - 1) % 10) + 1,
  now() - (gs % 72) * interval '1 hour',
  round((random() * 5000 + 10)::numeric, 2),
  case when gs % 2 = 0 then 'RUB' else 'EUR' end,
  case when gs % 3 = 0 then 'purchase' when gs % 3 = 1 then 'withdraw' else 'transfer' end,
  case when gs % 10 = 0 then 'failed' else 'ok' end
from generate_series(1, 500) gs
on conflict do nothing;
