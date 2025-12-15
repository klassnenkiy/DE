#!/usr/bin/env python
import os
import random
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dateutil import tz

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "bank")
PG_USER = os.getenv("PG_USER", "bank")
PG_PASS = os.getenv("PG_PASS", "bank")

fake = Faker("ru_RU")
# можешь поменять на свою таймзону, если хочешь
tz_utc = timezone.utc


def get_conn():
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
    )


def ensure_terminals(conn):
    """Создание набора терминалов в разных городах."""
    cities = [
        ("MSK_ATM_1", "Москва", "RU", 55.7558, 37.6173),
        ("MSK_ATM_2", "Москва", "RU", 55.7512, 37.6184),
        ("SPB_ATM_1", "Санкт-Петербург", "RU", 59.9343, 30.3351),
        ("NSK_ATM_1", "Новосибирск", "RU", 55.0084, 82.9357),
        ("VVO_ATM_1", "Владивосток", "RU", 43.1155, 131.8855),
        ("KRD_ATM_1", "Краснодар", "RU", 45.0355, 38.9753),
    ]

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bank.terminal;")
        (cnt,) = cur.fetchone()
        if cnt > 0:
            return

        execute_values(
            cur,
            """
            INSERT INTO bank.terminal(
                terminal_code, city, country, latitude, longitude
            ) VALUES %s
            """,
            cities,
        )
    conn.commit()
    print(f"Inserted {len(cities)} terminals")


def ensure_customers_accounts_cards(conn, target_customers=100):
    """Создаём клиентов, аккаунты и карты, если их меньше target_customers."""
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM bank.customer;")
        (cnt,) = cur.fetchone()
        if cnt >= target_customers:
            return

        to_create = target_customers - cnt
        customers = []
        for _ in range(to_create):
            customers.append(
                (
                    fake.name(),
                    fake.date_of_birth(minimum_age=18, maximum_age=75),
                    fake.email(),
                    fake.phone_number(),
                    fake.city(),
                )
            )

        # вставляем клиентов
        execute_values(
            cur,
            """
            INSERT INTO bank.customer(
                full_name, birth_date, email, phone, city
            ) VALUES %s
            RETURNING customer_id
            """,
            customers,
        )
        new_customer_ids = [row[0] for row in cur.fetchall()]

        # генерим счета
        accounts = []
        for cid in new_customer_ids:
            for _ in range(random.randint(1, 2)):  # 1-2 счета на клиента
                acc_num = f"40802810{random.randint(10**14, 10**15-1)}"
                currency = "RUB"  # можно потом добавить другие валюты
                limit_val = random.choice([50000, 75000, 100000, 150000, 200000])
                accounts.append((acc_num, cid, currency, limit_val))

        execute_values(
            cur,
            """
            INSERT INTO bank.account(
                account_number, customer_id, currency_code, daily_transfer_limit
            ) VALUES %s
            RETURNING account_id
            """,
            accounts,
        )
        new_account_ids = [row[0] for row in cur.fetchall()]

        # генерим карты
        cards = []
        for acc_id in new_account_ids:
            for _ in range(random.randint(1, 2)):  # 1–2 карты на аккаунт
                card_num = "".join(str(random.randint(0, 9)) for _ in range(16))
                cards.append((card_num, acc_id))

        execute_values(
            cur,
            """
            INSERT INTO bank.card(
                card_number, account_id
            ) VALUES %s
            """,
            cards,
        )

    conn.commit()
    print(f"Inserted {len(new_customer_ids)} customers, {len(new_account_ids)} accounts, {len(cards)} cards")


def get_max_txn_ts(conn):
    """Максимальное время транзакции в таблице transaction."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(txn_ts) FROM bank.transaction;")
        (max_ts,) = cur.fetchone()
    return max_ts


def pick_random_cards_and_terminals(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT card_id FROM bank.card;")
        cards = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT terminal_id FROM bank.terminal;")
        terminals = [row[0] for row in cur.fetchall()]
    return cards, terminals


def generate_normal_transactions(conn, start_ts, end_ts, approx_count=1000):
    """Генерация обычных снятий и внесений наличных."""
    cards, terminals = pick_random_cards_and_terminals(conn)
    if not cards or not terminals:
        print("No cards or terminals - skip txn generation")
        return

    total_seconds = (end_ts - start_ts).total_seconds()
    if total_seconds <= 0:
        return

    txns = []
    for _ in range(approx_count):
        rel = random.random()
        txn_ts = start_ts + timedelta(seconds=rel * total_seconds)

        card_id = random.choice(cards)
        terminal_id = random.choice(terminals)
        # выбор типа операции
        txn_type = random.choice(["atm_withdrawal", "atm_deposit"])
        # суммы: депозиты могут быть чуть крупнее
        if txn_type == "atm_withdrawal":
            amount = round(random.uniform(500, 40000), 2)
        else:
            amount = round(random.uniform(500, 100000), 2)

        currency = "RUB"  # для простоты
        status = "approved" if random.random() > 0.05 else "declined"

        txns.append(
            (
                card_id,
                terminal_id,
                txn_ts,
                amount,
                currency,
                txn_type,
                status,
            )
        )

    with conn.cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO bank.transaction(
                card_id, terminal_id, txn_ts,
                amount, currency_code, txn_type, status
            ) VALUES %s
            """,
            txns,
        )
    conn.commit()
    print(f"Inserted {len(txns)} normal ATM transactions")


def inject_fraud_impossible_travel(conn, base_ts):
    """
    Фрод-сценарий: снятие в двух очень удалённых городах в течение 10 минут одной и той же картой.
    """
    with conn.cursor() as cur:
        # выбираем случайную карту
        cur.execute("""
            SELECT card_id
            FROM bank.card
            ORDER BY random()
            LIMIT 1;
        """)
        row = cur.fetchone()
        if not row:
            return
        (card_id,) = row

        # терминал в Москве и во Владивостоке
        cur.execute("""
            SELECT terminal_id FROM bank.terminal
            WHERE city = 'Москва'
            ORDER BY random()
            LIMIT 1;
        """)
        msk = cur.fetchone()
        cur.execute("""
            SELECT terminal_id FROM bank.terminal
            WHERE city = 'Владивосток'
            ORDER BY random()
            LIMIT 1;
        """)
        vvo = cur.fetchone()

        if not msk or not vvo:
            print("No terminals for fraud scenario impossible_travel")
            return

        msk_term, = msk
        vvo_term, = vvo

        t1 = base_ts
        t2 = base_ts + timedelta(minutes=10)

        txns = [
            (
                card_id, msk_term, t1,
                20000.00, "RUB", "atm_withdrawal", "approved"
            ),
            (
                card_id, vvo_term, t2,
                18000.00, "RUB", "atm_withdrawal", "approved"
            ),
        ]

        execute_values(
            cur,
            """
            INSERT INTO bank.transaction(
                card_id, terminal_id, txn_ts,
                amount, currency_code, txn_type, status
            ) VALUES %s
            """,
            txns,
        )
    conn.commit()
    print("Injected fraud scenario: impossible_travel")


def inject_fraud_threshold_hopping(conn, base_ts):
    """
    Фрод-сценарий: подбор суммы снятия относительно дневного лимита по аккаунту.
    - берём аккаунт с лимитом
    - несколько попыток снятия:
      * одна явная > лимита (часто declined),
      * 1–2 около лимита, одна approved.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT account_id, daily_transfer_limit
            FROM bank.account
            WHERE status = 'active' AND is_deleted = FALSE
            ORDER BY random()
            LIMIT 1;
        """)
        row = cur.fetchone()
        if not row:
            return
        account_id, limit_val = row
        limit_val = float(limit_val)

        # карта этого аккаунта
        cur.execute("""
            SELECT card_id
            FROM bank.card
            WHERE account_id = %s AND is_deleted = FALSE
            ORDER BY random()
            LIMIT 1;
        """, (account_id,))
        card_row = cur.fetchone()
        if not card_row:
            return
        (card_id,) = card_row

        # любой терминал
        cur.execute("""
            SELECT terminal_id
            FROM bank.terminal
            ORDER BY random()
            LIMIT 1;
        """)
        term_row = cur.fetchone()
        if not term_row:
            return
        (terminal_id,) = term_row

        amounts = [
            limit_val + 5000,     # сильно выше лимита
            limit_val - 1000,     # чуть ниже
            limit_val - 500,      # ещё ниже
        ]
        statuses = ["declined", "declined", "approved"]

        txns = []
        for i, (amt, st) in enumerate(zip(amounts, statuses)):
            ts = base_ts + timedelta(minutes=i * 3)
            txns.append(
                (
                    card_id,
                    terminal_id,
                    ts,
                    round(amt, 2),
                    "RUB",
                    "atm_withdrawal",
                    st,
                )
            )

        execute_values(
            cur,
            """
            INSERT INTO bank.transaction(
                card_id, terminal_id, txn_ts,
                amount, currency_code, txn_type, status
            ) VALUES %s
            """,
            txns,
        )
    conn.commit()
    print("Injected fraud scenario: threshold_hopping")


def apply_random_updates_and_deletes(conn):
    """
    Показываем изменения в измерениях:
    - обновляем email/phone у клиентов
    - обновляем лимиты по счетам
    - soft delete клиентов
    - физически удаляем несколько карт (для CDC 'd')
    """
    with conn.cursor() as cur:
        # обновление клиентов
        cur.execute("""
            SELECT customer_id
            FROM bank.customer
            WHERE is_deleted = FALSE
            ORDER BY random()
            LIMIT 5;
        """)
        for (cid,) in cur.fetchall():
            cur.execute("""
                UPDATE bank.customer
                SET email = %s,
                    phone = %s,
                    updated_at = now()
                WHERE customer_id = %s;
            """, (fake.email(), fake.phone_number(), cid))

        # обновление лимитов по счетам
        cur.execute("""
            SELECT account_id
            FROM bank.account
            WHERE status = 'active' AND is_deleted = FALSE
            ORDER BY random()
            LIMIT 5;
        """)
        for (acc_id,) in cur.fetchall():
            new_limit = random.choice([50000, 75000, 100000, 150000, 200000])
            cur.execute("""
                UPDATE bank.account
                SET daily_transfer_limit = %s,
                    updated_at = now()
                WHERE account_id = %s;
            """, (new_limit, acc_id))

        # soft delete пары клиентов
        cur.execute("""
            UPDATE bank.customer
            SET is_deleted = TRUE,
                updated_at = now()
            WHERE customer_id IN (
                SELECT customer_id
                FROM bank.customer
                WHERE is_deleted = FALSE
                ORDER BY random()
                LIMIT 2
            );
        """)

        # физическое удаление нескольких карт
        cur.execute("""
            DELETE FROM bank.card
            WHERE card_id IN (
                SELECT card_id
                FROM bank.card
                ORDER BY random()
                LIMIT 2
            );
        """)

    conn.commit()
    print("Applied random updates and deletes on dimensions")


def main():
    conn = get_conn()
    ensure_terminals(conn)
    ensure_customers_accounts_cards(conn, target_customers=100)

    max_ts = get_max_txn_ts(conn)
    now_ts = datetime.now(tz_utc)

    if max_ts is None:
        # первый запуск: генерим за последние 30 дней
        start_ts = now_ts - timedelta(days=30)
    else:
        # следующий запуск: продолжаем после последней транзакции
        start_ts = max_ts + timedelta(minutes=1)

    end_ts = now_ts

    # 1. обычные ATM операции
    generate_normal_transactions(conn, start_ts, end_ts, approx_count=1000)

    # 2. фрод "путешествие" (снятие в разных городах)
    fraud_base_ts = start_ts + (end_ts - start_ts) / 2
    inject_fraud_impossible_travel(conn, fraud_base_ts)

    # 3. фрод "подбор лимита" на снятии
    inject_fraud_threshold_hopping(conn, fraud_base_ts + timedelta(minutes=30))

    # 4. изменения измерений
    apply_random_updates_and_deletes(conn)

    conn.close()


if __name__ == "__main__":
    main()
