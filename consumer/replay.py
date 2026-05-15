import psycopg2

import os

from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

conn.autocommit = True

cursor = conn.cursor()

print("Starting projection rebuild...")


cursor.execute("TRUNCATE TABLE account_balance")

print("Old projections cleared")


cursor.execute("""
    SELECT event_type, payload
    FROM events
    ORDER BY created_at ASC
""")

events = cursor.fetchall()



for row in events:

    event_type = row[0]
    payload = row[1]

    if event_type == "MoneyDeposited":

        account_id = payload["account_id"]
        amount = float(payload["amount"])

        cursor.execute(
            """
            INSERT INTO account_balance (account_id, balance)
            VALUES (%s, %s)
            ON CONFLICT (account_id)
            DO UPDATE SET
            balance = account_balance.balance + EXCLUDED.balance
            """,
            (account_id, amount)
        )

    elif event_type == "MoneyWithdrawn":

        account_id = payload["account_id"]
        amount = float(payload["amount"])

        cursor.execute(
            """
            INSERT INTO account_balance (account_id, balance)
            VALUES (%s, %s)
            ON CONFLICT (account_id)
            DO UPDATE SET
            balance = account_balance.balance - %s
            """,
            (account_id, 0, amount)
        )

    elif event_type == "TransferCompleted":

        from_account = payload["from_account_id"]
        to_account = payload["to_account_id"]
        amount = float(payload["amount"])

        # debit sender

        cursor.execute(
            """
            INSERT INTO account_balance (account_id, balance)
            VALUES (%s, %s)
            ON CONFLICT (account_id)
            DO UPDATE SET
            balance = account_balance.balance - %s
            """,
            (from_account, 0, amount)
        )

        # credit receiver

        cursor.execute(
            """
            INSERT INTO account_balance (account_id, balance)
            VALUES (%s, %s)
            ON CONFLICT (account_id)
            DO UPDATE SET
            balance = account_balance.balance + EXCLUDED.balance
            """,
            (to_account, amount)
        )

print("Projection rebuild completed successfully")
