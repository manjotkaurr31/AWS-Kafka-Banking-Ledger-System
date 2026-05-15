from confluent_kafka import Consumer
import psycopg2
import json
import uuid
import os

from dotenv import load_dotenv

load_dotenv()


KAFKA_BROKER = os.getenv("KAFKA_BROKER")

consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'ledger-consumer-group',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['transactions'])

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

conn.autocommit = True

cursor = conn.cursor()

print("Ledger Consumer Started...")

while True:

    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print("Consumer error:", msg.error())
        continue

    try:

        event = json.loads(msg.value().decode('utf-8'))

        print("Received Event:", event)

        event_id = event["event_id"]
        event_type = event["event_type"]


        if event_type in ["MoneyDeposited", "MoneyWithdrawn"]:

            account_id = event["account_id"]
            amount = float(event["amount"])

        elif event_type == "TransferCompleted":

            account_id = event["from_account_id"]
            amount = float(event["amount"])

        else:
            print("Unknown event type")
            continue



        cursor.execute(
            """
            INSERT INTO events (
                event_id,
                event_type,
                account_id,
                payload
            )
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                event_id,
                event_type,
                account_id,
                json.dumps(event)
            )
        )

        if cursor.rowcount == 0:
            print("Duplicate event detected. Skipping...")
            continue


        if event_type == "MoneyDeposited":

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

            from_account = event["from_account_id"]
            to_account = event["to_account_id"]

            # Debit sender

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

            # Credit receiver

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

        print("Event persisted successfully")

    except Exception as e:
        print("Processing error:", e)
