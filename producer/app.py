from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer
from typing import Optional
import psycopg2
import json
import uuid
from datetime import datetime

from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI(
    title="Distributed Banking Ledger API",
    description="Event-Driven Banking System using Kafka",
    version="1.0.0"
)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER
})

KAFKA_TOPIC = "transactions"

db_conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

db_conn.autocommit = True

db_cursor = db_conn.cursor()


class DepositRequest(BaseModel):
    account_id: str = Field(..., example="A101")
    amount: float = Field(..., gt=0, example=5000)


class WithdrawRequest(BaseModel):
    account_id: str = Field(..., example="A101")
    amount: float = Field(..., gt=0, example=1000)


class TransferRequest(BaseModel):
    from_account_id: str = Field(..., example="A101")
    to_account_id: str = Field(..., example="A102")
    amount: float = Field(..., gt=0, example=2000)



def publish_event(event: dict, partition_key: str):

    producer.produce(
        topic=KAFKA_TOPIC,
        key=partition_key,
        value=json.dumps(event)
    )

    producer.flush()


@app.get("/")
def home():
    return {
        "message": "Distributed Banking Ledger API Running"
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "banking-api"
    }



@app.post("/deposit")
def deposit(request: DepositRequest):

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "MoneyDeposited",
        "account_id": request.account_id,
        "amount": request.amount,
        "timestamp": datetime.utcnow().isoformat()
    }

    publish_event(event, request.account_id)

    return {
        "message": "Deposit event published successfully",
        "event": event
    }



@app.post("/withdraw")
def withdraw(request: WithdrawRequest):

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "MoneyWithdrawn",
        "account_id": request.account_id,
        "amount": request.amount,
        "timestamp": datetime.utcnow().isoformat()
    }

    publish_event(event, request.account_id)

    return {
        "message": "Withdraw event published successfully",
        "event": event
    }



@app.post("/transfer")
def transfer(request: TransferRequest):

    if request.from_account_id == request.to_account_id:
        raise HTTPException(
            status_code=400,
            detail="Sender and receiver accounts cannot be same"
        )

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "TransferCompleted",
        "from_account_id": request.from_account_id,
        "to_account_id": request.to_account_id,
        "amount": request.amount,
        "timestamp": datetime.utcnow().isoformat()
    }

    publish_event(event, request.from_account_id)

    return {
        "message": "Transfer event published successfully",
        "event": event
    }


@app.get("/balance/{account_id}")
def get_balance(account_id: str):

    db_cursor.execute(
        """
        SELECT balance
        FROM account_balance
        WHERE account_id = %s
        """,
        (account_id,)
    )

    result = db_cursor.fetchone()

    if result is None:
        raise HTTPException(
            status_code=404,
            detail="Account not found"
        )

    return {
        "account_id": account_id,
        "balance": float(result[0])
    }

@app.get("/transactions/{account_id}")
def get_transactions(account_id: str):

    db_cursor.execute(
        """
        SELECT
            event_id,
            event_type,
            payload,
            created_at
        FROM events
        WHERE account_id = %s
        ORDER BY created_at DESC
        """,
        (account_id,)
    )

    rows = db_cursor.fetchall()

    transactions = []

    for row in rows:
        transactions.append({
            "event_id": str(row[0]),
            "event_type": row[1],
            "payload": row[2],
            "created_at": row[3]
        })

    return {
        "account_id": account_id,
        "transactions": transactions
    }
