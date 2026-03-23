"""
Kafka consumer: reads taxi trips from Confluent topic and writes to Postgres.
Simulates a stream processor that lands messages into a bronze table.

Features:
- Consumes from taxi-trips-raw topic
- Deserializes JSON messages
- Writes to raw.kafka_taxi_trips in batches (micro-batching)
- Commits offsets only after successful DB write (at-least-once delivery)
- Dead letter queue: malformed messages go to raw.kafka_dlq

Usage:
    python3 src/consumer.py
"""

import json
import logging
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv
import os

from confluent_kafka import Consumer, KafkaError
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = "postgresql://taxi_user:taxi_pass@localhost:5433/taxi_db"
TOPIC = "taxi-trips-raw"
GROUP_ID = "taxi-consumer-group"
BATCH_SIZE = 500
POLL_TIMEOUT = 1.0

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

conf = {
    "bootstrap.servers": os.getenv("CONFLUENT_BOOTSTRAP_SERVER"),
    "sasl.mechanisms": "PLAIN",
    "security.protocol": "SASL_SSL",
    "sasl.username": os.getenv("CONFLUENT_API_KEY"),
    "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,   # manual commit after DB write
}

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS raw.kafka_taxi_trips (
    id              serial primary key,
    pickup_ts       timestamp,
    dropoff_ts      timestamp,
    pickup_location_id  integer,
    dropoff_location_id integer,
    passenger_count integer,
    trip_distance   numeric,
    fare_amount     numeric,
    tip_amount      numeric,
    total_amount    numeric,
    payment_type    integer,
    trip_duration_mins numeric,
    is_weekend      boolean,
    consumed_at     timestamptz default current_timestamp
);

CREATE TABLE IF NOT EXISTS raw.kafka_dlq (
    id              serial primary key,
    raw_message     text,
    error           text,
    consumed_at     timestamptz default current_timestamp
);
"""

running = True

def handle_shutdown(sig, frame):
    global running
    log.info("Shutdown signal received...")
    running = False

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


def consume():
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLES))

    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])

    log.info(f"Consuming from {TOPIC} (group: {GROUP_ID})")

    batch = []
    dlq_batch = []
    total = 0

    while running:
        msg = consumer.poll(POLL_TIMEOUT)

        if msg is None:
            # No message — flush whatever we have
            if batch:
                _write_batch(engine, batch, dlq_batch, consumer)
                total += len(batch)
                log.info(f"  Flushed {len(batch)} messages (total: {total:,})")
                batch = []
                dlq_batch = []
            if total > 0:
                log.info(f"No more messages. Total consumed: {total:,}")
                break
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            log.error(f"Kafka error: {msg.error()}")
            continue

        try:
            trip = json.loads(msg.value().decode("utf-8"))
            batch.append(trip)
        except Exception as e:
            dlq_batch.append({"raw_message": str(msg.value()), "error": str(e)})

        if len(batch) >= BATCH_SIZE:
            _write_batch(engine, batch, dlq_batch, consumer)
            total += len(batch)
            log.info(f"  Wrote {len(batch)} messages (total: {total:,})")
            batch = []
            dlq_batch = []

    consumer.close()
    log.info(f"Consumer shut down. Total messages processed: {total:,}")


def _write_batch(engine, batch, dlq_batch, consumer):
    """Write a batch to Postgres then commit offsets."""
    with engine.begin() as conn:
        if batch:
            conn.execute(
                text("""
                    INSERT INTO raw.kafka_taxi_trips
                        (pickup_ts, dropoff_ts, pickup_location_id, dropoff_location_id,
                         passenger_count, trip_distance, fare_amount, tip_amount,
                         total_amount, payment_type, trip_duration_mins, is_weekend)
                    VALUES
                        (:pickup_ts, :dropoff_ts, :pickup_location_id, :dropoff_location_id,
                         :passenger_count, :trip_distance, :fare_amount, :tip_amount,
                         :total_amount, :payment_type, :trip_duration_mins, :is_weekend)
                """),
                batch
            )
        if dlq_batch:
            conn.execute(
                text("INSERT INTO raw.kafka_dlq (raw_message, error) VALUES (:raw_message, :error)"),
                dlq_batch
            )
    # Commit offsets only after successful DB write
    consumer.commit(asynchronous=False)


if __name__ == "__main__":
    consume()
