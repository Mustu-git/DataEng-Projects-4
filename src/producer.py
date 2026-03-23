"""
Kafka producer: replays NYC taxi trips from Postgres into Confluent topic.
Simulates real-time streaming by sending one trip per message.

Features:
- Reads from staging.stg_taxi_trips in batches
- Serializes each trip as JSON
- Sends to taxi-trips-raw topic with pickup_location_id as partition key
- Controllable replay speed via SLEEP_MS
- Delivery confirmation callback for error tracking

Usage:
    python3 src/producer.py
"""

import json
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
import os

from confluent_kafka import Producer
from sqlalchemy import create_engine, text

load_dotenv()

# --- Config ---
DB_URL = "postgresql://taxi_user:taxi_pass@localhost:5433/taxi_db"
TOPIC = "taxi-trips-raw"
BATCH_SIZE = 1000
SLEEP_MS = 0       # set to e.g. 10 to slow down replay
MAX_MESSAGES = 10000  # cap for demo purposes (full dataset = 3M)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

conf = {
    "bootstrap.servers": os.getenv("CONFLUENT_BOOTSTRAP_SERVER"),
    "sasl.mechanisms": "PLAIN",
    "security.protocol": "SASL_SSL",
    "sasl.username": os.getenv("CONFLUENT_API_KEY"),
    "sasl.password": os.getenv("CONFLUENT_API_SECRET"),
}


def delivery_callback(err, msg):
    """Called by Kafka for each message — logs failures."""
    if err:
        log.error(f"Delivery failed: {err}")


def serialize_trip(row) -> str:
    """Convert a DB row to a JSON string."""
    return json.dumps({
        "pickup_ts":          str(row.pickup_ts),
        "dropoff_ts":         str(row.dropoff_ts),
        "pickup_location_id": row.pickup_location_id,
        "dropoff_location_id": row.dropoff_location_id,
        "passenger_count":    row.passenger_count,
        "trip_distance":      float(row.trip_distance) if row.trip_distance else None,
        "fare_amount":        float(row.fare_amount) if row.fare_amount else None,
        "tip_amount":         float(row.tip_amount) if row.tip_amount else None,
        "total_amount":       float(row.total_amount) if row.total_amount else None,
        "payment_type":       row.payment_type,
        "trip_duration_mins": float(row.trip_duration_mins) if row.trip_duration_mins else None,
        "is_weekend":         row.is_weekend,
    })


def produce():
    producer = Producer(conf)
    engine = create_engine(DB_URL)

    log.info(f"Starting replay → topic: {TOPIC} (max {MAX_MESSAGES:,} messages)")

    sent = 0
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT pickup_ts, dropoff_ts, pickup_location_id, dropoff_location_id,
                       passenger_count, trip_distance, fare_amount, tip_amount,
                       total_amount, payment_type, trip_duration_mins, is_weekend
                FROM staging.stg_taxi_trips
                WHERE pickup_ts >= '2023-01-01'
                ORDER BY pickup_ts
                LIMIT :limit
            """),
            {"limit": MAX_MESSAGES}
        )

        for row in result:
            message = serialize_trip(row)

            # Use pickup_location_id as partition key — trips from same zone
            # go to same partition, preserving locality
            key = str(row.pickup_location_id) if row.pickup_location_id else "unknown"

            producer.produce(
                topic=TOPIC,
                key=key,
                value=message,
                callback=delivery_callback
            )

            sent += 1

            # Poll to trigger delivery callbacks every 1000 messages
            if sent % BATCH_SIZE == 0:
                producer.poll(0)
                log.info(f"  Sent {sent:,} messages...")

            if SLEEP_MS > 0:
                time.sleep(SLEEP_MS / 1000)

    # Wait for all in-flight messages to be delivered
    log.info("Flushing remaining messages...")
    producer.flush()
    log.info(f"Done. {sent:,} messages sent to {TOPIC}.")


if __name__ == "__main__":
    produce()
