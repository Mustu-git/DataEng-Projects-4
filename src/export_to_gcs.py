"""
Exports raw.kafka_taxi_trips from Postgres to GCS as JSON.
This feeds the Dataflow pipeline input.
"""

import json
import logging
from google.cloud import storage
from sqlalchemy import create_engine, text

DB_URL = "postgresql://taxi_user:taxi_pass@localhost:5433/taxi_db"
BUCKET = "taxi-df-mustu"
GCS_PATH = "input/taxi_trips.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def export():
    engine = create_engine(DB_URL)
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blob = bucket.blob(GCS_PATH)

    log.info("Reading from raw.kafka_taxi_trips...")
    rows = []
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT pickup_ts, dropoff_ts, pickup_location_id, dropoff_location_id,
                   passenger_count, trip_distance, fare_amount, tip_amount,
                   total_amount, payment_type, trip_duration_mins, is_weekend
            FROM raw.kafka_taxi_trips
        """))
        for row in result:
            rows.append({
                "pickup_ts": str(row.pickup_ts),
                "dropoff_ts": str(row.dropoff_ts),
                "pickup_location_id": row.pickup_location_id,
                "dropoff_location_id": row.dropoff_location_id,
                "passenger_count": row.passenger_count,
                "trip_distance": float(row.trip_distance) if row.trip_distance else None,
                "fare_amount": float(row.fare_amount) if row.fare_amount else None,
                "tip_amount": float(row.tip_amount) if row.tip_amount else None,
                "total_amount": float(row.total_amount) if row.total_amount else None,
                "payment_type": row.payment_type,
                "trip_duration_mins": float(row.trip_duration_mins) if row.trip_duration_mins else None,
                "is_weekend": row.is_weekend,
            })

    log.info(f"Uploading {len(rows):,} rows to gs://{BUCKET}/{GCS_PATH}...")
    content = "\n".join(json.dumps(r) for r in rows)
    blob.upload_from_string(content, content_type="application/json")
    log.info("Upload complete.")


if __name__ == "__main__":
    export()
