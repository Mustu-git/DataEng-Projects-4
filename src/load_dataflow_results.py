"""
Downloads Dataflow pipeline output from GCS and loads into Postgres.
Completes the pipeline loop: Kafka → Postgres → GCS → Dataflow → Postgres
"""

import json
import logging
from google.cloud import storage
from sqlalchemy import create_engine, text

DB_URL = "postgresql://taxi_user:taxi_pass@localhost:5433/taxi_db"
BUCKET = "taxi-df-mustu"
GCS_PREFIX = "output/window_aggs"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS raw.dataflow_window_aggs (
    id                  serial primary key,
    pickup_location_id  integer,
    trip_count          integer,
    total_revenue       numeric,
    avg_fare            numeric,
    avg_distance        numeric,
    processed_at        timestamptz,
    loaded_at           timestamptz default current_timestamp
);
TRUNCATE raw.dataflow_window_aggs;
"""


def load():
    client = storage.Client()
    bucket = client.bucket(BUCKET)
    blobs = list(bucket.list_blobs(prefix=GCS_PREFIX))

    rows = []
    for blob in blobs:
        content = blob.download_as_text()
        for line in content.strip().split("\n"):
            if line:
                rows.append(json.loads(line))

    log.info(f"Downloaded {len(rows)} aggregation rows from {len(blobs)} file(s)")

    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        conn.execute(text(CREATE_TABLE))
        conn.execute(
            text("""
                INSERT INTO raw.dataflow_window_aggs
                    (pickup_location_id, trip_count, total_revenue, avg_fare, avg_distance, processed_at)
                VALUES
                    (:pickup_location_id, :trip_count, :total_revenue, :avg_fare, :avg_distance, :processed_at)
            """),
            rows
        )

    log.info(f"Loaded {len(rows)} rows into raw.dataflow_window_aggs")


if __name__ == "__main__":
    load()
