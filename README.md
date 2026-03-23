# NYC Taxi Streaming Pipeline

Real-time taxi trip streaming pipeline using Confluent Kafka and Apache Flink.
Replays NYC Yellow Taxi January 2023 data as a live event stream, processes
windowed aggregations, and lands results in Postgres.

## Architecture

```
staging.stg_taxi_trips (Postgres)
        │
        ▼
[ src/producer.py ]
  Replays trips as JSON events
        │
        ▼
Confluent Kafka
  topic: taxi-trips-raw (3 partitions)
  Partition key: pickup_location_id
        │
        ▼
[ src/consumer.py ]
  Micro-batch consumer (500 msgs/batch)
  At-least-once delivery
  DLQ for malformed messages
        │
        ▼
raw.kafka_taxi_trips (Postgres)
```

## What this demonstrates
- Kafka producer with partition key strategy
- At-least-once delivery (manual offset commit after DB write)
- Dead letter queue for malformed messages
- Micro-batching for efficient DB writes
- Confluent Cloud managed Kafka

## Stack
- Python 3.11, confluent-kafka, SQLAlchemy
- Confluent Cloud (Basic cluster, AWS us-east-1)
- PostgreSQL 15 (shared with Projects 1-3)

## How to run

### Prerequisites
```bash
pip3 install confluent-kafka python-dotenv sqlalchemy psycopg2-binary
```

Create `.env` file with your Confluent credentials:
```
CONFLUENT_BOOTSTRAP_SERVER=<your-bootstrap-server>
CONFLUENT_API_KEY=<your-api-key>
CONFLUENT_API_SECRET=<your-api-secret>
```

### Run producer
```bash
python3 src/producer.py
```

### Run consumer
```bash
python3 src/consumer.py
```
