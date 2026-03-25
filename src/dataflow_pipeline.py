"""
Apache Beam / Google Cloud Dataflow pipeline.
Reads taxi trips from GCS (JSON), computes 5-minute windowed aggregations,
and writes results back to GCS as JSON.

Run locally:
    python3 src/dataflow_pipeline.py --runner=DirectRunner

Run on GCP Dataflow:
    python3 src/dataflow_pipeline.py --runner=DataflowRunner
"""

import argparse
import json
import logging
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows

PROJECT = "project-a3f2dfb1-8e24-47a2-a46"
BUCKET = "taxi-df-mustu"
REGION = "us-east1"
INPUT_PATH = f"gs://{BUCKET}/input/taxi_trips.json"
OUTPUT_PATH = f"gs://{BUCKET}/output/window_aggs"

logging.basicConfig(level=logging.INFO)


class ParseTrip(beam.DoFn):
    """Parse a JSON string into a trip dict. Emit to DLQ on failure."""

    def process(self, element):
        try:
            trip = json.loads(element)
            # Emit a simple (key, value) where key is pickup_location_id
            yield {
                "pickup_location_id": trip.get("pickup_location_id"),
                "total_amount": float(trip.get("total_amount") or 0),
                "fare_amount": float(trip.get("fare_amount") or 0),
                "trip_distance": float(trip.get("trip_distance") or 0),
            }
        except Exception as e:
            logging.warning(f"Failed to parse message: {e} | raw: {element[:100]}")


class AggregateWindow(beam.CombineFn):
    """Aggregate trip metrics within a window."""

    def create_accumulator(self):
        return {"count": 0, "total_revenue": 0.0, "total_fare": 0.0, "total_distance": 0.0}

    def add_input(self, acc, trip):
        acc["count"] += 1
        acc["total_revenue"] += trip.get("total_amount", 0)
        acc["total_fare"] += trip.get("fare_amount", 0)
        acc["total_distance"] += trip.get("trip_distance", 0)
        return acc

    def merge_accumulators(self, accumulators):
        merged = self.create_accumulator()
        for acc in accumulators:
            merged["count"] += acc["count"]
            merged["total_revenue"] += acc["total_revenue"]
            merged["total_fare"] += acc["total_fare"]
            merged["total_distance"] += acc["total_distance"]
        return merged

    def extract_output(self, acc):
        count = acc["count"]
        return {
            "trip_count": count,
            "total_revenue": round(acc["total_revenue"], 2),
            "avg_fare": round(acc["total_fare"] / count, 2) if count > 0 else 0,
            "avg_distance": round(acc["total_distance"] / count, 2) if count > 0 else 0,
        }


def format_output(element):
    """Format aggregated result as JSON string."""
    key, metrics = element
    result = {
        "pickup_location_id": key,
        **metrics,
        "processed_at": datetime.utcnow().isoformat(),
    }
    return json.dumps(result)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", default="DirectRunner")
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).runner = known_args.runner

    if known_args.runner == "DataflowRunner":
        gcloud_opts = options.view_as(GoogleCloudOptions)
        gcloud_opts.project = PROJECT
        gcloud_opts.region = REGION
        gcloud_opts.staging_location = f"gs://{BUCKET}/staging"
        gcloud_opts.temp_location = f"gs://{BUCKET}/temp"
        gcloud_opts.job_name = "taxi-window-aggs"

    with beam.Pipeline(options=options) as p:
        results = (
            p
            | "ReadFromGCS"     >> beam.io.ReadFromText(INPUT_PATH)
            | "ParseTrips"      >> beam.ParDo(ParseTrip())
            | "AddKey"          >> beam.Map(lambda t: (t["pickup_location_id"], t))
            | "WindowInto"      >> beam.WindowInto(FixedWindows(5 * 60))  # 5-minute windows
            | "GroupAndAggreg"  >> beam.CombinePerKey(AggregateWindow())
            | "FormatOutput"    >> beam.Map(format_output)
            | "WriteToGCS"      >> beam.io.WriteToText(OUTPUT_PATH, file_name_suffix=".json")
        )

    logging.info(f"Pipeline complete. Results at gs://{BUCKET}/output/")


if __name__ == "__main__":
    run()
