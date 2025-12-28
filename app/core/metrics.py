from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

ingestion_runs_total = Counter(
    "ingestion_runs_total",
    "Total ingestion runs",
    ["source", "status"],
)

ingestion_records_processed = Counter(
    "ingestion_records_processed_total",
    "Total records processed by ingestion",
    ["source"],
)

ingestion_run_duration = Histogram(
    "ingestion_run_duration_seconds",
    "ingestion run duration in seconds",
    ["source"],
)

ingestion_last_success_ts = Gauge(
    "ingestion_last_success_timestamp",
    "Last successful ingestion run timestamp",
    ["source"],
)


transform_records_total = Counter(
    "transform_records_total",
    "Total records processed in transform",
    ["source", "status"],  # success | failed
)

transform_run_duration = Histogram(
    "transform_run_duration_seconds",
    "Duration of transform phase"
)
