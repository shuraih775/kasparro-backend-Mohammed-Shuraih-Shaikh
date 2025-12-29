import uuid
from sqlalchemy import (
    Table,
    Column,
    Text,
    JSON,
    TIMESTAMP,
    NUMERIC,
    Integer,
    MetaData,
    ForeignKey,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime,timezone

metadata = MetaData()

# ---------- RAW TABLES ----------

def create_raw_table(name):
    return Table(
        name,
        metadata,
        Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        Column("source_id", Text, nullable=False),
        Column("payload", JSON, nullable=False),
        Column("payload_hash", Text, nullable=False, index=True),
        Column("ingested_at", TIMESTAMP(timezone=True), nullable=False),
        UniqueConstraint(
            "source_id",
            "payload_hash",
            name=f"uq_{name}_source_payload",
        ),
    )

raw_coinpaprika = create_raw_table("raw_coinpaprika")
raw_coingecko = create_raw_table("raw_coingecko")
raw_csv = create_raw_table("raw_csv")

# ---------- NORMALIZED TABLES ----------

assets = Table(
    "assets",
    metadata,
    Column("asset_id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("symbol", Text, nullable=False),
    Column("name", Text, nullable=False),
    UniqueConstraint("symbol", "name", name="uq_asset_symbol_name")

)

asset_sources = Table(
    "asset_sources",
    metadata,
    Column("asset_id", UUID(as_uuid=True), ForeignKey("assets.asset_id", ondelete="CASCADE"),nullable=False),
    Column("source", Text, nullable=False),
    Column("source_asset_id", Text, nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc)),
    UniqueConstraint(
        "source",
        "source_asset_id",
        name="uq_asset_sources_source_identity",
    ),
)

asset_market_data = Table(
    "asset_market_data",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("asset_id", UUID(as_uuid=True), ForeignKey("assets.asset_id", ondelete="CASCADE"), nullable=False),
    Column("source", Text, nullable=False),
    Column("price_usd", NUMERIC),
    Column("market_cap_usd", NUMERIC),
    Column("volume_24h_usd", NUMERIC),
    Column("last_updated", TIMESTAMP(timezone=True), nullable=False),
    Column("created_at", TIMESTAMP(timezone=True), nullable=False),
    UniqueConstraint(
        "asset_id",
        "source",
        "last_updated",
        name="uq_asset_market_source_time",
    ),
)

# ---------- ETL STATE TABLES ----------

etl_checkpoints = Table(
    "etl_checkpoints",
    metadata,
    Column("source", Text, primary_key=True),
    Column("last_processed_at", TIMESTAMP(timezone=True)),
    Column("last_success_run_id", UUID(as_uuid=True)),
    Column("last_failure_at", TIMESTAMP(timezone=True)),
    Column("last_failure_error", Text),
    Column("status", Text),  # running | success | failed
    Column("updated_at", TIMESTAMP(timezone=True), nullable=False),
)

etl_runs = Table(
    "etl_runs",
    metadata,
    Column("run_id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("source", Text, nullable=False),
    Column("started_at", TIMESTAMP(timezone=True), nullable=False),
    Column("ended_at", TIMESTAMP(timezone=True)),
    Column("duration_ms", Integer),
    Column("status", Text, nullable=False),  # running | success | failed
    Column("records_processed", Integer),
    Column("error_message", Text),
    Column("metadata", JSON),
    Column("triggered_by", Text),  # manual | cron | retry
)

# ---------- SCHEMA DRIFT EVENTS ----------

schema_drift_events = Table(
    "schema_drift_events",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("source", Text, nullable=False),
    Column("run_id", UUID(as_uuid=True), ForeignKey("etl_runs.run_id")),
    Column("detected_at", TIMESTAMP(timezone=True), nullable=False),
    Column("field_name", Text, nullable=False),
    Column("change_type", Text, nullable=False),  # added | removed | type_changed | renamed
    Column("confidence_score", NUMERIC, nullable=False),
    Column("previous_signature", Text),
    Column("new_signature", Text),
    Column("warning_message", Text),
)

# ---------- Transform Failure table ----------

transform_failures = Table(
    "transform_failures",
    metadata,
    Column("id", UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
    Column("source", Text, nullable=False),
    Column("raw_table", Text, nullable=False),
    Column("raw_id", UUID(as_uuid=True), nullable=False),
    Column("run_id", UUID(as_uuid=True)),
    Column("failed_at", TIMESTAMP(timezone=True), nullable=False),
    Column("error_type", Text, nullable=False),
    Column("error_message", Text, nullable=False),
    Column("payload", JSON),
)

