from datetime import datetime, timezone
from sqlalchemy import select, insert, update
from sqlalchemy.engine import Engine
from app.schemas.tables import etl_checkpoints, etl_runs
from app.core.metrics import (
    ingestion_runs_total,
    ingestion_records_processed,
    ingestion_run_duration,
    ingestion_last_success_ts,
)



class CheckpointManager:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_checkpoint(self, source: str):
        with self.engine.connect() as conn:
            row = conn.execute(
                select(etl_checkpoints)
                .where(etl_checkpoints.c.source == source)
            ).mappings().fetchone()
            return dict(row) if row else None

    def initialize_if_missing(self, source: str):
        with self.engine.begin() as conn:
            exists = conn.execute(
                select(etl_checkpoints.c.source)
                .where(etl_checkpoints.c.source == source)
            ).first()

            if not exists:
                conn.execute(
                    insert(etl_checkpoints).values(
                        source=source,
                        last_processed_at=None,
                        status="idle",
                        updated_at=datetime.now(timezone.utc),
                    )
                )

    def start_run(self, source: str, run_id, triggered_by: str):
        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            conn.execute(
                insert(etl_runs).values(
                    run_id=run_id,
                    source=source,
                    started_at=now,
                    status="running",
                    triggered_by=triggered_by,
                )
            )

            conn.execute(
                update(etl_checkpoints)
                .where(etl_checkpoints.c.source == source)
                .values(
                    status="running",
                    updated_at=now,
                )
            )

    def mark_success(
    self,
    source: str,
    run_id,
    last_processed_at,
    records_processed: int,
):
        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            started_at = conn.execute(
                select(etl_runs.c.started_at)
                .where(etl_runs.c.run_id == run_id)
            ).scalar_one()

            if started_at.tzinfo is None:
                started_at = started_at.replace(tzinfo=timezone.utc)

            duration_ms = int((now - started_at).total_seconds() * 1000)
            duration_sec = (now - started_at).total_seconds()

            conn.execute(
                update(etl_runs)
                .where(etl_runs.c.run_id == run_id)
                .values(
                    ended_at=now,
                    duration_ms=duration_ms,
                    status="success",
                    records_processed=records_processed,
                )
            )

            conn.execute(
                update(etl_checkpoints)
                .where(etl_checkpoints.c.source == source)
                .values(
                    last_processed_at=last_processed_at,
                    last_success_run_id=run_id,
                    status="success",
                    last_failure_at=None,
                    last_failure_error=None,
                    updated_at=now,
                )
            )

        ingestion_runs_total.labels(source, "success").inc()
        ingestion_records_processed.labels(source).inc(records_processed)
        ingestion_run_duration.labels(source).observe(duration_sec)
        ingestion_last_success_ts.labels(source).set(now.timestamp())


    def mark_failure(self, source: str, run_id, error: str):
        now = datetime.now(timezone.utc)

        with self.engine.begin() as conn:
            conn.execute(
                update(etl_runs)
                .where(etl_runs.c.run_id == run_id)
                .values(
                    ended_at=now,
                    status="failed",
                    error_message=error,
                )
            )

            conn.execute(
                update(etl_checkpoints)
                .where(etl_checkpoints.c.source == source)
                .values(
                    status="failed",
                    last_failure_at=now,
                    last_failure_error=error,
                    updated_at=now,
                )
            )

        ingestion_runs_total.labels(source, "failed").inc()
