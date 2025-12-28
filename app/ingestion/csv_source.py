import csv
import json
import uuid
import hashlib
from io import StringIO
from datetime import datetime, timezone
from sqlalchemy import insert, select
from app.core.checkpoints import CheckpointManager
from app.schemas.tables import raw_csv
from app.core.http import RateLimitedSession

source = "csv_market_data"

csv_http = RateLimitedSession(
    min_interval_sec=60,
    max_retries=3,
)

CSV_URL = "https://raw.githubusercontent.com/shuraih775/kasparro-backend-Mohammed-Shuraih-Shaikh/refs/heads/master/data/market_data.csv"


def _hash_payload(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()


def ingest_csv(engine):
    cp = CheckpointManager(engine)
    cp.initialize_if_missing(source)

    run_id = uuid.uuid4()
    cp.start_run(source, run_id, triggered_by="manual")

    checkpoint = cp.get_checkpoint(source)
    last_ts = checkpoint["last_processed_at"]

    records_processed = 0
    max_seen_ts = last_ts

    try:
        resp = csv_http.get(CSV_URL, timeout=10)
        resp.raise_for_status()

        reader = csv.DictReader(StringIO(resp.text))

        with engine.begin() as conn:
            for row in reader:
                row_ts = datetime.fromisoformat(
                    row["Date"]
                ).replace(tzinfo=timezone.utc)

                if last_ts and last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)

                if last_ts and row_ts <= last_ts:
                    continue

                payload_hash = _hash_payload(row)

                exists = conn.execute(
                    select(raw_csv.c.id).where(
                        raw_csv.c.payload_hash == payload_hash
                    )
                ).first()

                if exists:
                    continue

                conn.execute(
                    insert(raw_csv).values(
                        source_id=row["Symbol"],
                        payload=row,
                        payload_hash=payload_hash,
                        ingested_at=datetime.now(timezone.utc),
                    )
                )

                records_processed += 1
                max_seen_ts = max(max_seen_ts or row_ts, row_ts)

        cp.mark_success(
            source,
            run_id,
            last_processed_at=max_seen_ts,
            records_processed=records_processed,
        )

        return records_processed

    except Exception as e:
        cp.mark_failure(source, run_id, str(e))
        raise
