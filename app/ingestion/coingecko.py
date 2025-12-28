import os
import json
import uuid
import hashlib
from datetime import datetime, timezone
from sqlalchemy import insert, select
from app.core.checkpoints import CheckpointManager
from app.schemas.tables import raw_coingecko
from app.core.http import RateLimitedSession




source = "coingecko_markets"

cg_http = RateLimitedSession(
    min_interval_sec=2, 
    max_retries=3,
)

def _hash_payload(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()


def ingest_coingecko(engine):
    cp = CheckpointManager(engine)
    cp.initialize_if_missing(source)

    run_id = uuid.uuid4()
    cp.start_run(source, run_id, triggered_by="cron")

    checkpoint = cp.get_checkpoint(source)
    last_ts = checkpoint["last_processed_at"]

    api_key = os.getenv("COINGECKO_API_KEY")
    if not api_key:
        raise RuntimeError("COINGECKO_API_KEY not set")

    response = cg_http.get(
        "https://api.coingecko.com/api/v3/coins/markets",
        params={
            "vs_currency": "usd",
            "x_cg_demo_api_key": api_key,
        },
        timeout=10,
    )
    response.raise_for_status()

    records_processed = 0
    max_seen_ts = last_ts

    try:
        with engine.begin() as conn:
            for item in response.json():
                updated_at = datetime.fromisoformat(
                    item["last_updated"].replace("Z", "")
                ).replace(tzinfo=timezone.utc)

                if last_ts and last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)

                if last_ts and updated_at <= last_ts:
                    continue

                payload_hash = _hash_payload(item)

                exists = conn.execute(
                    select(raw_coingecko.c.id).where(
                        raw_coingecko.c.payload_hash == payload_hash
                    )
                ).first()

                if exists:
                    continue

                conn.execute(
                    insert(raw_coingecko).values(
                        source_id=item["id"],
                        payload=item,
                        payload_hash=payload_hash,
                        ingested_at=datetime.now(timezone.utc),
                    )
                )

                records_processed += 1
                max_seen_ts = max(max_seen_ts or updated_at, updated_at)

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
