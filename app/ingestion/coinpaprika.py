import json
import uuid
import hashlib
from datetime import datetime, timezone
from sqlalchemy import insert, select
from app.core.checkpoints import CheckpointManager
from app.schemas.tables import raw_coinpaprika
from app.core.http import RateLimitedSession



source = "coinpaprika_tickers"

cp_http = RateLimitedSession(
    min_interval_sec=60,  
    max_retries=3,
)


def _hash_payload(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()


def ingest_coinpaprika(engine):
    cp = CheckpointManager(engine)
    cp.initialize_if_missing(source)

    run_id = uuid.uuid4()
    cp.start_run(source, run_id, triggered_by="cron")

    checkpoint = cp.get_checkpoint(source)
    last_ts = checkpoint["last_processed_at"]

    records_processed = 0
    max_seen_ts = last_ts

    try:
        coins = cp_http.get(
            "https://api.coinpaprika.com/v1/coins",
            timeout=10,
        ).json()

        with engine.begin() as conn:
            for coin in coins:
                coin_id = coin["id"]

                ticker = cp_http.get(
                    f"https://api.coinpaprika.com/v1/tickers/{coin_id}",
                    timeout=10,
                ).json()

                updated_at = datetime.fromisoformat(
                    ticker["last_updated"].replace("Z", "")
                ).replace(tzinfo=timezone.utc)

                if last_ts and last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)

                if last_ts and updated_at <= last_ts:
                    continue

                payload_hash = _hash_payload(ticker)

                exists = conn.execute(
                    select(raw_coinpaprika.c.id).where(
                        raw_coinpaprika.c.payload_hash == payload_hash
                    )
                ).first()

                if exists:
                    continue

                conn.execute(
                    insert(raw_coinpaprika).values(
                        source_id=coin_id,
                        payload=ticker,
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
