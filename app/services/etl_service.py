from app.core.logging import setup_logging
import logging
import time
import uuid

setup_logging()
logger = logging.getLogger(__name__)

from concurrent.futures import ThreadPoolExecutor

from app.ingestion.coingecko import ingest_coingecko
from app.ingestion.coinpaprika import ingest_coinpaprika
from app.ingestion.csv_source import ingest_csv
from app.core.checkpoints import CheckpointManager

from app.transform.loader import (
    load_raw_coingecko,
    load_raw_coinpaprika,
    load_raw_csv,
)
from app.transform.transformer import (
    transform_coingecko,
    transform_coinpaprika,
    transform_csv,
)


# ---------------- INGEST ----------------

def run_ingest(engine):
    logger.info("[INGEST] Starting ingestion")

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(ingest_coinpaprika, engine): "coinpaprika",
            pool.submit(ingest_coingecko, engine): "coingecko",
            pool.submit(ingest_csv, engine): "csv",
        }

        for future, source in futures.items():
            try:
                records = future.result()
                logger.info("[INGEST] %s ingested %d records", source, records)
            except Exception:
                logger.exception("[INGEST] %s ingestion failed", source)
                raise

    logger.info("[INGEST] All ingestion completed")


            



# ---------------- ETL ----------------

def run_etl(engine):

    cp = CheckpointManager(engine)

    cp_coinpaprika = cp.get_checkpoint("coinpaprika_tickers")
    cp_coingecko = cp.get_checkpoint("coingecko_markets")
    cp_csv = cp.get_checkpoint("csv_market_data")

    coinpaprika_since = None
    coingecko_since = None
    csv_since = None

    coinpaprika_since = cp_coinpaprika["last_processed_at"] if cp_coinpaprika else None
    coingecko_since = cp_coingecko["last_processed_at"] if cp_coingecko else None
    csv_since = cp_csv["last_processed_at"] if cp_csv else None


    transform_stats = {
    "coinpaprika": {"success": 0, "failed": 0},
    "coingecko": {"success": 0, "failed": 0},
    "csv": {"success": 0, "failed": 0},
}

    source_label = "all"
    start_ts = time.time()

    try:
        logger.info('[ETL] Run Started')

        # -------- INGEST (BRONZE) --------
        run_ingest(engine)

        logger.info('[ETL] Transformation Started')
        # -------- TRANSFORM (SILVER) --------
        run_id = uuid.uuid4()

        with engine.begin() as conn:
            for row in load_raw_coinpaprika(conn,coinpaprika_since):
                ok = transform_coinpaprika(conn, row=row, run_id=run_id)
                if ok:
                    transform_stats["coinpaprika"]["success"] += 1
                else:
                    transform_stats["coinpaprika"]["failed"] += 1

            for row in load_raw_coingecko(conn,coingecko_since):
                ok = transform_coingecko(conn, row=row, run_id=run_id)
                if ok:
                    transform_stats["coingecko"]["success"] += 1
                else:
                    transform_stats["coingecko"]["failed"] += 1

            for row in load_raw_csv(conn,csv_since):
                ok = transform_csv(conn, row=row, run_id=run_id)
                if ok:
                    transform_stats["csv"]["success"] += 1
                else:
                    transform_stats["csv"]["failed"] += 1


        logger.info('[ETL] Transformation Completed')
        logger.info("[ETL] Completed successfully")

    except Exception:
        logger.exception("[ETL] Run failed")
        raise
  
# ---------------- ENTRYPOINT ----------------

if __name__ == "__main__":
    from app.core.db import get_engine
    from app.core.db_waiter import wait_for_db
    engine = get_engine()
    wait_for_db(engine)
    run_etl(engine)
