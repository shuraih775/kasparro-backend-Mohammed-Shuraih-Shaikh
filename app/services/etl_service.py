from app.core.logging import setup_logging
import logging
import time

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

from app.core.metrics import (
    transform_records_total,
    transform_run_duration
)


# ---------------- INGEST ----------------

def run_ingest(engine):
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {
            pool.submit(ingest_coinpaprika, engine): "coinpaprika",
            pool.submit(ingest_coingecko, engine): "coingecko",
            pool.submit(ingest_csv, engine): "csv",
        }

        for future, source in futures.items():
            records = future.result()  

            



# ---------------- ETL ----------------

def run_etl(engine):

    cp = CheckpointManager(engine)

    cp_coinpaprika = cp.get_checkpoint("coinpaprika_tickers")
    cp_coingecko = cp.get_checkpoint("coingecko_markets")
    cp_csv = cp.get_checkpoint("csv_market_data")

    coinpaprika_since = None
    coingecko_since = None
    csv_since = None

    if cp_coinpaprika:
        coinpaprika_since = cp_coinpaprika["last_processed_at"]
    
    if cp_coingecko:
        coingecko_since = cp_coingecko["last_processed_at"]
    if csv_since:
        csv_since = cp_csv["last_processed_at"]


    transform_stats = {
    "coinpaprika": {"success": 0, "failed": 0},
    "coingecko": {"success": 0, "failed": 0},
    "csv": {"success": 0, "failed": 0},
}

    source_label = "all"
    start_ts = time.time()

    try:
        # -------- INGEST (BRONZE) --------
        run_ingest(engine)

        # -------- TRANSFORM (SILVER) --------
        with engine.begin() as conn:
            for row in load_raw_coinpaprika(conn,coinpaprika_since):
                ok = transform_coinpaprika(conn, row)
                if ok:
                    transform_stats["coinpaprika"]["success"] += 1
                else:
                    transform_stats["coinpaprika"]["failed"] += 1

            for row in load_raw_coingecko(conn,coingecko_since):
                ok = transform_coingecko(conn, row)
                if ok:
                    transform_stats["coingecko"]["success"] += 1
                else:
                    transform_stats["coingecko"]["failed"] += 1

            for row in load_raw_csv(conn,csv_since):
                ok = transform_csv(conn, row)
                if ok:
                    transform_stats["csv"]["success"] += 1
                else:
                    transform_stats["csv"]["failed"] += 1


       
        logger.info("[ETL] Completed successfully")

    finally:
        # -------- METRICS (DURATION) --------
        for source, stats in transform_stats.items():
            transform_records_total.labels(source, "success").inc(stats["success"])
            transform_records_total.labels(source, "failed").inc(stats["failed"])

        transform_run_duration.observe(time.time() - start_ts)


# ---------------- ENTRYPOINT ----------------

if __name__ == "__main__":
    from app.core.db import engine
    from app.core.db_waiter import wait_for_db

    wait_for_db(engine)
    run_etl(engine)
