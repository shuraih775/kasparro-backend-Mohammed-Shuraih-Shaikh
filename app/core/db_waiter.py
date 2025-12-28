import time
import logging
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


def wait_for_db(engine, retries=30, delay=2):
    for attempt in range(1, retries + 1):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("[DB] Ready")
            return
        except OperationalError:
            logger.warning(
                "[DB] Not ready (attempt %s/%s), retrying...",
                attempt,
                retries,
            )
            time.sleep(delay)

    raise RuntimeError("Database not ready after retries")
