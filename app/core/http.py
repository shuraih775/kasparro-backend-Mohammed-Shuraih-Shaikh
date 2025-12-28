import time
import logging
import requests

logger = logging.getLogger("etl.http")


class RateLimitedSession:
    def __init__(
        self,
        min_interval_sec: float,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        backoff_cap: float = 10.0,
    ):
        self.min_interval_sec = min_interval_sec
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self._last_request_ts = 0.0
        self.session = requests.Session()

    def get(self, url, **kwargs):
        attempt = 0

        while True:
            now = time.time()
            sleep_for = self.min_interval_sec - (now - self._last_request_ts)
            if sleep_for > 0:
                time.sleep(sleep_for)

            try:
                resp = self.session.get(url, **kwargs)
                self._last_request_ts = time.time()

                if resp.status_code < 400:
                    return resp

                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(
                        f"HTTP {resp.status_code}", response=resp
                    )

                resp.raise_for_status()

            except Exception as exc:
                if attempt >= self.max_retries:
                    logger.error(
                        "request_failed",
                        extra={
                            "url": url,
                            "attempt": attempt,
                            "error": str(exc),
                        },
                    )
                    raise

                backoff = min(
                    self.backoff_cap,
                    self.backoff_base * (2 ** attempt),
                )

                logger.warning(
                    "request_retry",
                    extra={
                        "url": url,
                        "attempt": attempt,
                        "backoff_sec": backoff,
                        "error": str(exc),
                    },
                )

                time.sleep(backoff)
                attempt += 1
