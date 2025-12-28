from datetime import datetime, timezone
from sqlalchemy import create_engine
from app.schemas.tables import (
    metadata,
    raw_coingecko,
    raw_coinpaprika,
    raw_csv,
)
from app.transform.loader import (
    load_raw_coingecko,
    load_raw_coinpaprika,
    load_raw_csv,
)


def _setup_engine():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


def test_load_raw_coingecko_ordered():
    engine = _setup_engine()

    with engine.begin() as conn:
        conn.execute(
            raw_coingecko.insert(),
            [
                {
                    "source_id": "a",
                    "payload": {},
                    "payload_hash": "1",
                    "ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
                },
                {
                    "source_id": "b",
                    "payload": {},
                    "payload_hash": "2",
                    "ingested_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                },
            ],
        )

        rows = list(load_raw_coingecko(conn))

    assert [r["source_id"] for r in rows] == ["b", "a"]


def test_load_raw_coinpaprika_ordered():
    engine = _setup_engine()

    with engine.begin() as conn:
        conn.execute(
            raw_coinpaprika.insert(),
            [
                {
                    "source_id": "eth",
                    "payload": {},
                    "payload_hash": "x",
                    "ingested_at": datetime(2024, 1, 3, tzinfo=timezone.utc),
                },
                {
                    "source_id": "btc",
                    "payload": {},
                    "payload_hash": "y",
                    "ingested_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                },
            ],
        )

        rows = list(load_raw_coinpaprika(conn))

    assert [r["source_id"] for r in rows] == ["btc", "eth"]


def test_load_raw_csv_ordered():
    engine = _setup_engine()

    with engine.begin() as conn:
        conn.execute(
            raw_csv.insert(),
            [
                {
                    "source_id": "AAPL",
                    "payload": {},
                    "payload_hash": "p1",
                    "ingested_at": datetime(2024, 1, 5, tzinfo=timezone.utc),
                },
                {
                    "source_id": "MSFT",
                    "payload": {},
                    "payload_hash": "p2",
                    "ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
                },
            ],
        )

        rows = list(load_raw_csv(conn))

    assert [r["source_id"] for r in rows] == ["MSFT", "AAPL"]
