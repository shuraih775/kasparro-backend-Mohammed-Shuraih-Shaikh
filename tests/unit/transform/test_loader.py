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

        rows = list(load_raw_coingecko(conn,None))

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

        rows = list(load_raw_coinpaprika(conn,None))

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

        rows = list(load_raw_csv(conn,None))

    assert [r["source_id"] for r in rows] == ["MSFT", "AAPL"]


def test_load_raw_coingecko_respects_since_and_order():
    engine = _setup_engine()
    since = datetime(2024, 1, 1, tzinfo=timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            raw_coingecko.insert(),
            [
                {
                    "source_id": "old",
                    "payload": {},
                    "payload_hash": "1",
                    "ingested_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                },
                {
                    "source_id": "newer",
                    "payload": {},
                    "payload_hash": "2",
                    "ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
                },
                {
                    "source_id": "latest",
                    "payload": {},
                    "payload_hash": "3",
                    "ingested_at": datetime(2024, 1, 3, tzinfo=timezone.utc),
                },
            ],
        )

        rows = list(load_raw_coingecko(conn, since))

    assert [r["source_id"] for r in rows] == ["newer", "latest"]


def test_load_raw_coinpaprika_respects_since_and_order():
    engine = _setup_engine()
    since = datetime(2024, 1, 2, tzinfo=timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            raw_coinpaprika.insert(),
            [
                {
                    "source_id": "btc",
                    "payload": {},
                    "payload_hash": "x",
                    "ingested_at": datetime(2024, 1, 1, tzinfo=timezone.utc),
                },
                {
                    "source_id": "eth",
                    "payload": {},
                    "payload_hash": "y",
                    "ingested_at": datetime(2024, 1, 3, tzinfo=timezone.utc),
                },
            ],
        )

        rows = list(load_raw_coinpaprika(conn, since))

    assert [r["source_id"] for r in rows] == ["eth"]


def test_load_raw_csv_respects_since_and_order():
    engine = _setup_engine()
    since = datetime(2024, 1, 3, tzinfo=timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            raw_csv.insert(),
            [
                {
                    "source_id": "MSFT",
                    "payload": {},
                    "payload_hash": "p1",
                    "ingested_at": datetime(2024, 1, 2, tzinfo=timezone.utc),
                },
                {
                    "source_id": "AAPL",
                    "payload": {},
                    "payload_hash": "p2",
                    "ingested_at": datetime(2024, 1, 5, tzinfo=timezone.utc),
                },
            ],
        )

        rows = list(load_raw_csv(conn, since))

    assert [r["source_id"] for r in rows] == ["AAPL"]
