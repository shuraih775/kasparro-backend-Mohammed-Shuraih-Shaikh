import pytest
from sqlalchemy import create_engine, select, func
from app.schemas.tables import metadata, raw_coinpaprika
from app.ingestion.coinpaprika import ingest_coinpaprika


@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine


def test_ingest_coinpaprika_inserts_data(mocker, engine):
    coins = [{"id": "btc"}]
    ticker = {
        "last_updated": "2024-01-01T00:00:00Z",
        "price_usd": 100,
    }

    def fake_get(url, timeout=10):
        if url.endswith("/coins"):
            return mocker.Mock(json=lambda: coins)
        if "/tickers/" in url:
            return mocker.Mock(json=lambda: ticker)
        raise AssertionError(f"unexpected url {url}")

    mocker.patch(
        "app.ingestion.coinpaprika.cp_http.get",
        side_effect=fake_get,
    )

    mocker.patch(
        "app.ingestion.coinpaprika.cp_http.get",
        side_effect=fake_get,
    )

    inserted = ingest_coinpaprika(engine)

    with engine.connect() as conn:
        count = conn.execute(
            select(func.count()).select_from(raw_coinpaprika)
        ).scalar()

    assert inserted == 1
    assert count == 1



def test_ingest_coinpaprika_idempotent(mocker, engine):
    coins = [{"id": "btc"}]
    ticker = {
        "last_updated": "2024-01-01T00:00:00Z",
        "price_usd": 100,
    }

    def fake_get(url, timeout=10):
        if url.endswith("/coins"):
            return mocker.Mock(json=lambda: coins)
        if "/tickers/" in url:
            return mocker.Mock(json=lambda: ticker)
        raise AssertionError(f"unexpected url {url}")

    mocker.patch(
        "app.ingestion.coinpaprika.cp_http.get",
        side_effect=fake_get,
    )

    mocker.patch(
        "app.ingestion.coinpaprika.cp_http.get",
        side_effect=fake_get,
    )

    ingest_coinpaprika(engine)
    ingest_coinpaprika(engine)

    with engine.connect() as conn:
        count = conn.execute(
            select(func.count()).select_from(raw_coinpaprika)
        ).scalar()

    assert count == 1
