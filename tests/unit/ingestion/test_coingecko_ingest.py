import pytest
from sqlalchemy import create_engine, select, func
from app.ingestion.coingecko import ingest_coingecko
from app.schemas.tables import metadata, raw_coingecko

@pytest.fixture
def engine():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)
    return engine

def test_ingest_coingecko_inserts_data(mocker, engine):
    fake_payload = [{
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 100,
        "market_cap": 1000,
        "total_volume": 10,
        "last_updated": "2024-01-01T00:00:00Z",
    }]

    mocker.patch(
        "app.ingestion.coingecko.cg_http.get",
        return_value=mocker.Mock(
            status_code=200,
            json=lambda: fake_payload,
        ),
    )

    inserted = ingest_coingecko(engine)

    with engine.connect() as conn:
        rows = conn.execute(select(raw_coingecko)).fetchall()

    assert inserted == 1
    assert len(rows) == 1

def test_ingest_coingecko_idempotent(mocker, engine):
    fake_payload = [{
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 100,
        "market_cap": 1000,
        "total_volume": 10,
        "last_updated": "2024-01-01T00:00:00Z",
    }]

    mocker.patch(
        "app.ingestion.coingecko.cg_http.get",
        return_value=mocker.Mock(
            status_code=200,
            json=lambda: fake_payload,
        ),
    )

    ingest_coingecko(engine)
    ingest_coingecko(engine)

    with engine.connect() as conn:
        count = conn.execute(select(func.count()).select_from(raw_coingecko)).scalar()

    assert count == 1