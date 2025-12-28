from datetime import datetime, timezone
from sqlalchemy import create_engine, select, func
from app.schemas.tables import metadata, assets, asset_market_data
from app.transform.transformer import transform_coingecko

def test_transform_coingecko_creates_asset_and_market_data():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

    row = {
        "payload": {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 100,
            "market_cap": 1000,
            "total_volume": 10,
            "last_updated": "2024-01-01T00:00:00Z",
        }
    }

    with engine.begin() as conn:
        transform_coingecko(conn, row)

    with engine.connect() as conn:
        asset = conn.execute(select(assets)).fetchone()
        market = conn.execute(select(asset_market_data)).fetchone()

    assert asset.symbol == "BTC"
    assert float(market.price_usd) == 100

def test_transform_coingecko_idempotent():
    engine = create_engine("sqlite:///:memory:")
    metadata.create_all(engine)

    row = {
        "payload": {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 100,
            "market_cap": 1000,
            "total_volume": 10,
            "last_updated": "2024-01-01T00:00:00Z",
        }
    }

    with engine.begin() as conn:
        transform_coingecko(conn, row)
        transform_coingecko(conn, row)

    with engine.connect() as conn:
        asset_count = conn.execute(select(func.count()).select_from(assets)).scalar()
        market_count = conn.execute(select(func.count()).select_from(asset_market_data)).scalar()

    assert asset_count == 1
    assert market_count == 1