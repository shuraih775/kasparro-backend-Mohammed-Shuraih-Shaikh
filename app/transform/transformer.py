from datetime import datetime, timezone
from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.schemas.tables import assets, asset_market_data
from decimal import Decimal
from pydantic import ValidationError
from app.schemas.models import AssetMarketData


def record_transform_failure(
    conn,
    *,
    source: str,
    raw_table: str,
    raw_id,
    run_id,
    payload,
    exc: Exception,
):
    conn.execute(
        insert(transform_failures).values(
            source=source,
            raw_table=raw_table,
            raw_id=raw_id,
            run_id=run_id,
            error_type=type(exc).__name__,
            error_message=str(exc),
            payload=payload,
            failed_at=datetime.now(timezone.utc),
        )
    )


def upsert_asset(conn, asset_id: str, symbol: str, name: str):
    stmt = pg_insert(assets).values(
        asset_id=asset_id,
        symbol=symbol,
        name=name,
    ).on_conflict_do_nothing()

    conn.execute(stmt)



def upsert_market_data(
    conn,
    asset_id: str,
    source: str,
    price_usd: Decimal,
    market_cap_usd: Decimal,
    volume_24h_usd: Decimal,
    last_updated: datetime,
):
    stmt = pg_insert(asset_market_data).values(
        asset_id=asset_id,
        source=source,
        price_usd=price_usd,
        market_cap_usd=market_cap_usd,
        volume_24h_usd=volume_24h_usd,
        last_updated=last_updated,
        created_at=datetime.now(timezone.utc),
    ).on_conflict_do_nothing()

    conn.execute(stmt)





def validate_and_upsert_market_data(
    conn,
    *,
    asset_id: str,
    source: str,
    price_usd: Decimal,
    market_cap_usd: Decimal,
    volume_24h_usd: Decimal,
    last_updated: datetime,
):
    try:
        model = AssetMarketData(
            asset_id=asset_id,
            source=source,
            price_usd=price_usd,
            market_cap_usd=market_cap_usd,
            volume_24h_usd=volume_24h_usd,
            last_updated=last_updated,
            created_at=datetime.now(timezone.utc),
        )
    except ValidationError as e:
        record_transform_failure(
            conn,
            source=source,
            raw_table=raw_table,
            raw_id=raw_row["id"],
            run_id=run_id,
            payload=raw_row["payload"],
            exc=e,
        )
        return False
    upsert_market_data(
        conn,
        asset_id=model.asset_id,
        source=model.source,
        price_usd=model.price_usd,
        market_cap_usd=model.market_cap_usd,
        volume_24h_usd=model.volume_24h_usd,
        last_updated=model.last_updated,
    )
    return True


def transform_coingecko(conn, row):
    payload = row["payload"]

    symbol = payload["symbol"].upper()
    asset_id = symbol
    name = payload["name"]

    upsert_asset(conn, asset_id, symbol, name)

    return validate_and_upsert_market_data(
        conn,
        asset_id=asset_id,
        source="coingecko",
        price_usd=payload.get("current_price"),
        market_cap_usd=payload.get("market_cap"),
        volume_24h_usd=payload.get("total_volume"),
        last_updated=datetime.fromisoformat(
            payload["last_updated"].replace("Z", "")
        ).replace(tzinfo=timezone.utc),
    )




def transform_coinpaprika(conn, row):
    payload = row["payload"]
    quotes = payload.get("quotes", {}).get("USD", {})

    symbol = payload["symbol"].upper()
    asset_id = symbol
    name = payload["name"]

    upsert_asset(conn, asset_id, symbol, name)

    return validate_and_upsert_market_data(
        conn,
        asset_id=asset_id,
        source="coinpaprika",
        price_usd=quotes.get("price"),
        market_cap_usd=quotes.get("market_cap"),
        volume_24h_usd=quotes.get("volume_24h"),
        last_updated=datetime.fromisoformat(
            payload["last_updated"].replace("Z", "")
        ).replace(tzinfo=timezone.utc),
    )




def transform_csv(conn, row):
    payload = row["payload"]

    symbol = payload["Symbol"].upper()
    asset_id = symbol
    name = payload["Name"]

    upsert_asset(conn, asset_id, symbol, name)

    return validate_and_upsert_market_data(
        conn,
        asset_id=asset_id,
        source="csv",
        price_usd=payload["Close"],
        market_cap_usd=payload["Marketcap"],
        volume_24h_usd=payload["Volume"],
        last_updated=datetime.fromisoformat(
            payload["Date"]
        ).replace(tzinfo=timezone.utc),
    )


