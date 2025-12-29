from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import insert, select
from pydantic import ValidationError
from app.schemas.models import AssetMarketData
from app.schemas.tables import (
    assets,
    asset_sources,
    asset_market_data,
    transform_failures,
)
import uuid


def upsert_asset(conn, *, asset_id, symbol, name):
    stmt = pg_insert(assets).values(
        asset_id=asset_id,
        symbol=symbol,
        name=name,
    ).on_conflict_do_nothing()

    conn.execute(stmt)


def resolve_asset_id(conn, *, source, source_asset_id, symbol, name):
    row = conn.execute(
        select(asset_sources.c.asset_id).where(
            asset_sources.c.source == source,
            asset_sources.c.source_asset_id == source_asset_id,
        )
    ).scalar()

    if row:
        return row

    asset_id = conn.execute(
        select(assets.c.asset_id).where(assets.c.symbol == symbol)
    ).scalar()

    if not asset_id:
        asset_id = uuid.uuid4()
        conn.execute(
            insert(assets).values(
                asset_id=asset_id,
                symbol=symbol,
                name=name,
            )
        )

    conn.execute(
        insert(asset_sources).values(
            asset_id=asset_id,
            source=source,
            source_asset_id=source_asset_id,
            created_at=datetime.now(timezone.utc),
        )
    )

    return asset_id



def record_transform_failure(
    conn,
    *,
    source,
    raw_table,
    raw_id,
    run_id,
    payload,
    exc,
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


def upsert_market_data(
    conn,
    *,
    asset_id,
    source,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    last_updated,
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
    run_id,
    source,
    raw_table,
    raw_row,
    asset_id,
    price_usd,
    market_cap_usd,
    volume_24h_usd,
    last_updated,
):
    try:
        model = AssetMarketData(
            asset_id=asset_id,
            source=source,
            price_usd=Decimal(price_usd),
            market_cap_usd=Decimal(market_cap_usd),
            volume_24h_usd=Decimal(volume_24h_usd),
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


def transform_coingecko(conn, *, row, run_id):
    payload = row["payload"]

    asset_id = resolve_asset_id(
        conn,
        source="coingecko",
        source_asset_id=payload["id"],
        symbol=payload["symbol"].upper(),
        name=payload["name"],
    )

    return validate_and_upsert_market_data(
        conn,
        run_id=run_id,
        source="coingecko",
        raw_table="raw_coingecko",
        raw_row=row,
        asset_id=asset_id,
        price_usd=payload["current_price"],
        market_cap_usd=payload["market_cap"],
        volume_24h_usd=payload["total_volume"],
        last_updated=datetime.fromisoformat(
            payload["last_updated"].replace("Z", "")
        ).replace(tzinfo=timezone.utc),
    )


def transform_coinpaprika(conn, *, row, run_id):
    payload = row["payload"]
    quotes = payload["quotes"]["USD"]

    asset_id = resolve_asset_id(
        conn,
        source="coinpaprika",
        source_asset_id=payload["id"],
        symbol=payload["symbol"].upper(),
        name=payload["name"],
    )

    return validate_and_upsert_market_data(
        conn,
        run_id=run_id,
        source="coinpaprika",
        raw_table="raw_coinpaprika",
        raw_row=row,
        asset_id=asset_id,
        price_usd=quotes["price"],
        market_cap_usd=quotes["market_cap"],
        volume_24h_usd=quotes["volume_24h"],
        last_updated=datetime.fromisoformat(
            payload["last_updated"].replace("Z", "")
        ).replace(tzinfo=timezone.utc),
    )


def transform_csv(conn, *, row, run_id):
    payload = row["payload"]

    asset_id = resolve_asset_id(
        conn,
        source="csv",
        source_asset_id=payload["Symbol"],
        symbol=payload["Symbol"].upper(),
        name=payload["Name"],
    )

    return validate_and_upsert_market_data(
        conn,
        run_id=run_id,
        source="csv",
        raw_table="raw_csv",
        raw_row=row,
        asset_id=asset_id,
        price_usd=payload["Close"],
        market_cap_usd=payload["Marketcap"],
        volume_24h_usd=payload["Volume"],
        last_updated=datetime.fromisoformat(
            payload["Date"]
        ).replace(tzinfo=timezone.utc),
    )
