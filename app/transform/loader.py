from sqlalchemy import select
from app.schemas.tables import raw_coingecko, raw_coinpaprika, raw_csv


def load_raw_coingecko(conn, since):
    stmt = select(
        raw_coingecko.c.id,
        raw_coingecko.c.source_id,
        raw_coingecko.c.payload,
        raw_coingecko.c.ingested_at,
    ).order_by(raw_coingecko.c.ingested_at.asc())

    if since:
        stmt = stmt.where(raw_coingecko.c.ingested_at > since)

    for row in conn.execute(stmt).mappings():
        yield row


def load_raw_coinpaprika(conn, since):
    stmt = select(
        raw_coinpaprika.c.id,
        raw_coinpaprika.c.source_id,
        raw_coinpaprika.c.payload,
        raw_coinpaprika.c.ingested_at,
    ).order_by(raw_coinpaprika.c.ingested_at.asc())

    if since:
        stmt = stmt.where(raw_coinpaprika.c.ingested_at > since)

    for row in conn.execute(stmt).mappings():
        yield row


def load_raw_csv(conn, since):
    stmt = select(
        raw_csv.c.id,
        raw_csv.c.source_id,
        raw_csv.c.payload,
        raw_csv.c.ingested_at,
    ).order_by(raw_csv.c.ingested_at.asc())

    if since:
        stmt = stmt.where(raw_csv.c.ingested_at > since)

    for row in conn.execute(stmt).mappings():
        yield row
