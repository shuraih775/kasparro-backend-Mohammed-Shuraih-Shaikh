from sqlalchemy import select
from app.schemas.tables import raw_coingecko, raw_coinpaprika, raw_csv


def load_raw_coingecko(conn):
    rows = conn.execute(
        select(
            raw_coingecko.c.id,
            raw_coingecko.c.source_id,
            raw_coingecko.c.payload,
            raw_coingecko.c.ingested_at,
        ).order_by(raw_coingecko.c.ingested_at.asc())
    ).mappings()

    for row in rows:
        yield row


def load_raw_coinpaprika(conn):
    rows = conn.execute(
        select(
            raw_coinpaprika.c.id,
            raw_coinpaprika.c.source_id,
            raw_coinpaprika.c.payload,
            raw_coinpaprika.c.ingested_at,
        ).order_by(raw_coinpaprika.c.ingested_at.asc())
    ).mappings()

    for row in rows:
        yield row


def load_raw_csv(conn):
    rows = conn.execute(
        select(
            raw_csv.c.id,
            raw_csv.c.source_id,
            raw_csv.c.payload,
            raw_csv.c.ingested_at,
        ).order_by(raw_csv.c.ingested_at.asc())
    ).mappings()

    for row in rows:
        yield row
