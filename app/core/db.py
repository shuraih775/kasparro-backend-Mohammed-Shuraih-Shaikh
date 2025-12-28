import os
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_engine: Engine | None = None

def build_db_url() -> str:

    url = os.getenv("DATABASE_URL")
    return url

def get_engine() -> Engine:
    url = build_db_url()

    global _engine
    if _engine is None:
        _engine = create_engine(
            url,
            future=True,
            pool_pre_ping=True,
        )
    return _engine
