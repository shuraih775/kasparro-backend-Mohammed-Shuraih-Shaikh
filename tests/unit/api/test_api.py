import uuid
from datetime import datetime, timezone, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, func
from sqlalchemy.pool import StaticPool

from app.api.main import app
from app.core.db import get_engine
from app.schemas.tables import (
    metadata,
    etl_checkpoints,
    etl_runs,
    assets,
    asset_market_data,
)
import uuid


@pytest.fixture
def engine():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata.create_all(engine)
    return engine


@pytest.fixture
def client(engine):
    def _get_engine():
        return engine

    app.dependency_overrides[get_engine] = _get_engine
    return TestClient(app)


# ---------- /health ----------
def test_health_ok(client, engine):
    with engine.begin() as conn:
        conn.execute(
            etl_checkpoints.insert(),
            {
                "source": "coingecko",
                "status": "success",
                "last_success_run_id": uuid.uuid4(),
                "last_processed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
            },
        )

    res = client.get("/health")
    body = res.json()

    assert res.status_code == 200
    assert body["status"] == "ok"
    assert body["db"]["connected"] is True
    assert len(body["etl"]) == 1


def test_health_degraded_on_failed_source(client, engine):
    with engine.begin() as conn:
        conn.execute(
            etl_checkpoints.insert(),
            {
                "source": "coinpaprika",
                "status": "failed",
                "last_failure_error": "boom",
                "updated_at": datetime.now(timezone.utc),
            },
        )

    res = client.get("/health")
    assert res.json()["status"] == "degraded"


# ---------- /stats ----------
def test_stats_aggregates_runs(client, engine):
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            etl_runs.insert(),
            [
                {
                    "run_id": uuid.uuid4(),
                    "source": "coingecko",
                    "status": "success",
                    "started_at": now - timedelta(minutes=5),
                    "ended_at": now - timedelta(minutes=4),
                    "duration_ms": 100,
                    "records_processed": 10,
                },
                {
                    "run_id": uuid.uuid4(),
                    "source": "coingecko",
                    "status": "failed",
                    "started_at": now - timedelta(minutes=3),
                    "ended_at": now - timedelta(minutes=2),
                    "duration_ms": 50,
                    "records_processed": 0,
                },
            ],
        )

    res = client.get("/stats")
    src = res.json()["sources"][0]

    assert src["source"] == "coingecko"
    assert src["success_count"] == 1
    assert src["failure_count"] == 1
    assert src["last_run"] is not None


# ---------- /data ----------
def test_get_data_basic(client, engine):
    asset_id = uuid.uuid4()
    ts = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            assets.insert(),
            {
                "asset_id": asset_id,
                "symbol": "BTC",
                "name": "Bitcoin",
            },
        )
        conn.execute(
            asset_market_data.insert(),
            {
                "asset_id": asset_id,
                "source": "coingecko",
                "price_usd": 100,
                "market_cap_usd": 1000,
                "volume_24h_usd": 10,
                "last_updated": ts,
                "created_at": ts,
            },
        )

    res = client.get("/data")
    body = res.json()

    assert body["pagination"]["count"] == 1

    row = body["data"][0]
    assert row["symbol"] == "BTC"
    assert row["source"] == "coingecko"
    assert float(row["price_usd"]) == 100


def test_get_data_returns_latest_per_asset_per_source(client, engine):
    asset_id = uuid.uuid4()
    t1 = datetime.now(timezone.utc) - timedelta(minutes=5)
    t2 = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            assets.insert(),
            {
                "asset_id": asset_id,
                "symbol": "BTC",
                "name": "Bitcoin",
            },
        )
        conn.execute(
            asset_market_data.insert(),
            [
                {
                    "asset_id": asset_id,
                    "source": "coingecko",
                    "price_usd": 90,
                    "market_cap_usd": 900,
                    "volume_24h_usd": 9,
                    "last_updated": t1,
                    "created_at": t1,
                },
                {
                    "asset_id": asset_id,
                    "source": "coingecko",
                    "price_usd": 100,
                    "market_cap_usd": 1000,
                    "volume_24h_usd": 10,
                    "last_updated": t2,
                    "created_at": t2,
                },
            ],
        )

    res = client.get("/data")
    body = res.json()

    assert body["pagination"]["count"] == 1
    assert float(body["data"][0]["price_usd"]) == 100



def test_get_data_multiple_sources(client, engine):
    asset_id = uuid.uuid4()
    ts = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            assets.insert(),
            {
                "asset_id": asset_id,
                "symbol": "BTC",
                "name": "Bitcoin",
            },
        )
        conn.execute(
            asset_market_data.insert(),
            [
                {
                    "asset_id": asset_id,
                    "source": "coingecko",
                    "price_usd": 100,
                    "market_cap_usd": 1000,
                    "volume_24h_usd": 10,
                    "last_updated": ts,
                    "created_at": ts,
                },
                {
                    "asset_id": asset_id,
                    "source": "coinpaprika",
                    "price_usd": 101,
                    "market_cap_usd": 1010,
                    "volume_24h_usd": 11,
                    "last_updated": ts,
                    "created_at": ts,
                },
            ],
        )

    res = client.get("/data")
    body = res.json()

    assert body["pagination"]["count"] == 2
    sources = {row["source"] for row in body["data"]}
    assert sources == {"coingecko", "coinpaprika"}



# ---------- /runs ----------
def test_list_runs_ordered(client, engine):
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            etl_runs.insert(),
            [
                {
                    "run_id": uuid.uuid4(),
                    "source": "csv",
                    "status": "success",
                    "started_at": now - timedelta(minutes=1),
                    "ended_at": now,
                    "duration_ms": 100,
                    "records_processed": 5,
                },
                {
                    "run_id": uuid.uuid4(),
                    "source": "csv",
                    "status": "success",
                    "started_at": now,
                    "ended_at": now + timedelta(seconds=1),
                    "duration_ms": 120,
                    "records_processed": 6,
                },
            ],
        )

    res = client.get("/runs?limit=1")
    body = res.json()

    assert "runs" in body
    assert len(body["runs"]) == 1


# ---------- /compare-runs ----------
def test_compare_runs_detects_failure(client, engine):
    now = datetime.now(timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            etl_runs.insert(),
            [
                {
                    "run_id": uuid.uuid4(),
                    "source": "coingecko",
                    "status": "success",
                    "started_at": now - timedelta(days=1),
                    "ended_at": now - timedelta(days=1, minutes=-1),
                    "duration_ms": 100,
                    "records_processed": 100,
                },
                {
                    "run_id": uuid.uuid4(),
                    "source": "coingecko",
                    "status": "failed",
                    "started_at": now,
                    "ended_at": now + timedelta(minutes=1),
                    "duration_ms": 300,
                    "records_processed": 10,
                },
            ],
        )

    res = client.get("/compare-runs")
    anomalies = res.json()["anomalies"]

    assert anomalies
    assert anomalies[0]["type"] == "run_failed"


# ---------- /metrics ----------
def test_metrics_endpoint(client):
    res = client.get("/metrics")

    assert res.status_code == 200
    assert res.headers["content-type"].startswith("text/plain")
