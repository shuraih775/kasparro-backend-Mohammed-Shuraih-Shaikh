from app.core.logging import setup_logging
import logging
setup_logging()
logger = logging.getLogger(__name__)
import os

import uuid
import time
from fastapi import FastAPI, APIRouter, Depends, Query,Response
from typing import Optional
from sqlalchemy import text, select, func, and_
from app.core.db import get_engine
from app.schemas.tables import etl_checkpoints, etl_runs, assets, asset_market_data
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from decimal import Decimal


SOURCE_EXPECTATIONS = {
    "coingecko_markets": {
        "expects_records": True,
    },
    "coinpaprika_tickers": {
        "expects_records": True,
    },
    "csv_market_data": {
        "expects_records": False,  # static source
    },
}

router = APIRouter()

@router.get("/")
def hello():
    return {
        "status": "success",
        "message": "Welcome to Kasparro ETL pipeline API"
    }

@router.get("/health")
def health(engine = Depends(get_engine)):
    start = time.time()
    request_id = str(uuid.uuid4())

    db_connected = True
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        db_connected = False

    etl_states = []
    overall_status = "ok"

    with engine.connect() as conn:
        rows = conn.execute(
            select(etl_checkpoints)
        ).mappings().all()

        for row in rows:
            state = {
                "source": row["source"],
                "status": row["status"],
                "last_success_run_id": row["last_success_run_id"],
                "last_processed_at": row["last_processed_at"],
                "last_failure_at": row["last_failure_at"],
                "last_failure_error": row["last_failure_error"],
            }

            if row["status"] == "failed":
                overall_status = "degraded"

            etl_states.append(state)

    latency_ms = int((time.time() - start) * 1000)

    return {
        "status": overall_status if db_connected else "degraded",
        "db": {"connected": db_connected},
        "etl": etl_states,
        "request_id": request_id,
        "api_latency_ms": latency_ms,
    }





@router.get("/stats")
def stats(engine = Depends(get_engine)):
    start = time.time()
    request_id = str(uuid.uuid4())

    result = []

    with engine.connect() as conn:
        sources = conn.execute(
            select(etl_runs.c.source).distinct()
        ).scalars().all()

        for source in sources:
            last_run = conn.execute(
                select(etl_runs)
                .where(etl_runs.c.source == source)
                .order_by(etl_runs.c.started_at.desc())
                .limit(1)
            ).mappings().first()

            success_count = conn.execute(
                select(func.count())
                .where(
                    etl_runs.c.source == source,
                    etl_runs.c.status == "success",
                )
            ).scalar()

            failure_count = conn.execute(
                select(func.count())
                .where(
                    etl_runs.c.source == source,
                    etl_runs.c.status == "failed",
                )
            ).scalar()

            result.append(
                {
                    "source": source,
                    "last_run": dict(last_run) if last_run else None,
                    "success_count": success_count,
                    "failure_count": failure_count,
                }
            )

    latency_ms = int((time.time() - start) * 1000)

    return {
        "sources": result,
        "request_id": request_id,
        "api_latency_ms": latency_ms,
    }



@router.get("/data")
def get_data(
    engine = Depends(get_engine),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    symbol: Optional[str] = None,
    source: Optional[str] = None,
    from_ts: Optional[str] = None,
    to_ts: Optional[str] = None,
):
    start = time.time()
    request_id = str(uuid.uuid4())

    filters = []

    if source:
        filters.append(asset_market_data.c.source == source)

    if from_ts:
        filters.append(asset_market_data.c.last_updated >= from_ts)

    if to_ts:
        filters.append(asset_market_data.c.last_updated <= to_ts)

    latest_subq = (
        select(
            asset_market_data.c.asset_id,
            asset_market_data.c.source,
            func.max(asset_market_data.c.last_updated).label("last_updated"),
        )
        .group_by(
            asset_market_data.c.asset_id,
            asset_market_data.c.source,
        )
    )

    if filters:
        latest_subq = latest_subq.where(and_(*filters))

    latest_subq = latest_subq.subquery("latest")

    stmt = (
        select(
            assets.c.symbol,
            assets.c.name,
            asset_market_data.c.source,
            asset_market_data.c.price_usd,
            asset_market_data.c.market_cap_usd,
            asset_market_data.c.volume_24h_usd,
            asset_market_data.c.last_updated,
        )
        .select_from(
            asset_market_data
            .join(
                latest_subq,
                and_(
                    asset_market_data.c.asset_id == latest_subq.c.asset_id,
                    asset_market_data.c.source == latest_subq.c.source,
                    asset_market_data.c.last_updated == latest_subq.c.last_updated,
                ),
            )
            .join(
                assets,
                assets.c.asset_id == asset_market_data.c.asset_id,
            )
        )
    )

    if symbol:
        stmt = stmt.where(assets.c.symbol == symbol)

    stmt = (
        stmt
        .order_by(
            asset_market_data.c.last_updated.desc(),
            assets.c.symbol.asc(),
            asset_market_data.c.source.asc(),
        )
        .limit(limit)
        .offset(offset)
    )

    with engine.connect() as conn:
        rows = conn.execute(stmt).mappings().all()

    return {
        "data": list(rows),
        "pagination": {
            "limit": limit,
            "offset": offset,
            "count": len(rows),
        },
        "request_id": request_id,
        "api_latency_ms": int((time.time() - start) * 1000),
    }


@router.get("/runs")
def list_runs(
    limit: int = Query(10, ge=1, le=100),
    engine=Depends(get_engine),
):
    start = time.time()
    request_id = str(uuid.uuid4())

    with engine.connect() as conn:
        rows = conn.execute(
            select(
                etl_runs.c.run_id,
                etl_runs.c.source,
                etl_runs.c.status,
                etl_runs.c.started_at,
                etl_runs.c.ended_at,
                etl_runs.c.duration_ms,
                etl_runs.c.records_processed,
            )
            .order_by(etl_runs.c.started_at.desc())
            .limit(limit)
        ).mappings().all()

    return {
        "runs": list(rows),
        "request_id": request_id,
        "api_latency_ms": int((time.time() - start) * 1000),
    }



@router.get("/compare-runs")
def compare_runs(engine=Depends(get_engine)):
    start = time.time()
    request_id = str(uuid.uuid4())
    anomalies = []

    with engine.connect() as conn:
        sources = conn.execute(
            select(etl_runs.c.source).distinct()
        ).scalars().all()

        for source in sources:
            expectations = SOURCE_EXPECTATIONS.get(
                source,
                {"expects_records": True},
            )

            recent_run = conn.execute(
                select(etl_runs)
                .where(etl_runs.c.source == source)
                .order_by(etl_runs.c.started_at.desc())
                .limit(1)
            ).mappings().first()

            if not recent_run:
                continue

            baseline = conn.execute(
                select(
                    func.avg(etl_runs.c.duration_ms).label("avg_duration"),
                    func.avg(etl_runs.c.records_processed).label("avg_records"),
                )
                .where(
                    etl_runs.c.source == source,
                    etl_runs.c.status == "success",
                )
            ).mappings().first()

            if not baseline:
                continue

            avg_duration = float(baseline["avg_duration"])
            avg_records = float(baseline["avg_records"] or 0)

            if recent_run["status"] == "failed":
                anomalies.append(
                    {
                        "source": source,
                        "run_id": recent_run["run_id"],
                        "type": "run_failed",
                        "message": "Latest run failed",
                    }
                )

            if (
                avg_duration is not None
                and recent_run["duration_ms"] is not None
                and recent_run["duration_ms"] > 2 * avg_duration
            ):
                anomalies.append(
                    {
                        "source": source,
                        "run_id": recent_run["run_id"],
                        "type": "duration_spike",
                        "baseline": int(avg_duration),
                        "current": recent_run["duration_ms"],
                        "message": "Run duration exceeded 2x historical average",
                    }
                )

            if (
                expectations["expects_records"]
                and avg_records is not None
                and avg_records > 0
                and recent_run["records_processed"] is not None
                and float(recent_run["records_processed"]) < (0.5 * avg_records)

            ):
                anomalies.append(
                    {
                        "source": source,
                        "run_id": recent_run["run_id"],
                        "type": "record_drop",
                        "baseline": int(avg_records),
                        "current": recent_run["records_processed"],
                        "message": "Records processed dropped below 50% of baseline",
                    }
                )

    return {
        "anomalies": anomalies,
        "request_id": request_id,
        "api_latency_ms": int((time.time() - start) * 1000),
    }

#to be implemented
@router.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


app = FastAPI(title="Kasparro Backend")
app.include_router(router)