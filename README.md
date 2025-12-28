

# Kasparro — Backend & ETL System

Kasparro is a **production-style backend and ETL system** designed with **correctness, recoverability, and observability** as first-class concerns.

The system ingests market data from multiple external sources, persists immutable raw data, derives normalized views, and exposes APIs and metrics for querying and operational visibility. It is containerized, failure-aware, and **safe to re-run without manual intervention**.

---

## High-Level Architecture

Kasparro follows a **Bronze → Silver** data architecture:

```
External Sources
   ↓
Ingestion (Bronze / raw_* tables)
   ↓
Transformation (Silver / normalized tables)
   ↓
API + Metrics + Operational Visibility
```

### Core Design Principles

* Raw data is **immutable and authoritative**
* Derived data is **disposable and recomputable**
* State advances **only on success**
* Failures must **never corrupt progress**
* Observability is **mandatory**

---

## Schema Design

### Raw Tables (`raw_*`)

Raw tables store **unmodified source payloads** exactly as received.

Tables:

* `raw_coinpaprika`
* `raw_coingecko`
* `raw_csv`

Common fields:

| Field          | Purpose                  |
| -------------- | ------------------------ |
| `id`           | Immutable row identifier |
| `source_id`    | Source-level identifier  |
| `payload`      | Full JSON payload        |
| `payload_hash` | Idempotency key          |
| `ingested_at`  | Ingestion timestamp      |

**Constraints**

```
(source_id, payload_hash) UNIQUE
```

**Rationale**

* Append-only, no updates or deletes
* Payload hashing enforces idempotent ingestion
* Enables replay, auditing, and debugging
* Raw data is **never mutated**

---

### Normalized Tables (Silver)

#### `assets`

Canonical asset identity table.

| Field      | Description        |
| ---------- | ------------------ |
| `asset_id` | Primary identifier |
| `symbol`   | Asset symbol       |
| `name`     | Asset name         |

**Constraint**

```
(symbol, name) UNIQUE
```

Ensures a single canonical identity per asset.

---

#### `asset_market_data`

Time-series market data derived from raw sources.

| Field            | Description      |
| ---------------- | ---------------- |
| `asset_id`       | FK → assets      |
| `source`         | Data source      |
| `price_usd`      | Current price    |
| `market_cap_usd` | Market cap       |
| `volume_24h_usd` | 24h volume       |
| `last_updated`   | Source timestamp |
| `created_at`     | Insert timestamp |

**Constraint**

```
(asset_id, source, last_updated) UNIQUE
```

**Rationale**

* Deterministic derivation
* Safe to recompute
* Prevents duplicate inserts on re-run

---

## ETL State & Control Tables

### `etl_checkpoints`

Tracks **last known good progress per ingestion source**.

| Field                 | Purpose                    |
| --------------------- | -------------------------- |
| `source`              | Ingestion source           |
| `last_processed_at`   | Progress marker            |
| `last_success_run_id` | Successful run             |
| `last_failure_at`     | Failure timestamp          |
| `last_failure_error`  | Error details              |
| `status`              | running / success / failed |
| `updated_at`          | Last update                |

**Guarantees**

* Progress advances **only on success**
* Safe resume after crash or restart
* Operational visibility per source

---

### `etl_runs`

Tracks **individual ingestion executions**.

| Field                   | Purpose                    |
| ----------------------- | -------------------------- |
| `run_id`                | Execution ID               |
| `source`                | Ingestion source           |
| `started_at / ended_at` | Timing                     |
| `duration_ms`           | Runtime                    |
| `status`                | running / success / failed |
| `records_processed`     | Throughput                 |
| `error_message`         | Failure info               |
| `metadata`              | Optional context           |
| `triggered_by`          | manual / cron / retry      |

**Important Design Choice**

* Only **ingestion** writes to `etl_runs`
* Transformations do **not**
* Avoids ambiguity when ingestion succeeds but transform fails

---

### `transform_failures`

Captures **row-level transformation errors**.

| Field           | Purpose                |
| --------------- | ---------------------- |
| `source`        | Data source            |
| `raw_table`     | Origin table           |
| `raw_id`        | Raw row ID             |
| `run_id`        | Optional ingestion run |
| `failed_at`     | Timestamp              |
| `error_type`    | Error category         |
| `error_message` | Details                |
| `payload`       | Offending payload      |

**Rationale**

* Prevents bad rows from blocking the pipeline
* Enables targeted debugging
* Keeps transforms fail-safe

---

## Data Validation (Pre-Transform)

Before any transformation, raw payloads are validated using **Pydantic**.

```python
from pydantic import BaseModel, ConfigDict, Field
from datetime import datetime
from decimal import Decimal

class AssetMarketData(BaseModel):
    asset_id: str
    source: str

    price_usd: Decimal = Field(gt=0)
    market_cap_usd: Decimal | None = Field(default=None, ge=0)
    volume_24h_usd: Decimal | None = Field(default=None, ge=0)

    last_updated: datetime
    created_at: datetime

    model_config = ConfigDict(extra="forbid")
```

### Why validation happens **before** transform

* Raw ingestion **never rejects data**
* All payloads are preserved exactly as received
* Validation happens only when deriving Silver data
* Invalid rows are:

  * Logged
  * Recorded in `transform_failures`
  * Skipped safely

This ensures:

* No data loss
* Full auditability
* Strong correctness guarantees in normalized tables

---

## Ingestion Layer (Bronze)

Each source has an independent ingestor responsible for:

* Fetching external data
* Rate limiting and retries
* Writing immutable raw rows
* Updating checkpoints and run state

Sources are **fully isolated**:

* One source failing does not affect others
* Each source owns its own checkpoint

---

## Loader Layer

Loaders provide deterministic iteration over raw tables.

Responsibilities:

* Read rows in stable order
* Respect ingestion checkpoints
* Yield rows for transformation
* Perform **no mutation**

This cleanly separates IO from computation.

---

## Transformation Layer (Silver)

Transformations are:

* Stateless
* Deterministic
* Idempotent

### Critical Design Decision

> **Transformations do not maintain checkpoints**

Instead:

* Each transform runs in a single DB transaction
* Partial work is rolled back on failure
* Uniqueness constraints guarantee safety

Derived data is **recomputed**, not incrementally tracked.

---

## Failure & Resume Semantics

### Ingestion Failure

* Transaction rolls back
* Checkpoint does not advance
* Next run resumes safely

### Transformation Failure

* Transaction rolls back
* Raw data remains intact
* Next run re-derives Silver data

No corruption. No manual intervention.

---

## Observability

### Metrics (`/metrics`)

Prometheus-compatible metrics include:

* Ingestion runs by source and status
* Records processed
* Transform successes and failures
* Run durations
* Last successful ingestion timestamps

Metrics are emitted **after commit**, never optimistically.

---

### APIs

* `/health` — DB connectivity + ingestion state
* `/stats` — aggregated ETL statistics
* `/list-runs` — recent ingestion runs
* `/compare-runs` — anomaly detection
* `/data` — normalized market data (pagination + filters)

---

## Rate Limiting (Ingestion)

All external data ingestion is **rate limited by design** to ensure compliance with the usage and throttling policies of upstream APIs.

Each ingestion source enforces:

* A minimum request interval
* Bounded retries
* Exponential backoff on transient failures (HTTP 429, 5xx)

**Rationale**

External APIs are shared, rate-limited resources.
The system prioritizes **correctness and long-term stability** over aggressive throughput and ensures that ingestion never violates provider policies or causes unnecessary throttling.

---

## Scheduled ETL Execution

ETL execution is driven by an **ECS Scheduled Task (EventBridge)**.

* Schedule: **Once every four hours**
* Execution model: one-shot task
* Each run:

  * Starts fresh
  * Processes all available data
  * Exits on completion

There are **no long-running ETL services** and no cron processes inside production containers.

---



## Deployment Architecture

### Database

* **Amazon RDS (PostgreSQL)**

### Compute

* **ECS Service** → API
* **ECS Scheduled Task (EventBridge)** → ETL

### Containers

* API runs as a long-lived ECS service
* ETL runs as a **one-shot task**, triggered on schedule, exits on completion

No cron runs in production containers.

---

## Docker Strategy

### Local Development

* `Dockerfile.etl`
* Contains cron for local convenience
* Used only for local testing
* Committed to Git for reproducibility

### AWS Production

* `Dockerfile.aws.etl`
* No cron
* Runs:

  ```
  python -m app.services.etl_service
  ```
* Triggered via ECS Scheduled Task

This avoids cron-in-container anti-patterns in production.

---

## Environment Configuration (`.env`)

All runtime configuration is supplied via environment variables to avoid hard-coding secrets or deployment-specific values into the codebase.

The `.env` file is expected to include:

* **External API credentials**, most importantly `COINGECKO_API_KEY`, which is required for authenticated and rate-limited access to the CoinGecko API.
* **Database connection and credentials**, used consistently by both the API and ETL components.

At a minimum, the following variables must be defined:

```env
COINGECKO_API_KEY=

DATABASE_URL=

POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=
POSTGRES_HOST=
POSTGRES_PORT=
```

These values are injected at runtime and are expected to differ between local development, staging, and production environments, while the application code remains unchanged.

---

## Local Development & Execution

Kasparro supports two local execution modes depending on whether ETL is driven by cron or executed manually.

### Cron-Driven ETL (Local Convenience)

This mode mirrors the original cron-based workflow and is intended **only for local development**.

```bash
make up
make migrate
make run-etl-cron
make down
```

Services started:

* PostgreSQL
* API server
* ETL worker (cron-driven)

---

### Manual / One-Shot ETL Execution

This mode matches the **production execution model**, where ETL runs as a one-shot task.

```bash
make up
make migrate
make run-etl
make down
```

In this setup:

* ETL runs once and exits
* No cron process is involved
* Behavior aligns with ECS Scheduled Task execution


---

## Testing

Tests focus on:

* Idempotent ingestion
* Loader determinism
* Transform correctness
* Failure recovery
* API behavior

Run locally with:

```bash
make test
```

---

## Known Trade-offs

* No transform checkpoints
* No automated schema evolution
* Single ETL worker
* On-demand anomaly baselines

These choices favor **correctness, simplicity, and debuggability** over premature optimization.

---

## Future Improvements & Extensions

The current system intentionally favors correctness and simplicity.
The following enhancements are **explicitly planned but not yet implemented**:

### API Layer

* Rate limiting on public API queries to protect backend resources and prevent abuse

### Ingestion & ETL

* Automated schema drift detection on raw payloads
* Parallelized transformation execution for higher throughput
* Improved transformation resume semantics
  (current behavior derives progress based on `last_transformed_at`)

These are deferred to avoid premature complexity and will be introduced only when operational scale demands them.

---

## Final Note

Kasparro is intentionally designed to be:

* Safe to re-run
* Easy to reason about
* Observable under failure
* Defensible in a production review

