# Polymarket Data Pipeline

An end-to-end Data Engineering pipeline that collects, processes, and visualises data from [Polymarket](https://polymarket.com), the largest on-chain prediction market platform.

This project originated as a live market-making bot that placed limit orders on both sides of the spread to earn liquidity rewards. It has been refactored into a production-grade data pipeline that captures the full market data lifecycle: async ingestion from the CLOB API, batch transformation with Apache Spark, hourly orchestration via Apache Airflow, and a Streamlit dashboard for analytical monitoring.

---

## Architecture

```
+---------------------------------------------------------------------+
|                      POLYMARKET DATA PIPELINE                       |
+---------------------------------------------------------------------+
|                                                                     |
|  +---------------+    +--------------+    +---------------------+   |
|  |   INGESTION   |    |  PROCESSING  |    |   ORCHESTRATION     |   |
|  |   (Bronze)    +----> (Silver)     +---->   Airflow DAG       |   |
|  |               |    |              |    |                     |   |
|  | Polymarket    |    | PySpark job  |    | Task 1: extract     |   |
|  | CLOB API      |    |              |    | Task 2: transform   |   |
|  |               |    | - volatility |    | Task 3: load metrics|   |
|  | httpx async   |    | - volume avg |    |                     |   |
|  | + tenacity    |    | - z-score    |    | schedule: @hourly   |   |
|  +-------+-------+    +------+-------+    +-----------+---------+   |
|          |                   |                        |             |
|          v                   v                        v             |
|  +------------------------------------------------------------------+
|  |                        PostgreSQL                                |
|  |   raw_markets  |  silver_markets  |  metrics_aggregated          |
|  +-----------------------------------+------------------------------+
|                                      |                              |
|                                      v                              |
|                           +--------------------+                    |
|                           |     STREAMLIT      |                    |
|                           |     DASHBOARD      |                    |
|                           |                    |                    |
|                           | - active markets   |                    |
|                           | - price evolution  |                    |
|                           | - anomaly alerts   |                    |
|                           +--------------------+                    |
+---------------------------------------------------------------------+
```

Data flows through three explicit quality tiers — bronze, silver, gold — following the medallion architecture pattern used in production data platforms.

---

## What Polymarket Is

Polymarket is a prediction market built on the Polygon blockchain. Participants trade shares in binary outcomes: a share pays $1.00 if the event resolves YES and $0.00 if it resolves NO. The current market price (between $0.01 and $0.99) represents the crowd's implied probability of the outcome occurring.

The platform exposes a Central Limit Order Book (CLOB) API that provides:

- Live bid/ask order books for each market token
- Historical price and volume data
- Market metadata: question text, resolution date, category, liquidity pool size
- Real-time maker/taker fee rates and reward eligibility

This makes Polymarket data well-suited for time-series analysis, anomaly detection, and liquidity pattern studies — the analytical workloads this pipeline is built to support.

---

## Pipeline Layers

### Layer 1 — Ingestion (Bronze)

The ingestion module polls the Polymarket CLOB API every hour, collecting active market data including prices, volumes, order book depth, and reward pool metrics. All API calls are made asynchronously using `httpx.AsyncClient` with `asyncio.gather` for concurrent requests.

Transient failures and rate-limit responses are handled transparently by `tenacity` retry decorators with exponential backoff and jitter. Every response is validated against Pydantic models before persistence.

Raw API payloads are written as-is to the `raw_markets` table in PostgreSQL (the bronze layer), preserving the original structure for reprocessing. Duplicate records are handled via `ON CONFLICT DO NOTHING` on `(market_id, fetched_at)`.

### Layer 2 — Processing (Silver)

A PySpark batch job reads from `raw_markets`, applies analytical transformations, and writes enriched records to `silver_markets`.

Transformations computed per market:

- **Price volatility** — rolling standard deviation of the mid-price over a configurable time window, using PySpark `Window` functions
- **Volume-weighted average price** — mean price weighted by trade size for each market token
- **Z-score anomaly flag** — standardised price deviation from the rolling mean; records with `|z| > 2` are flagged as statistical outliers

The Spark job is submitted via `spark-submit` from the Airflow DAG, connecting to PostgreSQL through the JDBC driver.

### Layer 3 — Orchestration

An Airflow DAG (`polymarket_pipeline`) runs on an hourly schedule and chains three tasks:

1. `extract` — calls the ingestion module, persists raw records, returns a summary via XCom
2. `transform` — submits the Spark job via `BashOperator` with `spark-submit`
3. `load_metrics` — aggregates `silver_markets` into `metrics_aggregated` using an upsert, making the latest state immediately available to the dashboard

The DAG uses Airflow's TaskFlow API (`@task` decorators) throughout, producing clean and testable Python rather than operator boilerplate. Retries are configured with exponential backoff at the DAG level.

### Layer 4 — Dashboard

A Streamlit application queries `metrics_aggregated` every 60 seconds and displays:

- Most active markets by 24-hour volume
- Price evolution time series per market
- Anomaly alerts for markets with statistically significant price deviations

---

## Tech Stack

| Layer          | Technology                   |
|----------------|------------------------------|
| Language       | Python 3.11                  |
| Ingestion      | httpx, asyncio, tenacity     |
| Validation     | Pydantic v2                  |
| Processing     | Apache Spark 3.5 (PySpark)   |
| Orchestration  | Apache Airflow 2.9           |
| Storage        | PostgreSQL 16                |
| Dashboard      | Streamlit 1.35               |
| Infrastructure | Docker Compose v2            |
| Logging        | structlog (JSON output)      |
| Testing        | pytest, pytest-asyncio       |
| Code quality   | ruff, mypy                   |

---

## Project Structure

```
polymarket-data-pipeline/
├── airflow/
│   ├── dags/
│   │   └── polymarket_pipeline.py   # Hourly DAG: extract -> transform -> load
│   └── plugins/
│       └── operators/
├── ingestion/
│   ├── polymarket_client.py         # Async API client with retry logic
│   ├── model.py                     # Dataclasses for Market and RawMarketRecord
│   ├── db.py                        # PostgreSQL persistence (bronze layer)
│   └── tests/
├── spark/
│   ├── jobs/
│   │   └── transform_markets.py     # PySpark job: volatility, volume, z-score
│   └── tests/
├── database/
│   ├── migrations/
│   │   └── 001_init.sql             # Three-layer schema definition
│   └── queries/
├── dashboard/
│   ├── app.py                       # Streamlit entry point
│   └── components/
├── modules/                         # Original trading bot (kept for reference)
├── docs/
├── .env.example
├── pyproject.toml
├── CHANGELOG.md
└── README.md
```

---

## Quick Start

```bash
git clone https://github.com/blackcat112/polymarket-data-pipeline.git
cd polymarket-data-pipeline
cp .env.example .env
docker compose up -d
```

Services after startup:

- Airflow webserver: `http://localhost:8080` (user: `admin`, password: `admin`)
- Streamlit dashboard: `http://localhost:8501`
- PostgreSQL: `localhost:5432`

Trigger the pipeline manually from the Airflow UI or wait for the first scheduled hourly run.

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest --cov=ingestion --cov=spark --cov-report=term-missing
```

---

## Design Decisions

**httpx over requests**
`requests` has no native async support. `httpx` provides an identical API surface with full `asyncio` compatibility and HTTP/2 support, making it the natural choice for the ingestion layer where concurrent market fetches are required.

**Medallion architecture in PostgreSQL**
Keeping all three layers (raw, silver, gold) in a single PostgreSQL instance simplifies the local development environment without sacrificing architectural clarity. In a production setting, the silver layer would move to a columnar store (Redshift, BigQuery) or object storage with Delta Lake format.

**PySpark for modest data volumes**
The volume of data from Polymarket does not require distributed processing in practice. PySpark is used deliberately to demonstrate familiarity with the DataFrame API, `Window` functions, and `spark-submit` job submission — patterns that apply directly at scale in production data platforms.

**TaskFlow API in Airflow**
The DAG uses `@task` decorators instead of classic Operators wherever possible. This reduces boilerplate, makes each task a plain Python function that can be unit-tested in isolation, and allows XCom passing between tasks through return values rather than manual `xcom_push` and `xcom_pull` calls.

**structlog for structured logging**
All modules emit JSON-formatted log records via `structlog`, making logs directly ingestible by log aggregation tools (Datadog, Loki, CloudWatch) without additional parsing configuration.

---

## Status

| Component           | Status      |
|---------------------|-------------|
| Ingestion layer     | Complete    |
| Spark processing    | In progress |
| Airflow DAG         | Complete    |
| Streamlit dashboard | In progress |
| Docker Compose      | In progress |
| Tests               | In progress |
