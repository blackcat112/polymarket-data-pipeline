# Polymarket Data Pipeline

An end-to-end Data Engineering pipeline that ingests prediction market data from the [Polymarket CLOB API](https://docs.polymarket.com), processes it with Apache Spark, orchestrates workflows with Apache Airflow, and exposes analytical metrics through a Streamlit dashboard.

Originally a trading bot, refactored into a production-grade pipeline demonstrating the full data lifecycle: async ingestion, batch transformation, scheduled orchestration, and real-time visualization — containerised with Docker Compose.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     POLYMARKET DATA PIPELINE                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌─────────────┐    ┌──────────────┐    ┌────────────────────────┐ │
│  │  INGESTION  │    │  PROCESSING  │    │    ORCHESTRATION       │ │
│  │  (Bronze)   │───▶│   (Silver)   │───▶│    Airflow DAG         │ │
│  │             │    │              │    │                        │ │
│  │ Polymarket  │    │  PySpark job │    │  1. Extract            │ │
│  │ CLOB API    │    │              │    │  2. Transform          │ │
│  │             │    │  volatility  │    │  3. Load metrics       │ │
│  │ httpx async │    │  vol avg     │    │                        │ │
│  │ + tenacity  │    │  z-score     │    │  runs every hour       │ │
│  └──────┬──────┘    └──────┬───────┘    └────────────┬───────────┘ │
│         │                 │                          │             │
│         ▼                 ▼                          ▼             │
│  ┌────────────────────────────────────────────────────────────┐    │
│  │                      PostgreSQL                            │    │
│  │   raw_markets  |  silver_markets  |  metrics_aggregated    │    │
│  └────────────────────────────────────┬───────────────────────┘    │
│                                       │                            │
│                                       ▼                            │
│                            ┌──────────────────┐                    │
│                            │    STREAMLIT     │                    │
│                            │    DASHBOARD     │                    │
│                            │                  │                    │
│                            │  active markets  │                    │
│                            │  price trends    │                    │
│                            │  anomaly alerts  │                    │
│                            └──────────────────┘                    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Techniques

- **[Async/await with asyncio](https://docs.python.org/3/library/asyncio.html)** — The ingestion layer uses `asyncio` with `httpx.AsyncClient` to fetch market data concurrently without blocking I/O. Multiple market endpoints are queried in parallel using `asyncio.gather`.

- **Exponential backoff with [tenacity](https://tenacity.readthedocs.io/en/latest/)** — All outbound API calls are wrapped with `@retry` decorators that implement exponential backoff with jitter, handling transient network failures and rate-limit responses transparently.

- **Medallion architecture** — Data flows through three explicit quality tiers in PostgreSQL: `raw_markets` (bronze, append-only source records), `silver_markets` (cleaned and enriched), and `metrics_aggregated` (gold, query-optimised for the dashboard).

- **Z-score anomaly detection** — The Spark job flags statistical outliers in price and volume series using z-score normalisation computed over rolling windows via PySpark's `Window` functions.

- **[Structured logging](https://www.structlog.org/en/stable/)** — All modules use `structlog` for JSON-formatted log output, making logs machine-parseable and compatible with log aggregation tools out of the box.

- **[Type hints throughout](https://docs.python.org/3/library/typing.html)** — Every function signature uses Python 3.11 type annotations. `mypy` enforces strict typing across all modules.

---

## Libraries

| Library | Purpose |
|---|---|
| [httpx](https://www.python-httpx.org/) | Async HTTP client with HTTP/2 support, replaces `aiohttp` with a cleaner API |
| [tenacity](https://tenacity.readthedocs.io/) | Retry logic with exponential backoff, jitter, and per-exception strategies |
| [PySpark 3.5](https://spark.apache.org/docs/latest/api/python/) | Distributed batch processing; used here with the DataFrame API and Window functions |
| [Apache Airflow 2.9](https://airflow.apache.org/docs/) | Workflow orchestration with the TaskFlow API and hourly DAG scheduling |
| [SQLAlchemy 2.0](https://docs.sqlalchemy.org/en/20/) | Async ORM for PostgreSQL interactions in the ingestion layer |
| [Pydantic v2](https://docs.pydantic.dev/latest/) | Runtime validation of API response payloads with typed models |
| [structlog](https://www.structlog.org/) | Structured, context-aware logging with JSON output |
| [Streamlit](https://docs.streamlit.io/) | Dashboard framework with direct PostgreSQL connectivity |
| [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) | Async test support for `asyncio`-based ingestion tests |

---

## Project Structure

```
polymarket-data-pipeline/
├── .github/
│   └── workflows/
├── airflow/
│   ├── dags/
│   └── plugins/
│       └── operators/
├── ingestion/
│   └── tests/
├── spark/
│   ├── jobs/
│   └── tests/
├── database/
│   ├── migrations/
│   └── queries/
├── dashboard/
│   └── components/
├── infra/
├── tests/
│   └── integration/
├── docs/
├── .env.example
├── pyproject.toml
├── CHANGELOG.md
└── README.md
```

**[`ingestion/`](./ingestion/)** — Async Polymarket API client. Handles rate limiting, retries, and raw data persistence to PostgreSQL. Entry point for the bronze layer.

**[`spark/jobs/`](./spark/jobs/)** — PySpark transformation job. Reads from `raw_markets`, computes volatility (rolling std dev of odds), mean volume per market, and z-score anomaly flags. Writes to `silver_markets`.

**[`airflow/dags/`](./airflow/dags/)** — Single DAG running every hour with three chained tasks: API extraction, Spark job submission, and metrics aggregation into PostgreSQL.

**[`database/migrations/`](./database/migrations/)** — Plain SQL migration files defining the three-layer schema. Applied in order on first container startup.

**[`dashboard/`](./dashboard/)** — Streamlit app querying `metrics_aggregated` every 60 seconds. Displays active markets, price evolution charts, and anomaly alerts.

**[`infra/`](./infra/)** — Docker Compose file and per-service Dockerfiles. Defines `postgres`, `airflow-webserver`, `airflow-scheduler`, `spark-master`, `spark-worker`, and `streamlit` services.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Ingestion | httpx, asyncio, tenacity |
| Processing | Apache Spark 3.5 (PySpark) |
| Orchestration | Apache Airflow 2.9 |
| Storage | PostgreSQL 16 |
| Dashboard | Streamlit 1.35 |
| Infrastructure | Docker Compose v2 |
| Testing | pytest, pytest-asyncio, pytest-cov |
| Code quality | ruff, mypy |

---

## Quick Start

```bash
git clone https://github.com/blackcat112/polymarket-data-pipeline.git
cd polymarket-data-pipeline
cp .env.example .env
docker compose up -d
```

Services once running:

- Airflow — `http://localhost:8080` (credentials: `admin` / `admin`)
- Streamlit — `http://localhost:8501`
- PostgreSQL — `localhost:5432`

---

## Running Tests

```bash
pip install -e ".[dev]"
pytest --cov=ingestion --cov=spark --cov-report=term-missing
```

---

## Design Decisions

**httpx over requests** — `requests` has no native async support. `httpx` provides an identical API with full async support and HTTP/2, making the switch zero-friction.

**PostgreSQL for all layers** — Keeps the local setup simple (one service) while still demonstrating medallion architecture through table separation. In production the silver layer would move to a columnar store or object storage with Delta Lake.

**PySpark for modest data volumes** — The data volume from Polymarket is not large enough to require distributed processing. PySpark is used deliberately to demonstrate familiarity with the DataFrame API, Window functions, and job submission patterns used at scale.

**TaskFlow API in Airflow** — The DAG uses Airflow's `@task` decorator pattern instead of classic Operators where possible. It produces cleaner, more testable Python and avoids boilerplate.

---

## Status

| Component | Status |
|---|---|
| Ingestion layer | In progress |
| Spark processing | In progress |
| Airflow DAG | In progress |
| Streamlit dashboard | In progress |
| Docker Compose | In progress |
| Tests | In progress |
