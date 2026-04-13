# modules/ — Original Trading Bot

This directory contains the original market-making bot that preceded this pipeline.
It is kept as **reference only** — none of these files are imported by the pipeline.

## Files

| File | Description |
|---|---|
| `scanner.py` | Scans `/rewards/markets/current` and filters eligible markets by pool, spread and maker count. The scoring formula (`pool_diario / max(num_makers, 1)`) defined here is mirrored exactly in `ingestion/polymarket_client.py` and `spark/jobs/transform_rewards.py`. |
| `maker.py` | Places limit orders on both sides of the spread via the Polymarket CLOB API to earn USDC liquidity rewards. |
| `notifier.py` | Sends Telegram alerts when a qualifying market is found or an order is filled. |
| `risk.py` | Basic position sizing and exposure limits to cap downside on open orders. |
| `rewards_sim.py` | Simulates historical reward earnings given a hypothetical maker strategy on past market data. |

## Why it's still here

The pipeline was built by refactoring this bot into a proper data engineering architecture:
- `scanner.py` → `ingestion/polymarket_client.py` (async rewrite, no order placement)
- Scoring logic → `spark/jobs/transform_rewards.py` (distributed, testable)
- Manual execution → `airflow/dags/rewards_pipeline.py` (scheduled, observable)

Keeping the original code makes the evolution of the project traceable.
