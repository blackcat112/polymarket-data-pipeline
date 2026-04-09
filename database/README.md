# Database

PostgreSQL schema following the **medallion architecture** pattern.

## Layers

| Layer | Table | Written by | Purpose |
|---|---|---|---|
| Bronze | `raw_rewards` | Ingestion (Python) | Append-only API snapshots. Never modified. |
| Silver | `silver_rewards` | PySpark job | Parsed, enriched, historical. One row per fetch. |
| Gold | `rewards_opportunities` | Airflow task | Upserted each run. Powers the dashboard directly. |

## Running migrations

```bash
psql -U $POSTGRES_USER -d $POSTGRES_DB -f database/migrations/001_rewards_schema.sql
```

With Docker Compose the migration runs automatically on container startup
via the `init.sql` volume mount defined in `docker-compose.yml`.

## Design decisions

- `raw_json TEXT` (not `JSONB`) — the Spark JDBC driver writes plain strings.
  Cast to `::jsonb` at query time when needed.
- `silver_rewards` is append-only — full history enables rolling 7-day aggregations
  and `best_hour_utc` computation.
- `rewards_opportunities` uses `ON CONFLICT (condition_id) DO UPDATE` (upsert)
  so the Airflow load task is idempotent.
