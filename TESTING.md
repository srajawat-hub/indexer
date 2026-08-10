# Manual testing with Docker Postgres (one chain)

Exact steps to run the indexer locally using Docker for Postgres and a single chain (Base).

## Prerequisites

- **Docker** (and Docker Compose)
- **Rust** (e.g. `cargo build` works)
- **psql** (PostgreSQL client). On macOS: `brew install libpq` then `brew link --force libpq` if needed.

## Step 1: Start Postgres

From the repo root:

```bash
cd /Users/shailendrasingh/indexer
docker compose up -d
```

Wait until Postgres is ready (a few seconds). Check:

```bash
docker compose ps
```

## Step 2: Run migrations

```bash
chmod +x scripts/run-migrations.sh
./scripts/run-migrations.sh
```

This creates all tables (and skips the `ohlc` migration that requires `pg_cron`). You should see "Migrations done." at the end.

If you don’t have `psql` on the host, run migrations from inside the container (from repo root):

```bash
docker compose exec postgres psql -U indexer -d indexer -f - < migrations/00000000000000_diesel_initial_setup/up.sql
# ... repeat for each migration in scripts/run-migrations.sh, or copy the script logic into a container shell
```

Or install the client: `brew install libpq`.

## Step 3: Set environment and start the indexer

Create a `.env` (or export in the shell):

```bash
cp .env.example .env
# Edit .env if needed: DB_CONNECTION_STRING, CONFIG_PATH
```

Then:

```bash
export DB_CONNECTION_STRING="postgres://indexer:indexer@localhost:5432/indexer?sslmode=disable"
export CONFIG_PATH=config.one-chain.toml
export RUST_LOG=info
cargo run
```

Or with `dotenv` loading (if you use a `.env` file):

```bash
export CONFIG_PATH=config.one-chain.toml
cargo run
```

(Ensure `.env` contains `DB_CONNECTION_STRING=postgres://indexer:indexer@localhost:5432/indexer?sslmode=disable`.)

You should see logs like "All tasks started..." and "total threads ...". The indexer will connect to Base (one chain) and start listening for events.

## Step 4: Hit the health check

In another terminal:

```bash
curl -s http://localhost:8085/health_check
```

Expected: `{"message":"Indexer connection is healthy","status":"ok"}` (or similar JSON).

## Step 5: Confirm data (optional)

While the indexer runs, check that it’s writing to the DB:

```bash
docker compose exec postgres psql -U indexer -d indexer -c "SELECT COUNT(*) FROM intent_state;"
docker compose exec postgres psql -U indexer -d indexer -c "SELECT chain_id, latest_block FROM chain_metadata;"
```

After some time you may see rows in `intent_state`, `order_created`, etc., and `chain_metadata` updated for Base.

## Stop

- Stop the indexer: `Ctrl+C` in the terminal where `cargo run` is running.
- Stop Postgres: `docker compose down`. Data is kept in the `indexer_pgdata` volume.
- Remove data too: `docker compose down -v`.

## One-chain config

`config.one-chain.toml` includes only **Base** (vault, mockln, AMM, hook executor). The indexer will create two EVM indexer tasks (vault + mockln) for that chain. No Solana or other chains are used.

## Troubleshooting

- **"DB_CONNECTION_STRING must be set"** — Export it or add it to `.env`.
- **Connection refused to localhost:5432** — Run `docker compose up -d` and wait a few seconds.
- **SSL required** — For local Docker use `?sslmode=disable` in the connection string.
- **Migration fails** — Ensure Postgres is healthy: `docker compose exec postgres pg_isready -U indexer -d indexer`.
