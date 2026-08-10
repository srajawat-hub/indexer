#!/usr/bin/env bash
# Run DB migrations (skips ohlc - requires pg_cron). Use after docker compose up.
# Requires: DB_CONNECTION_STRING or DATABASE_URL in env, or pass connection string as first arg.

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MIGRATIONS_DIR="$REPO_ROOT/migrations"

# Connection string: first arg, or DB_CONNECTION_STRING, or DATABASE_URL, or default local docker
CONN="${1:-${DB_CONNECTION_STRING:-${DATABASE_URL:-postgres://indexer:indexer@localhost:5432/indexer}}}"

# For local Docker Postgres without TLS
if [[ "$CONN" == *"localhost"* ]] && [[ "$CONN" != *"sslmode"* ]]; then
  CONN="${CONN}?sslmode=disable"
fi

echo "Using connection: ${CONN%%\?*}"

run_sql() {
  psql "$CONN" -v ON_ERROR_STOP=1 "$@"
}

# Diesel order (lexical by migration folder name). Skip 2025-06-03-091943_ohlc (pg_cron).
MIGRATIONS=(
  "00000000000000_diesel_initial_setup"
  "2025-05-29-132550_pool_enums"
  "2025-05-29-132602_pool_table"
  "2025-06-03-090713_swaps"
  "2025-06-16-185636_hook_executor"
  "2025-06-18-114641_add_launchpad_token"
  "2025-06-19-230100_add_latest_block_to_chain_metadata"
  "2025-07-03-142239_liquidity-lock"
  "2025-07-09-155338_rm-network-name"
  "2025-07-16-203550_solution_variant"
  "2025-07-18-103152_quote-price"
  "2025-07-22-030604_mayan_vaas"
  "2025-08-01-084417_launchpad_metrics"
)

for name in "${MIGRATIONS[@]}"; do
  f="$MIGRATIONS_DIR/$name/up.sql"
  if [[ -f "$f" ]]; then
    echo "Running migration: $name"
    run_sql -f "$f"
  else
    echo "Missing: $f"
    exit 1
  fi
done

# Fix intent_fees.id so INSERT ... VALUES(DEFAULT, ...) works (migration has integer PK without default)
echo "Setting intent_fees id default..."
run_sql -c "
  CREATE SEQUENCE IF NOT EXISTS intent_fees_id_seq;
  ALTER TABLE intent_fees ALTER COLUMN id SET DEFAULT nextval('intent_fees_id_seq');
  SELECT setval('intent_fees_id_seq', (SELECT COALESCE(MAX(id), 1) FROM intent_fees));
"

echo "Migrations done."
