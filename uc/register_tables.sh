#!/usr/bin/env bash
# register_tables.sh — Register all ColibriLocal Delta tables in the local OSS Unity Catalog server.
#
# What this does:
#   1. Creates the 'dev' catalog (idempotent).
#   2. Creates three schemas: bronze, silver, gold (idempotent).
#   3. Registers four Delta tables by pointing UC at their on-disk paths.
#      On subsequent runs, tables that already exist are skipped gracefully.
#
# Prerequisites:
#   - UC server is running: $UC_HOME/bin/start-uc-server
#   - UC_HOME env var is set (see ~/.zshrc).
#   - The pipeline has been run at least once so the Delta tables exist on disk.
#
# Usage:
#   ./uc/register_tables.sh

set -euo pipefail

UC_BIN="${UC_HOME:?UC_HOME must be set to the root of the unitycatalog repo}/bin/uc"
CATALOG="dev"

# Resolve the project root relative to this script so paths are always absolute.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DELTA_BASE="$PROJECT_ROOT/data/delta"

# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------
echo "=== Creating catalog '$CATALOG' (if not exists) ==="
"$UC_BIN" catalog create --name "$CATALOG" 2>/dev/null \
  && echo "Catalog created." \
  || echo "Catalog already exists — skipping."

echo ""

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
for SCHEMA in bronze silver gold; do
  echo "Creating schema $CATALOG.$SCHEMA (if not exists) ..."
  "$UC_BIN" schema create --catalog "$CATALOG" --name "$SCHEMA" 2>/dev/null \
    && echo "  Done." \
    || echo "  Already exists — skipping."
done

echo ""

# Helper: register one table, skip if it already exists.
register_table() {
  local full_name="$1"
  local storage_location="$2"
  local columns="$3"

  echo "Registering $full_name ..."
  "$UC_BIN" table create \
    --full_name "$full_name" \
    --format DELTA \
    --storage_location "$storage_location" \
    --columns "$columns" 2>/dev/null \
    && echo "  Done." \
    || echo "  Already registered — skipping."
}

# ---------------------------------------------------------------------------
# Bronze — raw wind turbine readings with audit metadata and MD5 surrogate key
# ---------------------------------------------------------------------------
register_table \
  "$CATALOG.bronze.wind_turbines" \
  "$DELTA_BASE/01_bronze/wind_turbines" \
  "timestamp TIMESTAMP,turbine_id STRING,wind_speed DOUBLE,wind_direction DOUBLE,power_output DOUBLE,ingested_at TIMESTAMP,source_file STRING,pk STRING"

# ---------------------------------------------------------------------------
# Silver clean — deduplicated, null-imputed, DQ-passed rows
# ---------------------------------------------------------------------------
register_table \
  "$CATALOG.silver.wind_turbine_clean" \
  "$DELTA_BASE/02_silver/wind_turbine_clean" \
  "timestamp TIMESTAMP,turbine_id STRING,wind_speed DOUBLE,wind_direction DOUBLE,power_output DOUBLE,ingested_at TIMESTAMP,source_file STRING,pk STRING"

# ---------------------------------------------------------------------------
# Silver quarantine — rows that failed DQ checks with failure reason
# ---------------------------------------------------------------------------
register_table \
  "$CATALOG.silver.wind_turbine_quarantine" \
  "$DELTA_BASE/02_silver/wind_turbine_quarantine" \
  "timestamp TIMESTAMP,turbine_id STRING,wind_speed DOUBLE,wind_direction DOUBLE,power_output DOUBLE,ingested_at TIMESTAMP,source_file STRING,pk STRING,dq_failure_reason STRING"

# ---------------------------------------------------------------------------
# Gold — aggregated power snapshot with fleet anomaly flags
# ---------------------------------------------------------------------------
register_table \
  "$CATALOG.gold.fct_turbine_power_snapshot" \
  "$DELTA_BASE/03_gold/fct_turbine_power_snapshot" \
  "turbine_id STRING,window_type STRING,period TIMESTAMP,period_min_power DOUBLE,period_max_power DOUBLE,period_avg_power DOUBLE,fleet_avg_min_power DOUBLE,fleet_avg_max_power DOUBLE,fleet_avg_power DOUBLE,fleet_stddev DOUBLE,fleet_deviation DOUBLE,fleet_sigmas DOUBLE,is_fleet_anomaly BOOLEAN"

echo ""
echo "=== Registration complete. Run uc/list_tables.sh to verify. ==="
