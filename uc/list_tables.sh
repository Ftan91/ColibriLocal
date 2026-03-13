#!/usr/bin/env bash
# list_tables.sh — List and describe all tables registered in the local OSS Unity Catalog server.
#
# What this does:
#   1. Lists all catalogs known to the UC server.
#   2. Lists all schemas in the 'dev' catalog.
#   3. Lists tables in each layer schema.
#   4. Prints full metadata (schema, storage location) for each pipeline table.
#
# Prerequisites:
#   - UC server is running (see uc/README.md for how to start it).
#   - UC_HOME env var points to the root of the unitycatalog repo.
#   - uc/register_tables.sh has been run at least once.

set -euo pipefail

UC_BIN="${UC_HOME:?UC_HOME must be set to the root of the unitycatalog repo}/bin/uc"
CATALOG="dev"

echo "=== Catalogs ==="
"$UC_BIN" catalog list

echo ""
echo "=== Schemas in '$CATALOG' ==="
"$UC_BIN" schema list --catalog "$CATALOG"

for SCHEMA in bronze silver gold; do
  echo ""
  echo "=== Tables in $CATALOG.$SCHEMA ==="
  "$UC_BIN" table list --catalog "$CATALOG" --schema "$SCHEMA"
done

echo ""
echo "=== Table details ==="

for FULL_NAME in \
  "$CATALOG.bronze.wind_turbines" \
  "$CATALOG.silver.wind_turbine_clean" \
  "$CATALOG.silver.wind_turbine_quarantine" \
  "$CATALOG.gold.fct_turbine_power_snapshot"; do
    echo ""
    echo "--- $FULL_NAME ---"
    "$UC_BIN" table get --full_name "$FULL_NAME"
done
