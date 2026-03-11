# ---------------------------------------------------------------------------
# Root paths — all Delta layer directories and raw ingest source
# ---------------------------------------------------------------------------

RAW_PATH = "data/raw/wind_turbines/*"
DELTA_BASE = "data/delta"

CONTROL_SCHEMA = f"{DELTA_BASE}/00_control"
BRONZE_SCHEMA = f"{DELTA_BASE}/01_bronze"
SILVER_SCHEMA = f"{DELTA_BASE}/02_silver"
GOLD_SCHEMA = f"{DELTA_BASE}/03_gold"

# ---------------------------------------------------------------------------
# Table paths — fully qualified Delta table locations used across layers
# ---------------------------------------------------------------------------

WATERMARK_PATH = f"{CONTROL_SCHEMA}/watermarks"
BRONZE_WATERMARK_KEY = "bronze_wind_turbines"

BRONZE_TABLE = f"{BRONZE_SCHEMA}/wind_turbines"
SILVER_CLEAN_TABLE = f"{SILVER_SCHEMA}/wind_turbine_clean"
SILVER_QUARANTINE_TABLE = f"{SILVER_SCHEMA}/wind_turbine_quarantine"
GOLD_TABLE = f"{GOLD_SCHEMA}/fct_turbine_power_snapshot"