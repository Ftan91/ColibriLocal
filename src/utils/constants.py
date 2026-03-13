import os

# Absolute paths ensure Delta table locations are consistent
# regardless of the working directory the pipeline is launched from.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

RAW_PATH = os.path.join(_PROJECT_ROOT, "data/raw/wind_turbines/*")
DELTA_BASE = os.path.join(_PROJECT_ROOT, "data/delta")

BRONZE_WATERMARK_KEY = "bronze_wind_turbines"