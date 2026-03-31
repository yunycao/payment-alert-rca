#!/usr/bin/env python3
"""Claude hook handler: Pull full funnel data from Snowflake.

This script is invoked by Claude Code hooks before analysis commands.
It checks for data freshness and pulls fresh data if stale or missing.
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.snowflake_connector import SnowflakeQueryRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STAGING_DIR = "data/staging"
FUNNEL_FILE = f"{STAGING_DIR}/funnel_data.parquet"
FRESHNESS_FILE = f"{STAGING_DIR}/.funnel_metadata.json"
STALE_HOURS = 4


def check_freshness() -> bool:
    """Return True if cached data is fresh enough."""
    meta_path = Path(FRESHNESS_FILE)
    if not meta_path.exists():
        return False

    metadata = json.loads(meta_path.read_text())
    last_pull = datetime.fromisoformat(metadata.get("last_pull", "2000-01-01"))
    return (datetime.now() - last_pull) < timedelta(hours=STALE_HOURS)


def update_metadata(row_count: int):
    """Write freshness metadata after a successful pull."""
    Path(STAGING_DIR).mkdir(parents=True, exist_ok=True)
    metadata = {
        "last_pull": datetime.now().isoformat(),
        "row_count": row_count,
        "query": "sql/funnel/full_funnel.sql",
        "source": "snowflake",
    }
    Path(FRESHNESS_FILE).write_text(json.dumps(metadata, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Pull funnel data from Snowflake")
    parser.add_argument("--force", action="store_true", help="Force refresh even if cache is fresh")
    parser.add_argument("--validate-connection", action="store_true", help="Only test connection")
    args = parser.parse_args()

    runner = SnowflakeQueryRunner()

    if args.validate_connection:
        success = runner.test_connection()
        sys.exit(0 if success else 1)

    if not args.force and check_freshness() and Path(FUNNEL_FILE).exists():
        logger.info("Funnel data is fresh. Skipping pull.")
        print(json.dumps({"status": "cached", "file": FUNNEL_FILE}))
        return

    logger.info("Pulling fresh funnel data from Snowflake...")
    df = runner.execute_funnel_query(output_dir=STAGING_DIR)
    update_metadata(len(df))

    result = {
        "status": "refreshed",
        "file": FUNNEL_FILE,
        "rows": len(df),
        "columns": list(df.columns),
        "date_range": {
            "min": str(df["report_date"].min()) if "report_date" in df.columns else None,
            "max": str(df["report_date"].max()) if "report_date" in df.columns else None,
        },
    }
    print(json.dumps(result, indent=2))
    logger.info(f"Funnel data pull complete: {len(df):,} rows")


if __name__ == "__main__":
    main()
