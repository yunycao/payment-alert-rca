#!/usr/bin/env python3
"""Claude hook handler: Pull competitor messaging data from Snowflake."""

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.utils.snowflake_connector import SnowflakeQueryRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STAGING_DIR = "data/staging"
COMPETITOR_FILE = f"{STAGING_DIR}/competitor_data.parquet"
META_FILE = f"{STAGING_DIR}/.competitor_metadata.json"
STALE_HOURS = 4


def check_cache() -> bool:
    meta_path = Path(META_FILE)
    if not meta_path.exists():
        return False
    metadata = json.loads(meta_path.read_text())
    last_pull = datetime.fromisoformat(metadata.get("last_pull", "2000-01-01"))
    return (datetime.now() - last_pull) < timedelta(hours=STALE_HOURS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--check-cache", action="store_true")
    args = parser.parse_args()

    if args.check_cache:
        is_fresh = check_cache() and Path(COMPETITOR_FILE).exists()
        print(json.dumps({"cached": is_fresh, "file": COMPETITOR_FILE}))
        return

    runner = SnowflakeQueryRunner()

    if not args.force and check_cache() and Path(COMPETITOR_FILE).exists():
        logger.info("Competitor data is fresh. Skipping pull.")
        print(json.dumps({"status": "cached", "file": COMPETITOR_FILE}))
        return

    logger.info("Pulling competitor messaging data...")
    df = runner.execute_competitor_query(output_dir=STAGING_DIR)

    Path(META_FILE).write_text(json.dumps({
        "last_pull": datetime.now().isoformat(),
        "row_count": len(df),
    }, indent=2))

    print(json.dumps({"status": "refreshed", "file": COMPETITOR_FILE, "rows": len(df)}))


if __name__ == "__main__":
    main()
