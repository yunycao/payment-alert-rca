#!/usr/bin/env python3
"""Claude hook handler: Pull all anomaly event data from Snowflake."""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.utils.snowflake_connector import SnowflakeQueryRunner

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

STAGING_DIR = "data/staging"


def main():
    runner = SnowflakeQueryRunner()
    logger.info("Pulling all anomaly datasets...")

    results = runner.execute_anomaly_queries(output_dir=STAGING_DIR)

    summary = {
        "status": "refreshed",
        "timestamp": datetime.now().isoformat(),
        "datasets": {
            name: {"rows": len(df), "file": f"{STAGING_DIR}/{name}.parquet"}
            for name, df in results.items()
        },
    }

    Path(f"{STAGING_DIR}/.anomaly_metadata.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    logger.info("All anomaly data pulled successfully.")


if __name__ == "__main__":
    main()
