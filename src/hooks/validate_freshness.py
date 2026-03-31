#!/usr/bin/env python3
"""Claude hook handler: Validate data freshness before analysis runs."""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

STAGING_DIR = "data/staging"
STALE_HOURS = 4

DATASETS = {
    "funnel": ".funnel_metadata.json",
    "competitor": ".competitor_metadata.json",
    "anomaly": ".anomaly_metadata.json",
}


def main():
    stale = []
    missing = []

    for name, meta_file in DATASETS.items():
        meta_path = Path(STAGING_DIR) / meta_file
        if not meta_path.exists():
            missing.append(name)
            continue

        metadata = json.loads(meta_path.read_text())
        last_pull = datetime.fromisoformat(metadata.get("last_pull", "2000-01-01"))
        if (datetime.now() - last_pull) > timedelta(hours=STALE_HOURS):
            stale.append(name)

    result = {
        "all_fresh": len(stale) == 0 and len(missing) == 0,
        "stale_datasets": stale,
        "missing_datasets": missing,
        "recommendation": None,
    }

    if missing:
        result["recommendation"] = f"Run data pull for missing datasets: {', '.join(missing)}"
    elif stale:
        result["recommendation"] = f"Data is stale for: {', '.join(stale)}. Consider refreshing."

    print(json.dumps(result, indent=2))
    sys.exit(0 if result["all_fresh"] else 1)


if __name__ == "__main__":
    main()
