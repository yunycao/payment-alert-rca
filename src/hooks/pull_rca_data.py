#!/usr/bin/env python3
"""Claude hook: Pull RCA decomposition data from Snowflake.

Triggered when an analysis session involves root cause analysis for
spend drops or on-time payment rate declines. Validates freshness
and pulls decomposition data across all dimensions.
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta

METADATA_PATH = Path("data/staging/.rca_metadata.json")
FRESHNESS_HOURS = 4


def check_freshness() -> bool:
    """Check if RCA data was pulled within the freshness window."""
    if not METADATA_PATH.exists():
        return False
    meta = json.loads(METADATA_PATH.read_text())
    last_pull = datetime.fromisoformat(meta.get("last_pull", "2000-01-01"))
    return (datetime.now() - last_pull).total_seconds() < FRESHNESS_HOURS * 3600


def pull_rca_data():
    """Pull RCA decomposition data from Snowflake."""
    if check_freshness():
        print("✅ RCA data is fresh (pulled within last 4 hours)")
        return

    print("🔄 Pulling RCA decomposition data from Snowflake...")

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from src.utils.snowflake_connector import SnowflakeQueryRunner

        runner = SnowflakeQueryRunner()
        results = runner.execute_rca_queries()

        # Save metadata
        METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        METADATA_PATH.write_text(json.dumps({
            "last_pull": datetime.now().isoformat(),
            "queries": list(results.keys()),
            "row_counts": {k: len(v) for k, v in results.items()},
        }, indent=2))

        for name, df in results.items():
            print(f"  ✅ {name}: {len(df):,} rows")

    except ImportError:
        print("⚠️ Snowflake connector not available. Using cached data if present.")
    except Exception as e:
        print(f"❌ RCA data pull failed: {e}")
        print("   Falling back to cached data if available.")


if __name__ == "__main__":
    pull_rca_data()
