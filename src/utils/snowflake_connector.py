"""Snowflake connection and query execution utilities."""

import os
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


class SnowflakeQueryRunner:
    """Manages Snowflake connections and query execution for RCA analysis."""

    def __init__(
        self,
        env_path: str = "config/snowflake.env",
        config_path: str = "config/analysis_config.yaml",
    ):
        load_dotenv(env_path)
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.connection_params = {
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
        }

        self.analysis_params = {
            "start_date": os.getenv("ANALYSIS_START_DATE"),
            "end_date": os.getenv("ANALYSIS_END_DATE"),
            "intent_name": os.getenv("INTENT_NAME", "payment_alert"),
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        }

    def get_connection(self):
        """Create a new Snowflake connection."""
        import snowflake.connector

        return snowflake.connector.connect(**self.connection_params)

    def execute_query(
        self,
        sql_path: str,
        extra_params: Optional[dict] = None,
        output_path: Optional[str] = None,
    ) -> pd.DataFrame:
        """Execute a SQL template and return results as a DataFrame.

        Args:
            sql_path: Path to the SQL template file
            extra_params: Additional template parameters beyond defaults
            output_path: Optional path to save results as parquet

        Returns:
            pandas DataFrame with query results
        """
        from .sql_renderer import render_sql_template

        params = {**self.analysis_params}
        if extra_params:
            params.update(extra_params)

        sql = render_sql_template(sql_path, params)
        logger.info(f"Executing query from {sql_path}")

        conn = self.get_connection()
        try:
            df = pd.read_sql(sql, conn)
            logger.info(f"Query returned {len(df):,} rows")

            if output_path:
                Path(output_path).parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(output_path, index=False)
                logger.info(f"Saved results to {output_path}")

            return df
        finally:
            conn.close()

    def execute_funnel_query(self, output_dir: str = "data/staging/") -> pd.DataFrame:
        """Execute the full funnel analysis query."""
        return self.execute_query(
            "sql/funnel/full_funnel.sql",
            output_path=f"{output_dir}/funnel_data.parquet",
        )

    def execute_competitor_query(
        self, output_dir: str = "data/staging/"
    ) -> pd.DataFrame:
        """Execute the competitor overlap analysis query."""
        extra_params = {
            "window_hours": self.config["competitor_analysis"]["window_hours"]
        }
        return self.execute_query(
            "sql/competitor/competitor_overlap.sql",
            extra_params=extra_params,
            output_path=f"{output_dir}/competitor_data.parquet",
        )

    def execute_anomaly_queries(
        self, output_dir: str = "data/staging/"
    ) -> dict[str, pd.DataFrame]:
        """Execute all anomaly detection queries."""
        results = {}

        # Propensity drift
        drift_config = self.config["anomaly"]["propensity_drift"]
        results["propensity_drift"] = self.execute_query(
            "sql/anomaly/propensity_drift.sql",
            extra_params={
                "percentile_bins": drift_config["percentile_bins"],
                "reference_window_days": drift_config["reference_window_days"],
                "detection_start": self.analysis_params["start_date"],
                "detection_end": self.analysis_params["end_date"],
            },
            output_path=f"{output_dir}/propensity_drift.parquet",
        )

        # Default score timeout
        timeout_config = self.config["anomaly"]["default_scores"]
        results["default_scores"] = self.execute_query(
            "sql/anomaly/default_score_timeout.sql",
            extra_params={
                "timeout_threshold_ms": timeout_config["timeout_threshold_ms"]
            },
            output_path=f"{output_dir}/default_scores.parquet",
        )

        # Campaign takeover
        takeover_config = self.config["anomaly"]["campaign_takeover"]
        results["campaign_takeover"] = self.execute_query(
            "sql/anomaly/campaign_takeover.sql",
            extra_params={
                "impression_share_threshold": takeover_config[
                    "impression_share_threshold"
                ],
                "hhi_threshold": takeover_config["concentration_hhi_threshold"],
                "min_campaigns": takeover_config["min_campaigns_expected"],
            },
            output_path=f"{output_dir}/campaign_takeover.parquet",
        )

        return results

    def test_connection(self) -> bool:
        """Test Snowflake connectivity."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_TIMESTAMP()")
            result = cursor.fetchone()
            logger.info(f"Connection successful. Server time: {result[0]}")
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
