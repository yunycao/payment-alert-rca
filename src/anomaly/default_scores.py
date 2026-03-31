"""ML platform timeout and default score analysis."""

import pandas as pd
import numpy as np
from typing import Optional


class DefaultScoreAnalyzer:
    """Analyzes ML platform timeout events and default score impact."""

    def __init__(self, data_path: str = "data/staging/default_scores.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            self._df["report_date"] = pd.to_datetime(self._df["report_date"])
        return self._df

    def daily_timeout_summary(self) -> pd.DataFrame:
        """Daily summary of timeout rates and latency."""
        return (
            self.df.groupby(["report_date", "channel"])
            .agg({
                "total_decisions": "sum",
                "default_score_count": "sum",
                "model_score_count": "sum",
                "timeout_count": "sum",
                "avg_latency_ms": "mean",
                "p95_latency_ms": "mean",
                "p99_latency_ms": "mean",
            })
            .assign(
                default_rate=lambda x: (x["default_score_count"] / x["total_decisions"] * 100).round(2),
                timeout_rate=lambda x: (x["timeout_count"] / x["total_decisions"] * 100).round(2),
            )
            .reset_index()
        )

    def hourly_pattern(self) -> pd.DataFrame:
        """Analyze timeout patterns by hour of day."""
        return (
            self.df.groupby(["decision_hour", "channel"])
            .agg({
                "total_decisions": "sum",
                "default_score_count": "sum",
                "timeout_count": "sum",
                "avg_latency_ms": "mean",
                "p95_latency_ms": "mean",
            })
            .assign(
                default_rate=lambda x: (x["default_score_count"] / x["total_decisions"] * 100).round(2)
            )
            .reset_index()
            .sort_values(["channel", "decision_hour"])
        )

    def outcome_comparison(self) -> pd.DataFrame:
        """Compare outcomes between default-scored and model-scored users."""
        metrics = self.df.groupby("channel").agg({
            "default_open_rate": "mean",
            "model_open_rate": "mean",
            "default_click_rate": "mean",
            "model_click_rate": "mean",
            "default_conversion_rate": "mean",
            "model_conversion_rate": "mean",
            "default_revenue": "sum",
            "model_revenue": "sum",
            "default_score_count": "sum",
            "model_score_count": "sum",
        }).reset_index()

        metrics["open_rate_gap"] = ((metrics["model_open_rate"] - metrics["default_open_rate"]) * 100).round(2)
        metrics["click_rate_gap"] = ((metrics["model_click_rate"] - metrics["default_click_rate"]) * 100).round(2)
        metrics["conversion_gap"] = ((metrics["model_conversion_rate"] - metrics["default_conversion_rate"]) * 100).round(2)

        return metrics

    def estimate_revenue_impact(self) -> dict:
        """Estimate total revenue lost due to default scoring."""
        comparison = self.outcome_comparison()
        total_impact = 0

        channel_impacts = {}
        for _, row in comparison.iterrows():
            conv_gap = row["model_conversion_rate"] - row["default_conversion_rate"]
            if conv_gap > 0 and row["default_score_count"] > 0:
                avg_rev_per_conversion = (
                    row["model_revenue"] / (row["model_score_count"] * row["model_conversion_rate"])
                    if row["model_conversion_rate"] > 0 else 0
                )
                lost_conversions = row["default_score_count"] * conv_gap
                channel_loss = lost_conversions * avg_rev_per_conversion
                channel_impacts[row["channel"]] = round(channel_loss, 2)
                total_impact += channel_loss

        return {
            "total_estimated_loss": round(total_impact, 2),
            "by_channel": channel_impacts,
        }

    def generate_report(self) -> str:
        """Generate timeout analysis report."""
        lines = ["# ML Platform Timeout — Default Score Analysis\n"]

        daily = self.daily_timeout_summary()
        overall = daily.groupby("channel").agg({
            "default_rate": "mean",
            "timeout_rate": "mean",
            "p95_latency_ms": "mean",
            "total_decisions": "sum",
            "default_score_count": "sum",
        }).reset_index()

        lines.append("## Overall Summary by Channel\n")
        lines.append(overall.to_markdown(index=False))

        comparison = self.outcome_comparison()
        lines.append("\n## Outcome Comparison: Default vs Model Scores\n")
        lines.append(comparison[["channel", "open_rate_gap", "click_rate_gap", "conversion_gap"]].to_markdown(index=False))

        impact = self.estimate_revenue_impact()
        lines.append(f"\n## Estimated Revenue Impact\n")
        lines.append(f"- **Total estimated loss**: ${impact['total_estimated_loss']:,.2f}")
        for ch, loss in impact["by_channel"].items():
            lines.append(f"  - {ch}: ${loss:,.2f}")

        return "\n".join(lines)
