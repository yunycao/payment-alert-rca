"""Competitor messaging analysis for eligible payment alert audience."""

import pandas as pd
import numpy as np
from typing import Optional


class CompetitorAnalyzer:
    """Analyzes competing intents that suppress or displace payment alerts."""

    def __init__(self, data_path: str = "data/staging/competitor_data.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            self._df["report_date"] = pd.to_datetime(self._df["report_date"])
        return self._df

    def top_competitors(self, n: int = 10) -> pd.DataFrame:
        """Rank competing intents by suppression impact."""
        return (
            self.df.groupby("competitor_intent")
            .agg({
                "users_suppressed_by_competitor": "sum",
                "users_receiving_competitor_msg": "sum",
                "eligible_audience_size": "sum",
                "competitor_campaign_count": "sum",
            })
            .assign(
                suppression_rate=lambda x: (
                    x["users_suppressed_by_competitor"] / x["eligible_audience_size"] * 100
                ).round(2),
                overlap_rate=lambda x: (
                    x["users_receiving_competitor_msg"] / x["eligible_audience_size"] * 100
                ).round(2),
            )
            .sort_values("users_suppressed_by_competitor", ascending=False)
            .head(n)
            .reset_index()
        )

    def channel_overlap_matrix(self) -> pd.DataFrame:
        """Create channel x competitor overlap matrix."""
        return self.df.pivot_table(
            index="competitor_intent",
            columns="channel",
            values="competitor_overlap_pct",
            aggfunc="mean",
        ).round(2)

    def daily_suppression_trend(self) -> pd.DataFrame:
        """Daily trend of suppression by top competitors."""
        return (
            self.df.groupby(["report_date", "competitor_intent"])
            .agg({
                "users_suppressed_by_competitor": "sum",
                "suppression_rate_pct": "mean",
            })
            .reset_index()
            .sort_values(["report_date", "users_suppressed_by_competitor"], ascending=[True, False])
        )

    def segment_vulnerability(self) -> pd.DataFrame:
        """Identify which user segments are most affected by competitor suppression."""
        return (
            self.df.groupby(["segment", "competitor_intent"])
            .agg({
                "users_suppressed_by_competitor": "sum",
                "eligible_audience_size": "sum",
                "avg_pa_propensity_suppressed_users": "mean",
            })
            .assign(
                suppression_rate=lambda x: (
                    x["users_suppressed_by_competitor"] / x["eligible_audience_size"] * 100
                ).round(2)
            )
            .sort_values("suppression_rate", ascending=False)
            .reset_index()
        )

    def priority_analysis(self) -> pd.DataFrame:
        """Compare priority levels between payment alert and competitors."""
        return (
            self.df.groupby("competitor_intent")
            .agg({
                "avg_competitor_priority": "mean",
                "avg_pa_propensity_suppressed_users": "mean",
                "users_suppressed_by_competitor": "sum",
            })
            .sort_values("avg_competitor_priority", ascending=False)
            .reset_index()
        )

    def generate_report(self) -> str:
        """Generate a text summary of competitor analysis."""
        lines = ["# Payment Alert — Competitor Messaging Analysis\n"]

        top = self.top_competitors()
        lines.append("## Top Competing Intents by Suppression Impact\n")
        lines.append(top.to_markdown(index=False))

        channel_matrix = self.channel_overlap_matrix()
        lines.append("\n## Channel Overlap Matrix (% of PA eligible audience)\n")
        lines.append(channel_matrix.to_markdown())

        vulnerable = self.segment_vulnerability().head(10)
        lines.append("\n## Most Vulnerable Segments\n")
        lines.append(vulnerable.to_markdown(index=False))

        return "\n".join(lines)
