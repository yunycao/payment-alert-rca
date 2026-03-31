"""Campaign impression takeover analysis."""

import pandas as pd
import numpy as np
from typing import Optional


class CampaignTakeoverAnalyzer:
    """Analyzes campaign concentration and impression takeover events."""

    def __init__(self, data_path: str = "data/staging/campaign_takeover.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            self._df["report_date"] = pd.to_datetime(self._df["report_date"])
        return self._df

    def daily_concentration(self) -> pd.DataFrame:
        """Daily HHI and top campaign share metrics."""
        return (
            self.df.groupby(["report_date", "channel"])
            .agg({
                "hhi_index": "first",
                "max_single_campaign_share": "first",
                "active_campaigns": "first",
                "dominant_campaign_id": "first",
                "dominant_campaign_name": "first",
            })
            .reset_index()
            .sort_values(["channel", "report_date"])
        )

    def identify_takeover_days(
        self, share_threshold: float = 0.4, hhi_threshold: float = 0.25
    ) -> pd.DataFrame:
        """Find days where impression concentration exceeded thresholds."""
        daily = self.daily_concentration()
        return daily[
            (daily["max_single_campaign_share"] > share_threshold)
            | (daily["hhi_index"] > hhi_threshold)
        ].copy()

    def campaign_performance_comparison(self) -> pd.DataFrame:
        """Compare performance of dominant campaigns vs others."""
        dominant = self.df[self.df["is_takeover_campaign"] == True]
        others = self.df[self.df["is_takeover_campaign"] == False]

        comparison = pd.DataFrame({
            "metric": ["Avg Open Rate", "Avg CTR", "Avg Rev/User", "Total Revenue", "Total Users"],
            "dominant_campaigns": [
                dominant["open_rate"].mean(),
                dominant["ctr"].mean(),
                dominant["revenue_per_user"].mean(),
                dominant["revenue"].sum(),
                dominant["users_messaged"].sum(),
            ],
            "other_campaigns": [
                others["open_rate"].mean(),
                others["ctr"].mean(),
                others["revenue_per_user"].mean(),
                others["revenue"].sum(),
                others["users_messaged"].sum(),
            ],
        })
        comparison["gap"] = comparison["dominant_campaigns"] - comparison["other_campaigns"]
        return comparison

    def displaced_campaigns(self) -> pd.DataFrame:
        """Identify campaigns that lost volume during takeover days."""
        takeover_days = set(self.identify_takeover_days()["report_date"].unique())
        non_takeover_days = set(self.df["report_date"].unique()) - takeover_days

        if not takeover_days or not non_takeover_days:
            return pd.DataFrame()

        takeover_vol = (
            self.df[self.df["report_date"].isin(takeover_days)]
            .groupby("campaign_id")["users_messaged"].mean()
        )
        normal_vol = (
            self.df[self.df["report_date"].isin(non_takeover_days)]
            .groupby("campaign_id")["users_messaged"].mean()
        )

        comparison = pd.DataFrame({
            "avg_users_takeover_days": takeover_vol,
            "avg_users_normal_days": normal_vol,
        }).dropna()

        comparison["volume_change_pct"] = (
            (comparison["avg_users_takeover_days"] - comparison["avg_users_normal_days"])
            / comparison["avg_users_normal_days"] * 100
        ).round(2)

        return comparison.sort_values("volume_change_pct").reset_index()

    def generate_report(self) -> str:
        """Generate campaign takeover analysis report."""
        lines = ["# Campaign Impression Takeover Analysis\n"]

        takeover_days = self.identify_takeover_days()
        lines.append(f"## Takeover Days Detected: {len(takeover_days)}\n")
        if len(takeover_days) > 0:
            lines.append(takeover_days[["report_date", "channel", "hhi_index",
                         "max_single_campaign_share", "dominant_campaign_name"]].to_markdown(index=False))

        comparison = self.campaign_performance_comparison()
        lines.append("\n## Performance: Dominant vs Other Campaigns\n")
        lines.append(comparison.to_markdown(index=False))

        displaced = self.displaced_campaigns()
        if len(displaced) > 0:
            worst = displaced.head(10)
            lines.append("\n## Most Displaced Campaigns (during takeover days)\n")
            lines.append(worst.to_markdown(index=False))

        return "\n".join(lines)
