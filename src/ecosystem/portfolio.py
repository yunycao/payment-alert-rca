"""Portfolio-level efficiency: cross-intent resource allocation analysis.

Measures whether the messaging system allocates impressions efficiently
across intents. Key insight: optimizing ONE intent in isolation can
hurt the portfolio. This module measures system-level efficiency.
"""

import pandas as pd
import numpy as np
from typing import Optional


class PortfolioEfficiencyAnalyzer:
    """Analyzes cross-intent messaging efficiency at the portfolio level."""

    def __init__(self, data_path: str = "data/staging/portfolio_efficiency.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            self._df["send_date"] = pd.to_datetime(self._df["send_date"])
        return self._df

    def intent_efficiency_ranking(self) -> pd.DataFrame:
        """Rank intents by conversion efficiency and revenue per impression."""
        return (
            self.df.groupby("intent_name")
            .agg({
                "users_messaged": "sum",
                "messages_sent": "sum",
                "same_intent_conversions": "sum",
                "cross_intent_conversions": "sum",
                "no_conversion": "sum",
                "attributed_revenue": "sum",
                "avg_propensity": "mean",
                "default_score_pct": "mean",
            })
            .assign(
                intent_cvr=lambda x: (x["same_intent_conversions"] / x["users_messaged"]).round(4),
                any_cvr=lambda x: (
                    (x["same_intent_conversions"] + x["cross_intent_conversions"]) / x["users_messaged"]
                ).round(4),
                revenue_per_impression=lambda x: (x["attributed_revenue"] / x["messages_sent"]).round(4),
                waste_rate=lambda x: (x["no_conversion"] / x["users_messaged"]).round(4),
            )
            .sort_values("revenue_per_impression", ascending=False)
            .reset_index()
        )

    def frequency_saturation_curve(self) -> pd.DataFrame:
        """Analyze how conversion rate changes with message frequency.

        Returns: frequency bucket → conversion rate mapping.
        Useful for finding the saturation point where more messages
        no longer improve (or start hurting) conversion.
        """
        if "avg_messages_per_user" not in self.df.columns:
            return pd.DataFrame()

        daily = self.df.groupby("send_date").agg({
            "avg_messages_per_user": "mean",
            "avg_intents_per_user": "mean",
            "conversion_rate_by_frequency": "mean",
        }).reset_index()

        # Bucket by frequency
        daily["freq_bucket"] = pd.cut(
            daily["avg_messages_per_user"],
            bins=[0, 1, 2, 3, 5, 10, np.inf],
            labels=["1", "2", "3", "4-5", "6-10", "10+"],
        )

        return (
            daily.groupby("freq_bucket", observed=True)
            .agg({
                "conversion_rate_by_frequency": ["mean", "std", "count"],
                "avg_intents_per_user": "mean",
            })
            .round(4)
            .reset_index()
        )

    def intent_interaction_effects(self) -> pd.DataFrame:
        """Measure how PA performance changes based on co-occurring intents.

        If PA conversion improves when sent alongside intent X (complementary)
        or worsens (substitutive), that informs portfolio optimization.
        """
        pa_data = self.df[self.df["intent_name"] == "payment_alert"].copy()

        if "avg_intents_per_user" not in pa_data.columns:
            return pd.DataFrame()

        pa_data["multi_intent_day"] = pa_data["avg_intents_per_user"] > 1.5

        comparison = pa_data.groupby("multi_intent_day").agg({
            "intent_conversion_rate": "mean",
            "any_conversion_rate": "mean",
            "revenue_per_user": "mean",
            "send_date": "count",
        }).rename(columns={"send_date": "n_days"}).reset_index()

        comparison["multi_intent_day"] = comparison["multi_intent_day"].map(
            {True: "Multi-intent days", False: "Single-intent days"}
        )

        return comparison

    def generate_report(self) -> str:
        """Generate portfolio efficiency report."""
        lines = ["# Portfolio Efficiency — Cross-Intent Allocation\n"]

        ranking = self.intent_efficiency_ranking()
        lines.append("## 1. Intent Efficiency Ranking\n")
        lines.append(ranking[["intent_name", "users_messaged", "intent_cvr", "any_cvr",
                              "revenue_per_impression", "waste_rate",
                              "default_score_pct"]].to_markdown(index=False))

        interaction = self.intent_interaction_effects()
        if len(interaction) > 0:
            lines.append("\n## 2. PA Performance: Single vs Multi-Intent Days\n")
            lines.append(interaction.to_markdown(index=False))

        return "\n".join(lines)
