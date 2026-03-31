"""Cannibalization analysis: does this intent steal conversions from other intents?

Measures cross-intent overlap among converters to quantify how much of
the attributed conversion is truly incremental vs displaced from
another intent's natural pathway.

Framework:
- Gross lift: treatment vs holdout conversion rate
- Cannibalized conversions: users who engaged with BOTH intents before converting
- Net incremental lift: gross lift minus estimated cannibalization
- Organic baseline: conversion rate among users who received no messaging at all
"""

import pandas as pd
import numpy as np
from typing import Optional


class CannibalizationAnalyzer:
    """Quantifies cross-intent cannibalization for attributed conversions."""

    def __init__(self, data_path: str = "data/staging/cannibalization.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
        return self._df

    def overlap_summary(self) -> pd.DataFrame:
        """Summarize dual-exposure rates by competing intent."""
        summary = self.df.groupby("other_intent").agg({
            "dual_exposed_converters": "sum",
            "total_pa_converters": "first",
            "dual_exposure_pct": "mean",
            "other_intent_open_rate": "mean",
            "other_intent_click_rate": "mean",
            "pa_attributed_revenue_overlap": "sum",
            "other_sent_first_count": "sum",
        }).reset_index()

        # Users who engaged with the other intent AND it was sent first
        # are the strongest cannibalization candidates
        summary["cannibalization_risk"] = (
            summary["other_intent_click_rate"] *
            (summary["other_sent_first_count"] / summary["dual_exposed_converters"].clip(lower=1))
        ).round(4)

        return summary.sort_values("cannibalization_risk", ascending=False)

    def estimate_net_incrementality(
        self,
        gross_lift: float,
        gross_lift_users: int,
    ) -> dict:
        """Estimate net incremental conversions after cannibalization adjustment.

        Args:
            gross_lift: Treatment - holdout conversion rate difference
            gross_lift_users: Number of treatment users

        Returns:
            Dict with gross, cannibalized, and net estimates
        """
        total_converters = self.df["total_pa_converters"].iloc[0] if len(self.df) > 0 else 0
        dual_exposed = self.df["dual_exposed_converters"].sum()
        organic_rate = self.df["segment_organic_rate"].mean()

        # Conservative estimate: all dual-exposed who clicked the other intent
        # AND received the other message first are likely cannibalized
        cannibalized_est = int(
            self.df.apply(
                lambda r: r["dual_exposed_converters"] * r["other_intent_click_rate"]
                * (r["other_sent_first_count"] / max(r["dual_exposed_converters"], 1)),
                axis=1,
            ).sum()
        )

        gross_incremental = int(gross_lift * gross_lift_users)
        net_incremental = max(0, gross_incremental - cannibalized_est)

        return {
            "gross_incremental_conversions": gross_incremental,
            "estimated_cannibalized": cannibalized_est,
            "net_incremental_conversions": net_incremental,
            "cannibalization_rate_pct": round(
                cannibalized_est / max(gross_incremental, 1) * 100, 1
            ),
            "organic_conversion_rate": round(organic_rate, 4),
            "total_pa_converters": total_converters,
            "dual_exposed_converters": dual_exposed,
            "dual_exposure_rate_pct": round(
                dual_exposed / max(total_converters, 1) * 100, 1
            ),
        }

    def temporal_overlap_pattern(self) -> pd.DataFrame:
        """Analyze timing: does the other intent typically precede or follow PA?"""
        return (
            self.df.groupby("other_intent")
            .agg({
                "avg_hours_before_conversion": "mean",
                "other_sent_first_count": "sum",
                "dual_exposed_converters": "sum",
            })
            .assign(
                other_first_pct=lambda x: (
                    x["other_sent_first_count"] / x["dual_exposed_converters"].clip(lower=1) * 100
                ).round(1)
            )
            .reset_index()
        )

    def generate_report(self, gross_lift: float = 0.015, gross_lift_users: int = 1900000) -> str:
        """Generate cannibalization analysis report."""
        lines = ["# Cannibalization Analysis — Cross-Intent Overlap\n"]

        overlap = self.overlap_summary()
        lines.append("## 1. Cross-Intent Overlap Among PA Converters\n")
        lines.append(overlap[["other_intent", "dual_exposed_converters", "dual_exposure_pct",
                              "other_intent_click_rate", "cannibalization_risk"]].to_markdown(index=False))

        net = self.estimate_net_incrementality(gross_lift, gross_lift_users)
        lines.append("\n## 2. Net Incrementality Estimate\n")
        lines.append(f"- Gross incremental conversions: **{net['gross_incremental_conversions']:,}**")
        lines.append(f"- Estimated cannibalized: **{net['estimated_cannibalized']:,}**")
        lines.append(f"- Net incremental: **{net['net_incremental_conversions']:,}**")
        lines.append(f"- Cannibalization rate: **{net['cannibalization_rate_pct']}%**")
        lines.append(f"- Organic baseline rate: **{net['organic_conversion_rate']:.2%}**")

        temporal = self.temporal_overlap_pattern()
        lines.append("\n## 3. Temporal Overlap — Who Messages First?\n")
        lines.append(temporal.to_markdown(index=False))

        lines.append("\n## 4. Interpretation\n")
        lines.append(f"Of the {net['gross_incremental_conversions']:,} gross incremental conversions, "
                     f"approximately {net['cannibalization_rate_pct']}% may be attributed to cannibalization "
                     f"from overlapping intents. The net incremental value is "
                     f"{net['net_incremental_conversions']:,} conversions.")

        return "\n".join(lines)
