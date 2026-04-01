"""Metric decomposition engine for root cause analysis.

Decomposes a metric change (e.g., spend drop, on-time rate decline) into
additive components using mix-shift / rate-change decomposition:

  ΔMetric = Σ_d [ (Δmix_d × baseline_rate_d) + (Δrate_d × current_mix_d) ]
          = MIX EFFECT + RATE EFFECT

This isolates whether the metric dropped because:
  (1) The population mix shifted (different users entering the funnel)
  (2) Within-group rates changed (same users, worse outcomes)
"""

import pandas as pd
import numpy as np
from typing import Optional


class MetricDecomposer:
    """Decomposes metric drops into mix-shift vs rate-change contributions."""

    def __init__(self, data_path: str = "data/staging/rca_decomposition.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
        return self._df

    def decompose_by_dimension(
        self,
        dimension: str,
        metric: str = "avg_spend",
        volume_col: str = "n_users",
    ) -> pd.DataFrame:
        """Decompose metric change along a single dimension.

        For each level of `dimension`, computes:
        - mix_shift: how much the population share changed
        - rate_change: how much the within-group metric changed
        - mix_effect: contribution to overall change from mix shift
        - rate_effect: contribution to overall change from rate change
        - total_contribution: mix_effect + rate_effect

        Returns sorted by |total_contribution| descending.
        """
        data = self.df[self.df["dimension"] == dimension].copy()
        if data.empty:
            return pd.DataFrame()

        # Compute totals
        baseline_total = data["baseline_users"].sum()
        current_total = data["current_users"].sum()

        if baseline_total == 0 or current_total == 0:
            return pd.DataFrame()

        # For spend: use total_spend columns; for rate: use rate columns directly
        if metric == "avg_spend":
            baseline_metric_col = "baseline_avg_spend"
            current_metric_col = "current_avg_spend"
        elif metric == "on_time_rate":
            baseline_metric_col = "baseline_on_time_rate"
            current_metric_col = "current_on_time_rate"
        else:
            baseline_metric_col = f"baseline_{metric}"
            current_metric_col = f"current_{metric}"

        results = []
        for _, row in data.iterrows():
            baseline_mix = row["baseline_users"] / baseline_total
            current_mix = row["current_users"] / current_total
            mix_shift = current_mix - baseline_mix

            baseline_rate = row[baseline_metric_col]
            current_rate = row[current_metric_col]
            rate_change = current_rate - baseline_rate

            # Shapley-style additive decomposition
            mix_effect = mix_shift * baseline_rate
            rate_effect = rate_change * current_mix
            interaction = mix_shift * rate_change  # small, distributed equally
            total = mix_effect + rate_effect + interaction

            results.append({
                "dimension": dimension,
                "dimension_value": row["dimension_value"],
                "baseline_users": int(row["baseline_users"]),
                "current_users": int(row["current_users"]),
                "baseline_mix_pct": round(baseline_mix * 100, 2),
                "current_mix_pct": round(current_mix * 100, 2),
                "mix_shift_pp": round(mix_shift * 100, 2),
                f"baseline_{metric}": round(baseline_rate, 4),
                f"current_{metric}": round(current_rate, 4),
                "rate_change": round(rate_change, 4),
                "mix_effect": round(mix_effect, 6),
                "rate_effect": round(rate_effect, 6),
                "total_contribution": round(total, 6),
                "contribution_pct": 0,  # filled below
            })

        result_df = pd.DataFrame(results)
        total_change = result_df["total_contribution"].sum()
        if total_change != 0:
            result_df["contribution_pct"] = (
                result_df["total_contribution"] / abs(total_change) * 100
            ).round(1)

        return result_df.sort_values("total_contribution", key=abs, ascending=False)

    def waterfall_decomposition(
        self, metric: str = "avg_spend"
    ) -> pd.DataFrame:
        """Run decomposition across all dimensions and rank top contributors.

        Returns a unified waterfall showing the largest contributors
        to the metric change across all decomposition dimensions.
        """
        dimensions = self.df["dimension"].unique()
        all_contributions = []

        for dim in dimensions:
            decomp = self.decompose_by_dimension(dim, metric)
            if len(decomp) > 0:
                # Keep the top contributors from each dimension
                top = decomp.head(5)
                all_contributions.append(top)

        if not all_contributions:
            return pd.DataFrame()

        combined = pd.concat(all_contributions, ignore_index=True)
        return combined.sort_values("total_contribution", key=abs, ascending=False).head(15)

    def identify_root_causes(
        self,
        metric: str = "avg_spend",
        min_contribution_pct: float = 5.0,
    ) -> list[dict]:
        """Identify and rank the top root causes for a metric drop.

        Returns a list of root cause hypotheses with evidence, sorted by impact.
        """
        waterfall = self.waterfall_decomposition(metric)
        if waterfall.empty:
            return []

        causes = []
        for _, row in waterfall.iterrows():
            if abs(row["contribution_pct"]) < min_contribution_pct:
                continue

            # Classify the type of root cause
            if abs(row["mix_shift_pp"]) > abs(row["rate_change"]) * 50:
                cause_type = "population_shift"
                explanation = (
                    f"Population mix of {row['dimension']}='{row['dimension_value']}' "
                    f"shifted by {row['mix_shift_pp']:+.1f}pp"
                )
            elif abs(row["rate_change"]) > 0.001:
                cause_type = "rate_degradation"
                explanation = (
                    f"Within-group {metric} for {row['dimension']}='{row['dimension_value']}' "
                    f"changed by {row['rate_change']:+.4f}"
                )
            else:
                cause_type = "mixed"
                explanation = (
                    f"Both mix shift ({row['mix_shift_pp']:+.1f}pp) and rate change "
                    f"({row['rate_change']:+.4f}) in {row['dimension']}='{row['dimension_value']}'"
                )

            causes.append({
                "rank": len(causes) + 1,
                "dimension": row["dimension"],
                "dimension_value": row["dimension_value"],
                "cause_type": cause_type,
                "explanation": explanation,
                "contribution_pct": row["contribution_pct"],
                "mix_effect": row["mix_effect"],
                "rate_effect": row["rate_effect"],
                "total_contribution": row["total_contribution"],
            })

        return causes

    def generate_report(self, metric: str = "avg_spend") -> str:
        """Generate markdown root cause analysis report."""
        lines = [f"# Root Cause Analysis: {metric.replace('_', ' ').title()} Drop\n"]

        # Overall change
        waterfall = self.waterfall_decomposition(metric)
        if waterfall.empty:
            lines.append("No decomposition data available.")
            return "\n".join(lines)

        total_change = waterfall["total_contribution"].sum()
        lines.append(f"**Total {metric} change: {total_change:+.4f}**\n")

        # Top root causes
        causes = self.identify_root_causes(metric)
        if causes:
            lines.append("## Top Root Causes\n")
            for cause in causes:
                severity = "🔴" if abs(cause["contribution_pct"]) > 20 else "🟡"
                lines.append(
                    f"{severity} **#{cause['rank']}** ({cause['contribution_pct']:+.1f}% of change): "
                    f"{cause['explanation']}"
                )
                lines.append(
                    f"   - Type: {cause['cause_type']} | "
                    f"Mix effect: {cause['mix_effect']:+.6f} | "
                    f"Rate effect: {cause['rate_effect']:+.6f}"
                )
                lines.append("")

        # Dimension-level detail
        for dim in self.df["dimension"].unique():
            decomp = self.decompose_by_dimension(dim, metric)
            if len(decomp) > 0:
                lines.append(f"\n## Decomposition by {dim}\n")
                display_cols = [
                    "dimension_value", "baseline_mix_pct", "current_mix_pct",
                    "mix_shift_pp", "rate_change", "total_contribution", "contribution_pct",
                ]
                lines.append(decomp[display_cols].to_markdown(index=False))

        return "\n".join(lines)
