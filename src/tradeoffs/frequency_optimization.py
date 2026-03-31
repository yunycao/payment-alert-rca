"""Frequency optimization: finding the messaging sweet spot.

Models the relationship between message frequency and conversion/engagement,
accounting for the fatigue penalty. The core tradeoff:

- More messages → more short-term conversions (positive)
- More messages → higher fatigue/unsubscribe risk (negative)
- Optimal frequency balances these competing effects

Uses a response curve model with a fatigue penalty to find the
frequency that maximizes health-adjusted return.
"""

import pandas as pd
import numpy as np
from scipy.optimize import minimize_scalar
from typing import Optional


class FrequencyOptimizer:
    """Finds optimal messaging frequency balancing conversion and fatigue."""

    def __init__(self, data_path: str = "data/staging/funnel_data.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            self._df["report_date"] = pd.to_datetime(self._df["report_date"])
        return self._df

    def observed_frequency_response(self) -> pd.DataFrame:
        """Compute conversion rate at each observed messaging frequency level.

        Uses weekly user-level frequency as the grouping variable.
        """
        # Approximate: use daily eligible vs targeted ratio as frequency proxy
        weekly = self.df.groupby(pd.Grouper(key="report_date", freq="W")).agg({
            "eligible_users": "sum",
            "sent_users": "sum",
            "converted_users": "sum",
            "total_revenue": "sum",
        }).reset_index()

        weekly["messages_per_eligible"] = (weekly["sent_users"] / weekly["eligible_users"].clip(lower=1)).round(2)
        weekly["cvr"] = (weekly["converted_users"] / weekly["sent_users"].clip(lower=1)).round(4)
        weekly["revenue_per_message"] = (weekly["total_revenue"] / weekly["sent_users"].clip(lower=1)).round(4)

        return weekly

    def fit_response_curve(
        self,
        fatigue_penalty_weight: float = 0.3,
    ) -> dict:
        """Fit a response curve model and find optimal frequency.

        Model: net_value(f) = a * log(1 + b*f) - fatigue_penalty * f^2
        where f = messages per user per week

        The log term captures diminishing returns.
        The quadratic penalty captures fatigue.
        """
        weekly = self.observed_frequency_response()
        if len(weekly) < 4:
            return {"error": "Insufficient data for response curve fitting"}

        freq = weekly["messages_per_eligible"].values
        cvr = weekly["cvr"].values
        revenue = weekly["revenue_per_message"].values

        # Estimate parameters from observed data
        avg_revenue = np.mean(revenue)
        avg_freq = np.mean(freq)
        avg_cvr = np.mean(cvr)

        # Response function: revenue_per_msg(f) = a * log(1 + b*f)
        # Fatigue penalty: penalty(f) = c * f^2
        a = avg_revenue * 1.5
        b = 1.0 / avg_freq if avg_freq > 0 else 1.0
        c = fatigue_penalty_weight * avg_revenue / (avg_freq ** 2) if avg_freq > 0 else 0.01

        def net_value(f):
            if f <= 0:
                return 0
            return -(a * np.log(1 + b * f) - c * f ** 2)  # Negative for minimization

        # Find optimal frequency
        result = minimize_scalar(net_value, bounds=(0.1, 10.0), method="bounded")
        optimal_freq = result.x

        # Compute response at various frequencies
        freq_range = np.arange(0.5, 8.5, 0.5)
        curve = pd.DataFrame({
            "frequency": freq_range,
            "gross_response": [a * np.log(1 + b * f) for f in freq_range],
            "fatigue_penalty": [c * f ** 2 for f in freq_range],
            "net_value": [a * np.log(1 + b * f) - c * f ** 2 for f in freq_range],
        })
        curve["marginal_value"] = curve["net_value"].diff()
        curve["is_optimal"] = np.isclose(curve["frequency"], round(optimal_freq * 2) / 2, atol=0.25)

        return {
            "optimal_frequency": round(optimal_freq, 1),
            "current_avg_frequency": round(avg_freq, 2),
            "frequency_gap": round(optimal_freq - avg_freq, 2),
            "direction": "increase" if optimal_freq > avg_freq else "decrease",
            "model_params": {"a": round(a, 4), "b": round(b, 4), "c": round(c, 4)},
            "fatigue_penalty_weight": fatigue_penalty_weight,
            "response_curve": curve,
        }

    def segment_optimal_frequency(self) -> pd.DataFrame:
        """Find optimal frequency per user segment.

        Different segments have different fatigue thresholds:
        - Active users tolerate more messages
        - Dormant users are easily fatigued
        """
        results = []
        for segment in self.df["segment"].unique():
            seg_data = self.df[self.df["segment"] == segment]
            weekly = seg_data.groupby(pd.Grouper(key="report_date", freq="W")).agg({
                "eligible_users": "sum",
                "sent_users": "sum",
                "converted_users": "sum",
            }).reset_index()

            avg_freq = (weekly["sent_users"] / weekly["eligible_users"].clip(lower=1)).mean()
            avg_cvr = (weekly["converted_users"] / weekly["sent_users"].clip(lower=1)).mean()

            # Heuristic: dormant users have lower optimal frequency
            fatigue_multiplier = {
                "active": 0.8,
                "new": 1.0,
                "dormant": 1.8,
                "at_risk": 1.4,
            }.get(segment, 1.0)

            results.append({
                "segment": segment,
                "current_avg_frequency": round(avg_freq, 2),
                "current_cvr": round(avg_cvr, 4),
                "fatigue_sensitivity": fatigue_multiplier,
                "estimated_optimal_freq": round(avg_freq / fatigue_multiplier, 2),
                "recommended_change": "decrease" if fatigue_multiplier > 1.0 else "increase",
            })

        return pd.DataFrame(results)

    def frequency_tradeoff_matrix(self) -> pd.DataFrame:
        """Show the tradeoff between frequency, conversion, and fatigue at each level."""
        curve_result = self.fit_response_curve()
        if "error" in curve_result:
            return pd.DataFrame()

        curve = curve_result["response_curve"]
        curve["roi_efficiency"] = (curve["net_value"] / curve["frequency"]).round(4)
        curve["fatigue_pct_of_gross"] = (curve["fatigue_penalty"] / curve["gross_response"].clip(lower=0.001) * 100).round(1)

        return curve

    def generate_report(self) -> str:
        """Generate frequency optimization report."""
        lines = ["# Frequency Optimization — Conversion vs Fatigue Tradeoff\n"]

        observed = self.observed_frequency_response()
        lines.append("## 1. Observed Frequency-Response Data\n")
        lines.append(observed[["report_date", "messages_per_eligible", "cvr",
                              "revenue_per_message"]].tail(12).to_markdown(index=False))

        curve_result = self.fit_response_curve()
        if "error" not in curve_result:
            lines.append(f"\n## 2. Optimal Frequency\n")
            lines.append(f"- **Optimal: {curve_result['optimal_frequency']} messages/user/week**")
            lines.append(f"- Current average: {curve_result['current_avg_frequency']}")
            lines.append(f"- Recommendation: **{curve_result['direction']}** by {abs(curve_result['frequency_gap']):.1f}")

            tradeoff = self.frequency_tradeoff_matrix()
            lines.append("\n## 3. Frequency-Value Tradeoff Matrix\n")
            lines.append(tradeoff[["frequency", "gross_response", "fatigue_penalty",
                                  "net_value", "fatigue_pct_of_gross"]].to_markdown(index=False))

        seg_freq = self.segment_optimal_frequency()
        lines.append("\n## 4. Segment-Level Frequency Recommendations\n")
        lines.append(seg_freq.to_markdown(index=False))

        return "\n".join(lines)
