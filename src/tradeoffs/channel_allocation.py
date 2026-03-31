"""Channel allocation efficiency analysis.

Models the diminishing returns curve for each channel and finds the
efficient frontier — the allocation that maximizes conversions per
dollar spent on messaging.

Key tradeoff: cheap channels (in-app) have low marginal cost but also
lower engagement. Expensive channels (email) have higher engagement
but faster saturation.
"""

import pandas as pd
import numpy as np
from scipy.optimize import minimize_scalar
from typing import Optional


class ChannelAllocationAnalyzer:
    """Finds the efficiency frontier for cross-channel messaging allocation."""

    COST_PER_MESSAGE = {
        "email": 0.002,
        "push": 0.0005,
        "in_app": 0.0001,
    }

    def __init__(self, data_path: str = "data/staging/funnel_data.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            self._df["report_date"] = pd.to_datetime(self._df["report_date"])
        return self._df

    def channel_unit_economics(self) -> pd.DataFrame:
        """Compute per-channel unit economics: cost, conversion, ROI."""
        metrics = (
            self.df.groupby("channel")
            .agg({
                "sent_users": "sum",
                "delivered_users": "sum",
                "opened_users": "sum",
                "clicked_users": "sum",
                "converted_users": "sum",
                "total_revenue": "sum",
            })
            .reset_index()
        )

        metrics["cost_per_msg"] = metrics["channel"].map(self.COST_PER_MESSAGE)
        metrics["total_cost"] = metrics["sent_users"] * metrics["cost_per_msg"]
        metrics["delivery_rate"] = (metrics["delivered_users"] / metrics["sent_users"]).round(4)
        metrics["cvr"] = (metrics["converted_users"] / metrics["delivered_users"]).round(4)
        metrics["cost_per_conversion"] = (metrics["total_cost"] / metrics["converted_users"].clip(lower=1)).round(4)
        metrics["revenue_per_conversion"] = (metrics["total_revenue"] / metrics["converted_users"].clip(lower=1)).round(2)
        metrics["roas"] = (metrics["total_revenue"] / metrics["total_cost"].clip(lower=0.01)).round(1)
        metrics["profit"] = (metrics["total_revenue"] - metrics["total_cost"]).round(2)
        metrics["marginal_profit_per_msg"] = (metrics["profit"] / metrics["sent_users"]).round(4)

        return metrics

    def diminishing_returns_model(self, channel: str, max_volume_multiplier: float = 3.0) -> pd.DataFrame:
        """Model the diminishing returns curve for a channel.

        Fits a log-concave response function: conversions = a * log(1 + b * volume)
        Uses daily data to estimate how conversion rate decays with volume.
        """
        ch_data = self.df[self.df["channel"] == channel].sort_values("report_date")
        if len(ch_data) < 7:
            return pd.DataFrame()

        # Use daily variation in volume to estimate marginal returns
        ch_data = ch_data.groupby("report_date").agg({
            "sent_users": "sum",
            "converted_users": "sum",
        }).reset_index()

        ch_data["cvr"] = ch_data["converted_users"] / ch_data["sent_users"].clip(lower=1)
        avg_vol = ch_data["sent_users"].mean()

        # Simulate response at different volume levels
        volume_range = np.linspace(avg_vol * 0.1, avg_vol * max_volume_multiplier, 20)

        # Log-concave model: CVR decays as log(volume) increases
        observed_correlation = ch_data["sent_users"].corr(ch_data["cvr"])
        base_cvr = ch_data["cvr"].mean()
        decay_rate = max(0.1, -observed_correlation * 0.5)

        simulated = pd.DataFrame({
            "volume": volume_range.astype(int),
            "estimated_cvr": [
                base_cvr * (1 - decay_rate * np.log(v / avg_vol))
                for v in volume_range
            ],
        })
        simulated["estimated_cvr"] = simulated["estimated_cvr"].clip(lower=0)
        simulated["estimated_conversions"] = (simulated["volume"] * simulated["estimated_cvr"]).astype(int)
        simulated["marginal_conversion"] = simulated["estimated_conversions"].diff().fillna(0)
        simulated["channel"] = channel
        simulated["cost"] = simulated["volume"] * self.COST_PER_MESSAGE.get(channel, 0.001)

        return simulated

    def efficiency_frontier(self) -> pd.DataFrame:
        """Compare channels on the efficiency frontier.

        For each budget level, find the optimal channel mix.
        """
        econ = self.channel_unit_economics()

        # Simple frontier: rank by marginal profit, fill budget greedily
        frontier = econ[["channel", "marginal_profit_per_msg", "roas", "cvr",
                        "cost_per_conversion", "sent_users", "profit"]].copy()
        frontier = frontier.sort_values("marginal_profit_per_msg", ascending=False)
        frontier["cumulative_profit"] = frontier["profit"].cumsum()
        frontier["allocation_rank"] = range(1, len(frontier) + 1)

        return frontier

    def optimal_budget_split(self, total_budget: float) -> dict:
        """Given a fixed budget, find the optimal channel allocation."""
        econ = self.channel_unit_economics()
        channels = []

        for _, row in econ.iterrows():
            channels.append({
                "channel": row["channel"],
                "cvr": row["cvr"],
                "cost_per_msg": row["cost_per_msg"],
                "marginal_profit": row["marginal_profit_per_msg"],
            })

        # Sort by marginal profit and fill greedily
        channels.sort(key=lambda x: x["marginal_profit"], reverse=True)
        remaining_budget = total_budget
        allocation = {}

        for ch in channels:
            if remaining_budget <= 0:
                allocation[ch["channel"]] = {"budget": 0, "messages": 0, "estimated_conversions": 0}
                continue

            # Allocate proportionally to marginal efficiency
            channel_budget = min(remaining_budget, total_budget * 0.6)  # Cap at 60% per channel
            n_messages = int(channel_budget / ch["cost_per_msg"])
            est_conversions = int(n_messages * ch["cvr"])

            allocation[ch["channel"]] = {
                "budget": round(channel_budget, 2),
                "messages": n_messages,
                "estimated_conversions": est_conversions,
                "cost_per_conversion": round(channel_budget / max(est_conversions, 1), 4),
            }
            remaining_budget -= channel_budget

        total_conv = sum(a["estimated_conversions"] for a in allocation.values())
        return {
            "total_budget": total_budget,
            "allocation": allocation,
            "total_estimated_conversions": total_conv,
            "blended_cost_per_conversion": round(total_budget / max(total_conv, 1), 4),
        }

    def generate_report(self) -> str:
        """Generate channel allocation report."""
        lines = ["# Channel Allocation Efficiency Analysis\n"]

        econ = self.channel_unit_economics()
        lines.append("## 1. Unit Economics by Channel\n")
        lines.append(econ[["channel", "sent_users", "cvr", "cost_per_conversion",
                          "roas", "marginal_profit_per_msg"]].to_markdown(index=False))

        frontier = self.efficiency_frontier()
        lines.append("\n## 2. Efficiency Frontier\n")
        lines.append(frontier.to_markdown(index=False))

        return "\n".join(lines)
