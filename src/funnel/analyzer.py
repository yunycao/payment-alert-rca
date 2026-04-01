"""Full funnel analysis engine for payment alert intent."""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional


class FunnelAnalyzer:
    """Analyzes the full messaging funnel from eligibility to conversion."""

    STAGE_COLUMNS = {
        "eligible": "eligible_users",
        "targeted": "targeted_users",
        "suppressed": "suppressed_users",
        "sent": "sent_users",
        "delivered": "delivered_users",
        "opened": "opened_users",
        "clicked": "clicked_users",
        "converted": "converted_users",
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

    def funnel_summary(self, channel: str = "all") -> pd.DataFrame:
        """Compute aggregate funnel metrics by channel."""
        data = self.df if channel == "all" else self.df[self.df["channel"] == channel]

        stage_totals = data[list(self.STAGE_COLUMNS.values())].sum()
        eligible = stage_totals["eligible_users"]

        summary = pd.DataFrame({
            "stage": list(self.STAGE_COLUMNS.keys()),
            "users": stage_totals.values,
            "pct_of_eligible": (stage_totals.values / eligible * 100).round(2) if eligible > 0 else 0,
        })

        # Add step conversion rates
        summary["step_conversion"] = (
            summary["users"] / summary["users"].shift(1) * 100
        ).round(2)
        summary.loc[0, "step_conversion"] = 100.0

        return summary

    # Business outcome columns (spend + on-time payment rate)
    OUTCOME_COLUMNS = [
        "total_spend", "avg_spend_per_user", "users_with_payment_due",
        "on_time_payment_rate", "on_time_users", "late_users", "missed_users",
        "avg_days_relative_to_due",
    ]

    def daily_metrics(self) -> pd.DataFrame:
        """Compute daily funnel metrics with derived rates."""
        agg_cols = {col: "sum" for col in self.STAGE_COLUMNS.values()}
        # Add business outcome aggregations if present
        if "total_spend" in self.df.columns:
            agg_cols["total_spend"] = "sum"
            agg_cols["users_with_payment_due"] = "sum"
            agg_cols["on_time_users"] = "sum"
            agg_cols["late_users"] = "sum"
            agg_cols["missed_users"] = "sum"

        daily = self.df.groupby(["report_date", "channel"]).agg(agg_cols).reset_index()

        # Derived rates
        daily["targeting_rate"] = (daily["targeted_users"] / daily["eligible_users"] * 100).round(2)
        daily["delivery_rate"] = (daily["delivered_users"] / daily["sent_users"] * 100).round(2)
        daily["open_rate"] = (daily["opened_users"] / daily["delivered_users"] * 100).round(2)
        daily["ctr"] = (daily["clicked_users"] / daily["opened_users"] * 100).round(2)
        daily["conversion_rate"] = (daily["converted_users"] / daily["delivered_users"] * 100).round(2)
        daily["suppression_rate"] = (daily["suppressed_users"] / daily["eligible_users"] * 100).round(2)

        # Business outcome rates
        if "users_with_payment_due" in daily.columns:
            daily["on_time_payment_rate"] = (
                daily["on_time_users"] / daily["users_with_payment_due"] * 100
            ).round(2)
            daily["avg_spend_per_user"] = (
                daily["total_spend"] / daily["eligible_users"]
            ).round(2)
            daily["missed_payment_rate"] = (
                daily["missed_users"] / daily["users_with_payment_due"] * 100
            ).round(2)

        return daily

    def business_outcome_summary(self) -> pd.DataFrame:
        """Summarize spend and on-time payment rate by channel × segment."""
        if "total_spend" not in self.df.columns:
            return pd.DataFrame({"error": ["Business outcome columns not present in data"]})

        return self.df.groupby(["channel", "segment"]).agg({
            "eligible_users": "sum",
            "total_spend": "sum",
            "users_with_payment_due": "sum",
            "on_time_users": "sum",
            "late_users": "sum",
            "missed_users": "sum",
        }).assign(
            avg_spend_per_user=lambda x: (x["total_spend"] / x["eligible_users"]).round(2),
            on_time_rate=lambda x: (x["on_time_users"] / x["users_with_payment_due"] * 100).round(2),
            missed_rate=lambda x: (x["missed_users"] / x["users_with_payment_due"] * 100).round(2),
        ).reset_index()

    def detect_outcome_drops(
        self, metric: str = "on_time_payment_rate", window: int = 7, threshold_pct: float = 5.0
    ) -> pd.DataFrame:
        """Detect week-over-week drops in business outcome metrics.

        Compares the most recent `window` days to the prior `window` days.
        Returns channels/segments where the drop exceeds threshold_pct.
        """
        daily = self.daily_metrics()
        if metric not in daily.columns:
            return pd.DataFrame()

        daily = daily.sort_values("report_date")
        cutoff = daily["report_date"].max() - pd.Timedelta(days=window)
        prior_cutoff = cutoff - pd.Timedelta(days=window)

        current = daily[daily["report_date"] > cutoff]
        baseline = daily[(daily["report_date"] > prior_cutoff) & (daily["report_date"] <= cutoff)]

        results = []
        for channel in daily["channel"].unique():
            c = current[current["channel"] == channel][metric].mean()
            b = baseline[baseline["channel"] == channel][metric].mean()
            if b > 0:
                change_pct = (c - b) / b * 100
                if change_pct < -threshold_pct:
                    results.append({
                        "channel": channel,
                        "metric": metric,
                        "baseline_value": round(b, 4),
                        "current_value": round(c, 4),
                        "change_pct": round(change_pct, 2),
                        "severity": "critical" if change_pct < -2 * threshold_pct else "warning",
                    })

        return pd.DataFrame(results)

    def suppression_analysis(self) -> pd.DataFrame:
        """Break down suppression reasons by channel and segment."""
        suppression_cols = [
            "suppressed_frequency_cap", "suppressed_priority",
            "suppressed_fatigue", "suppressed_holdout", "suppressed_competitor",
        ]
        return self.df.groupby(["channel", "segment"])[
            ["suppressed_users"] + suppression_cols
        ].sum().reset_index()

    def segment_performance(self) -> pd.DataFrame:
        """Compare funnel performance across user segments."""
        return self.df.groupby("segment").agg({
            "eligible_users": "sum",
            "targeted_users": "sum",
            "delivered_users": "sum",
            "opened_users": "sum",
            "clicked_users": "sum",
            "converted_users": "sum",
            "total_revenue": "sum",
            "avg_propensity_score": "mean",
        }).reset_index()

    def scoring_diagnostics(self) -> pd.DataFrame:
        """Analyze propensity score distributions and default score rates."""
        return self.df.groupby(["report_date", "channel"]).agg({
            "avg_propensity_score": "mean",
            "median_propensity_score": "mean",
            "default_score_users": "sum",
            "eligible_users": "sum",
            "avg_model_latency_ms": "mean",
        }).assign(
            default_score_rate=lambda x: (x["default_score_users"] / x["eligible_users"] * 100).round(2)
        ).reset_index()

    def find_anomalous_days(self, metric: str = "conversion_rate", z_threshold: float = 2.0) -> pd.DataFrame:
        """Identify days where a metric deviates significantly from its rolling average."""
        daily = self.daily_metrics()
        result = []

        for channel in daily["channel"].unique():
            ch_data = daily[daily["channel"] == channel].sort_values("report_date").copy()
            ch_data["rolling_mean"] = ch_data[metric].rolling(7, min_periods=3).mean()
            ch_data["rolling_std"] = ch_data[metric].rolling(7, min_periods=3).std()
            ch_data["z_score"] = (ch_data[metric] - ch_data["rolling_mean"]) / ch_data["rolling_std"]
            anomalies = ch_data[ch_data["z_score"].abs() > z_threshold]
            result.append(anomalies)

        return pd.concat(result, ignore_index=True) if result else pd.DataFrame()

    def generate_report(self) -> str:
        """Generate a text summary of the funnel analysis."""
        lines = ["# Payment Alert — Funnel Analysis Report\n"]

        for channel in ["email", "push", "in_app", "all"]:
            summary = self.funnel_summary(channel)
            lines.append(f"\n## {channel.replace('_', ' ').title()} Channel\n")
            lines.append(summary.to_markdown(index=False))
            lines.append("")

        # Business outcomes
        if "total_spend" in self.df.columns:
            lines.append("\n## Business Outcomes: Spend & On-Time Payment Rate\n")
            outcomes = self.business_outcome_summary()
            lines.append(outcomes.to_markdown(index=False))

            # Detect drops
            for metric in ["on_time_payment_rate", "avg_spend_per_user"]:
                drops = self.detect_outcome_drops(metric=metric)
                if len(drops) > 0:
                    lines.append(f"\n### ⚠️ Drop Detected: {metric}\n")
                    lines.append(drops.to_markdown(index=False))

        anomalies = self.find_anomalous_days()
        if len(anomalies) > 0:
            lines.append("\n## Anomalous Days Detected\n")
            lines.append(anomalies[["report_date", "channel", "conversion_rate", "z_score"]].to_markdown(index=False))

        return "\n".join(lines)
