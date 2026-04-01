"""Long-term value effects: does messaging help or hurt engagement over time?

Compares treatment vs holdout cohorts at 7d, 30d, 90d windows to detect:
- Short-term lift that decays (messaging accelerates but doesn't create)
- Long-term fatigue (messaging degrades engagement over time)
- Healthy lift with positive LTV trajectory
"""

import pandas as pd
import numpy as np
from scipy import stats
from typing import Optional


class LTVEffectsAnalyzer:
    """Measures long-term value and engagement health effects of messaging."""

    def __init__(self, data_path: str = "data/staging/ltv_effects.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            if "cohort_week" in self._df.columns:
                self._df["cohort_week"] = pd.to_datetime(self._df["cohort_week"])
        return self._df

    def ltv_lift_by_window(self) -> pd.DataFrame:
        """Compare LTV between treatment and holdout at each time window."""
        results = []
        for window in ["7d", "30d", "90d"]:
            rev_col = f"avg_ltv_{window}"
            t = self.df[self.df["holdout_group"] == "treatment"]
            h = self.df[self.df["holdout_group"] == "holdout"]

            t_mean = t[rev_col].mean()
            h_mean = h[rev_col].mean()
            lift = t_mean - h_mean
            rel_lift = lift / h_mean if h_mean > 0 else np.nan

            # Confidence interval
            if len(t) > 1 and len(h) > 1:
                t_stat, p_val = stats.ttest_ind(t[rev_col], h[rev_col], equal_var=False)
                se = np.sqrt(t[rev_col].std() ** 2 / len(t) + h[rev_col].std() ** 2 / len(h))
                ci_lo = lift - 1.96 * se
                ci_hi = lift + 1.96 * se
            else:
                p_val, ci_lo, ci_hi = np.nan, np.nan, np.nan

            results.append({
                "window": window,
                "treatment_ltv": round(t_mean, 2),
                "holdout_ltv": round(h_mean, 2),
                "absolute_lift": round(lift, 2),
                "relative_lift_pct": round(rel_lift * 100, 1) if not np.isnan(rel_lift) else np.nan,
                "ci_lower": round(ci_lo, 2) if not np.isnan(ci_lo) else np.nan,
                "ci_upper": round(ci_hi, 2) if not np.isnan(ci_hi) else np.nan,
                "p_value": round(p_val, 4) if not np.isnan(p_val) else np.nan,
            })

        return pd.DataFrame(results)

    def lift_decay_assessment(self) -> dict:
        """Determine if lift decays, sustains, or amplifies over time.

        Categories:
        - "amplifying": 90d relative lift > 30d > 7d (messaging creates lasting value)
        - "sustaining": lifts roughly equal across windows
        - "decaying": 7d lift > 30d > 90d (messaging only accelerates, doesn't create)
        - "harmful": 90d lift is negative (messaging destroys long-term value)
        """
        ltv = self.ltv_lift_by_window()
        lifts = dict(zip(ltv["window"], ltv["relative_lift_pct"]))

        l7 = lifts.get("7d", 0) or 0
        l30 = lifts.get("30d", 0) or 0
        l90 = lifts.get("90d", 0) or 0

        if l90 < 0:
            pattern = "harmful"
        elif l90 > l30 > l7:
            pattern = "amplifying"
        elif abs(l90 - l7) < 2.0:
            pattern = "sustaining"
        elif l7 > l30 > l90:
            pattern = "decaying"
        else:
            pattern = "mixed"

        return {
            "pattern": pattern,
            "lift_7d_pct": l7,
            "lift_30d_pct": l30,
            "lift_90d_pct": l90,
            "decay_rate_7d_to_90d": round((l90 - l7) / max(abs(l7), 0.01) * 100, 1),
        }

    def fatigue_indicators(self) -> pd.DataFrame:
        """Measure fatigue signals: unsubscribe, opt-out, app deletion rates."""
        result = []
        for signal in ["unsubscribe_rate", "opt_out_rate", "app_delete_rate"]:
            t = self.df[self.df["holdout_group"] == "treatment"]
            h = self.df[self.df["holdout_group"] == "holdout"]

            t_rate = t[signal].mean()
            h_rate = h[signal].mean()
            excess = t_rate - h_rate

            result.append({
                "signal": signal.replace("_rate", "").replace("_", " ").title(),
                "treatment_rate": round(t_rate * 100, 3),
                "holdout_rate": round(h_rate * 100, 3),
                "excess_rate_pp": round(excess * 100, 3),
                "relative_increase_pct": round(excess / max(h_rate, 0.0001) * 100, 1),
            })

        return pd.DataFrame(result)

    def engagement_retention_curve(self) -> pd.DataFrame:
        """Weekly cohort-level engagement retention: active days over time."""
        cohort_data = []
        for window in ["7d", "30d", "90d"]:
            col = f"avg_active_days_{window}"
            for group in ["treatment", "holdout"]:
                g = self.df[self.df["holdout_group"] == group]
                cohort_data.append({
                    "window": window,
                    "group": group,
                    "avg_active_days": round(g[col].mean(), 2),
                })
        return pd.DataFrame(cohort_data)

    def health_adjusted_ltv(self) -> pd.DataFrame:
        """LTV adjusted for negative externalities (fatigue penalty).

        health_adjusted_ltv = raw_ltv - (fatigue_rate × raw_ltv × penalty_weight)

        This penalizes messaging strategies that boost short-term LTV
        at the cost of user churn.
        """
        if "health_adjusted_ltv_90d" in self.df.columns:
            return (
                self.df.groupby(["holdout_group", "channel"])
                .agg({
                    "avg_ltv_90d": "mean",
                    "health_adjusted_ltv_90d": "mean",
                    "unsubscribe_rate": "mean",
                    "opt_out_rate": "mean",
                })
                .reset_index()
            )
        else:
            return pd.DataFrame()

    def spend_trajectory(self) -> pd.DataFrame:
        """Compare spend between treatment and holdout at each time window."""
        results = []
        for window in ["7d", "30d", "90d"]:
            col = f"avg_spend_{window}"
            if col not in self.df.columns:
                continue
            t = self.df[self.df["holdout_group"] == "treatment"]
            h = self.df[self.df["holdout_group"] == "holdout"]

            t_mean = t[col].mean()
            h_mean = h[col].mean()
            lift = t_mean - h_mean
            rel_lift = lift / h_mean if h_mean > 0 else np.nan

            results.append({
                "window": window,
                "treatment_spend": round(t_mean, 2),
                "holdout_spend": round(h_mean, 2),
                "spend_lift": round(lift, 2),
                "relative_lift_pct": round(rel_lift * 100, 1) if not np.isnan(rel_lift) else np.nan,
            })
        return pd.DataFrame(results)

    def on_time_rate_trajectory(self) -> pd.DataFrame:
        """Compare on-time payment rate between treatment and holdout."""
        results = []
        for window in ["7d", "30d", "90d"]:
            col = f"avg_on_time_rate_{window}"
            if col not in self.df.columns:
                continue
            t = self.df[self.df["holdout_group"] == "treatment"]
            h = self.df[self.df["holdout_group"] == "holdout"]

            t_mean = t[col].mean()
            h_mean = h[col].mean()
            lift = t_mean - h_mean

            results.append({
                "window": window,
                "treatment_on_time_rate": round(t_mean, 4),
                "holdout_on_time_rate": round(h_mean, 4),
                "lift_pp": round(lift * 100, 2),
            })
        return pd.DataFrame(results)

    def outcome_decay_assessment(self, metric: str = "spend") -> dict:
        """Classify whether spend or on-time rate lift decays over time."""
        if metric == "spend":
            traj = self.spend_trajectory()
            lift_col = "relative_lift_pct"
        else:
            traj = self.on_time_rate_trajectory()
            lift_col = "lift_pp"

        if traj.empty:
            return {"pattern": "no_data", "metric": metric}

        lifts = dict(zip(traj["window"], traj[lift_col]))
        l7 = lifts.get("7d", 0) or 0
        l30 = lifts.get("30d", 0) or 0
        l90 = lifts.get("90d", 0) or 0

        if l90 < 0:
            pattern = "harmful"
        elif l90 > l30 > l7:
            pattern = "amplifying"
        elif abs(l90 - l7) < 2.0:
            pattern = "sustaining"
        elif l7 > l30 > l90:
            pattern = "decaying"
        else:
            pattern = "mixed"

        return {
            "metric": metric,
            "pattern": pattern,
            "lift_7d": l7,
            "lift_30d": l30,
            "lift_90d": l90,
            "decay_rate_7d_to_90d": round((l90 - l7) / max(abs(l7), 0.01) * 100, 1),
        }

    def generate_report(self) -> str:
        """Generate comprehensive LTV effects report."""
        lines = ["# Long-Term Value Effects Analysis\n"]

        ltv = self.ltv_lift_by_window()
        lines.append("## 1. LTV Lift by Measurement Window\n")
        lines.append(ltv.to_markdown(index=False))

        decay = self.lift_decay_assessment()
        lines.append(f"\n## 2. Lift Trajectory: **{decay['pattern'].upper()}**\n")
        lines.append(f"- 7-day lift: {decay['lift_7d_pct']}%")
        lines.append(f"- 30-day lift: {decay['lift_30d_pct']}%")
        lines.append(f"- 90-day lift: {decay['lift_90d_pct']}%")
        lines.append(f"- Decay rate (7d→90d): {decay['decay_rate_7d_to_90d']}%")

        fatigue = self.fatigue_indicators()
        lines.append("\n## 3. Fatigue Signals (Treatment vs Holdout)\n")
        lines.append(fatigue.to_markdown(index=False))

        retention = self.engagement_retention_curve()
        lines.append("\n## 4. Engagement Retention\n")
        lines.append(retention.to_markdown(index=False))

        # Spend trajectory
        spend_traj = self.spend_trajectory()
        if not spend_traj.empty:
            lines.append("\n## 5. Spend Trajectory (Treatment vs Holdout)\n")
            lines.append(spend_traj.to_markdown(index=False))
            spend_decay = self.outcome_decay_assessment("spend")
            lines.append(f"\n**Spend pattern: {spend_decay['pattern'].upper()}**")

        # On-time rate trajectory
        otp_traj = self.on_time_rate_trajectory()
        if not otp_traj.empty:
            lines.append("\n## 6. On-Time Payment Rate Trajectory\n")
            lines.append(otp_traj.to_markdown(index=False))
            otp_decay = self.outcome_decay_assessment("on_time_rate")
            lines.append(f"\n**On-time rate pattern: {otp_decay['pattern'].upper()}**")

        return "\n".join(lines)
