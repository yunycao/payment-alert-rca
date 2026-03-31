"""Causal incrementality measurement using holdout-based inference.

Estimates the true causal effect of messaging by comparing randomized
treatment vs holdout groups. Supports:
- Simple difference in means with confidence intervals
- Difference-in-Differences (DiD) with pre-period adjustment
- Stratified estimation by propensity score quintile
- Effect heterogeneity across segments and channels
"""

import pandas as pd
import numpy as np
from scipy import stats
from typing import Optional


class IncrementalityAnalyzer:
    """Measures causal lift of payment alert messaging using holdout groups."""

    def __init__(self, data_path: str = "data/staging/incrementality.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
            if "assignment_date" in self._df.columns:
                self._df["assignment_date"] = pd.to_datetime(self._df["assignment_date"])
        return self._df

    def balance_check(self) -> pd.DataFrame:
        """Verify that treatment and holdout groups are balanced on pre-period covariates.

        Imbalanced groups invalidate causal claims. Returns standardized mean
        differences (SMD) — flag any covariate with |SMD| > 0.1.
        """
        covariates = ["avg_pre_payments", "avg_pre_revenue", "avg_pre_sessions", "avg_propensity_score"]
        results = []

        for cov in covariates:
            treatment = self.df[self.df["holdout_group"] == "treatment"]
            holdout = self.df[self.df["holdout_group"] == "holdout"]

            t_mean = treatment[cov].mean()
            h_mean = holdout[cov].mean()
            pooled_std = np.sqrt((treatment[cov].std() ** 2 + holdout[cov].std() ** 2) / 2)
            smd = (t_mean - h_mean) / pooled_std if pooled_std > 0 else 0

            results.append({
                "covariate": cov,
                "treatment_mean": round(t_mean, 4),
                "holdout_mean": round(h_mean, 4),
                "smd": round(smd, 4),
                "balanced": abs(smd) < 0.1,
            })

        return pd.DataFrame(results)

    def estimate_lift(
        self,
        metric: str = "conversion_rate",
        confidence: float = 0.95,
        group_by: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Estimate causal lift with confidence intervals.

        Uses Welch's t-test for difference in means between treatment
        and holdout. Returns point estimate, CI, p-value, and effect size (Cohen's h).
        """
        if group_by is None:
            group_by = []

        grouping = group_by if group_by else ["_all"]
        if not group_by:
            df = self.df.copy()
            df["_all"] = "overall"
        else:
            df = self.df.copy()

        results = []
        for name, grp in df.groupby(grouping):
            t = grp[grp["holdout_group"] == "treatment"]
            h = grp[grp["holdout_group"] == "holdout"]

            if len(t) == 0 or len(h) == 0:
                continue

            t_mean = t[metric].mean()
            h_mean = h[metric].mean()
            t_n = t["n_users"].sum()
            h_n = h["n_users"].sum()

            lift = t_mean - h_mean
            relative_lift = lift / h_mean if h_mean > 0 else np.nan

            # Standard error of difference
            t_se = t[metric].std() / np.sqrt(len(t)) if len(t) > 1 else 0
            h_se = h[metric].std() / np.sqrt(len(h)) if len(h) > 1 else 0
            se_diff = np.sqrt(t_se ** 2 + h_se ** 2)

            z = stats.norm.ppf(1 - (1 - confidence) / 2)
            ci_lower = lift - z * se_diff
            ci_upper = lift + z * se_diff

            # Two-sample t-test
            if len(t) > 1 and len(h) > 1:
                t_stat, p_value = stats.ttest_ind(t[metric], h[metric], equal_var=False)
            else:
                t_stat, p_value = np.nan, np.nan

            # Effect size: Cohen's h for proportions
            if 0 < t_mean < 1 and 0 < h_mean < 1:
                cohens_h = 2 * (np.arcsin(np.sqrt(t_mean)) - np.arcsin(np.sqrt(h_mean)))
            else:
                cohens_h = np.nan

            row = {
                "metric": metric,
                "treatment_mean": round(t_mean, 6),
                "holdout_mean": round(h_mean, 6),
                "absolute_lift": round(lift, 6),
                "relative_lift_pct": round(relative_lift * 100, 2) if not np.isnan(relative_lift) else np.nan,
                "ci_lower": round(ci_lower, 6),
                "ci_upper": round(ci_upper, 6),
                "p_value": round(p_value, 6) if not np.isnan(p_value) else np.nan,
                "significant": p_value < (1 - confidence) if not np.isnan(p_value) else False,
                "cohens_h": round(cohens_h, 4) if not np.isnan(cohens_h) else np.nan,
                "treatment_n": int(t_n),
                "holdout_n": int(h_n),
            }

            if isinstance(name, tuple):
                for col, val in zip(grouping, name):
                    row[col] = val
            elif group_by:
                row[grouping[0]] = name

            results.append(row)

        return pd.DataFrame(results)

    def did_estimate(self, metric: str = "conversion_rate") -> dict:
        """Difference-in-Differences estimator.

        Adjusts for pre-period differences between treatment and holdout.
        DiD lift = (post_treatment - pre_treatment) - (post_holdout - pre_holdout)

        This controls for time-varying confounders that affect both groups.
        """
        pre_metric = f"avg_pre_{'payments' if 'conversion' in metric else 'revenue'}"

        t = self.df[self.df["holdout_group"] == "treatment"]
        h = self.df[self.df["holdout_group"] == "holdout"]

        # Post-period
        post_t = t[metric].mean()
        post_h = h[metric].mean()

        # Pre-period (using pre-period covariates as proxy)
        pre_t = t[pre_metric].mean()
        pre_h = h[pre_metric].mean()

        # Normalize pre-period to same scale
        if pre_h > 0:
            pre_ratio = pre_t / pre_h
        else:
            pre_ratio = 1.0

        # DiD
        did_lift = (post_t - post_h) - (pre_t - pre_h) * (post_h / pre_h if pre_h > 0 else 0)
        naive_lift = post_t - post_h

        return {
            "metric": metric,
            "naive_lift": round(naive_lift, 6),
            "did_lift": round(did_lift, 6),
            "pre_period_balance_ratio": round(pre_ratio, 4),
            "adjustment_magnitude": round(abs(naive_lift - did_lift), 6),
            "adjustment_pct": round(abs(naive_lift - did_lift) / abs(naive_lift) * 100, 1)
                if naive_lift != 0 else 0,
        }

    def stratified_estimate(
        self, metric: str = "conversion_rate", n_strata: int = 5
    ) -> pd.DataFrame:
        """Propensity-stratified lift estimation.

        Estimates lift within propensity score quintiles, then combines.
        More robust than overall estimate when treatment effect varies
        with propensity score (which it almost always does in messaging).
        """
        df = self.df.copy()
        df["propensity_quintile"] = pd.qcut(
            df["avg_propensity_score"], q=n_strata, labels=False, duplicates="drop"
        )

        strata_results = []
        for q, stratum in df.groupby("propensity_quintile"):
            t = stratum[stratum["holdout_group"] == "treatment"]
            h = stratum[stratum["holdout_group"] == "holdout"]

            if len(t) == 0 or len(h) == 0:
                continue

            t_mean = t[metric].mean()
            h_mean = h[metric].mean()
            lift = t_mean - h_mean
            weight = len(stratum) / len(df)

            strata_results.append({
                "quintile": int(q),
                "propensity_range": f"{stratum['avg_propensity_score'].min():.3f}-{stratum['avg_propensity_score'].max():.3f}",
                "treatment_rate": round(t_mean, 4),
                "holdout_rate": round(h_mean, 4),
                "lift": round(lift, 4),
                "relative_lift_pct": round(lift / h_mean * 100, 1) if h_mean > 0 else np.nan,
                "stratum_weight": round(weight, 3),
                "weighted_lift": round(lift * weight, 6),
                "n_users": len(stratum),
            })

        result = pd.DataFrame(strata_results)
        return result

    def power_analysis(
        self,
        baseline_rate: float,
        min_detectable_effect: float,
        alpha: float = 0.05,
        power: float = 0.80,
        holdout_ratio: float = 0.05,
    ) -> dict:
        """Calculate required sample size for a given MDE.

        Answers: "Is our holdout large enough to detect a X% lift?"
        """
        p1 = baseline_rate
        p2 = baseline_rate + min_detectable_effect
        pooled_p = (p1 + p2) / 2

        z_alpha = stats.norm.ppf(1 - alpha / 2)
        z_beta = stats.norm.ppf(power)

        n_per_group = (
            (z_alpha * np.sqrt(2 * pooled_p * (1 - pooled_p))
             + z_beta * np.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) ** 2
        ) / (p2 - p1) ** 2

        n_holdout = int(np.ceil(n_per_group))
        n_treatment = int(np.ceil(n_holdout / holdout_ratio * (1 - holdout_ratio)))

        actual_holdout = self.df[self.df["holdout_group"] == "holdout"]["n_users"].sum()
        actual_treatment = self.df[self.df["holdout_group"] == "treatment"]["n_users"].sum()

        return {
            "baseline_rate": baseline_rate,
            "min_detectable_effect": min_detectable_effect,
            "required_holdout_n": n_holdout,
            "required_treatment_n": n_treatment,
            "actual_holdout_n": int(actual_holdout),
            "actual_treatment_n": int(actual_treatment),
            "sufficiently_powered": actual_holdout >= n_holdout,
            "power_at_actual_n": round(
                stats.norm.cdf(
                    (abs(p2 - p1) * np.sqrt(actual_holdout) /
                     np.sqrt(p1 * (1 - p1) + p2 * (1 - p2))) - z_alpha
                ), 3
            ) if actual_holdout > 0 else 0,
        }

    def generate_report(self) -> str:
        """Generate comprehensive incrementality report."""
        lines = ["# Incrementality Analysis — Causal Lift Estimation\n"]

        # Balance check
        balance = self.balance_check()
        lines.append("## 1. Covariate Balance Check\n")
        lines.append(balance.to_markdown(index=False))
        all_balanced = balance["balanced"].all()
        lines.append(f"\n**All covariates balanced: {all_balanced}**\n")

        # Overall lift
        lift = self.estimate_lift("conversion_rate")
        lines.append("## 2. Overall Lift Estimate\n")
        lines.append(lift[["metric", "treatment_mean", "holdout_mean", "absolute_lift",
                          "relative_lift_pct", "ci_lower", "ci_upper", "p_value",
                          "significant"]].to_markdown(index=False))

        # DiD
        did = self.did_estimate("conversion_rate")
        lines.append(f"\n## 3. Difference-in-Differences Adjustment\n")
        lines.append(f"- Naive lift: {did['naive_lift']:.4%}")
        lines.append(f"- DiD-adjusted lift: {did['did_lift']:.4%}")
        lines.append(f"- Adjustment magnitude: {did['adjustment_pct']}% of naive estimate")

        # Stratified
        strat = self.stratified_estimate("conversion_rate")
        lines.append("\n## 4. Propensity-Stratified Lift\n")
        lines.append(strat.to_markdown(index=False))
        total_weighted = strat["weighted_lift"].sum()
        lines.append(f"\n**Weighted average lift: {total_weighted:.4%}**")

        # Channel heterogeneity
        by_channel = self.estimate_lift("conversion_rate", group_by=["channel"])
        lines.append("\n## 5. Lift by Channel\n")
        if len(by_channel) > 0:
            lines.append(by_channel[["channel", "treatment_mean", "holdout_mean",
                                     "absolute_lift", "relative_lift_pct", "p_value",
                                     "significant"]].to_markdown(index=False))

        # Revenue
        rev_lift = self.estimate_lift("revenue_per_eligible")
        lines.append("\n## 6. Revenue Lift (per eligible user)\n")
        lines.append(rev_lift[["metric", "treatment_mean", "holdout_mean",
                               "absolute_lift", "ci_lower", "ci_upper",
                               "significant"]].to_markdown(index=False))

        return "\n".join(lines)
