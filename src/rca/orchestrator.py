"""RCA Orchestrator: end-to-end root cause analysis for business metric drops.

Given an alert (e.g., "spend dropped 8% WoW" or "on-time rate fell 3pp"),
this module orchestrates the full diagnostic workflow:

  1. DETECT — Confirm the drop, quantify magnitude, identify affected segments
  2. DECOMPOSE — Mix-shift vs rate-change attribution across dimensions
  3. DIAGNOSE — Cross-reference with anomaly signals (drift, timeouts, takeover)
  4. QUANTIFY — Estimate $ impact and incremental attribution
  5. RECOMMEND — Actionable next steps ranked by expected impact

This is the top-level entry point that a Claude subagent or notebook invokes.
"""

import pandas as pd
import numpy as np
from typing import Optional
from pathlib import Path
from datetime import datetime

from .decomposer import MetricDecomposer
from .validator import RCAValidator
from .react_engine import ReActEngine


class RCAOrchestrator:
    """Orchestrates root cause analysis for spend or on-time payment rate drops."""

    def __init__(
        self,
        decomposition_path: str = "data/staging/rca_decomposition.parquet",
        funnel_path: str = "data/staging/funnel_data.parquet",
        anomaly_dir: str = "data/staging/",
    ):
        self.decomposer = MetricDecomposer(decomposition_path)
        self.validator = RCAValidator()
        self.funnel_path = funnel_path
        self.anomaly_dir = anomaly_dir
        self._funnel_df: Optional[pd.DataFrame] = None
        self._anomaly_data: dict[str, pd.DataFrame] = {}
        self._execution_log: dict = {
            "data_validation_count": 0,
            "anomaly_check_details": {"checks": []},
            "start_time": datetime.now().isoformat(),
        }

    @property
    def funnel_df(self) -> pd.DataFrame:
        if self._funnel_df is None:
            self._funnel_df = pd.read_parquet(self.funnel_path)
            if "report_date" in self._funnel_df.columns:
                self._funnel_df["report_date"] = pd.to_datetime(self._funnel_df["report_date"])
        return self._funnel_df

    def _load_anomaly(self, name: str) -> pd.DataFrame:
        if name not in self._anomaly_data:
            path = Path(self.anomaly_dir) / f"{name}.parquet"
            if path.exists():
                self._anomaly_data[name] = pd.read_parquet(path)
            else:
                self._anomaly_data[name] = pd.DataFrame()
        return self._anomaly_data[name]

    # ------------------------------------------------------------------ #
    # Step 1: DETECT — confirm and quantify the metric drop
    # ------------------------------------------------------------------ #
    def detect_drop(
        self,
        metric: str = "avg_spend",
        window_days: int = 7,
    ) -> dict:
        """Confirm and quantify the business metric drop.

        Compares the most recent `window_days` against the prior period.
        Returns magnitude, statistical significance, and affected scope.
        """
        df = self.funnel_df.copy()
        if metric not in df.columns and "total_spend" in df.columns:
            if metric == "avg_spend":
                df["avg_spend"] = df["total_spend"] / df["eligible_users"].replace(0, np.nan)
            elif metric == "on_time_rate":
                df["on_time_rate"] = df["on_time_users"] / df["users_with_payment_due"].replace(0, np.nan)

        df = df.sort_values("report_date")
        max_date = df["report_date"].max()
        cutoff = max_date - pd.Timedelta(days=window_days)
        prior_cutoff = cutoff - pd.Timedelta(days=window_days)

        current = df[df["report_date"] > cutoff]
        baseline = df[(df["report_date"] > prior_cutoff) & (df["report_date"] <= cutoff)]

        c_val = current[metric].mean()
        b_val = baseline[metric].mean()

        abs_change = c_val - b_val
        pct_change = abs_change / b_val * 100 if b_val != 0 else np.nan

        # Per-channel breakdown
        channel_impact = []
        for ch in df["channel"].unique():
            ch_c = current[current["channel"] == ch][metric].mean()
            ch_b = baseline[baseline["channel"] == ch][metric].mean()
            if ch_b > 0:
                channel_impact.append({
                    "channel": ch,
                    "baseline": round(ch_b, 4),
                    "current": round(ch_c, 4),
                    "change_pct": round((ch_c - ch_b) / ch_b * 100, 2),
                })

        return {
            "metric": metric,
            "baseline_value": round(b_val, 4),
            "current_value": round(c_val, 4),
            "absolute_change": round(abs_change, 4),
            "pct_change": round(pct_change, 2),
            "is_drop": pct_change < 0 if not np.isnan(pct_change) else False,
            "severity": (
                "critical" if abs(pct_change) > 10
                else "warning" if abs(pct_change) > 5
                else "minor"
            ) if not np.isnan(pct_change) else "unknown",
            "period": f"{cutoff.date()} to {max_date.date()}",
            "channel_breakdown": channel_impact,
        }

    # ------------------------------------------------------------------ #
    # Step 2: DECOMPOSE — attribution across dimensions
    # ------------------------------------------------------------------ #
    def decompose(self, metric: str = "avg_spend") -> dict:
        """Run full decomposition and return ranked root causes."""
        waterfall = self.decomposer.waterfall_decomposition(metric)
        causes = self.decomposer.identify_root_causes(metric)

        return {
            "metric": metric,
            "n_contributors": len(causes),
            "top_causes": causes[:5],
            "waterfall": waterfall,
            "primary_driver": causes[0] if causes else None,
        }

    # ------------------------------------------------------------------ #
    # Step 3: DIAGNOSE — cross-reference with anomaly signals
    # ------------------------------------------------------------------ #
    def cross_reference_anomalies(self) -> list[dict]:
        """Check if any active anomaly signals correlate with the metric drop.

        Examines:
        - Propensity drift → mis-targeting → wrong users messaged
        - ML timeouts → default scores → untargeted delivery
        - Campaign takeover → concentration → reduced diversity
        """
        findings = []

        # Check propensity drift
        drift = self._load_anomaly("propensity_drift")
        if not drift.empty and "psi_component" in drift.columns:
            total_psi = drift.groupby("channel")["psi_component"].sum()
            drifted = total_psi[total_psi > 0.1]
            if len(drifted) > 0:
                findings.append({
                    "anomaly": "propensity_drift",
                    "severity": "high" if drifted.max() > 0.25 else "medium",
                    "detail": f"PSI > 0.1 on channels: {drifted.index.tolist()}",
                    "max_psi": round(float(drifted.max()), 3),
                    "hypothesis": (
                        "Model drift is mis-targeting users, sending payment alerts "
                        "to low-propensity users who are less likely to pay on time."
                    ),
                })

        # Check ML timeouts / default scores
        timeouts = self._load_anomaly("default_scores")
        if not timeouts.empty and "default_rate" in timeouts.columns:
            avg_timeout = timeouts["default_rate"].mean()
            if avg_timeout > 0.02:
                findings.append({
                    "anomaly": "ml_timeout",
                    "severity": "high" if avg_timeout > 0.05 else "medium",
                    "detail": f"Average default score rate: {avg_timeout:.1%}",
                    "hypothesis": (
                        "ML platform timeouts are assigning default scores, "
                        "bypassing model precision and delivering to users "
                        "unlikely to convert or pay on time."
                    ),
                })

        # Check campaign takeover
        takeover = self._load_anomaly("campaign_takeover")
        if not takeover.empty and "hhi_index" in takeover.columns:
            high_hhi = takeover[takeover["hhi_index"] > 0.25]
            if len(high_hhi) > 0:
                findings.append({
                    "anomaly": "campaign_takeover",
                    "severity": "high" if high_hhi["hhi_index"].max() > 0.40 else "medium",
                    "detail": f"{len(high_hhi)} days with HHI > 0.25",
                    "hypothesis": (
                        "A dominant campaign is crowding out targeted campaigns, "
                        "reducing message relevance and payment conversion rates."
                    ),
                })

        return findings

    # ------------------------------------------------------------------ #
    # Step 4: QUANTIFY — estimate dollar impact
    # ------------------------------------------------------------------ #
    def quantify_impact(self, detection: dict, decomposition: dict) -> dict:
        """Translate the metric drop into estimated dollar impact.

        For spend: direct revenue impact = abs_change × eligible_users
        For on-time rate: estimated late payment cost = rate_drop × users × avg_late_fee
        """
        metric = detection["metric"]
        eligible = self.funnel_df["eligible_users"].sum() if "eligible_users" in self.funnel_df.columns else 0

        if metric == "avg_spend":
            impact = abs(detection["absolute_change"]) * eligible
            return {
                "metric": metric,
                "estimated_weekly_revenue_loss": round(impact, 2),
                "annualized_loss": round(impact * 52, 2),
                "eligible_users": int(eligible),
            }
        elif metric == "on_time_rate":
            avg_late_fee = 25.0  # configurable assumption
            users_with_due = (
                self.funnel_df["users_with_payment_due"].sum()
                if "users_with_payment_due" in self.funnel_df.columns else eligible
            )
            late_increase = abs(detection["absolute_change"]) * users_with_due
            return {
                "metric": metric,
                "additional_late_payments_weekly": round(late_increase),
                "estimated_weekly_late_fee_impact": round(late_increase * avg_late_fee, 2),
                "annualized_impact": round(late_increase * avg_late_fee * 52, 2),
                "users_with_payment_due": int(users_with_due),
                "assumed_avg_late_fee": avg_late_fee,
            }
        return {}

    # ------------------------------------------------------------------ #
    # Step 5: RECOMMEND — actionable next steps
    # ------------------------------------------------------------------ #
    def generate_recommendations(
        self, decomposition: dict, anomalies: list[dict]
    ) -> list[dict]:
        """Generate ranked recommendations based on root cause findings."""
        recommendations = []

        # From decomposition
        if decomposition.get("primary_driver"):
            driver = decomposition["primary_driver"]
            if driver["cause_type"] == "population_shift":
                recommendations.append({
                    "priority": 1,
                    "action": "Investigate upstream eligibility changes",
                    "detail": (
                        f"The {driver['dimension']}='{driver['dimension_value']}' population "
                        f"shifted by {driver.get('mix_shift_pp', 'N/A')}pp. "
                        "Check if eligibility criteria, audience definitions, or "
                        "upstream data pipelines changed."
                    ),
                    "expected_recovery_pct": abs(driver["contribution_pct"]),
                })
            elif driver["cause_type"] == "rate_degradation":
                recommendations.append({
                    "priority": 1,
                    "action": "Deep-dive into within-group rate decline",
                    "detail": (
                        f"Users in {driver['dimension']}='{driver['dimension_value']}' "
                        f"show degraded outcomes. Investigate message content, "
                        f"timing, and competitive pressure for this cohort."
                    ),
                    "expected_recovery_pct": abs(driver["contribution_pct"]),
                })

        # From anomalies
        for anomaly in anomalies:
            if anomaly["anomaly"] == "propensity_drift":
                recommendations.append({
                    "priority": 2 if anomaly["severity"] == "high" else 3,
                    "action": "Retrain or rollback propensity model",
                    "detail": (
                        f"PSI = {anomaly.get('max_psi', 'N/A')}. "
                        "Score distributions have drifted, likely mis-targeting users. "
                        "Consider rolling back to the prior model version or retraining."
                    ),
                    "expected_recovery_pct": 15,
                })
            elif anomaly["anomaly"] == "ml_timeout":
                recommendations.append({
                    "priority": 2,
                    "action": "Escalate ML platform latency to infrastructure team",
                    "detail": (
                        "Default scores bypass targeting precision. "
                        "Work with ML platform team to reduce P95 latency below SLA."
                    ),
                    "expected_recovery_pct": 10,
                })
            elif anomaly["anomaly"] == "campaign_takeover":
                recommendations.append({
                    "priority": 2,
                    "action": "Implement per-campaign impression caps",
                    "detail": (
                        "A dominant campaign is crowding out others. "
                        "Add per-campaign impression share caps (e.g., max 35%) "
                        "to restore campaign diversity."
                    ),
                    "expected_recovery_pct": 10,
                })

        # Sort by priority
        recommendations.sort(key=lambda x: x["priority"])
        return recommendations

    # ------------------------------------------------------------------ #
    # Consolidated anomaly check (eliminates redundant sequential passes)
    # ------------------------------------------------------------------ #
    def _consolidated_anomaly_check(self) -> list[dict]:
        """Single-pass anomaly cross-reference across all signal types.

        Replaces the previous pattern of checking data validity, timeout impact,
        and competitor impact in separate sequential passes. All anomaly data
        is loaded once and checked in a single pass.
        """
        self._execution_log["anomaly_check_details"]["checks"] = []
        findings = []

        # Load all anomaly data once (not per-type)
        anomaly_types = {
            "propensity_drift": {"key_col": "psi_component", "threshold": 0.1},
            "default_scores": {"key_col": "default_rate", "threshold": 0.02},
            "campaign_takeover": {"key_col": "hhi_index", "threshold": 0.25},
        }

        for anomaly_type, config in anomaly_types.items():
            data = self._load_anomaly(anomaly_type)
            self._execution_log["anomaly_check_details"]["checks"].append({
                "type": anomaly_type,
                "loaded": not data.empty,
                "rows": len(data),
            })

            if data.empty or config["key_col"] not in data.columns:
                continue

            # Unified threshold check
            if anomaly_type == "propensity_drift":
                total_psi = data.groupby("channel")[config["key_col"]].sum()
                flagged = total_psi[total_psi > config["threshold"]]
                if len(flagged) > 0:
                    findings.append({
                        "anomaly": anomaly_type,
                        "severity": "high" if flagged.max() > 0.25 else "medium",
                        "detail": f"PSI > {config['threshold']} on channels: {flagged.index.tolist()}",
                        "max_psi": round(float(flagged.max()), 3),
                        "hypothesis": (
                            "Model drift is mis-targeting users, sending payment alerts "
                            "to low-propensity users who are less likely to pay on time."
                        ),
                    })
            elif anomaly_type == "default_scores":
                avg_rate = data[config["key_col"]].mean()
                if avg_rate > config["threshold"]:
                    findings.append({
                        "anomaly": "ml_timeout",
                        "severity": "high" if avg_rate > 0.05 else "medium",
                        "detail": f"Average default score rate: {avg_rate:.1%}",
                        "hypothesis": (
                            "ML platform timeouts are assigning default scores, "
                            "bypassing model precision and delivering to users "
                            "unlikely to convert or pay on time."
                        ),
                    })
            elif anomaly_type == "campaign_takeover":
                high_conc = data[data[config["key_col"]] > config["threshold"]]
                if len(high_conc) > 0:
                    findings.append({
                        "anomaly": anomaly_type,
                        "severity": "high" if high_conc[config["key_col"]].max() > 0.40 else "medium",
                        "detail": f"{len(high_conc)} days with HHI > {config['threshold']}",
                        "hypothesis": (
                            "A dominant campaign is crowding out targeted campaigns, "
                            "reducing message relevance and payment conversion rates."
                        ),
                    })

        return findings

    # ------------------------------------------------------------------ #
    # Full RCA pipeline
    # ------------------------------------------------------------------ #
    def run_full_rca(self, metric: str = "avg_spend") -> dict:
        """Execute the complete RCA pipeline and return structured findings.

        This is the main entry point for notebooks and subagent skills.
        """
        self._execution_log["data_validation_count"] = 1  # validated once at data load

        # Step 1: Detect
        detection = self.detect_drop(metric)

        # Step 2: Decompose
        decomposition = self.decompose(metric)

        # Step 3: Consolidated anomaly cross-reference (single pass)
        anomalies = self._consolidated_anomaly_check()

        # Step 4: Quantify impact
        impact = self.quantify_impact(detection, decomposition)

        # Step 5: Recommend
        recommendations = self.generate_recommendations(decomposition, anomalies)

        return {
            "detection": detection,
            "decomposition": decomposition,
            "anomaly_correlation": anomalies,
            "impact": impact,
            "recommendations": recommendations,
        }

    def run_validated_rca(self, metric: str = "avg_spend") -> dict:
        """Execute the full RCA pipeline with validation scoring.

        Returns the RCA results plus a validation scorecard that scores
        the investigation on completeness and conciseness.
        """
        rca_result = self.run_full_rca(metric)

        # Validate the output
        scorecard = self.validator.validate(rca_result, self._execution_log)

        # If score is too low, log for review
        if scorecard["action"] == "re-run":
            missing = scorecard["completeness"]["missing_phases"]
            rca_result["_validation_warning"] = (
                f"Investigation scored {scorecard['combined_score']}/100 "
                f"(grade: {scorecard['combined_grade']}). "
                f"Missing phases: {missing}. Consider re-running."
            )

        rca_result["validation"] = scorecard
        return rca_result

    # ------------------------------------------------------------------ #
    # ReAct Mode: dynamic reasoning loop
    # ------------------------------------------------------------------ #
    def run_react_rca(
        self,
        metric: str = "avg_spend",
        max_steps: int = 20,
        verbose: bool = True,
    ) -> dict:
        """Execute RCA using the ReAct reasoning framework.

        Instead of the fixed 5-step pipeline (run_full_rca), this mode uses
        a Thought-Action-Observation loop that adapts the investigation
        based on evidence gathered at each step.

        The ReAct engine:
        - Reasons about what to investigate next (Thought)
        - Calls a specific diagnostic action (Action)
        - Interprets the result and updates evidence (Observation)
        - Repeats until sufficient evidence is gathered

        Reference: Yao et al. "ReAct: Synergizing Reasoning and Acting
        in Language Models." ICLR 2023.

        Args:
            metric: Business metric to investigate ("avg_spend" or "on_time_rate")
            max_steps: Maximum T-A-O iterations
            verbose: Print reasoning trace during execution

        Returns:
            dict compatible with run_full_rca output, plus:
                - trace: ReActTrace with full reasoning chain
                - n_steps: total steps taken
                - phases_covered: set of completed phases
                - conclusion: synthesized finding
                - mode: "react"
        """
        engine = ReActEngine(
            orchestrator=self,
            max_steps=max_steps,
            verbose=verbose,
        )
        return engine.run(metric)

    def run_validated_react_rca(
        self,
        metric: str = "avg_spend",
        max_steps: int = 20,
        verbose: bool = True,
    ) -> dict:
        """Execute ReAct RCA with validation scoring.

        Combines the adaptive ReAct loop with the completeness/conciseness
        validation layer.
        """
        rca_result = self.run_react_rca(metric, max_steps, verbose)

        # Build execution log from ReAct trace
        trace = rca_result.get("trace")
        execution_log = {
            "data_validation_count": 1,  # validated once at data load
            "anomaly_check_details": {"checks": []},
            "start_time": trace.start_time if trace else datetime.now().isoformat(),
            "mode": "react",
            "n_steps": rca_result.get("n_steps", 0),
        }

        # Extract anomaly check details from trace
        if trace:
            for step in trace.steps:
                if step.action_name == "check_anomalies" and step.result:
                    if isinstance(step.result, list):
                        for a in step.result:
                            execution_log["anomaly_check_details"]["checks"].append({
                                "type": a.get("anomaly", "unknown"),
                                "loaded": True,
                                "rows": 1,
                            })

        # Validate
        scorecard = self.validator.validate(rca_result, execution_log)

        if scorecard["action"] == "re-run":
            missing = scorecard["completeness"]["missing_phases"]
            rca_result["_validation_warning"] = (
                f"Investigation scored {scorecard['combined_score']}/100 "
                f"(grade: {scorecard['combined_grade']}). "
                f"Missing phases: {missing}. Consider re-running with fixed pipeline."
            )

        rca_result["validation"] = scorecard
        return rca_result

    def generate_report(self, metric: str = "avg_spend") -> str:
        """Generate a comprehensive markdown RCA report."""
        rca = self.run_full_rca(metric)
        det = rca["detection"]
        lines = [
            f"# Root Cause Analysis: {metric.replace('_', ' ').title()}\n",
            f"**Period**: {det['period']}",
            f"**Severity**: {det['severity'].upper()}",
            f"**Change**: {det['pct_change']:+.1f}% "
            f"({det['baseline_value']:.4f} → {det['current_value']:.4f})\n",
        ]

        # Channel breakdown
        if det["channel_breakdown"]:
            lines.append("## Channel Breakdown\n")
            lines.append(pd.DataFrame(det["channel_breakdown"]).to_markdown(index=False))
            lines.append("")

        # Decomposition
        lines.append("## Root Cause Decomposition\n")
        decomp_report = self.decomposer.generate_report(metric)
        # Extract just the causes section
        for cause in rca["decomposition"].get("top_causes", []):
            severity = "🔴" if abs(cause["contribution_pct"]) > 20 else "🟡"
            lines.append(
                f"{severity} **#{cause['rank']}** ({cause['contribution_pct']:+.1f}%): "
                f"{cause['explanation']}"
            )
        lines.append("")

        # Anomaly correlation
        if rca["anomaly_correlation"]:
            lines.append("## Correlated Anomaly Signals\n")
            for a in rca["anomaly_correlation"]:
                icon = "🔴" if a["severity"] == "high" else "🟡"
                lines.append(f"{icon} **{a['anomaly']}**: {a['detail']}")
                lines.append(f"   *Hypothesis*: {a['hypothesis']}")
                lines.append("")

        # Dollar impact
        lines.append("## Estimated Impact\n")
        for k, v in rca["impact"].items():
            if k != "metric":
                lines.append(f"- **{k.replace('_', ' ').title()}**: "
                             f"{'${:,.2f}'.format(v) if isinstance(v, float) else f'{v:,}'}")
        lines.append("")

        # Recommendations
        lines.append("## Recommendations\n")
        for rec in rca["recommendations"]:
            lines.append(f"**P{rec['priority']}**: {rec['action']}")
            lines.append(f"   {rec['detail']}")
            lines.append(f"   *Expected recovery*: ~{rec['expected_recovery_pct']:.0f}% of drop")
            lines.append("")

        return "\n".join(lines)
