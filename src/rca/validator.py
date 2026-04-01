"""RCA Validation Layer: scores agent output on completeness and conciseness.

This module serves two purposes:
  1. AGENT SCORING — Evaluates the completeness and conciseness of each
     RCA investigation, preventing redundant steps and incomplete analyses.
  2. OPERATIONAL METRICS — Tracks Mean Time to Detection (MTTD), False
     Discovery Rate (FDR), and Resolution Velocity across RCA investigations.

The validator runs as a post-processing step after the RCA orchestrator
completes. It produces a validation scorecard that the agent uses to
decide whether to re-run any phase or accept the findings.

Eliminates redundancy by:
  - Deduplicating overlapping anomaly checks (e.g., data validity is checked
    once during data pull, not again in every anomaly analyzer)
  - Merging timeout impact and competitor impact into a single cross-reference
    pass instead of separate sequential checks
  - Scoring conciseness to penalize investigations that repeat the same
    finding across multiple dimensions
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path
import json


class RCAValidator:
    """Validates RCA output quality and tracks operational metrics."""

    # Completeness checklist: each RCA must cover these phases
    REQUIRED_PHASES = [
        "detection",          # Metric drop confirmed with magnitude
        "decomposition",      # Mix-shift vs rate-change attribution
        "causal_verification",  # Treatment vs holdout comparison
        "anomaly_cross_ref",  # Cross-reference with active anomaly signals
        "impact_quantification",  # Dollar estimate attached
        "recommendations",    # At least one actionable recommendation
    ]

    # Conciseness penalties: deductions for redundant work
    CONCISENESS_PENALTIES = {
        "duplicate_data_validation": -5,     # Data validity checked more than once
        "redundant_anomaly_check": -5,       # Same anomaly checked in multiple places
        "repeated_finding": -10,             # Same root cause stated in multiple dimensions
        "excessive_dimensions": -3,          # More than 5 dimensions decomposed
        "unactionable_recommendation": -5,   # Recommendation without expected recovery
    }

    def __init__(self, metrics_path: str = "data/staging/.rca_metrics.json"):
        self.metrics_path = Path(metrics_path)
        self._history = self._load_history()

    def _load_history(self) -> list[dict]:
        if self.metrics_path.exists():
            return json.loads(self.metrics_path.read_text())
        return []

    def _save_history(self):
        self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
        self.metrics_path.write_text(json.dumps(self._history, indent=2, default=str))

    # ------------------------------------------------------------------ #
    # Completeness Scoring
    # ------------------------------------------------------------------ #
    def score_completeness(self, rca_result: dict) -> dict:
        """Score how complete the RCA investigation is.

        Returns a score from 0-100 with per-phase breakdown.
        Each phase is worth ~16.7 points (100 / 6 phases).
        """
        phase_scores = {}
        points_per_phase = 100 / len(self.REQUIRED_PHASES)

        # Detection
        det = rca_result.get("detection", {})
        det_score = 0
        if det.get("pct_change") is not None:
            det_score += 0.4  # magnitude quantified
        if det.get("severity"):
            det_score += 0.2  # severity classified
        if det.get("channel_breakdown"):
            det_score += 0.4  # per-channel breakdown present
        phase_scores["detection"] = round(det_score * points_per_phase, 1)

        # Decomposition
        decomp = rca_result.get("decomposition", {})
        decomp_score = 0
        causes = decomp.get("top_causes", [])
        if causes:
            decomp_score += 0.4  # at least one root cause identified
        if len(causes) >= 3:
            decomp_score += 0.2  # multiple contributors found
        if any(c.get("cause_type") for c in causes):
            decomp_score += 0.2  # causes are classified (mix vs rate)
        if any(c.get("contribution_pct") for c in causes):
            decomp_score += 0.2  # contributions are quantified
        phase_scores["decomposition"] = round(decomp_score * points_per_phase, 1)

        # Causal verification
        causal = rca_result.get("causal_verification", {})
        causal_score = 0
        if causal.get("holdout_compared"):
            causal_score += 0.5  # holdout vs treatment compared
        if causal.get("external_vs_messaging"):
            causal_score += 0.3  # classified as external or messaging-driven
        if causal.get("did_applied"):
            causal_score += 0.2  # DiD adjustment applied
        phase_scores["causal_verification"] = round(causal_score * points_per_phase, 1)

        # Anomaly cross-reference
        anomalies = rca_result.get("anomaly_correlation", [])
        anom_score = 0.5  # base score for running the check
        if anomalies:
            if all(a.get("hypothesis") for a in anomalies):
                anom_score += 0.3  # hypotheses formulated
            if all(a.get("severity") for a in anomalies):
                anom_score += 0.2  # severity classified
        else:
            anom_score += 0.5  # no anomalies is a valid finding
        phase_scores["anomaly_cross_ref"] = round(anom_score * points_per_phase, 1)

        # Impact quantification
        impact = rca_result.get("impact", {})
        impact_score = 0
        if any("loss" in k or "impact" in k for k in impact.keys()):
            impact_score += 0.6  # dollar figure present
        if any("annualized" in k for k in impact.keys()):
            impact_score += 0.2  # annualized projection
        if impact.get("eligible_users") or impact.get("users_with_payment_due"):
            impact_score += 0.2  # user count for context
        phase_scores["impact_quantification"] = round(impact_score * points_per_phase, 1)

        # Recommendations
        recs = rca_result.get("recommendations", [])
        rec_score = 0
        if recs:
            rec_score += 0.4  # at least one recommendation
        if len(recs) >= 2:
            rec_score += 0.2  # multiple options
        if all(r.get("expected_recovery_pct") for r in recs):
            rec_score += 0.2  # recovery estimates attached
        if all(r.get("priority") for r in recs):
            rec_score += 0.2  # prioritized
        phase_scores["recommendations"] = round(rec_score * points_per_phase, 1)

        total = sum(phase_scores.values())
        missing = [p for p in self.REQUIRED_PHASES if phase_scores.get(p, 0) == 0]

        return {
            "total_score": round(total, 1),
            "phase_scores": phase_scores,
            "missing_phases": missing,
            "grade": (
                "A" if total >= 90 else
                "B" if total >= 75 else
                "C" if total >= 60 else
                "D" if total >= 40 else "F"
            ),
        }

    # ------------------------------------------------------------------ #
    # Conciseness Scoring
    # ------------------------------------------------------------------ #
    def score_conciseness(self, rca_result: dict, execution_log: Optional[dict] = None) -> dict:
        """Score the conciseness of the investigation.

        Penalizes redundant checks, duplicate findings, and excessive decomposition.
        Base score is 100, with deductions for each redundancy.
        """
        penalties = []
        score = 100

        # Check for duplicate data validation (should only happen once in hooks)
        if execution_log:
            data_validation_count = execution_log.get("data_validation_count", 1)
            if data_validation_count > 1:
                penalty = self.CONCISENESS_PENALTIES["duplicate_data_validation"]
                penalties.append({
                    "type": "duplicate_data_validation",
                    "penalty": penalty,
                    "detail": f"Data validity checked {data_validation_count} times (should be 1)",
                })
                score += penalty * (data_validation_count - 1)

            # Check for redundant anomaly checks
            anomaly_checks = execution_log.get("anomaly_check_details", {})
            checked_types = set()
            for check in anomaly_checks.get("checks", []):
                if check["type"] in checked_types:
                    penalty = self.CONCISENESS_PENALTIES["redundant_anomaly_check"]
                    penalties.append({
                        "type": "redundant_anomaly_check",
                        "penalty": penalty,
                        "detail": f"Anomaly type '{check['type']}' checked redundantly",
                    })
                    score += penalty
                checked_types.add(check["type"])

        # Check for repeated findings across dimensions
        decomp = rca_result.get("decomposition", {})
        causes = decomp.get("top_causes", [])
        seen_explanations = set()
        for cause in causes:
            # Normalize explanation to detect near-duplicates
            key = (cause.get("dimension_value", ""), cause.get("cause_type", ""))
            if key in seen_explanations:
                penalty = self.CONCISENESS_PENALTIES["repeated_finding"]
                penalties.append({
                    "type": "repeated_finding",
                    "penalty": penalty,
                    "detail": f"Finding for '{key[0]}' repeated",
                })
                score += penalty
            seen_explanations.add(key)

        # Excessive dimensions
        waterfall = decomp.get("waterfall")
        if waterfall is not None and hasattr(waterfall, "__len__") and len(waterfall) > 15:
            penalty = self.CONCISENESS_PENALTIES["excessive_dimensions"]
            penalties.append({
                "type": "excessive_dimensions",
                "penalty": penalty,
                "detail": f"{len(waterfall)} contributors decomposed (limit: 15)",
            })
            score += penalty

        # Unactionable recommendations
        recs = rca_result.get("recommendations", [])
        for rec in recs:
            if not rec.get("expected_recovery_pct"):
                penalty = self.CONCISENESS_PENALTIES["unactionable_recommendation"]
                penalties.append({
                    "type": "unactionable_recommendation",
                    "penalty": penalty,
                    "detail": f"Recommendation '{rec.get('action', 'N/A')}' has no recovery estimate",
                })
                score += penalty

        return {
            "score": max(0, score),
            "penalties": penalties,
            "total_penalty": sum(p["penalty"] for p in penalties),
            "n_redundancies": len(penalties),
        }

    # ------------------------------------------------------------------ #
    # Operational Metrics: MTTD, FDR, Resolution Velocity
    # ------------------------------------------------------------------ #
    def record_investigation(
        self,
        rca_result: dict,
        metric_drop_timestamp: datetime,
        detection_timestamp: datetime,
        resolution_timestamp: Optional[datetime] = None,
        was_true_positive: Optional[bool] = None,
    ):
        """Record a completed RCA investigation for operational metric tracking.

        Args:
            rca_result: Output from RCAOrchestrator.run_full_rca()
            metric_drop_timestamp: When the metric actually started dropping
            detection_timestamp: When the system detected the drop
            resolution_timestamp: When the root cause was resolved (if known)
            was_true_positive: Whether the flagged drop was a real issue (not noise)
        """
        detection = rca_result.get("detection", {})
        completeness = self.score_completeness(rca_result)

        record = {
            "investigation_id": f"rca_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "metric": detection.get("metric", "unknown"),
            "severity": detection.get("severity", "unknown"),
            "pct_change": detection.get("pct_change"),
            "metric_drop_timestamp": metric_drop_timestamp.isoformat(),
            "detection_timestamp": detection_timestamp.isoformat(),
            "resolution_timestamp": resolution_timestamp.isoformat() if resolution_timestamp else None,
            "mttd_hours": (detection_timestamp - metric_drop_timestamp).total_seconds() / 3600,
            "mttr_hours": (
                (resolution_timestamp - detection_timestamp).total_seconds() / 3600
                if resolution_timestamp else None
            ),
            "was_true_positive": was_true_positive,
            "completeness_score": completeness["total_score"],
            "completeness_grade": completeness["grade"],
            "n_root_causes": len(rca_result.get("decomposition", {}).get("top_causes", [])),
            "n_anomalies_correlated": len(rca_result.get("anomaly_correlation", [])),
            "n_recommendations": len(rca_result.get("recommendations", [])),
            "primary_cause_type": (
                rca_result.get("decomposition", {}).get("primary_driver", {}).get("cause_type")
            ),
            "recorded_at": datetime.now().isoformat(),
        }

        self._history.append(record)
        self._save_history()
        return record

    def compute_mttd(self, lookback_days: int = 90) -> dict:
        """Compute Mean Time to Detection (MTTD) across recent investigations.

        MTTD = avg(detection_timestamp - metric_drop_timestamp)
        Lower is better. Measures how quickly the system catches drops.
        """
        cutoff = datetime.now() - timedelta(days=lookback_days)
        recent = [
            r for r in self._history
            if datetime.fromisoformat(r["recorded_at"]) > cutoff
            and r.get("mttd_hours") is not None
        ]

        if not recent:
            return {"mttd_hours": None, "n_investigations": 0}

        mttd_values = [r["mttd_hours"] for r in recent]
        return {
            "mttd_hours": round(np.mean(mttd_values), 2),
            "mttd_median_hours": round(np.median(mttd_values), 2),
            "mttd_p95_hours": round(np.percentile(mttd_values, 95), 2),
            "n_investigations": len(recent),
            "trend": self._compute_trend(recent, "mttd_hours"),
        }

    def compute_fdr(self, lookback_days: int = 90) -> dict:
        """Compute False Discovery Rate (FDR) across recent investigations.

        FDR = false_positives / (true_positives + false_positives)
        Lower is better. Measures how often the system flags noise as real drops.
        A "false positive" is a flagged drop that turned out to be noise,
        seasonality, or was below the actionable threshold.
        """
        cutoff = datetime.now() - timedelta(days=lookback_days)
        recent = [
            r for r in self._history
            if datetime.fromisoformat(r["recorded_at"]) > cutoff
            and r.get("was_true_positive") is not None
        ]

        if not recent:
            return {"fdr": None, "n_labeled": 0}

        tp = sum(1 for r in recent if r["was_true_positive"])
        fp = sum(1 for r in recent if not r["was_true_positive"])
        total = tp + fp

        return {
            "fdr": round(fp / total, 4) if total > 0 else 0,
            "fdr_pct": round(fp / total * 100, 1) if total > 0 else 0,
            "true_positives": tp,
            "false_positives": fp,
            "n_labeled": total,
            "precision": round(tp / total, 4) if total > 0 else 0,
        }

    def compute_resolution_velocity(self, lookback_days: int = 90) -> dict:
        """Compute Resolution Velocity: how fast root causes are resolved.

        Resolution Velocity = investigations_resolved / total_investigations
        Also tracks Mean Time to Resolution (MTTR).
        """
        cutoff = datetime.now() - timedelta(days=lookback_days)
        recent = [
            r for r in self._history
            if datetime.fromisoformat(r["recorded_at"]) > cutoff
        ]

        if not recent:
            return {"resolution_rate": None, "n_investigations": 0}

        resolved = [r for r in recent if r.get("mttr_hours") is not None]
        mttr_values = [r["mttr_hours"] for r in resolved]

        return {
            "resolution_rate": round(len(resolved) / len(recent), 4) if recent else 0,
            "resolution_rate_pct": round(len(resolved) / len(recent) * 100, 1) if recent else 0,
            "mttr_hours": round(np.mean(mttr_values), 2) if mttr_values else None,
            "mttr_median_hours": round(np.median(mttr_values), 2) if mttr_values else None,
            "n_resolved": len(resolved),
            "n_open": len(recent) - len(resolved),
            "n_total": len(recent),
            "avg_completeness_score": round(np.mean([r["completeness_score"] for r in recent]), 1),
        }

    def _compute_trend(self, records: list[dict], metric_key: str) -> str:
        """Compute whether a metric is improving, stable, or degrading."""
        if len(records) < 4:
            return "insufficient_data"

        mid = len(records) // 2
        first_half = np.mean([r[metric_key] for r in records[:mid] if r.get(metric_key)])
        second_half = np.mean([r[metric_key] for r in records[mid:] if r.get(metric_key)])

        if second_half < first_half * 0.9:
            return "improving"
        elif second_half > first_half * 1.1:
            return "degrading"
        return "stable"

    # ------------------------------------------------------------------ #
    # Full Validation Scorecard
    # ------------------------------------------------------------------ #
    def validate(
        self,
        rca_result: dict,
        execution_log: Optional[dict] = None,
    ) -> dict:
        """Run the full validation and return a scorecard.

        This is the main entry point for the validation layer.
        """
        completeness = self.score_completeness(rca_result)
        conciseness = self.score_conciseness(rca_result, execution_log)

        # Combined score: weighted average (completeness 60%, conciseness 40%)
        combined = round(completeness["total_score"] * 0.6 + conciseness["score"] * 0.4, 1)

        scorecard = {
            "combined_score": combined,
            "combined_grade": (
                "A" if combined >= 90 else
                "B" if combined >= 75 else
                "C" if combined >= 60 else
                "D" if combined >= 40 else "F"
            ),
            "completeness": completeness,
            "conciseness": conciseness,
            "pass": combined >= 70,
            "action": (
                "accept" if combined >= 85
                else "review" if combined >= 70
                else "re-run" if combined >= 50
                else "escalate"
            ),
        }

        # Add operational metrics if history exists
        if self._history:
            scorecard["operational_metrics"] = {
                "mttd": self.compute_mttd(),
                "fdr": self.compute_fdr(),
                "resolution_velocity": self.compute_resolution_velocity(),
            }

        return scorecard

    def generate_report(self) -> str:
        """Generate an operational metrics dashboard report."""
        lines = ["# RCA Operational Metrics Dashboard\n"]

        mttd = self.compute_mttd()
        lines.append("## Mean Time to Detection (MTTD)\n")
        if mttd["mttd_hours"] is not None:
            lines.append(f"- **Mean**: {mttd['mttd_hours']:.1f} hours")
            lines.append(f"- **Median**: {mttd['mttd_median_hours']:.1f} hours")
            lines.append(f"- **P95**: {mttd['mttd_p95_hours']:.1f} hours")
            lines.append(f"- **Trend**: {mttd['trend']}")
            lines.append(f"- **N**: {mttd['n_investigations']} investigations")
        else:
            lines.append("No MTTD data available yet.")

        fdr = self.compute_fdr()
        lines.append("\n## False Discovery Rate (FDR)\n")
        if fdr["fdr"] is not None:
            lines.append(f"- **FDR**: {fdr['fdr_pct']:.1f}%")
            lines.append(f"- **Precision**: {fdr['precision']:.1%}")
            lines.append(f"- **True Positives**: {fdr['true_positives']}")
            lines.append(f"- **False Positives**: {fdr['false_positives']}")
        else:
            lines.append("No labeled investigations yet. Mark investigations as TP/FP to track FDR.")

        rv = self.compute_resolution_velocity()
        lines.append("\n## Resolution Velocity\n")
        if rv["resolution_rate"] is not None:
            lines.append(f"- **Resolution Rate**: {rv['resolution_rate_pct']:.1f}%")
            if rv["mttr_hours"] is not None:
                lines.append(f"- **MTTR (Mean)**: {rv['mttr_hours']:.1f} hours")
                lines.append(f"- **MTTR (Median)**: {rv['mttr_median_hours']:.1f} hours")
            lines.append(f"- **Resolved**: {rv['n_resolved']} / {rv['n_total']}")
            lines.append(f"- **Open**: {rv['n_open']}")
            lines.append(f"- **Avg Completeness**: {rv['avg_completeness_score']:.0f}/100")
        else:
            lines.append("No resolution data available yet.")

        return "\n".join(lines)
