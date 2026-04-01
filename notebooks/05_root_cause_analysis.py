#!/usr/bin/env python3
"""
Payment Alert RCA — Root Cause Analysis for Business Outcome Drops
===================================================================
Diagnoses the root cause of declining spend or falling on-time payment rate.

Pipeline:
  1. DETECT — Confirm the drop, quantify magnitude
  2. DECOMPOSE — Mix-shift vs rate-change attribution
  3. DIAGNOSE — Cross-reference with anomaly signals
  4. CAUSAL CHECK — Verify with treatment vs holdout
  5. QUANTIFY — Estimate $ impact
  6. RECOMMEND — Actionable next steps
"""

# %% [markdown]
# # Root Cause Analysis: Spend & On-Time Payment Rate

# %% Setup
import sys
sys.path.insert(0, "..")

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["figure.dpi"] = 150

# %% [markdown]
# ---
# ## Part 1: Detect Business Metric Drops

# %% Initialize RCA orchestrator
from src.rca import RCAOrchestrator, MetricDecomposer

rca = RCAOrchestrator()

# %% Detect spend drop
spend_detection = rca.detect_drop(metric="avg_spend", window_days=7)
print("=" * 60)
print("SPEND DROP DETECTION")
print("=" * 60)
print(f"  Baseline: ${spend_detection['baseline_value']:.2f}")
print(f"  Current:  ${spend_detection['current_value']:.2f}")
print(f"  Change:   {spend_detection['pct_change']:+.1f}%")
print(f"  Severity: {spend_detection['severity'].upper()}")
print(f"\n  Channel Breakdown:")
for ch in spend_detection["channel_breakdown"]:
    print(f"    {ch['channel']}: {ch['change_pct']:+.1f}% (${ch['baseline']:.2f} → ${ch['current']:.2f})")

# %% Detect on-time rate drop
otp_detection = rca.detect_drop(metric="on_time_rate", window_days=7)
print("\n" + "=" * 60)
print("ON-TIME PAYMENT RATE DETECTION")
print("=" * 60)
print(f"  Baseline: {otp_detection['baseline_value']:.2%}")
print(f"  Current:  {otp_detection['current_value']:.2%}")
print(f"  Change:   {otp_detection['pct_change']:+.1f}%")
print(f"  Severity: {otp_detection['severity'].upper()}")

# %% Visualize metric trends
from src.funnel import FunnelAnalyzer
funnel = FunnelAnalyzer()
daily = funnel.daily_metrics()

fig, axes = plt.subplots(1, 2, figsize=(16, 5))

# Spend trend
if "avg_spend_per_user" in daily.columns:
    for ch in daily["channel"].unique():
        ch_data = daily[daily["channel"] == ch].sort_values("report_date")
        axes[0].plot(ch_data["report_date"], ch_data["avg_spend_per_user"], label=ch, marker="o", markersize=3)
    axes[0].set_title("Avg Spend Per User — Daily Trend")
    axes[0].set_ylabel("Avg Spend ($)")
    axes[0].legend(frameon=False)

# On-time rate trend
if "on_time_payment_rate" in daily.columns:
    for ch in daily["channel"].unique():
        ch_data = daily[daily["channel"] == ch].sort_values("report_date")
        axes[1].plot(ch_data["report_date"], ch_data["on_time_payment_rate"], label=ch, marker="o", markersize=3)
    axes[1].set_title("On-Time Payment Rate — Daily Trend")
    axes[1].set_ylabel("On-Time Rate (%)")
    axes[1].legend(frameon=False)

plt.tight_layout()
plt.savefig("../output/plots/business_outcome_trends.png", dpi=150)
plt.show()

# %% [markdown]
# ---
# ## Part 2: Decompose — Why Are Metrics Dropping?

# %% Spend decomposition
print("\n" + "=" * 60)
print("SPEND DECOMPOSITION — Top Root Causes")
print("=" * 60)
spend_decomp = rca.decompose(metric="avg_spend")
for cause in spend_decomp.get("top_causes", []):
    icon = "🔴" if abs(cause["contribution_pct"]) > 20 else "🟡"
    print(f"  {icon} #{cause['rank']}: {cause['explanation']}")
    print(f"      Contribution: {cause['contribution_pct']:+.1f}% | Type: {cause['cause_type']}")

# %% On-time rate decomposition
print("\n" + "=" * 60)
print("ON-TIME RATE DECOMPOSITION — Top Root Causes")
print("=" * 60)
otp_decomp = rca.decompose(metric="on_time_rate")
for cause in otp_decomp.get("top_causes", []):
    icon = "🔴" if abs(cause["contribution_pct"]) > 20 else "🟡"
    print(f"  {icon} #{cause['rank']}: {cause['explanation']}")
    print(f"      Contribution: {cause['contribution_pct']:+.1f}% | Type: {cause['cause_type']}")

# %% Visualize waterfall
decomposer = MetricDecomposer()
waterfall = decomposer.waterfall_decomposition("avg_spend")

if not waterfall.empty:
    fig, ax = plt.subplots(figsize=(14, 6))
    labels = [f"{r['dimension']}\n{r['dimension_value']}" for _, r in waterfall.head(10).iterrows()]
    values = waterfall.head(10)["total_contribution"].values
    colors = ["#E74C3C" if v < 0 else "#2ECC71" for v in values]

    ax.barh(range(len(labels)), values, color=colors, alpha=0.8)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9)
    ax.axvline(x=0, color="black", linewidth=0.5)
    ax.set_title("Spend Drop Waterfall — Top Contributors")
    ax.set_xlabel("Contribution to Spend Change")
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig("../output/plots/spend_waterfall.png", dpi=150)
    plt.show()

# %% Dimension-level detail
for dim in decomposer.df["dimension"].unique():
    decomp = decomposer.decompose_by_dimension(dim, "avg_spend")
    if len(decomp) > 0:
        print(f"\n--- Decomposition by {dim} ---")
        print(decomp[["dimension_value", "baseline_mix_pct", "current_mix_pct",
                       "mix_shift_pp", "rate_change", "total_contribution",
                       "contribution_pct"]].to_string(index=False))

# %% [markdown]
# ---
# ## Part 3: Causal Verification — Is This a Messaging Problem?

# %% Check treatment vs holdout for business outcomes
from src.ecosystem import IncrementalityAnalyzer

inc = IncrementalityAnalyzer()

# Spend lift: is messaging still increasing spend?
if "avg_spend" in inc.df.columns:
    spend_lift = inc.estimate_lift("avg_spend")
    print("\nCausal Spend Lift (Treatment vs Holdout):")
    print(spend_lift[["metric", "treatment_mean", "holdout_mean", "absolute_lift",
                       "relative_lift_pct", "p_value", "significant"]].to_string(index=False))

# On-time rate lift: is messaging still improving on-time payments?
if "on_time_payment_rate" in inc.df.columns:
    otp_lift = inc.estimate_lift("on_time_payment_rate")
    print("\nCausal On-Time Rate Lift (Treatment vs Holdout):")
    print(otp_lift[["metric", "treatment_mean", "holdout_mean", "absolute_lift",
                     "relative_lift_pct", "p_value", "significant"]].to_string(index=False))

    # Key diagnostic: did holdout ALSO drop?
    # If yes → external factor. If no → messaging-driven.
    otp_did = inc.did_estimate("on_time_payment_rate")
    print(f"\nDiD Adjustment for On-Time Rate:")
    print(f"  Naive lift: {otp_did['naive_lift']:.4%}")
    print(f"  DiD-adjusted: {otp_did['did_lift']:.4%}")

# %% Spend and on-time rate trajectories
from src.ecosystem import LTVEffectsAnalyzer
ltv = LTVEffectsAnalyzer()

spend_traj = ltv.spend_trajectory()
if not spend_traj.empty:
    print("\nSpend Trajectory (Treatment vs Holdout):")
    print(spend_traj.to_string(index=False))
    spend_decay = ltv.outcome_decay_assessment("spend")
    print(f"Pattern: {spend_decay['pattern'].upper()}")

otp_traj = ltv.on_time_rate_trajectory()
if not otp_traj.empty:
    print("\nOn-Time Rate Trajectory:")
    print(otp_traj.to_string(index=False))
    otp_decay = ltv.outcome_decay_assessment("on_time_rate")
    print(f"Pattern: {otp_decay['pattern'].upper()}")

# %% [markdown]
# ---
# ## Part 4: Cross-Reference Anomaly Signals

# %% Check active anomalies
anomalies = rca.cross_reference_anomalies()
print("\n" + "=" * 60)
print("CORRELATED ANOMALY SIGNALS")
print("=" * 60)
if anomalies:
    for a in anomalies:
        icon = "🔴" if a["severity"] == "high" else "🟡"
        print(f"\n  {icon} {a['anomaly'].upper()}")
        print(f"     {a['detail']}")
        print(f"     Hypothesis: {a['hypothesis']}")
else:
    print("  ✅ No active anomaly signals detected")

# %% [markdown]
# ---
# ## Part 5: Impact Quantification

# %% Dollar impact
spend_impact = rca.quantify_impact(spend_detection, spend_decomp)
print("\n" + "=" * 60)
print("ESTIMATED SPEND IMPACT")
print("=" * 60)
for k, v in spend_impact.items():
    if k != "metric":
        print(f"  {k.replace('_', ' ').title()}: {'${:,.2f}'.format(v) if isinstance(v, float) else f'{v:,}'}")

otp_impact = rca.quantify_impact(otp_detection, otp_decomp)
print("\n" + "=" * 60)
print("ESTIMATED ON-TIME RATE IMPACT")
print("=" * 60)
for k, v in otp_impact.items():
    if k != "metric":
        print(f"  {k.replace('_', ' ').title()}: {'${:,.2f}'.format(v) if isinstance(v, float) else f'{v:,}'}")

# %% [markdown]
# ---
# ## Part 6: Recommendations

# %% Generate ranked recommendations
recommendations = rca.generate_recommendations(spend_decomp, anomalies)
print("\n" + "=" * 60)
print("RANKED RECOMMENDATIONS")
print("=" * 60)
for rec in recommendations:
    print(f"\n  P{rec['priority']}: {rec['action']}")
    print(f"     {rec['detail']}")
    print(f"     Expected recovery: ~{rec['expected_recovery_pct']:.0f}% of drop")

# %% [markdown]
# ---
# ## Part 7: Validation Layer — Agent Quality Scoring

# %% Run validated RCA (includes completeness + conciseness scoring)
validated_spend = rca.run_validated_rca(metric="avg_spend")
scorecard = validated_spend["validation"]

print("\n" + "=" * 60)
print("VALIDATION SCORECARD — SPEND RCA")
print("=" * 60)
print(f"  Combined Score: {scorecard['combined_score']}/100 (Grade: {scorecard['combined_grade']})")
print(f"  Action: {scorecard['action'].upper()}")

print(f"\n  Completeness: {scorecard['completeness']['total_score']}/100")
for phase, score in scorecard["completeness"]["phase_scores"].items():
    status = "✅" if score > 10 else "❌"
    print(f"    {status} {phase}: {score:.1f}")
if scorecard["completeness"]["missing_phases"]:
    print(f"    ⚠️ Missing: {scorecard['completeness']['missing_phases']}")

print(f"\n  Conciseness: {scorecard['conciseness']['score']}/100")
if scorecard["conciseness"]["penalties"]:
    for p in scorecard["conciseness"]["penalties"]:
        print(f"    ⚠️ {p['type']}: {p['detail']} ({p['penalty']} pts)")
else:
    print("    ✅ No redundancies detected")

# %% Record investigation for operational metrics
from src.rca import RCAValidator
from datetime import datetime

validator = RCAValidator()

# Example: record this investigation with timestamps
# In production, metric_drop_timestamp comes from alerting system
validator.record_investigation(
    rca_result=validated_spend,
    metric_drop_timestamp=datetime.now(),  # placeholder — would come from alert
    detection_timestamp=datetime.now(),     # placeholder — when RCA started
    was_true_positive=None,                 # label after resolution
)

# %% Operational metrics dashboard
print("\n" + "=" * 60)
print("OPERATIONAL METRICS")
print("=" * 60)

mttd = validator.compute_mttd()
if mttd["mttd_hours"] is not None:
    print(f"\n  MTTD: {mttd['mttd_hours']:.1f}h (target: <4h)")
    print(f"  MTTD Median: {mttd['mttd_median_hours']:.1f}h")
    print(f"  MTTD P95: {mttd['mttd_p95_hours']:.1f}h")
else:
    print("\n  MTTD: No data yet (record investigations to build history)")

fdr = validator.compute_fdr()
if fdr["fdr"] is not None:
    print(f"\n  FDR: {fdr['fdr_pct']:.1f}% (target: <15%)")
    print(f"  Precision: {fdr['precision']:.1%}")
    print(f"  TP: {fdr['true_positives']} | FP: {fdr['false_positives']}")
else:
    print(f"\n  FDR: No labeled investigations yet")

rv = validator.compute_resolution_velocity()
if rv["resolution_rate"] is not None:
    print(f"\n  Resolution Rate: {rv['resolution_rate_pct']:.1f}% (target: >80%)")
    if rv["mttr_hours"] is not None:
        print(f"  MTTR: {rv['mttr_hours']:.1f}h (target: <24h)")
    print(f"  Resolved: {rv['n_resolved']} / {rv['n_total']}")
else:
    print(f"\n  Resolution Velocity: No data yet")

# %% [markdown]
# ---
# ## Full Reports

# %% Generate and save all RCA reports
from pathlib import Path
Path("../output/reports").mkdir(parents=True, exist_ok=True)

# Spend RCA report
spend_report = rca.generate_report(metric="avg_spend")
Path("../output/reports/spend_drop_rca.md").write_text(spend_report)
print("\n📄 Spend RCA report saved")

# On-time rate RCA report
otp_report = rca.generate_report(metric="on_time_rate")
Path("../output/reports/ontime_rate_rca.md").write_text(otp_report)
print("📄 On-time rate RCA report saved")

# Decomposition detail
decomp_report = decomposer.generate_report("avg_spend")
Path("../output/reports/spend_decomposition.md").write_text(decomp_report)
print("📄 Spend decomposition report saved")

# Validation dashboard
validation_report = validator.generate_report()
Path("../output/reports/rca_operational_metrics.md").write_text(validation_report)
print("📄 Operational metrics dashboard saved")

print("\n✅ All RCA reports saved to output/reports/")
