#!/usr/bin/env python3
"""
Payment Alert RCA — Ecosystem Impact Analysis
===============================================
System-level analysis: incrementality, cannibalization, LTV effects,
and portfolio-level efficiency tradeoffs.

This notebook answers: "Is our messaging creating net value, or just
moving conversions around?"
"""

# %% [markdown]
# # Ecosystem Impact & Tradeoff Analysis

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
# ## Part 1: Incrementality — Is the Lift Causal?

# %% Load incrementality data
from src.ecosystem import IncrementalityAnalyzer
inc = IncrementalityAnalyzer()

# %% Step 1: Balance Check
balance = inc.balance_check()
print("Covariate Balance Check (|SMD| < 0.1 = balanced):")
print(balance.to_string(index=False))
print(f"\nAll balanced: {balance['balanced'].all()}")

# %% Step 2: Overall lift with CI
lift_overall = inc.estimate_lift("conversion_rate")
print("\nOverall Incremental Lift:")
print(lift_overall.to_string(index=False))

# %% Step 3: DiD adjustment
did = inc.did_estimate("conversion_rate")
print(f"\nDifference-in-Differences:")
print(f"  Naive lift: {did['naive_lift']:.4%}")
print(f"  DiD-adjusted lift: {did['did_lift']:.4%}")
print(f"  Adjustment: {did['adjustment_pct']}% of naive estimate")

# %% Step 4: Stratified estimation by propensity
strat = inc.stratified_estimate("conversion_rate")
print("\nPropensity-Stratified Lift:")
print(strat.to_string(index=False))
print(f"\nWeighted average lift: {strat['weighted_lift'].sum():.4%}")

# Visualize heterogeneous treatment effects
fig, ax = plt.subplots(figsize=(10, 5))
ax.bar(strat["quintile"].astype(str), strat["lift"] * 100, color="#0E8C7F", alpha=0.8)
ax.axhline(y=strat["weighted_lift"].sum() * 100, color="red", linestyle="--", label="Weighted avg")
ax.set_title("Incremental Lift by Propensity Quintile")
ax.set_xlabel("Propensity Quintile (0=lowest)")
ax.set_ylabel("Lift (pp)")
ax.legend(frameon=False)
plt.tight_layout()
plt.savefig("../output/plots/stratified_lift.png", dpi=150)
plt.show()

# %% Step 5: Channel heterogeneity
lift_channel = inc.estimate_lift("conversion_rate", group_by=["channel"])
print("\nLift by Channel:")
print(lift_channel[["channel", "treatment_mean", "holdout_mean", "absolute_lift",
                     "relative_lift_pct", "p_value", "significant"]].to_string(index=False))

# %% Step 6: Revenue lift
rev_lift = inc.estimate_lift("revenue_per_eligible")
print("\nRevenue Lift (per eligible user):")
print(rev_lift.to_string(index=False))

# %% Step 7: Power analysis
power = inc.power_analysis(baseline_rate=0.042, min_detectable_effect=0.005)
print(f"\nPower Analysis:")
print(f"  Required holdout: {power['required_holdout_n']:,}")
print(f"  Actual holdout: {power['actual_holdout_n']:,}")
print(f"  Sufficiently powered: {power['sufficiently_powered']}")
print(f"  Actual power: {power['power_at_actual_n']}")

# %% [markdown]
# ---
# ## Part 2: Cannibalization — Are We Stealing From Other Intents?

# %% Load cannibalization data
from src.ecosystem import CannibalizationAnalyzer
cannibal = CannibalizationAnalyzer()

# %% Cross-intent overlap
overlap = cannibal.overlap_summary()
print("Cross-Intent Overlap (among PA converters):")
print(overlap.to_string(index=False))

# %% Net incrementality
net = cannibal.estimate_net_incrementality(
    gross_lift=float(lift_overall["absolute_lift"].iloc[0]),
    gross_lift_users=int(lift_overall["treatment_n"].iloc[0]),
)
print(f"\nNet Incrementality Assessment:")
print(f"  Gross incremental: {net['gross_incremental_conversions']:,}")
print(f"  Estimated cannibalized: {net['estimated_cannibalized']:,}")
print(f"  Net incremental: {net['net_incremental_conversions']:,}")
print(f"  Cannibalization rate: {net['cannibalization_rate_pct']}%")

# %% Temporal pattern
temporal = cannibal.temporal_overlap_pattern()
print("\nTemporal Overlap (who messages first?):")
print(temporal.to_string(index=False))

# %% [markdown]
# ---
# ## Part 3: LTV Effects — Are We Helping or Hurting Long-Term?

# %% Load LTV data
from src.ecosystem import LTVEffectsAnalyzer
ltv = LTVEffectsAnalyzer()

# %% LTV lift at each window
ltv_windows = ltv.ltv_lift_by_window()
print("LTV Lift by Measurement Window:")
print(ltv_windows.to_string(index=False))

# %% Lift trajectory classification
trajectory = ltv.lift_decay_assessment()
print(f"\nLift Trajectory: {trajectory['pattern'].upper()}")
print(f"  7d lift: {trajectory['lift_7d_pct']}%")
print(f"  30d lift: {trajectory['lift_30d_pct']}%")
print(f"  90d lift: {trajectory['lift_90d_pct']}%")
print(f"  Decay rate (7d→90d): {trajectory['decay_rate_7d_to_90d']}%")

# Visualize LTV trajectory
fig, ax = plt.subplots(figsize=(8, 5))
windows = ["7d", "30d", "90d"]
lifts = [trajectory[f"lift_{w}_pct"] for w in windows]
colors = ["#2ECC71" if l > 0 else "#E74C3C" for l in lifts]
ax.bar(windows, lifts, color=colors, alpha=0.8, width=0.5)
ax.axhline(y=0, color="black", linewidth=0.5)
ax.set_title(f"LTV Lift Trajectory: {trajectory['pattern'].upper()}")
ax.set_xlabel("Measurement Window")
ax.set_ylabel("Relative Lift (%)")
plt.tight_layout()
plt.savefig("../output/plots/ltv_trajectory.png", dpi=150)
plt.show()

# %% Fatigue indicators
fatigue = ltv.fatigue_indicators()
print("\nFatigue Signals (Treatment excess over Holdout):")
print(fatigue.to_string(index=False))

# %% [markdown]
# ---
# ## Part 4: Portfolio Tradeoffs

# %% Priority tradeoffs
from src.tradeoffs import PriorityTradeoffAnalyzer, ChannelAllocationAnalyzer, FrequencyOptimizer

priority = PriorityTradeoffAnalyzer()
pareto = priority.pareto_frontier()
print("Pareto Frontier — Marginal Value by Intent:")
print(pareto.to_string(index=False))

# %% Reallocation scenarios
for change in [0.05, 0.10]:
    sim = priority.simulate_reallocation("payment_alert", share_change=change)
    print(f"\n{sim['scenario']}:")
    print(f"  Net conversions: {sim['net_conversions']:+,}")
    print(f"  Net revenue: ${sim['net_revenue']:+,.2f}")

# %% Channel efficiency
channel = ChannelAllocationAnalyzer()
econ = channel.channel_unit_economics()
print("\nChannel Unit Economics:")
print(econ[["channel", "cvr", "cost_per_conversion", "roas",
            "marginal_profit_per_msg"]].to_string(index=False))

# %% Frequency optimization
freq = FrequencyOptimizer()
curve_result = freq.fit_response_curve(fatigue_penalty_weight=0.3)
if "error" not in curve_result:
    print(f"\nOptimal Frequency: {curve_result['optimal_frequency']} msgs/user/week")
    print(f"Current: {curve_result['current_avg_frequency']}")
    print(f"Direction: {curve_result['direction']} by {abs(curve_result['frequency_gap']):.1f}")

    # Segment-level
    seg_freq = freq.segment_optimal_frequency()
    print("\nSegment-Level Frequency Recommendations:")
    print(seg_freq.to_string(index=False))

# %% [markdown]
# ---
# ## Summary: Ecosystem Health Scorecard

# %% Generate all reports
from pathlib import Path
Path("../output/reports").mkdir(parents=True, exist_ok=True)

Path("../output/reports/incrementality_report.md").write_text(inc.generate_report())
Path("../output/reports/cannibalization_report.md").write_text(cannibal.generate_report())
Path("../output/reports/ltv_effects_report.md").write_text(ltv.generate_report())
Path("../output/reports/portfolio_efficiency_report.md").write_text(priority.generate_report())
Path("../output/reports/channel_allocation_report.md").write_text(channel.generate_report())
Path("../output/reports/frequency_optimization_report.md").write_text(freq.generate_report())

print("\nAll ecosystem reports saved to output/reports/")
