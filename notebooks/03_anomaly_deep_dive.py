#!/usr/bin/env python3
"""
Payment Alert RCA — Anomaly Event Deep Dive
=============================================
Deep-dive analysis across three anomaly types:
1. Propensity score drift
2. ML platform timeout / default scores
3. Campaign impression takeover

Each section can be run independently or as a subagent skill.
"""

# %% [markdown]
# # Payment Alert — Anomaly Event Deep Dive

# %% Setup
import sys
sys.path.insert(0, "..")

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.anomaly import PropensityDriftAnalyzer, DefaultScoreAnalyzer, CampaignTakeoverAnalyzer
from src.utils.plotting import AnomalyPlotter

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["figure.dpi"] = 150
plotter = AnomalyPlotter()

# %% [markdown]
# ---
# ## Part 1: Propensity Score Drift

# %% Load drift data
drift = PropensityDriftAnalyzer()

# %% PSI Summary
psi = drift.compute_psi()
print("PSI Summary by Channel:")
print(psi.to_string(index=False))

# %% Model version check
version_info = drift.model_version_check()
print(f"\nModel versions - Reference: {version_info['reference_versions']}")
print(f"Model versions - Detection: {version_info['detection_versions']}")
print(f"Version changed: {version_info['version_changed']}")

# %% KS Tests
for channel in drift.df["channel"].unique():
    ks = drift.ks_test(channel)
    print(f"\nKS Test [{channel}]: stat={ks['ks_statistic']}, p={ks['p_value']:.6f}, "
          f"significant={ks['significant']}, mean_shift={ks['mean_shift']}")

# %% Visualize drift
plotter.plot_psi_heatmap(drift.df, save_path="../output/plots/psi_heatmap.png")
plt.show()

# %% Decile analysis
for channel in drift.df["channel"].unique():
    decile = drift.decile_shift_analysis(channel)
    print(f"\nDecile Shift — {channel}:")
    print(decile[["score_decile", "ref_pct", "det_pct", "population_shift",
                   "ref_avg_score", "det_avg_score", "psi_component"]].to_string(index=False))

# %% [markdown]
# ---
# ## Part 2: ML Platform Timeout / Default Scores

# %% Load timeout data
timeout = DefaultScoreAnalyzer()

# %% Daily summary
daily_timeout = timeout.daily_timeout_summary()
print("\nDaily Timeout Summary (sample):")
print(daily_timeout.head(20).to_string(index=False))

# %% Hourly pattern
hourly = timeout.hourly_pattern()
fig, ax = plt.subplots(figsize=(12, 5))
for channel in hourly["channel"].unique():
    ch = hourly[hourly["channel"] == channel]
    ax.plot(ch["decision_hour"], ch["default_rate"], label=channel, marker="o")
ax.set_title("Default Score Rate by Hour of Day")
ax.set_xlabel("Hour")
ax.set_ylabel("Default Score Rate (%)")
ax.legend(frameon=False)
plt.tight_layout()
plt.savefig("../output/plots/timeout_hourly_pattern.png", dpi=150)
plt.show()

# %% Outcome comparison
comparison = timeout.outcome_comparison()
print("\nOutcome Comparison: Default vs Model Scores:")
print(comparison.to_string(index=False))

# %% Revenue impact
impact = timeout.estimate_revenue_impact()
print(f"\n💰 Estimated Revenue Loss from Default Scoring: ${impact['total_estimated_loss']:,.2f}")
for ch, loss in impact["by_channel"].items():
    print(f"   {ch}: ${loss:,.2f}")

# %% Visualize
plotter.plot_timeout_rate(daily_timeout, save_path="../output/plots/timeout_rate_trend.png")
plt.show()

# %% [markdown]
# ---
# ## Part 3: Campaign Impression Takeover

# %% Load takeover data
takeover = CampaignTakeoverAnalyzer()

# %% Daily concentration
daily_conc = takeover.daily_concentration()
print("\nDaily Campaign Concentration:")
print(daily_conc.head(20).to_string(index=False))

# %% Takeover days
takeover_days = takeover.identify_takeover_days()
print(f"\n⚠️ Takeover Days: {len(takeover_days)}")
if len(takeover_days) > 0:
    print(takeover_days.to_string(index=False))

# %% Performance comparison
perf = takeover.campaign_performance_comparison()
print("\nDominant vs Other Campaigns:")
print(perf.to_string(index=False))

# %% Displaced campaigns
displaced = takeover.displaced_campaigns()
if len(displaced) > 0:
    print("\nMost Displaced Campaigns During Takeover:")
    print(displaced.head(10).to_string(index=False))

# %% Visualize
plotter.plot_campaign_concentration(takeover.df, save_path="../output/plots/campaign_concentration.png")
plt.show()

# %% [markdown]
# ---
# ## Summary Reports

# %% Generate all reports
from pathlib import Path
Path("../output/reports").mkdir(parents=True, exist_ok=True)

Path("../output/reports/propensity_drift_report.md").write_text(drift.generate_report())
Path("../output/reports/default_score_report.md").write_text(timeout.generate_report())
Path("../output/reports/campaign_takeover_report.md").write_text(takeover.generate_report())

print("\n📄 All anomaly reports saved to output/reports/")
