#!/usr/bin/env python3
"""
Payment Alert RCA — Full Funnel Analysis
=========================================
Interactive analysis notebook for the payment alert messaging funnel.
Run as: jupyter lab notebooks/ or python notebooks/01_full_funnel_analysis.py

Covers:
- Funnel summary by channel (eligible → converted)
- Daily trend analysis with anomaly detection
- Suppression breakdown
- Segment performance comparison
- Scoring diagnostics
"""

# %% [markdown]
# # Payment Alert — Full Funnel Analysis

# %% Setup
import sys
sys.path.insert(0, "..")

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.funnel import FunnelAnalyzer
from src.utils.plotting import FunnelPlotter

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["figure.dpi"] = 150

# %% Load data
analyzer = FunnelAnalyzer()
plotter = FunnelPlotter()

print(f"Data loaded: {len(analyzer.df):,} rows")
print(f"Date range: {analyzer.df['report_date'].min()} to {analyzer.df['report_date'].max()}")
print(f"Channels: {analyzer.df['channel'].unique().tolist()}")

# %% [markdown]
# ## 1. Funnel Summary — All Channels

# %% Overall funnel
for channel in ["all", "email", "push", "in_app"]:
    print(f"\n{'='*60}")
    print(f"FUNNEL: {channel.upper()}")
    print(f"{'='*60}")
    summary = analyzer.funnel_summary(channel)
    print(summary.to_string(index=False))
    print()
    plotter.plot_funnel(analyzer.df, channel=channel, save_path=f"../output/plots/funnel_{channel}.png")

# %% [markdown]
# ## 2. Daily Trends

# %% Daily metrics
daily = analyzer.daily_metrics()

for metric in ["conversion_rate", "open_rate", "ctr", "targeting_rate"]:
    plotter.plot_daily_trend(daily, metric=metric, save_path=f"../output/plots/daily_{metric}.png")
    plt.show()

# %% [markdown]
# ## 3. Suppression Analysis

# %% Suppression breakdown
suppression = analyzer.suppression_analysis()
print("\nSuppression by Channel & Segment:")
print(suppression.to_string(index=False))

plotter.plot_suppression_breakdown(analyzer.df, save_path="../output/plots/suppression_breakdown.png")
plt.show()

# %% [markdown]
# ## 4. Segment Performance

# %% Segment comparison
segments = analyzer.segment_performance()
print("\nSegment Performance:")
print(segments.to_string(index=False))

# %% [markdown]
# ## 5. Scoring Diagnostics

# %% Score health
scoring = analyzer.scoring_diagnostics()
print("\nScoring Diagnostics (sample):")
print(scoring.head(20).to_string(index=False))

# %% [markdown]
# ## 6. Anomaly Detection

# %% Find anomalous days
anomalies = analyzer.find_anomalous_days(metric="conversion_rate", z_threshold=2.0)
if len(anomalies) > 0:
    print(f"\n⚠️ {len(anomalies)} anomalous days detected:")
    print(anomalies[["report_date", "channel", "conversion_rate", "rolling_mean", "z_score"]].to_string(index=False))
else:
    print("\n✅ No anomalous days detected in conversion rate.")

# %% Generate full report
report = analyzer.generate_report()
from pathlib import Path
Path("../output/reports").mkdir(parents=True, exist_ok=True)
Path("../output/reports/funnel_report.md").write_text(report)
print("\n📄 Report saved to output/reports/funnel_report.md")
