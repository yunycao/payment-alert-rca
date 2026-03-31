#!/usr/bin/env python3
"""
Payment Alert RCA — Competitor Messaging Analysis
====================================================
Analyzes competing intents that suppress payment alert messages.

Covers:
- Top competing intents ranked by suppression impact
- Channel overlap matrix
- Segment vulnerability analysis
- Priority comparison
- Daily suppression trends
"""

# %% [markdown]
# # Payment Alert — Competitor Messaging Analysis

# %% Setup
import sys
sys.path.insert(0, "..")

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.competitor import CompetitorAnalyzer

sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["figure.dpi"] = 150

# %% Load data
analyzer = CompetitorAnalyzer()
print(f"Competitor data loaded: {len(analyzer.df):,} rows")

# %% [markdown]
# ## 1. Top Competing Intents

# %% Top competitors
top = analyzer.top_competitors(n=10)
print("Top 10 Competing Intents by Suppression Impact:")
print(top.to_string(index=False))

# Visualize
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(top["competitor_intent"], top["users_suppressed_by_competitor"], color="#FF6B6B")
ax.set_xlabel("Users Suppressed")
ax.set_title("Top Competing Intents — Suppression Impact on Payment Alert")
ax.invert_yaxis()
plt.tight_layout()
plt.savefig("../output/plots/top_competitors.png", dpi=150)
plt.show()

# %% [markdown]
# ## 2. Channel Overlap Matrix

# %% Channel x Competitor overlap
matrix = analyzer.channel_overlap_matrix()
print("\nChannel Overlap (% of PA eligible audience):")
print(matrix.to_string())

fig, ax = plt.subplots(figsize=(10, 8))
sns.heatmap(matrix, annot=True, fmt=".1f", cmap="YlOrRd", ax=ax, linewidths=0.5)
ax.set_title("Competitor Overlap by Channel (% of Eligible Audience)")
plt.tight_layout()
plt.savefig("../output/plots/competitor_overlap_heatmap.png", dpi=150)
plt.show()

# %% [markdown]
# ## 3. Segment Vulnerability

# %% Most vulnerable segments
vulnerable = analyzer.segment_vulnerability()
print("\nMost Vulnerable Segments:")
print(vulnerable.head(15).to_string(index=False))

# %% [markdown]
# ## 4. Priority Analysis

# %% Priority comparison
priority = analyzer.priority_analysis()
print("\nPriority Comparison:")
print(priority.to_string(index=False))

# %% [markdown]
# ## 5. Daily Suppression Trends

# %% Daily trend
daily = analyzer.daily_suppression_trend()
top_3_competitors = top["competitor_intent"].head(3).tolist()

fig, ax = plt.subplots(figsize=(14, 6))
for intent in top_3_competitors:
    intent_data = daily[daily["competitor_intent"] == intent].sort_values("report_date")
    ax.plot(intent_data["report_date"], intent_data["users_suppressed_by_competitor"],
            label=intent, linewidth=2, marker="o", markersize=3)
ax.set_title("Daily Suppression by Top 3 Competitors")
ax.set_ylabel("Users Suppressed")
ax.legend(frameon=False)
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("../output/plots/daily_competitor_suppression.png", dpi=150)
plt.show()

# %% Generate report
report = analyzer.generate_report()
from pathlib import Path
Path("../output/reports").mkdir(parents=True, exist_ok=True)
Path("../output/reports/competitor_report.md").write_text(report)
print("\n📄 Report saved to output/reports/competitor_report.md")
