---
name: campaign-takeover-analysis
description: "Deep-dive analysis on large campaign impression takeover events. Use when a single campaign monopolizes the messaging impression inventory, crowding out other payment alert campaigns and reducing diversity. Analyzes concentration metrics (HHI), impression share trends, and performance impact."
---

# Campaign Impression Takeover Deep-Dive

## Overview

This skill investigates events where a single large campaign dominates the payment alert messaging inventory, consuming a disproportionate share of impressions and suppressing smaller, potentially better-performing campaigns. Campaign concentration reduces experimentation diversity and can mask intent-level performance issues.

## When to Use

Invoke this skill when:
- Any single campaign exceeds 40% impression share in a day
- Herfindahl-Hirschman Index (HHI) exceeds 0.25 (concentrated market)
- Active campaign count drops below expected minimum (5)
- Intent-level metrics decline while top campaign metrics remain stable

## Analysis Steps

### 1. Load Data
```python
import pandas as pd
takeover_data = pd.read_parquet("data/staging/campaign_takeover.parquet")
```

### 2. Concentration Analysis
- Compute daily HHI index across all campaigns
- Track impression share of top-1, top-3, top-5 campaigns over time
- Identify specific "takeover days" where thresholds are breached

### 3. Performance Comparison
For takeover days vs non-takeover days:
- Compare open rates, CTR, conversion rates
- Compare revenue per user and total revenue
- Check if the dominant campaign outperforms or underperforms the portfolio average

### 4. Displacement Analysis
Identify which campaigns were displaced:
- Campaigns active before takeover that lost volume
- New campaigns that couldn't get sufficient impressions
- Test campaigns in holdout/experiment groups that were starved

### 5. Root Cause Investigation
Common causes of campaign takeover:
1. **Budget misconfiguration**: Campaign budget set too high relative to intent allocation
2. **Broad targeting**: Campaign eligible audience is too broad, overlapping all segments
3. **Priority override**: Campaign given higher priority that preempts all others
4. **Scheduling conflict**: Campaign launch timing coincides with low-diversity window
5. **Missing caps**: No per-campaign impression cap configured

### 6. Output
Generate a markdown report with:
- HHI trend chart with threshold line
- Top campaign share stacked area chart
- Performance comparison table (takeover vs non-takeover days)
- Displaced campaign analysis
- Root cause findings
- Recommended guardrails

## Key Metrics

| Metric | Formula | Threshold |
|--------|---------|-----------|
| Impression Share | campaign_impressions / total_impressions | > 40% |
| HHI | Σ (share_i²) | > 0.25 |
| Campaign Diversity | COUNT(DISTINCT active campaigns) | < 5 |
| Performance Gap | portfolio_avg - dominant_campaign | varies |
