---
name: default-score-timeout-analysis
description: "Deep-dive analysis on ML platform timeouts causing default propensity scores. Use when the scoring service times out and assigns fallback scores instead of real model predictions. Analyzes latency patterns, timeout rates, and downstream impact on messaging performance."
---

# Default Score / ML Platform Timeout Deep-Dive

## Overview

This skill investigates cases where the ML scoring platform fails to return predictions within the allowed SLA, causing the messaging system to fall back to default (usually 0.5) propensity scores. Default scores bypass the model's targeting precision, leading to mis-targeting and degraded conversion rates.

## When to Use

Invoke this skill when:
- Default score rate exceeds 2% of total decisions
- P95 latency exceeds the timeout threshold (typically 5000ms)
- Conversion rates diverge between model-scored and default-scored users
- Hourly spikes in timeout rates are observed

## Analysis Steps

### 1. Load Data
```python
import pandas as pd
timeout_data = pd.read_parquet("data/staging/default_scores.parquet")
```

### 2. Temporal Pattern Analysis
- Plot default score rate by hour-of-day to find peak timeout windows
- Overlay with total decision volume to distinguish load-driven vs systemic issues
- Compare weekday vs weekend patterns

### 3. Latency Distribution Analysis
- Plot P50, P95, P99 latency trends over time
- Identify if latency degradation is gradual (capacity) or sudden (incident)
- Check if specific channels have worse latency than others

### 4. Impact Assessment
Compare outcomes for default-scored vs model-scored users:
- Open rate differential
- Click rate differential
- Conversion rate differential
- Revenue per user differential
- Estimate total revenue loss: (model_conversion_rate - default_conversion_rate) × default_score_users × avg_revenue

### 5. Root Cause Categories
Classify timeout events into:
1. **Load spikes**: High volume periods overwhelming the scoring service
2. **Service degradation**: Gradual latency increase across all hours
3. **Cold start**: First requests after idle periods timing out
4. **Feature computation**: Specific features taking too long to compute
5. **Network issues**: Intermittent connectivity between messaging and ML platform

### 6. Output
Generate a markdown report with:
- Timeout rate time series with threshold line
- Latency percentile trends
- Outcome comparison table (default vs model)
- Estimated revenue impact
- Root cause classification
- Recommended mitigations

## Key Metrics

| Metric | Formula | Threshold |
|--------|---------|-----------|
| Default Score Rate | default_count / total_decisions | > 2% |
| P95 Latency | 95th percentile of model_latency_ms | > 5000ms |
| Conversion Gap | model_conv_rate - default_conv_rate | > 0 |
| Revenue Impact | conv_gap × default_users × avg_rev | $ estimate |
