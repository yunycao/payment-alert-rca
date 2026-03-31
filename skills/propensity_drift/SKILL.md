---
name: propensity-drift-analysis
description: "Deep-dive analysis on propensity score drift for payment alert intent. Use when investigating shifts in ML model score distributions that affect targeting quality. Computes PSI, KS tests, distribution comparisons, and identifies root causes of drift."
---

# Propensity Score Drift Deep-Dive

## Overview

This skill performs a comprehensive analysis of propensity score drift for the payment alert messaging intent. It detects when the ML scoring model's output distribution has shifted relative to a baseline, quantifies the impact on targeting decisions, and identifies likely root causes.

## When to Use

Invoke this skill when:
- PSI (Population Stability Index) exceeds 0.1 for any channel
- KS test p-value < 0.01 between reference and detection windows
- Targeting rates have shifted without intentional model changes
- A new model version was deployed and needs validation

## Analysis Steps

### 1. Load and Validate Data
```python
import pandas as pd
drift_data = pd.read_parquet("data/staging/propensity_drift.parquet")
```

### 2. Compute PSI (Population Stability Index)
For each channel, compute total PSI by summing decile-level PSI components:
- PSI < 0.1: No significant drift
- 0.1 <= PSI < 0.25: Moderate drift — investigate
- PSI >= 0.25: Significant drift — immediate action required

### 3. Statistical Tests
- **Kolmogorov-Smirnov test**: Compare full CDFs between reference and detection windows
- **Jensen-Shannon divergence**: Symmetric measure of distribution difference
- **Decile shift analysis**: Identify which score ranges shifted most

### 4. Root Cause Investigation
Check these common causes in order:
1. **Model version change**: Did `model_version` change between windows?
2. **Feature drift**: Did input features shift (population mix, seasonality)?
3. **Data pipeline issues**: Were features missing or delayed?
4. **Population change**: Did the eligible audience composition change?

### 5. Impact Quantification
- How many users were mis-targeted due to drift?
- What is the estimated conversion rate loss?
- Which segments were most affected?

### 6. Output
Generate a markdown report with:
- PSI heatmap by channel x decile
- Score distribution overlay (reference vs detection)
- Root cause findings
- Recommended actions

## Key Metrics

| Metric | Formula | Threshold |
|--------|---------|-----------|
| PSI | Σ (det% - ref%) × ln(det% / ref%) | > 0.1 |
| KS Statistic | max|F_ref(x) - F_det(x)| | p < 0.01 |
| Mean Shift | avg_det - avg_ref | > 0.05 |
| Targeting Impact | Δ targeted users / eligible | > 5% |
