---
name: spend-drop-rca
description: "Root cause analysis for dropping spend in payment alert messaging. Use when total or average spend per user declines week-over-week. Runs the full RCA pipeline: detect → decompose (mix-shift vs rate-change) → cross-reference anomalies → quantify $ impact → recommend actions. Covers population mix shifts, propensity drift, ML timeouts, campaign takeover, and messaging effectiveness degradation."
---

# Spend Drop Root Cause Analysis

## Overview

This skill investigates why user spend is declining after payment alert messaging. Spend drops can be caused by upstream issues (eligibility changes, model drift) or downstream issues (message relevance, campaign fatigue, competitive suppression).

The analysis follows a 5-step diagnostic pipeline that separates correlation from causation and quantifies dollar impact.

## When to Use

Invoke this skill when:
- Average spend per user drops > 5% week-over-week
- Total spend declines despite stable or growing eligible audience
- Spend drops in specific channels or segments but not others
- Stakeholders ask "why is payment volume declining?"

## Analysis Workflow

### Phase 1: Detect and Quantify the Drop

```python
from src.rca import RCAOrchestrator

rca = RCAOrchestrator()
detection = rca.detect_drop(metric="avg_spend", window_days=7)

# Key outputs:
# - detection["pct_change"]: WoW percent change
# - detection["severity"]: critical / warning / minor
# - detection["channel_breakdown"]: per-channel impact
```

**Decision gate**: If `pct_change > -3%`, this may be normal variance. Check `detection["severity"]` before proceeding.

### Phase 2: Decompose into Root Causes

```python
decomposition = rca.decompose(metric="avg_spend")

# Key outputs:
# - decomposition["top_causes"]: ranked list of contributors
# - decomposition["primary_driver"]: single biggest factor
#   - cause_type: "population_shift" or "rate_degradation"
#   - dimension: which dimension (segment, channel, propensity_decile, etc.)
#   - contribution_pct: % of total drop explained
```

**Interpretation framework**:
- `population_shift` → The user mix changed (e.g., more dormant users, fewer high-LTV)
  - **Next step**: Check upstream eligibility, audience expansion, or seasonal effects
- `rate_degradation` → Same users, worse outcomes
  - **Next step**: Check message content, timing, competitive pressure, model performance

### Phase 3: Cross-Reference Anomaly Signals

```python
anomalies = rca.cross_reference_anomalies()

# Returns list of active anomaly signals:
# - propensity_drift: PSI > 0.1, model mis-targeting
# - ml_timeout: default scores bypassing model precision
# - campaign_takeover: single campaign crowding out targeted delivery
```

**Correlation check**: If an anomaly's timing aligns with the spend drop onset, it's a strong candidate root cause. If the anomaly pre-dates the drop by > 7 days, it's likely not the cause.

### Phase 4: Quantify Dollar Impact

```python
impact = rca.quantify_impact(detection, decomposition)

# Key outputs:
# - impact["estimated_weekly_revenue_loss"]
# - impact["annualized_loss"]
```

### Phase 5: Generate Recommendations

```python
recommendations = rca.generate_recommendations(decomposition, anomalies)

# Returns prioritized actions with expected recovery estimates
```

### Full Pipeline (One-Shot)

```python
results = rca.run_full_rca(metric="avg_spend")
report = rca.generate_report(metric="avg_spend")
```

## Key Metrics

| Metric | What It Measures | Alert Threshold |
|--------|-----------------|-----------------|
| Avg Spend / User | Mean payment amount post-messaging | WoW drop > 5% |
| Total Spend | Aggregate payment volume | WoW drop > 5% |
| Spend Lift (Causal) | Treatment - holdout spend | Negative lift |
| Spend Decay Rate | 90d spend lift ÷ 7d spend lift | < 0.5 (rapid decay) |
| Mix-Shift Contribution | % of drop from population change | > 50% = upstream issue |
| Rate-Change Contribution | % of drop from within-group decline | > 50% = effectiveness issue |

## Diagnostic Decision Tree

```
Spend dropping?
├── Is eligible audience size changing?
│   ├── YES → Check eligibility pipeline, seasonal patterns, holdout leakage
│   └── NO → Population mix stable, investigate rate degradation
│       ├── Is propensity model drifting? (PSI > 0.1)
│       │   ├── YES → Mis-targeting: wrong users receiving alerts
│       │   └── NO → Model is stable
│       ├── Are ML timeouts elevated? (default rate > 2%)
│       │   ├── YES → Default scores bypassing targeting precision
│       │   └── NO → Scoring is healthy
│       ├── Is a campaign dominating? (HHI > 0.25)
│       │   ├── YES → Campaign takeover reducing message relevance
│       │   └── NO → Campaign diversity is fine
│       └── Is message effectiveness declining?
│           ├── Check open rate, CTR, conversion rate trends
│           ├── Check competitor suppression rates
│           └── Check frequency saturation (fatigue)
```

## Output

Generate a markdown report saved to `output/reports/spend_drop_rca.md` containing:
1. Executive summary with severity and dollar impact
2. Waterfall chart of top contributors (mix vs rate effects)
3. Anomaly correlation findings
4. Ranked recommendations with expected recovery
