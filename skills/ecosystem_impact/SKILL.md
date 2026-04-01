---
name: ecosystem-impact-analysis
description: "Deep-dive into ecosystem-level impact of messaging: incrementality via causal inference, cross-intent cannibalization, LTV effects, and portfolio-level efficiency. Use when investigating whether observed messaging lift is truly incremental, whether it displaces conversions from other intents, and whether it creates or destroys long-term user value."
---

# Ecosystem Impact Deep-Dive

## Overview

This skill performs system-level analysis that goes beyond single-intent funnel metrics. It answers the questions that separate descriptive analytics from causal understanding:

1. **Incrementality**: Is the observed lift causal, or would users have converted anyway?
2. **Cannibalization**: Are we stealing conversions from other intent pathways?
3. **LTV Effects**: Does messaging improve or damage long-term user engagement?
4. **Portfolio Efficiency**: Is the intent portfolio collectively optimal?

## When to Use

Invoke this skill when:
- Stakeholders question whether messaging lift is truly incremental
- Conversion attribution overlaps with other intents
- Long-term engagement metrics are declining despite stable short-term KPIs
- A priority reallocation or frequency change is being considered
- You need to quantify the full cost of an anomaly (not just the local impact)

## Analysis Workflow

### Phase 1: Incrementality Estimation

```python
from src.ecosystem import IncrementalityAnalyzer
inc = IncrementalityAnalyzer()

# Step 1: Always check balance first — imbalanced holdouts invalidate everything
balance = inc.balance_check()
# If any covariate has |SMD| > 0.1, flag and use DiD to adjust

# Step 2: Estimate lift with confidence intervals
lift = inc.estimate_lift("conversion_rate", group_by=["channel"])

# Step 3: DiD adjustment for residual imbalance
did = inc.did_estimate("conversion_rate")

# Step 4: Stratified estimation for heterogeneous effects
strat = inc.stratified_estimate("conversion_rate")

# Step 5: Power check — is our holdout large enough?
power = inc.power_analysis(baseline_rate=0.042, min_detectable_effect=0.005)
```

### Phase 2: Cannibalization Quantification

```python
from src.ecosystem import CannibalizationAnalyzer
cannibal = CannibalizationAnalyzer()

# Dual-exposure rates
overlap = cannibal.overlap_summary()

# Net incrementality after cannibalization adjustment
net = cannibal.estimate_net_incrementality(gross_lift=0.015, gross_lift_users=1900000)

# Temporal patterns: who messages first matters
temporal = cannibal.temporal_overlap_pattern()
```

### Phase 3: LTV and Engagement Health

```python
from src.ecosystem import LTVEffectsAnalyzer
ltv = LTVEffectsAnalyzer()

# Compare LTV at 7d, 30d, 90d windows
ltv_windows = ltv.ltv_lift_by_window()

# Classify lift trajectory: amplifying, sustaining, decaying, or harmful
trajectory = ltv.lift_decay_assessment()

# Measure fatigue externalities
fatigue = ltv.fatigue_indicators()
```

### Phase 4: Portfolio Tradeoff Analysis

```python
from src.tradeoffs import PriorityTradeoffAnalyzer, ChannelAllocationAnalyzer, FrequencyOptimizer

# Priority reallocation simulation
priority = PriorityTradeoffAnalyzer()
pareto = priority.pareto_frontier()  # Which intents should get more/less share?
sim = priority.simulate_reallocation("payment_alert", share_change=0.05)

# Channel efficiency frontier
channel = ChannelAllocationAnalyzer()
econ = channel.channel_unit_economics()  # ROAS by channel

# Frequency optimization
freq = FrequencyOptimizer()
optimal = freq.fit_response_curve(fatigue_penalty_weight=0.3)
```

## Key Metrics

| Metric | What It Measures | Good Range |
|--------|-----------------|------------|
| Incremental Lift | Causal conversion rate difference (treatment - holdout) | > 0 with p < 0.05 |
| Cannibalization Rate | % of conversions that overlap with other intents | < 20% |
| LTV Decay Rate | Change in relative lift from 7d to 90d | > -10% (no rapid decay) |
| Net Incremental Conversions | Gross incremental - cannibalized | > 0 |
| Fatigue Excess Rate | Unsubscribe rate (treatment - holdout) | < 0.1pp |
| Portfolio HHI | Impression concentration across intents | < 0.25 |
| Marginal Revenue Per Impression | Revenue of next impression for each intent | Compare across intents |
| Optimal Frequency | Messages/user/week that maximizes health-adjusted value | Varies by segment |

## Interpretation Framework

**Healthy ecosystem**: Positive incremental lift, low cannibalization, sustaining/amplifying LTV, balanced portfolio.

**Warning signs**: Decaying LTV, high cannibalization (>30%), rising fatigue, concentrated portfolio, frequency above optimal.

**Action triggers**: Negative 90d LTV lift, cannibalization > 40%, unsubscribe excess > 0.2pp, HHI > 0.25.

## Integration with Business Outcome RCA

When a spend drop or on-time rate decline is detected, the ecosystem analysis provides causal verification:

```python
# Check if spend/on-time lift is still positive (messaging still working)
spend_lift = inc.estimate_lift("avg_spend")
otp_lift = inc.estimate_lift("on_time_payment_rate")

# Track spend and on-time rate trajectories over time
from src.ecosystem import LTVEffectsAnalyzer
ltv = LTVEffectsAnalyzer()
spend_traj = ltv.spend_trajectory()          # 7d / 30d / 90d windows
otp_traj = ltv.on_time_rate_trajectory()     # 7d / 30d / 90d windows
spend_decay = ltv.outcome_decay_assessment("spend")
otp_decay = ltv.outcome_decay_assessment("on_time_rate")
```

**Critical question**: Is the drop messaging-driven or external?
- If holdout group also shows the drop → external factor (not messaging)
- If only treatment group drops → messaging is causing harm
- If causal lift is shrinking → messaging effectiveness degrading

See `skills/spend_drop_rca/` and `skills/ontime_rate_rca/` for full diagnostic workflows.
