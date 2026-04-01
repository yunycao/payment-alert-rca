---
name: ontime-rate-rca
description: "Root cause analysis for declining on-time payment rate in payment alert messaging. Use when the percentage of users making payments by their due date drops. Diagnoses whether the decline is driven by population mix shifts (more at-risk users), messaging effectiveness changes (lower engagement), model degradation (propensity drift, timeouts), or external factors (due date distribution, payment category shifts)."
---

# On-Time Payment Rate Root Cause Analysis

## Overview

This skill investigates why on-time payment rates are declining among users in the payment alert messaging funnel. On-time payment rate is the most direct measure of payment alert effectiveness — it answers "are our messages actually helping users pay on time?"

A decline can signal messaging ineffectiveness, but it can also be caused by upstream changes (harder-to-reach users entering the funnel) or external factors (economic conditions, due date clustering).

## When to Use

Invoke this skill when:
- On-time payment rate drops > 3pp week-over-week
- Late or missed payment counts are rising
- On-time rate declines in specific segments (at_risk, dormant) but holds in others
- The causal lift on on-time rate (treatment vs holdout) is shrinking or turning negative
- Stakeholders ask "are our payment alerts still working?"

## Analysis Workflow

### Phase 1: Detect the Drop

```python
from src.rca import RCAOrchestrator

rca = RCAOrchestrator()
detection = rca.detect_drop(metric="on_time_rate", window_days=7)

# Check severity and channel breakdown
print(f"Change: {detection['pct_change']:+.1f}%")
print(f"Severity: {detection['severity']}")
```

### Phase 2: Decompose — Why Is the Rate Falling?

```python
decomposition = rca.decompose(metric="on_time_rate")

for cause in decomposition["top_causes"]:
    print(f"#{cause['rank']}: {cause['explanation']} ({cause['contribution_pct']:+.1f}%)")
```

**Key decomposition dimensions for on-time rate**:

| Dimension | What Mix Shift Means | What Rate Change Means |
|-----------|---------------------|----------------------|
| segment | More at-risk/dormant users | Same users paying later |
| channel | Shift from push → email (lower urgency) | Channel effectiveness declining |
| propensity_decile | More low-propensity users targeted | Model accuracy degrading |
| payment_due_bucket | More distant-due users | Timing of alerts not matching urgency |
| score_source | More default-scored users | Default vs model gap widening |

### Phase 3: Causal Verification

Is the on-time rate drop a messaging problem, or an external trend?

```python
from src.ecosystem import IncrementalityAnalyzer

inc = IncrementalityAnalyzer()

# If holdout on-time rate ALSO dropped → external factor, not messaging
otp_lift = inc.estimate_lift("on_time_payment_rate")
otp_did = inc.did_estimate("on_time_payment_rate")

# Check if the LIFT is stable even if both groups dropped
# Stable lift + both drop = macro trend (not our fault)
# Declining lift = messaging effectiveness is degrading
```

**Decision gate**:
- Holdout rate dropped too → External factor (economy, seasonality)
- Only treatment dropped → Messaging is making things worse (harmful)
- Lift is shrinking → Messaging effectiveness is degrading
- Lift is stable, absolute rates dropped → Messaging still works, but harder population

### Phase 4: Cross-Reference with Anomalies

```python
anomalies = rca.cross_reference_anomalies()
```

**On-time rate specific anomaly hypotheses**:
- **Propensity drift** → Model is targeting users who will open but won't pay (engagement ≠ payment)
- **ML timeout** → Default scores deliver to non-responsive users who miss payments
- **Campaign takeover** → Generic campaign message lacks payment urgency, users ignore it
- **Competitor suppression** → Bill reminder or account balance alerts are suppressing payment alerts at critical timing windows

### Phase 5: Timing Analysis

On-time rate is uniquely sensitive to message timing relative to due date:

```python
from src.rca import MetricDecomposer

decomposer = MetricDecomposer()
due_bucket = decomposer.decompose_by_dimension("payment_due_bucket", metric="on_time_rate")

# If "0-3d" bucket has rate decline → users not getting alerts early enough
# If "15+d" bucket has rate decline → too early, message forgotten by due date
# If mix shifted toward "15+d" → alert timing strategy changed upstream
```

### Phase 6: Quantify Impact and Recommend

```python
impact = rca.quantify_impact(detection, decomposition)
recommendations = rca.generate_recommendations(decomposition, anomalies)

# Full pipeline
report = rca.generate_report(metric="on_time_rate")
```

## Key Metrics

| Metric | What It Measures | Alert Threshold |
|--------|-----------------|-----------------|
| On-Time Rate | % of users paying by due date | WoW drop > 3pp |
| Late Payment Count | Absolute late payments | WoW increase > 10% |
| Missed Payment Count | Users who never paid | WoW increase > 15% |
| Causal Lift (On-Time) | Treatment - holdout on-time rate | < 0 = harmful |
| Days-to-Due at Messaging | How early the alert was sent | Shift > 2 days |
| On-Time Rate by Segment | Per-segment on-time performance | at_risk < 60% |

## Diagnostic Decision Tree

```
On-time rate dropping?
├── Is holdout on-time rate also dropping?
│   ├── YES → External factor (seasonality, economic, due date clustering)
│   │   └── Check: due date distribution shifts, payment category mix
│   └── NO → Messaging-driven issue
│       ├── Is the causal lift shrinking?
│       │   ├── YES → Messaging effectiveness degrading
│       │   │   ├── Check message content/creative
│       │   │   ├── Check alert timing vs due date
│       │   │   └── Check frequency (fatigue)
│       │   └── NO → Population mix shift
│       │       ├── More at_risk or dormant users?
│       │       ├── Propensity model targeting different users?
│       │       └── Eligibility criteria expanded?
│       ├── Is propensity model drifting?
│       │   └── Drift may target "engagers" not "payers"
│       ├── Are default scores elevated?
│       │   └── Untargeted delivery to unlikely payers
│       └── Has alert timing relative to due date shifted?
│           └── "0-3d" bucket rate drop = urgency window missed
```

## Output

Generate a markdown report saved to `output/reports/ontime_rate_rca.md` containing:
1. Executive summary: severity, on-time rate change, late payment count change
2. Causal verification: is this messaging-driven or external?
3. Waterfall decomposition of top contributors
4. Timing analysis (days-to-due impact)
5. Anomaly correlation
6. Ranked recommendations
