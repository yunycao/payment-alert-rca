---
name: rca-validation
description: "Validation layer for RCA investigations. Scores agent output on completeness (did it cover all 6 phases?) and conciseness (did it avoid redundant checks?). Tracks operational metrics: Mean Time to Detection (MTTD), False Discovery Rate (FDR), and Resolution Velocity. Invoke after every RCA run to ensure quality and eliminate waste."
---

# RCA Validation & Operational Metrics

## Overview

This skill validates the quality of root cause analysis investigations and tracks operational performance over time. It serves as the quality gate that prevents two failure modes:

1. **Incomplete investigations** — skipping phases leads to wrong conclusions
2. **Redundant investigations** — repeating checks wastes time and obscures signal

## When to Use

Invoke this skill:
- **After every RCA run** — validate before sharing findings with stakeholders
- **Periodically** — to review operational metrics (MTTD, FDR, Resolution Velocity)
- **When an investigation feels too long** — conciseness scoring identifies waste

## Validation Workflow

### Step 1: Run Validated RCA (Recommended)

```python
from src.rca import RCAOrchestrator

rca = RCAOrchestrator()
# run_validated_rca() includes validation automatically
result = rca.run_validated_rca(metric="avg_spend")

scorecard = result["validation"]
print(f"Score: {scorecard['combined_score']}/100 (Grade: {scorecard['combined_grade']})")
print(f"Action: {scorecard['action']}")  # accept / review / re-run / escalate
```

### Step 2: Inspect Scores

```python
# Completeness: did we cover all 6 phases?
comp = scorecard["completeness"]
print(f"Completeness: {comp['total_score']}/100")
for phase, score in comp["phase_scores"].items():
    status = "✅" if score > 10 else "❌"
    print(f"  {status} {phase}: {score:.1f}")
if comp["missing_phases"]:
    print(f"  ⚠️ Missing: {comp['missing_phases']}")

# Conciseness: any redundant work?
conc = scorecard["conciseness"]
print(f"\nConciseness: {conc['score']}/100")
for penalty in conc["penalties"]:
    print(f"  ⚠️ {penalty['type']}: {penalty['detail']} ({penalty['penalty']} pts)")
```

### Step 3: Record Investigation for Operational Metrics

```python
from src.rca import RCAValidator
from datetime import datetime

validator = RCAValidator()

# Record the investigation with timestamps
validator.record_investigation(
    rca_result=result,
    metric_drop_timestamp=datetime(2025, 3, 28, 6, 0),   # when metric started dropping
    detection_timestamp=datetime(2025, 3, 28, 10, 30),     # when system caught it
    resolution_timestamp=datetime(2025, 3, 29, 14, 0),     # when fix was deployed
    was_true_positive=True,                                  # was it a real issue?
)
```

### Step 4: Review Operational Dashboard

```python
# Mean Time to Detection
mttd = validator.compute_mttd()
print(f"MTTD: {mttd['mttd_hours']:.1f}h (target: <4h)")

# False Discovery Rate
fdr = validator.compute_fdr()
print(f"FDR: {fdr['fdr_pct']:.1f}% (target: <15%)")

# Resolution Velocity
rv = validator.compute_resolution_velocity()
print(f"Resolution Rate: {rv['resolution_rate_pct']:.1f}% (target: >80%)")
print(f"MTTR: {rv['mttr_hours']:.1f}h (target: <24h)")

# Full dashboard
report = validator.generate_report()
```

## Completeness Rubric (6 Required Phases)

| Phase | What It Checks | Full Credit |
|-------|---------------|-------------|
| Detection | Metric drop confirmed with magnitude | Change %, severity, per-channel breakdown |
| Decomposition | Root causes identified | ≥3 causes, classified (mix/rate), % contributions |
| Causal Verification | Treatment vs holdout compared | Holdout checked, external-vs-messaging classified, DiD |
| Anomaly Cross-Reference | Active signals checked | All 3 types checked, hypotheses formulated |
| Impact Quantification | Dollar estimate attached | Weekly loss, annualized, user count for context |
| Recommendations | Actionable next steps | ≥2 recommendations, prioritized, recovery estimates |

## Conciseness Penalties

| Penalty | Points | Trigger |
|---------|--------|---------|
| Duplicate data validation | -5 each | Data freshness checked >1 time |
| Redundant anomaly check | -5 each | Same anomaly type checked twice |
| Repeated finding | -10 each | Same root cause in multiple dimensions |
| Excessive dimensions | -3 | >15 contributors decomposed |
| Unactionable recommendation | -5 each | Recommendation without recovery estimate |

## Operational Metrics

### Mean Time to Detection (MTTD)
- **Definition**: Time between when the metric started dropping and when the system detected it
- **Target**: < 4 hours
- **Improvement levers**: More frequent data refresh, tighter alert thresholds, hook-based auto-detection

### False Discovery Rate (FDR)
- **Definition**: % of flagged drops that turned out to be noise, not real issues
- **Target**: < 15%
- **Improvement levers**: Higher severity thresholds, seasonal adjustment, minimum sample sizes

### Resolution Velocity
- **Resolution Rate**: % of investigations that led to a deployed fix
- **MTTR**: Mean time from detection to resolution
- **Target**: >80% resolution rate, <24h MTTR
- **Improvement levers**: Pre-built remediation playbooks, faster stakeholder alignment

## How Validation Eliminates Redundancy

The old workflow ran these checks sequentially (each loading data independently):
```
1. Check data validity          ← loads funnel data, validates schema
2. Check propensity drift       ← loads drift data, validates schema
3. Check timeout impact         ← loads timeout data, validates schema
4. Check competitor impact      ← loads competitor data, validates schema
5. Cross-reference findings     ← re-loads all data to correlate
```

The validated workflow consolidates:
```
1. Data hooks validate freshness once at session start
2. Consolidated anomaly check loads all 3 types in one pass
3. Single-pass cross-reference (no re-loading)
```

Result: ~40% fewer data loads, no repeated schema checks, no duplicate findings.
