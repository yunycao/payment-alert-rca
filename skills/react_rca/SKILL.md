---
name: react-rca
description: "ReAct-powered root cause analysis using dynamic Thought-Action-Observation reasoning. Replaces the fixed 5-step pipeline with an adaptive investigation loop that decides what to check next based on evidence gathered so far. Invoke when a business metric drop needs diagnosis and the fixed pipeline either (a) runs unnecessary checks or (b) misses follow-up investigations. Based on Yao et al. 'ReAct: Synergizing Reasoning and Acting in Language Models' (ICLR 2023)."
---

# ReAct RCA — Adaptive Root Cause Analysis

## Overview

This skill uses the ReAct (Reasoning + Acting) framework to investigate business metric drops through a dynamic reasoning loop instead of a fixed pipeline. The agent reasons about what to investigate next based on evidence gathered so far, making the investigation both more efficient (skipping irrelevant steps) and more thorough (drilling deeper into promising leads).

### When to Use

- **Complex drops** where the fixed pipeline might miss nuance (e.g., a rate degradation that only affects one channel, where the standard pipeline would also check irrelevant anomaly signals)
- **Multi-metric drops** where spend and on-time rate are both declining and may share a root cause
- **Investigations requiring depth-first exploration** — the ReAct engine can drill into a specific dimension before moving to the next phase
- **Auditable investigations** — the full Thought-Action-Observation trace provides a reasoning chain that stakeholders can review

### When NOT to Use

- **Quick checks** — if you just need to confirm a drop exists, use `detect_drop()` directly
- **Standard weekly reviews** — the fixed pipeline (`run_full_rca()`) is sufficient for routine checks
- **When data is incomplete** — ReAct adapts to available data, but if most analyzers will fail, use the fixed pipeline which has better error handling for missing data

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ReAct Engine                          │
│                                                         │
│  ┌─────────┐    ┌─────────┐    ┌──────────────┐       │
│  │ THOUGHT │───▶│ ACTION  │───▶│ OBSERVATION  │──┐    │
│  │ (reason)│    │ (call)  │    │ (interpret)  │  │    │
│  └─────────┘    └─────────┘    └──────────────┘  │    │
│       ▲                                           │    │
│       └───────────── loop ───────────────────────┘    │
│                                                         │
│  ┌──────────────────────────────────────────────┐      │
│  │           Action Registry                     │      │
│  │  detect_drop → RCAOrchestrator.detect_drop   │      │
│  │  decompose   → RCAOrchestrator.decompose     │      │
│  │  check_anomalies → consolidated_anomaly_check│      │
│  │  check_incrementality → IncrementalityAnalyzer│     │
│  │  check_ltv_trajectory → LTVEffectsAnalyzer   │      │
│  │  quantify_impact → RCAOrchestrator.quantify  │      │
│  │  generate_recommendations → orchestrator     │      │
│  └──────────────────────────────────────────────┘      │
│                                                         │
│  ┌──────────────────────────────────────────────┐      │
│  │           Reasoning Policy                    │      │
│  │  Evidence state → next action decision        │      │
│  │  Conditional branching on cause_type          │      │
│  │  Drilldown triggers on high-contribution dims │      │
│  └──────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────┘
```

## Workflow

### Step 1: Launch ReAct Investigation

```python
from src.rca import RCAOrchestrator

rca = RCAOrchestrator()

# ReAct mode — adaptive reasoning loop
result = rca.run_react_rca(metric="avg_spend", verbose=True)

# Or with validation scoring
result = rca.run_validated_react_rca(metric="avg_spend")
```

### Step 2: Inspect the Reasoning Trace

```python
trace = result["trace"]

# Human-readable summary of all T-A-O steps
print(trace.summary())

# Programmatic access
for step in trace.steps:
    print(f"[{step.step_type.value}] {step.content}")
    if step.action_name:
        print(f"  → {step.action_name}({step.action_args})")

# What phases were covered?
print(f"Phases: {result['phases_covered']}")
print(f"Steps taken: {result['n_steps']}")
```

### Step 3: Review Conclusion

```python
print(result["conclusion"])
# e.g., "Avg Spend dropped 8.3% WoW (warning severity). Primary driver:
#  rate degradation in segment='dormant' (+42.1% of total change).
#  High-severity anomalies correlated: propensity_drift.
#  Estimated weekly impact: $12,450.00.
#  Top recommendation: Retrain or rollback propensity model."
```

### Step 4: Compare with Fixed Pipeline

```python
# Fixed pipeline for comparison
fixed_result = rca.run_full_rca(metric="avg_spend")

# ReAct typically takes fewer steps when the root cause is obvious,
# and more steps when it needs to drill deeper
print(f"ReAct steps: {result['n_steps']}")
print(f"Fixed steps: 5 (always)")

# Both produce the same output structure
print(result.keys())   # detection, decomposition, anomaly_correlation, impact, recommendations, trace, ...
print(fixed_result.keys())  # detection, decomposition, anomaly_correlation, impact, recommendations
```

## Reasoning Policy: How the Agent Decides

The reasoning policy implements these decision rules:

| Current Evidence State | Next Action | Reasoning |
|------------------------|-------------|-----------|
| No detection yet | `detect_drop` | Must confirm and quantify the drop first |
| Drop confirmed, no secondary | `detect_drop_secondary` | Check if both metrics are affected |
| Drop confirmed, no decomposition | `decompose` | Determine if mix-shift or rate-change |
| Primary driver >40% contribution | `decompose_dimension` | Drill deeper into dominant dimension |
| Decomposition done, no anomalies | `check_anomalies` | Cross-reference with system signals |
| Anomalies found | `check_incrementality` | Causal verification: holdout also dropped? |
| Rate degradation detected | `check_ltv_trajectory` | Check for fatigue/decay pattern |
| Detection + decomposition done | `quantify_impact` | Translate to dollar impact |
| All evidence gathered | `generate_recommendations` | Actionable next steps |

### Conditional Branching Examples

**Scenario A: Clear population shift, no anomalies**
```
THOUGHT → detect_drop → OBSERVATION (8% drop, warning)
THOUGHT → decompose → OBSERVATION (population shift in segment='new', 55%)
THOUGHT → decompose_dimension → OBSERVATION (drill into segment)
THOUGHT → check_anomalies → OBSERVATION (no signals)
THOUGHT → quantify_impact → OBSERVATION ($12K weekly)
THOUGHT → generate_recommendations → OBSERVATION (investigate eligibility changes)
CONCLUSION → 6 actions, skipped incrementality and LTV (not relevant)
```

**Scenario B: Rate degradation with anomaly signal**
```
THOUGHT → detect_drop → OBSERVATION (5% drop, warning)
THOUGHT → detect_drop_secondary → OBSERVATION (on-time also dropping)
THOUGHT → decompose → OBSERVATION (rate degradation in propensity_decile='1-3')
THOUGHT → check_anomalies → OBSERVATION (propensity drift detected, PSI=0.18)
THOUGHT → check_incrementality → OBSERVATION (holdout stable, messaging-driven)
THOUGHT → check_ltv_trajectory → OBSERVATION (decaying pattern)
THOUGHT → quantify_impact → OBSERVATION ($8K weekly)
THOUGHT → generate_recommendations → OBSERVATION (retrain model, check fatigue)
CONCLUSION → 8 actions, deeper investigation due to anomaly + rate cause
```

## Available Actions

| Action | Phase | Description |
|--------|-------|-------------|
| `detect_drop` | detection | Confirm metric drop with WoW comparison |
| `detect_drop_secondary` | detection | Check secondary metric for correlated drops |
| `decompose` | decomposition | Full waterfall decomposition across dimensions |
| `decompose_dimension` | decomposition | Single-dimension deep dive |
| `check_anomalies` | anomaly_cross_ref | Consolidated anomaly signal check |
| `check_incrementality` | causal_verification | Treatment vs holdout comparison |
| `check_ltv_trajectory` | causal_verification | LTV trajectory and decay assessment |
| `quantify_impact` | impact_quantification | Dollar impact estimation |
| `generate_recommendations` | recommendations | Ranked actionable recommendations |

## Key Metrics

| Metric | Description | Target |
|--------|-------------|--------|
| Steps taken | T-A-O iterations per investigation | 6-10 (adaptive) |
| Phases covered | % of 6 required phases completed | >80% |
| Completeness score | Validation layer score | >70/100 |
| Conciseness score | Redundancy penalty score | >80/100 |
| Drilldown rate | % of investigations that triggered dimension drilldown | Track (informational) |

## Integration with Validation

ReAct investigations are fully compatible with the validation layer:

```python
result = rca.run_validated_react_rca(metric="avg_spend")

# Same scorecard as fixed pipeline
scorecard = result["validation"]
print(f"Score: {scorecard['combined_score']}/100")
print(f"Grade: {scorecard['combined_grade']}")
print(f"Action: {scorecard['action']}")

# Plus ReAct-specific metadata
print(f"Mode: {result['mode']}")  # "react"
print(f"Steps: {result['n_steps']}")
print(f"Phases: {result['phases_covered']}")
```
