# Payment Alert Intent — Root Cause Analysis & Ecosystem Impact

## Overview

End-to-end RCA framework for the **Payment Alert** messaging intent across email, push, and in-app channels. Goes beyond descriptive funnel metrics to answer causal and ecosystem-level questions: Is the observed lift truly incremental? Are we cannibalizing other intents? Does messaging improve or damage long-term user value? Is the portfolio allocation optimal?

## Project Structure

```
payment-alert-rca/
├── .claude/hooks/             # Claude Code hooks for automated SQL data pull
├── config/                    # Environment config, analysis parameters
├── sql/
│   ├── funnel/                # Full funnel: eligible → targeted → converted
│   ├── competitor/            # Competitor overlap & suppression queries
│   ├── anomaly/               # Drift, timeout, campaign takeover detection
│   └── ecosystem/             # Incrementality, cannibalization, LTV, portfolio
├── src/
│   ├── funnel/                # Funnel analysis engine
│   ├── competitor/            # Competitor messaging analysis
│   ├── anomaly/               # Anomaly classifiers (drift, timeout, takeover)
│   ├── ecosystem/             # Causal inference, cannibalization, LTV effects
│   │   ├── incrementality.py  # Holdout-based causal lift (DiD, stratified, power)
│   │   ├── cannibalization.py # Cross-intent overlap & net incrementality
│   │   ├── ltv_effects.py     # LTV trajectory, fatigue, health-adjusted value
│   │   └── portfolio.py       # Cross-intent efficiency & interaction effects
│   ├── tradeoffs/             # Portfolio optimization & tradeoff analysis
│   │   ├── priority_optimizer.py   # Pareto frontier, reallocation simulation
│   │   ├── channel_allocation.py   # Unit economics, diminishing returns, ROAS
│   │   └── frequency_optimization.py # Response curves, fatigue-adjusted optimum
│   ├── hooks/                 # Hook handler scripts
│   └── utils/                 # Snowflake connector, SQL renderer, plotting
├── notebooks/
│   ├── 01_full_funnel_analysis.py
│   ├── 02_competitor_analysis.py
│   ├── 03_anomaly_deep_dive.py
│   └── 04_ecosystem_impact.py       # Incrementality + cannibalization + LTV + tradeoffs
├── skills/                    # Claude subagent skill definitions
│   ├── propensity_drift/
│   ├── default_score_timeout/
│   ├── campaign_takeover/
│   └── ecosystem_impact/      # Ecosystem-level deep-dive skill
└── docs/plans/
```

## Quick Start

```bash
pip install -r requirements.txt
cp config/snowflake.env.example config/snowflake.env
# Edit snowflake.env with credentials, then:
jupyter lab notebooks/
```

## Analysis Layers

### Layer 1: Descriptive — What happened?
Full funnel analysis, suppression breakdowns, channel trends. (`notebooks/01`, `notebooks/02`)

### Layer 2: Diagnostic — Why did it happen?
Anomaly detection (propensity drift, ML timeout, campaign takeover), root cause classification. (`notebooks/03`)

### Layer 3: Causal — Is the impact real?
Holdout-based incrementality (DiD, stratified estimation), cannibalization quantification, LTV trajectory analysis. (`notebooks/04`, `src/ecosystem/`)

### Layer 4: Prescriptive — What should we change?
Priority reallocation simulation (Pareto frontier), channel efficiency frontier, frequency optimization with fatigue penalty. (`src/tradeoffs/`)

## Claude Hooks (Automated Data Pull)

| Hook | Trigger Pattern | Action |
|------|----------------|--------|
| `pull-funnel-data` | `.*pull.*(funnel\|data).*` | Snowflake → `data/staging/funnel_data.parquet` |
| `pull-competitor-data` | `.*competitor.*` | Snowflake → `data/staging/competitor_data.parquet` |
| `pull-anomaly-data` | `.*anomaly.*` | Snowflake → drift, timeout, takeover parquets |
| `validate-freshness` | `.*run.*(analysis\|notebook).*` | Checks all datasets for staleness (4h TTL) |

## Subagent Skills

| Skill | Scope |
|-------|-------|
| `propensity_drift` | PSI computation, KS testing, model version tracking, decile shift analysis |
| `default_score_timeout` | Latency diagnostics, outcome comparison (default vs model), revenue impact estimation |
| `campaign_takeover` | HHI concentration, displacement analysis, performance comparison |
| `ecosystem_impact` | Incrementality (DiD + stratified), cannibalization, LTV trajectory, portfolio tradeoffs |

## Key Metrics

| Metric | What It Answers | Method |
|--------|----------------|--------|
| Incremental Lift | How much conversion is caused by messaging? | Treatment vs holdout (DiD-adjusted) |
| Cannibalization Rate | How much is stolen from other intents? | Cross-intent overlap among converters |
| LTV Decay Rate | Does lift persist or erode over time? | 7d/30d/90d cohort comparison |
| Health-Adjusted LTV | What's the true value after fatigue? | LTV − (fatigue_rate × penalty_weight × LTV) |
| Marginal Rev/Impression | Which intent deserves more share? | Pareto frontier across portfolio |
| Optimal Frequency | How many messages maximize net value? | Response curve with fatigue penalty |
