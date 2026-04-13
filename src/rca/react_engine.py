"""ReAct Engine for Root Cause Analysis.

Implements the ReAct (Reasoning + Acting) framework from Yao et al. (ICLR 2023)
for dynamic, evidence-driven root cause analysis. Instead of a fixed 5-step
pipeline, the agent interleaves:

  THOUGHT — Reason about current evidence and decide what to investigate next
  ACTION  — Call a specific diagnostic method (detect, decompose, check anomaly, etc.)
  OBSERVATION — Receive the result and update the evidence state

The loop continues until the agent has sufficient evidence to conclude,
or hits a configurable maximum step count.

Key advantages over the fixed pipeline:
  1. ADAPTIVE — Skips irrelevant steps (e.g., no anomaly check if decomposition
     shows a clear population shift with no rate degradation)
  2. DEPTH-FIRST — Can drill deeper into a promising lead before moving on
  3. EVIDENCE-DRIVEN — Each action is justified by a reasoning trace, making
     the investigation auditable
  4. SELF-CORRECTING — If an action yields inconclusive results, the agent
     can reason about alternative hypotheses

Reference:
  Yao, S., et al. "ReAct: Synergizing Reasoning and Acting in Language Models."
  ICLR 2023. https://arxiv.org/abs/2210.03629
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from datetime import datetime
from enum import Enum


# ------------------------------------------------------------------ #
# Data structures
# ------------------------------------------------------------------ #

class StepType(Enum):
    THOUGHT = "thought"
    ACTION = "action"
    OBSERVATION = "observation"


@dataclass
class ReActStep:
    """A single step in the ReAct trace."""
    step_number: int
    step_type: StepType
    content: str
    action_name: Optional[str] = None
    action_args: Optional[dict] = None
    result: Any = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> dict:
        return {
            "step": self.step_number,
            "type": self.step_type.value,
            "content": self.content,
            "action_name": self.action_name,
            "action_args": self.action_args,
            "result": _serialize(self.result),
            "timestamp": self.timestamp,
        }


@dataclass
class ReActTrace:
    """Complete execution trace of a ReAct investigation."""
    metric: str
    steps: list[ReActStep] = field(default_factory=list)
    conclusion: Optional[str] = None
    evidence: dict = field(default_factory=dict)
    phases_covered: set = field(default_factory=set)
    start_time: str = field(default_factory=lambda: datetime.now().isoformat())
    end_time: Optional[str] = None

    def add_thought(self, content: str) -> ReActStep:
        step = ReActStep(
            step_number=len(self.steps) + 1,
            step_type=StepType.THOUGHT,
            content=content,
        )
        self.steps.append(step)
        return step

    def add_action(self, action_name: str, args: dict, description: str) -> ReActStep:
        step = ReActStep(
            step_number=len(self.steps) + 1,
            step_type=StepType.ACTION,
            content=description,
            action_name=action_name,
            action_args=args,
        )
        self.steps.append(step)
        return step

    def add_observation(self, content: str, result: Any = None) -> ReActStep:
        step = ReActStep(
            step_number=len(self.steps) + 1,
            step_type=StepType.OBSERVATION,
            content=content,
            result=result,
        )
        self.steps.append(step)
        return step

    def to_dict(self) -> dict:
        return {
            "metric": self.metric,
            "n_steps": len(self.steps),
            "phases_covered": list(self.phases_covered),
            "conclusion": self.conclusion,
            "evidence_keys": list(self.evidence.keys()),
            "start_time": self.start_time,
            "end_time": self.end_time,
            "steps": [s.to_dict() for s in self.steps],
        }

    def summary(self) -> str:
        """Human-readable summary of the reasoning trace."""
        lines = [f"## ReAct Trace: {self.metric} RCA", ""]
        for step in self.steps:
            prefix = {
                StepType.THOUGHT: "💭 THOUGHT",
                StepType.ACTION: "⚡ ACTION",
                StepType.OBSERVATION: "👁 OBSERVATION",
            }[step.step_type]
            lines.append(f"**Step {step.step_number} — {prefix}**")
            lines.append(f"  {step.content}")
            if step.action_name:
                lines.append(f"  → `{step.action_name}({step.action_args})`")
            lines.append("")
        if self.conclusion:
            lines.append(f"**CONCLUSION**: {self.conclusion}")
        return "\n".join(lines)


# ------------------------------------------------------------------ #
# Action Registry
# ------------------------------------------------------------------ #

@dataclass
class ActionSpec:
    """Specification for a registered action."""
    name: str
    description: str
    callable: Callable
    phase: str  # Which RCA phase this action covers
    args_schema: dict = field(default_factory=dict)


class ActionRegistry:
    """Maps action names to callable analyzer methods.

    The registry is the bridge between the ReAct agent's symbolic actions
    and the concrete diagnostic methods in the analyzers.
    """

    def __init__(self):
        self._actions: dict[str, ActionSpec] = {}

    def register(
        self,
        name: str,
        callable: Callable,
        description: str,
        phase: str,
        args_schema: Optional[dict] = None,
    ):
        self._actions[name] = ActionSpec(
            name=name,
            description=description,
            callable=callable,
            phase=phase,
            args_schema=args_schema or {},
        )

    def execute(self, name: str, **kwargs) -> Any:
        if name not in self._actions:
            raise ValueError(f"Unknown action: {name}. Available: {list(self._actions.keys())}")
        return self._actions[name].callable(**kwargs)

    def get_phase(self, name: str) -> str:
        return self._actions[name].phase

    def describe_actions(self) -> str:
        """Return a formatted list of available actions for the agent."""
        lines = []
        for name, spec in self._actions.items():
            lines.append(f"  - {name}: {spec.description} [phase: {spec.phase}]")
        return "\n".join(lines)

    @property
    def action_names(self) -> list[str]:
        return list(self._actions.keys())


# ------------------------------------------------------------------ #
# Reasoning Policies
# ------------------------------------------------------------------ #

class ReasoningPolicy:
    """Determines the next action based on current evidence state.

    This is the "brain" of the ReAct loop. In a full LLM-backed system,
    this would be a prompt to Claude. Here we implement a rule-based policy
    that captures the expert reasoning patterns from the RCA workflow.
    """

    # Priority-ordered investigation plan
    INVESTIGATION_SEQUENCE = [
        {
            "phase": "detection",
            "action": "detect_drop",
            "precondition": lambda evidence: "detection" not in evidence,
            "thought": "First, I need to confirm and quantify the metric drop. "
                       "Without knowing the magnitude and scope, I can't prioritize investigations.",
        },
        {
            "phase": "detection",
            "action": "detect_drop_secondary",
            "precondition": lambda evidence: (
                "detection" in evidence
                and evidence["detection"].get("is_drop")
                and "secondary_detection" not in evidence
            ),
            "thought": "The primary metric is dropping. Let me also check the secondary "
                       "business metric to understand if this is an isolated issue or systemic.",
        },
        {
            "phase": "decomposition",
            "action": "decompose",
            "precondition": lambda evidence: (
                "detection" in evidence
                and evidence["detection"].get("is_drop")
                and "decomposition" not in evidence
            ),
            "thought": "The drop is confirmed ({pct_change:+.1f}%, severity={severity}). "
                       "Now I need to decompose it: is this a population mix shift "
                       "(different users entering) or a rate degradation (same users, worse outcomes)?",
        },
        {
            "phase": "decomposition",
            "action": "decompose_dimension",
            "precondition": lambda evidence: (
                "decomposition" in evidence
                and evidence.get("_needs_dimension_drilldown")
                and "dimension_drilldown" not in evidence
            ),
            "thought": "The primary driver is in {driver_dimension}='{driver_value}'. "
                       "Let me drill deeper into this specific dimension to understand "
                       "the mechanism.",
        },
        {
            "phase": "anomaly_cross_ref",
            "action": "check_anomalies",
            "precondition": lambda evidence: (
                "decomposition" in evidence
                and "anomalies" not in evidence
            ),
            "thought": "Root causes identified. Now I need to cross-reference with "
                       "anomaly signals. If propensity drift or ML timeouts coincide "
                       "with the drop, that's a strong causal signal.",
        },
        {
            "phase": "causal_verification",
            "action": "check_incrementality",
            "precondition": lambda evidence: (
                "anomalies" in evidence
                and "incrementality" not in evidence
                and evidence.get("_has_anomaly_signal", False)
            ),
            "thought": "Anomaly signals detected. I need causal verification: "
                       "did the holdout group also experience the drop? "
                       "If yes → external factor. If no → messaging-driven.",
        },
        {
            "phase": "causal_verification",
            "action": "check_ltv_trajectory",
            "precondition": lambda evidence: (
                "decomposition" in evidence
                and "ltv_trajectory" not in evidence
                and evidence.get("_primary_cause_type") == "rate_degradation"
            ),
            "thought": "The drop is rate-driven, not mix-driven. Let me check the "
                       "LTV trajectory — if the treatment group shows decaying outcomes "
                       "over time, this may be a messaging fatigue problem.",
        },
        {
            "phase": "impact_quantification",
            "action": "quantify_impact",
            "precondition": lambda evidence: (
                "detection" in evidence
                and "decomposition" in evidence
                and "impact" not in evidence
            ),
            "thought": "I have the detection and decomposition results. "
                       "Now I need to translate the drop into dollar impact "
                       "to size the business urgency.",
        },
        {
            "phase": "recommendations",
            "action": "generate_recommendations",
            "precondition": lambda evidence: (
                "decomposition" in evidence
                and "impact" in evidence
                and "recommendations" not in evidence
            ),
            "thought": "Evidence gathering is complete. Time to generate "
                       "actionable recommendations ranked by expected recovery.",
        },
    ]

    @classmethod
    def next_action(cls, evidence: dict) -> Optional[dict]:
        """Determine the next action based on current evidence."""
        for plan in cls.INVESTIGATION_SEQUENCE:
            if plan["precondition"](evidence):
                # Format thought with evidence context
                thought = plan["thought"]
                detection = evidence.get("detection", {})
                decomp = evidence.get("decomposition", {})
                driver = decomp.get("primary_driver", {})
                thought = thought.format(
                    pct_change=detection.get("pct_change", 0),
                    severity=detection.get("severity", "unknown"),
                    driver_dimension=driver.get("dimension", ""),
                    driver_value=driver.get("dimension_value", ""),
                )
                return {
                    "action": plan["action"],
                    "phase": plan["phase"],
                    "thought": thought,
                }
        return None  # Investigation complete

    @classmethod
    def should_drilldown(cls, decomposition: dict) -> bool:
        """Determine if the primary cause warrants deeper investigation."""
        driver = decomposition.get("primary_driver", {})
        if not driver:
            return False
        # Drill down if single dimension explains >40% of the drop
        return abs(driver.get("contribution_pct", 0)) > 40

    @classmethod
    def generate_conclusion(cls, evidence: dict) -> str:
        """Synthesize a conclusion from all gathered evidence."""
        detection = evidence.get("detection", {})
        decomp = evidence.get("decomposition", {})
        anomalies = evidence.get("anomalies", [])
        impact = evidence.get("impact", {})
        recs = evidence.get("recommendations", [])

        parts = []

        # Metric summary
        metric = detection.get("metric", "metric")
        pct = detection.get("pct_change", 0)
        severity = detection.get("severity", "unknown")
        parts.append(
            f"{metric.replace('_', ' ').title()} dropped {abs(pct):.1f}% "
            f"WoW ({severity} severity)."
        )

        # Primary driver
        driver = decomp.get("primary_driver")
        if driver:
            cause_type = driver.get("cause_type", "unknown")
            if cause_type == "population_shift":
                parts.append(
                    f"Primary driver: population shift in "
                    f"{driver['dimension']}='{driver['dimension_value']}' "
                    f"({driver['contribution_pct']:+.1f}% of total change)."
                )
            elif cause_type == "rate_degradation":
                parts.append(
                    f"Primary driver: rate degradation in "
                    f"{driver['dimension']}='{driver['dimension_value']}' "
                    f"({driver['contribution_pct']:+.1f}% of total change)."
                )

        # Anomaly correlation
        if anomalies:
            high = [a for a in anomalies if a.get("severity") == "high"]
            if high:
                names = [a["anomaly"] for a in high]
                parts.append(f"High-severity anomalies correlated: {', '.join(names)}.")

        # Dollar impact
        for key in ["estimated_weekly_revenue_loss", "estimated_weekly_late_fee_impact"]:
            if key in impact:
                parts.append(f"Estimated weekly impact: ${impact[key]:,.2f}.")
                break

        # Top recommendation
        if recs:
            parts.append(f"Top recommendation: {recs[0].get('action', 'N/A')}.")

        return " ".join(parts)


# ------------------------------------------------------------------ #
# ReAct Engine
# ------------------------------------------------------------------ #

class ReActEngine:
    """Executes a ReAct reasoning loop for root cause analysis.

    The engine coordinates:
    1. A reasoning policy that decides what to investigate next
    2. An action registry that maps symbolic actions to analyzer methods
    3. A trace that records the full Thought-Action-Observation chain

    Usage:
        engine = ReActEngine(orchestrator)
        result = engine.run("avg_spend")
        print(result["trace"].summary())
    """

    def __init__(
        self,
        orchestrator,
        max_steps: int = 20,
        verbose: bool = True,
    ):
        """Initialize the ReAct engine with an RCA orchestrator.

        Args:
            orchestrator: RCAOrchestrator instance (provides all diagnostic methods)
            max_steps: Maximum T-A-O iterations before forced conclusion
            verbose: Print reasoning trace during execution
        """
        self.orchestrator = orchestrator
        self.max_steps = max_steps
        self.verbose = verbose
        self.registry = ActionRegistry()
        self._register_actions()

    def _register_actions(self):
        """Register all available diagnostic actions from the orchestrator."""
        o = self.orchestrator

        # Detection actions
        self.registry.register(
            name="detect_drop",
            callable=o.detect_drop,
            description="Confirm and quantify the primary metric drop (WoW comparison "
                        "with severity classification and per-channel breakdown)",
            phase="detection",
            args_schema={"metric": "str", "window_days": "int"},
        )

        self.registry.register(
            name="detect_drop_secondary",
            callable=self._detect_secondary,
            description="Check the secondary business metric for correlated drops "
                        "(if primary=spend, secondary=on_time_rate and vice versa)",
            phase="detection",
            args_schema={"primary_metric": "str"},
        )

        # Decomposition actions
        self.registry.register(
            name="decompose",
            callable=o.decompose,
            description="Run full mix-shift vs rate-change decomposition across all "
                        "dimensions and return ranked root causes",
            phase="decomposition",
            args_schema={"metric": "str"},
        )

        self.registry.register(
            name="decompose_dimension",
            callable=self._decompose_single_dimension,
            description="Deep-dive decomposition on a single dimension "
                        "(e.g., segment, channel, propensity_decile)",
            phase="decomposition",
            args_schema={"dimension": "str", "metric": "str"},
        )

        # Anomaly actions
        self.registry.register(
            name="check_anomalies",
            callable=o._consolidated_anomaly_check,
            description="Single-pass cross-reference of all anomaly signals "
                        "(propensity drift, ML timeouts, campaign takeover)",
            phase="anomaly_cross_ref",
            args_schema={},
        )

        # Causal verification actions
        self.registry.register(
            name="check_incrementality",
            callable=self._check_incrementality,
            description="Compare treatment vs holdout for the target metric "
                        "(causal verification: did holdout also drop?)",
            phase="causal_verification",
            args_schema={"metric": "str"},
        )

        self.registry.register(
            name="check_ltv_trajectory",
            callable=self._check_ltv_trajectory,
            description="Check spend and on-time rate trajectories over time "
                        "to detect fatigue or decay patterns",
            phase="causal_verification",
            args_schema={"metric": "str"},
        )

        # Impact quantification
        self.registry.register(
            name="quantify_impact",
            callable=self._quantify_impact,
            description="Translate metric drop into estimated dollar impact "
                        "(weekly revenue loss or late fee impact, annualized)",
            phase="impact_quantification",
            args_schema={},
        )

        # Recommendations
        self.registry.register(
            name="generate_recommendations",
            callable=self._generate_recommendations,
            description="Generate actionable recommendations ranked by expected "
                        "recovery percentage based on decomposition and anomaly findings",
            phase="recommendations",
            args_schema={},
        )

    # ------------------------------------------------------------------ #
    # Action wrappers (adapt orchestrator methods to ReAct interface)
    # ------------------------------------------------------------------ #

    def _detect_secondary(self, primary_metric: str = "avg_spend") -> dict:
        """Detect drop on the secondary metric."""
        secondary = "on_time_rate" if primary_metric == "avg_spend" else "avg_spend"
        return self.orchestrator.detect_drop(metric=secondary)

    def _decompose_single_dimension(
        self, dimension: str, metric: str = "avg_spend"
    ) -> pd.DataFrame:
        """Decompose along a single dimension for deeper investigation."""
        return self.orchestrator.decomposer.decompose_by_dimension(dimension, metric)

    def _check_incrementality(self, metric: str = "avg_spend") -> dict:
        """Run incrementality check if ecosystem analyzers are available."""
        try:
            from ..ecosystem.incrementality import IncrementalityAnalyzer
            inc = IncrementalityAnalyzer()
            col_map = {"avg_spend": "avg_spend", "on_time_rate": "on_time_payment_rate"}
            col = col_map.get(metric, metric)
            if col in inc.df.columns:
                lift = inc.estimate_lift(col)
                did = inc.did_estimate(col) if hasattr(inc, "did_estimate") else {}
                return {
                    "lift": lift.to_dict() if isinstance(lift, pd.DataFrame) else lift,
                    "did": did,
                    "holdout_compared": True,
                }
            return {"holdout_compared": False, "reason": f"Column {col} not in data"}
        except Exception as e:
            return {"holdout_compared": False, "error": str(e)}

    def _check_ltv_trajectory(self, metric: str = "avg_spend") -> dict:
        """Check LTV trajectory for decay patterns."""
        try:
            from ..ecosystem.ltv_effects import LTVEffectsAnalyzer
            ltv = LTVEffectsAnalyzer()
            if metric == "avg_spend":
                traj = ltv.spend_trajectory()
                decay = ltv.outcome_decay_assessment("spend")
            else:
                traj = ltv.on_time_rate_trajectory()
                decay = ltv.outcome_decay_assessment("on_time_rate")
            return {
                "trajectory": traj.to_dict() if isinstance(traj, pd.DataFrame) else traj,
                "decay_assessment": decay,
            }
        except Exception as e:
            return {"error": str(e)}

    def _quantify_impact(self) -> dict:
        """Quantify impact using evidence already gathered."""
        detection = self._current_evidence.get("detection", {})
        decomposition = self._current_evidence.get("decomposition", {})
        return self.orchestrator.quantify_impact(detection, decomposition)

    def _generate_recommendations(self) -> list[dict]:
        """Generate recommendations using evidence already gathered."""
        decomposition = self._current_evidence.get("decomposition", {})
        anomalies = self._current_evidence.get("anomalies", [])
        return self.orchestrator.generate_recommendations(decomposition, anomalies)

    # ------------------------------------------------------------------ #
    # Main ReAct Loop
    # ------------------------------------------------------------------ #

    def run(self, metric: str = "avg_spend") -> dict:
        """Execute the ReAct reasoning loop for root cause analysis.

        Returns:
            dict with keys:
                - detection, decomposition, anomaly_correlation, impact,
                  recommendations (same structure as run_full_rca)
                - trace: ReActTrace with full reasoning chain
                - n_steps: total T-A-O steps taken
                - phases_covered: set of RCA phases completed
        """
        trace = ReActTrace(metric=metric)
        self._current_evidence = {}
        self._current_metric = metric
        step_count = 0

        if self.verbose:
            print("=" * 70)
            print(f"  ReAct RCA Engine — Investigating: {metric}")
            print("=" * 70)

        while step_count < self.max_steps:
            # THOUGHT: What should I do next?
            next_action = ReasoningPolicy.next_action(self._current_evidence)

            if next_action is None:
                # No more actions needed — conclude
                trace.add_thought(
                    "All required evidence has been gathered. "
                    "I can now synthesize a conclusion."
                )
                break

            thought = next_action["thought"]
            trace.add_thought(thought)
            if self.verbose:
                print(f"\n💭 THOUGHT [{step_count + 1}]: {thought}")

            # ACTION: Execute the chosen diagnostic
            action_name = next_action["action"]
            action_args = self._build_action_args(action_name)
            trace.add_action(
                action_name=action_name,
                args=action_args,
                description=f"Calling {action_name}({action_args})",
            )
            if self.verbose:
                print(f"⚡ ACTION: {action_name}({action_args})")

            try:
                result = self.registry.execute(action_name, **action_args)
            except Exception as e:
                result = {"error": str(e)}

            # OBSERVATION: Interpret the result
            observation, evidence_key = self._interpret_result(action_name, result)
            trace.add_observation(observation, result)
            trace.phases_covered.add(next_action["phase"])

            if self.verbose:
                # Truncate long observations for readability
                display = observation[:200] + "..." if len(observation) > 200 else observation
                print(f"👁 OBSERVATION: {display}")

            # Update evidence state
            if evidence_key:
                self._current_evidence[evidence_key] = result

            # Update evidence signals for conditional branching
            self._update_signals(action_name, result)

            step_count += 1

        # CONCLUSION
        conclusion = ReasoningPolicy.generate_conclusion(self._current_evidence)
        trace.conclusion = conclusion
        trace.evidence = {k: _serialize(v) for k, v in self._current_evidence.items()}
        trace.end_time = datetime.now().isoformat()

        if self.verbose:
            print(f"\n{'=' * 70}")
            print(f"  CONCLUSION: {conclusion}")
            print(f"  Steps: {len(trace.steps)} | Phases: {trace.phases_covered}")
            print(f"{'=' * 70}")

        # Build output compatible with existing pipeline
        return self._build_output(trace)

    def _build_action_args(self, action_name: str) -> dict:
        """Construct arguments for the action based on current context."""
        metric = self._current_metric

        if action_name == "detect_drop":
            return {"metric": metric, "window_days": 7}
        elif action_name == "detect_drop_secondary":
            return {"primary_metric": metric}
        elif action_name == "decompose":
            return {"metric": metric}
        elif action_name == "decompose_dimension":
            driver = self._current_evidence.get("decomposition", {}).get("primary_driver", {})
            return {"dimension": driver.get("dimension", "segment"), "metric": metric}
        elif action_name == "check_anomalies":
            return {}
        elif action_name == "check_incrementality":
            return {"metric": metric}
        elif action_name == "check_ltv_trajectory":
            return {"metric": metric}
        elif action_name == "quantify_impact":
            return {}
        elif action_name == "generate_recommendations":
            return {}
        return {}

    def _interpret_result(self, action_name: str, result: Any) -> tuple[str, str]:
        """Interpret an action result into a human-readable observation.

        Returns (observation_text, evidence_key).
        """
        if isinstance(result, dict) and "error" in result:
            return f"Error: {result['error']}", ""

        if action_name == "detect_drop":
            d = result
            obs = (
                f"Metric {d.get('metric', '?')}: baseline={d.get('baseline_value')}, "
                f"current={d.get('current_value')}, change={d.get('pct_change', 0):+.1f}%, "
                f"severity={d.get('severity', '?')}. "
                f"Channels affected: {len(d.get('channel_breakdown', []))}."
            )
            return obs, "detection"

        elif action_name == "detect_drop_secondary":
            d = result
            is_also_dropping = d.get("is_drop", False)
            obs = (
                f"Secondary metric {d.get('metric', '?')}: "
                f"change={d.get('pct_change', 0):+.1f}%, "
                f"{'ALSO dropping' if is_also_dropping else 'stable'}."
            )
            return obs, "secondary_detection"

        elif action_name == "decompose":
            n_causes = result.get("n_contributors", 0)
            driver = result.get("primary_driver", {})
            obs = (
                f"Found {n_causes} contributing factors. "
                f"Primary driver: {driver.get('dimension', '?')}='{driver.get('dimension_value', '?')}' "
                f"({driver.get('cause_type', '?')}, {driver.get('contribution_pct', 0):+.1f}% of change)."
            )
            return obs, "decomposition"

        elif action_name == "decompose_dimension":
            if isinstance(result, pd.DataFrame) and not result.empty:
                obs = (
                    f"Dimension drilldown: {len(result)} sub-levels analyzed. "
                    f"Largest contributor: {result.iloc[0].get('dimension_value', '?')} "
                    f"({result.iloc[0].get('contribution_pct', 0):+.1f}%)."
                )
            else:
                obs = "Dimension drilldown returned no results."
            return obs, "dimension_drilldown"

        elif action_name == "check_anomalies":
            if result:
                names = [a["anomaly"] for a in result]
                severities = [a["severity"] for a in result]
                obs = (
                    f"Found {len(result)} anomaly signal(s): {names}. "
                    f"Severities: {severities}."
                )
            else:
                obs = "No active anomaly signals detected."
            return obs, "anomalies"

        elif action_name == "check_incrementality":
            if result.get("holdout_compared"):
                obs = "Holdout comparison completed. Causal lift data available."
            else:
                obs = f"Holdout comparison skipped: {result.get('reason', result.get('error', 'N/A'))}."
            return obs, "incrementality"

        elif action_name == "check_ltv_trajectory":
            decay = result.get("decay_assessment", {})
            pattern = decay.get("pattern", "unknown")
            obs = f"LTV trajectory pattern: {pattern.upper()}."
            return obs, "ltv_trajectory"

        elif action_name == "quantify_impact":
            for key in ["estimated_weekly_revenue_loss", "estimated_weekly_late_fee_impact"]:
                if key in result:
                    obs = f"Estimated weekly impact: ${result[key]:,.2f}."
                    return obs, "impact"
            obs = "Impact quantified (see details)."
            return obs, "impact"

        elif action_name == "generate_recommendations":
            if result:
                obs = (
                    f"Generated {len(result)} recommendation(s). "
                    f"Top: P{result[0].get('priority', '?')}: {result[0].get('action', '?')} "
                    f"(~{result[0].get('expected_recovery_pct', 0):.0f}% recovery)."
                )
            else:
                obs = "No recommendations generated."
            return obs, "recommendations"

        return f"Result received for {action_name}.", action_name

    def _update_signals(self, action_name: str, result: Any):
        """Update evidence signals used by ReasoningPolicy preconditions."""
        if action_name == "decompose":
            # Signal whether we need a dimension drilldown
            self._current_evidence["_needs_dimension_drilldown"] = (
                ReasoningPolicy.should_drilldown(result)
            )
            # Signal the primary cause type
            driver = result.get("primary_driver", {})
            self._current_evidence["_primary_cause_type"] = driver.get("cause_type")

        elif action_name == "check_anomalies":
            # Signal whether anomalies were found
            self._current_evidence["_has_anomaly_signal"] = bool(result)

    def _build_output(self, trace: ReActTrace) -> dict:
        """Build output dict compatible with existing RCA pipeline format."""
        evidence = self._current_evidence
        output = {
            "detection": evidence.get("detection", {}),
            "decomposition": evidence.get("decomposition", {}),
            "anomaly_correlation": evidence.get("anomalies", []),
            "impact": evidence.get("impact", {}),
            "recommendations": evidence.get("recommendations", []),
            "trace": trace,
            "n_steps": len(trace.steps),
            "phases_covered": trace.phases_covered,
            "conclusion": trace.conclusion,
            "mode": "react",
        }
        # Include secondary detection and causal verification if available
        if "secondary_detection" in evidence:
            output["secondary_detection"] = evidence["secondary_detection"]
        if "incrementality" in evidence:
            output["causal_verification"] = evidence["incrementality"]
        if "ltv_trajectory" in evidence:
            output["ltv_trajectory"] = evidence["ltv_trajectory"]
        return output


# ------------------------------------------------------------------ #
# Helpers
# ------------------------------------------------------------------ #

def _serialize(obj: Any) -> Any:
    """Make an object JSON-serializable for trace logging."""
    if isinstance(obj, pd.DataFrame):
        return obj.head(20).to_dict(orient="records") if not obj.empty else []
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_serialize(v) for v in obj]
    elif isinstance(obj, (ReActTrace, ReActStep)):
        return obj.to_dict()
    return obj
