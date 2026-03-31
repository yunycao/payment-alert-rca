"""Intent priority tradeoff analysis.

Simulates the effect of re-ranking intent priorities on portfolio-level
outcomes. The key tradeoff: boosting one intent's priority necessarily
suppresses others. The question is whether that reallocation creates or
destroys net value.

Approach: counterfactual simulation using observed propensity scores
and priority assignments.
"""

import pandas as pd
import numpy as np
from typing import Optional


class PriorityTradeoffAnalyzer:
    """Simulates intent priority reallocation and measures portfolio impact."""

    def __init__(
        self,
        portfolio_data_path: str = "data/staging/portfolio_efficiency.parquet",
        competitor_data_path: str = "data/staging/competitor_data.parquet",
    ):
        self.portfolio_path = portfolio_data_path
        self.competitor_path = competitor_data_path
        self._portfolio: Optional[pd.DataFrame] = None
        self._competitor: Optional[pd.DataFrame] = None

    @property
    def portfolio(self) -> pd.DataFrame:
        if self._portfolio is None:
            self._portfolio = pd.read_parquet(self.portfolio_path)
        return self._portfolio

    @property
    def competitor(self) -> pd.DataFrame:
        if self._competitor is None:
            self._competitor = pd.read_parquet(self.competitor_path)
        return self._competitor

    def current_priority_allocation(self) -> pd.DataFrame:
        """Show current impression allocation by intent."""
        return (
            self.portfolio.groupby("intent_name")
            .agg({
                "users_messaged": "sum",
                "messages_sent": "sum",
                "same_intent_conversions": "sum",
                "attributed_revenue": "sum",
                "avg_propensity": "mean",
            })
            .assign(
                impression_share=lambda x: x["messages_sent"] / x["messages_sent"].sum(),
                cvr=lambda x: x["same_intent_conversions"] / x["users_messaged"],
                revenue_per_impression=lambda x: x["attributed_revenue"] / x["messages_sent"],
            )
            .sort_values("impression_share", ascending=False)
            .reset_index()
        )

    def simulate_reallocation(
        self,
        intent_name: str,
        share_change: float,  # e.g., +0.05 means give this intent 5pp more share
        donor_intent: Optional[str] = None,
    ) -> dict:
        """Simulate reallocating impression share from one intent to another.

        Estimates net portfolio impact using observed marginal conversion rates.
        Assumptions:
        - Marginal users reallocated have average propensity for the receiving intent
        - Donor intent loses its lowest-propensity users first (rational allocation)
        """
        current = self.current_priority_allocation()
        total_impressions = current["messages_sent"].sum()
        impressions_to_move = int(total_impressions * abs(share_change))

        target_row = current[current["intent_name"] == intent_name]
        if len(target_row) == 0:
            return {"error": f"Intent {intent_name} not found"}

        target_cvr = target_row["cvr"].values[0]
        target_rpi = target_row["revenue_per_impression"].values[0]

        # If no specific donor, take proportionally from all others
        if donor_intent:
            donor_rows = current[current["intent_name"] == donor_intent]
        else:
            donor_rows = current[current["intent_name"] != intent_name]

        donor_cvr = (donor_rows["same_intent_conversions"].sum() / donor_rows["users_messaged"].sum())
        donor_rpi = (donor_rows["attributed_revenue"].sum() / donor_rows["messages_sent"].sum())

        # Net impact
        gained_conversions = impressions_to_move * target_cvr
        lost_conversions = impressions_to_move * donor_cvr
        net_conversions = gained_conversions - lost_conversions

        gained_revenue = impressions_to_move * target_rpi
        lost_revenue = impressions_to_move * donor_rpi
        net_revenue = gained_revenue - lost_revenue

        return {
            "scenario": f"+{share_change*100:.0f}pp to {intent_name}" + (
                f" from {donor_intent}" if donor_intent else " from portfolio"
            ),
            "impressions_moved": impressions_to_move,
            "gained_conversions": int(gained_conversions),
            "lost_conversions": int(lost_conversions),
            "net_conversions": int(net_conversions),
            "gained_revenue": round(gained_revenue, 2),
            "lost_revenue": round(lost_revenue, 2),
            "net_revenue": round(net_revenue, 2),
            "target_cvr": round(target_cvr, 4),
            "donor_cvr": round(donor_cvr, 4),
            "cvr_advantage": round(target_cvr - donor_cvr, 4),
        }

    def pareto_frontier(self) -> pd.DataFrame:
        """Find the Pareto-optimal priority allocations.

        For each intent, compute the marginal value of +1% share.
        Intents on the frontier have the highest marginal value.
        """
        current = self.current_priority_allocation()
        scenarios = []

        for _, row in current.iterrows():
            sim = self.simulate_reallocation(row["intent_name"], share_change=0.01)
            scenarios.append({
                "intent_name": row["intent_name"],
                "current_share": round(row["impression_share"], 3),
                "marginal_cvr": sim["target_cvr"],
                "marginal_rpi": round(sim["gained_revenue"] / max(sim["impressions_moved"], 1), 4),
                "net_revenue_per_1pp": sim["net_revenue"],
                "net_conversions_per_1pp": sim["net_conversions"],
            })

        result = pd.DataFrame(scenarios).sort_values("net_revenue_per_1pp", ascending=False)
        result["priority_rank"] = range(1, len(result) + 1)
        return result

    def suppression_cost(self) -> pd.DataFrame:
        """Quantify the cost of competitor suppression on payment alert.

        For each competing intent that suppresses PA, estimate:
        - Revenue PA would have generated from those suppressed users
        - Revenue the competing intent actually generated
        - Net portfolio impact of the suppression decision
        """
        if len(self.competitor) == 0:
            return pd.DataFrame()

        comp = self.competitor.groupby("competitor_intent").agg({
            "users_suppressed_by_competitor": "sum",
            "avg_pa_propensity_suppressed_users": "mean",
        }).reset_index()

        # Estimate PA revenue from suppressed users using propensity as proxy
        pa_current = self.current_priority_allocation()
        pa_row = pa_current[pa_current["intent_name"] == "payment_alert"]
        pa_rpi = pa_row["revenue_per_impression"].values[0] if len(pa_row) > 0 else 0

        comp["estimated_pa_revenue_lost"] = (
            comp["users_suppressed_by_competitor"] * comp["avg_pa_propensity_suppressed_users"] * pa_rpi
        ).round(2)

        return comp

    def generate_report(self) -> str:
        """Generate priority tradeoff report."""
        lines = ["# Intent Priority Tradeoff Analysis\n"]

        current = self.current_priority_allocation()
        lines.append("## 1. Current Allocation\n")
        lines.append(current[["intent_name", "impression_share", "cvr",
                              "revenue_per_impression"]].to_markdown(index=False))

        pareto = self.pareto_frontier()
        lines.append("\n## 2. Marginal Value Ranking (Pareto Frontier)\n")
        lines.append(pareto.to_markdown(index=False))

        lines.append("\n## 3. Reallocation Scenarios\n")
        for change in [0.05, 0.10]:
            sim = self.simulate_reallocation("payment_alert", share_change=change)
            lines.append(f"\n**{sim['scenario']}**")
            lines.append(f"- Net conversions: {sim['net_conversions']:+,}")
            lines.append(f"- Net revenue: ${sim['net_revenue']:+,.2f}")
            lines.append(f"- CVR advantage: {sim['cvr_advantage']:+.4f}")

        cost = self.suppression_cost()
        if len(cost) > 0:
            lines.append("\n## 4. Suppression Cost by Competing Intent\n")
            lines.append(cost.to_markdown(index=False))

        return "\n".join(lines)
