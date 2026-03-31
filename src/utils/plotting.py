"""Visualization utilities for funnel, competitor, and anomaly analysis."""

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns


class FunnelPlotter:
    """Creates funnel visualizations for messaging analytics."""

    STAGE_ORDER = [
        "eligible",
        "targeted",
        "sent",
        "delivered",
        "opened",
        "clicked",
        "converted",
    ]

    CHANNEL_COLORS = {
        "email": "#2196F3",
        "push": "#FF9800",
        "in_app": "#4CAF50",
    }

    def plot_funnel(
        self, df: pd.DataFrame, channel: str = "all", save_path: str = None
    ) -> plt.Figure:
        """Plot a horizontal funnel chart for a specific channel or aggregate."""
        fig, ax = plt.subplots(figsize=(12, 6))

        if channel != "all":
            data = df[df["channel"] == channel]
        else:
            data = df

        stage_cols = [
            "eligible_users", "targeted_users", "sent_users",
            "delivered_users", "opened_users", "clicked_users", "converted_users",
        ]
        totals = data[stage_cols].sum()

        colors = sns.color_palette("Blues_d", len(self.STAGE_ORDER))
        bars = ax.barh(
            range(len(self.STAGE_ORDER)),
            totals.values,
            color=colors,
            edgecolor="white",
            linewidth=0.5,
        )

        for i, (bar, val) in enumerate(zip(bars, totals.values)):
            pct = val / totals.values[0] * 100 if totals.values[0] > 0 else 0
            ax.text(
                bar.get_width() + totals.values[0] * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"{val:,.0f} ({pct:.1f}%)",
                va="center",
                fontsize=10,
            )

        ax.set_yticks(range(len(self.STAGE_ORDER)))
        ax.set_yticklabels([s.title() for s in self.STAGE_ORDER])
        ax.invert_yaxis()
        ax.set_title(f"Payment Alert Funnel — {channel.title()}", fontsize=14, pad=15)
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        plt.tight_layout()

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
        return fig

    def plot_daily_trend(
        self, df: pd.DataFrame, metric: str = "conversion_rate", save_path: str = None
    ) -> plt.Figure:
        """Plot daily trend of a funnel metric by channel."""
        fig, ax = plt.subplots(figsize=(14, 6))

        for channel, color in self.CHANNEL_COLORS.items():
            channel_data = df[df["channel"] == channel].sort_values("report_date")
            if len(channel_data) == 0:
                continue
            ax.plot(
                channel_data["report_date"],
                channel_data[metric],
                color=color,
                label=channel.replace("_", " ").title(),
                linewidth=2,
                marker="o",
                markersize=4,
            )

        ax.set_title(f"Daily {metric.replace('_', ' ').title()} by Channel", fontsize=14)
        ax.legend(frameon=False)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        plt.xticks(rotation=45)
        plt.tight_layout()

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
        return fig

    def plot_suppression_breakdown(
        self, df: pd.DataFrame, save_path: str = None
    ) -> plt.Figure:
        """Stacked bar chart of suppression reasons over time."""
        suppression_cols = [
            "suppressed_frequency_cap", "suppressed_priority",
            "suppressed_fatigue", "suppressed_holdout", "suppressed_competitor",
        ]
        labels = ["Freq Cap", "Priority", "Fatigue", "Holdout", "Competitor"]
        colors = ["#FF6B6B", "#4ECDC4", "#45B7D1", "#96CEB4", "#FFEAA7"]

        daily = df.groupby("report_date")[suppression_cols].sum()

        fig, ax = plt.subplots(figsize=(14, 6))
        daily.plot(kind="bar", stacked=True, ax=ax, color=colors, width=0.8)
        ax.legend(labels, frameon=False, loc="upper right")
        ax.set_title("Suppression Reasons Over Time", fontsize=14)
        ax.set_ylabel("Suppressed Users")
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        plt.xticks(rotation=45)
        plt.tight_layout()

        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
        return fig


class AnomalyPlotter:
    """Visualization for anomaly events: drift, timeouts, campaign takeover."""

    def plot_psi_heatmap(
        self, df: pd.DataFrame, save_path: str = None
    ) -> plt.Figure:
        """Heatmap of PSI components by channel and score decile."""
        pivot = df.pivot_table(
            index="channel",
            columns="score_decile",
            values="psi_component",
            aggfunc="sum",
        )
        fig, ax = plt.subplots(figsize=(12, 4))
        sns.heatmap(
            pivot, annot=True, fmt=".4f", cmap="YlOrRd",
            ax=ax, linewidths=0.5, cbar_kws={"label": "PSI Component"},
        )
        ax.set_title("Propensity Score Drift — PSI by Channel x Decile", fontsize=14)
        ax.set_xlabel("Score Decile")
        ax.set_ylabel("Channel")
        plt.tight_layout()
        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
        return fig

    def plot_timeout_rate(
        self, df: pd.DataFrame, save_path: str = None
    ) -> plt.Figure:
        """Daily default score rate with latency overlay."""
        fig, ax1 = plt.subplots(figsize=(14, 6))
        ax2 = ax1.twinx()

        daily = df.groupby("report_date").agg(
            default_rate=("default_score_pct", "mean"),
            p95_latency=("p95_latency_ms", "mean"),
        ).reset_index()

        ax1.bar(daily["report_date"], daily["default_rate"], color="#FF6B6B", alpha=0.7, label="Default Score %")
        ax2.plot(daily["report_date"], daily["p95_latency"], color="#2196F3", linewidth=2, marker="o", markersize=4, label="P95 Latency (ms)")

        ax1.set_ylabel("Default Score Rate (%)", color="#FF6B6B")
        ax2.set_ylabel("P95 Latency (ms)", color="#2196F3")
        ax1.set_title("ML Platform Health: Default Scores & Latency", fontsize=14)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left", frameon=False)
        ax1.spines["top"].set_visible(False)
        plt.xticks(rotation=45)
        plt.tight_layout()
        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
        return fig

    def plot_campaign_concentration(
        self, df: pd.DataFrame, save_path: str = None
    ) -> plt.Figure:
        """HHI index and top campaign share over time."""
        daily = df.groupby("report_date").agg(
            hhi=("hhi_index", "first"),
            max_share=("max_single_campaign_share", "first"),
            n_campaigns=("active_campaigns", "first"),
        ).reset_index()

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)

        ax1.fill_between(daily["report_date"], daily["hhi"], alpha=0.3, color="#FF9800")
        ax1.plot(daily["report_date"], daily["hhi"], color="#FF9800", linewidth=2)
        ax1.axhline(y=0.25, color="red", linestyle="--", alpha=0.5, label="HHI Threshold")
        ax1.set_ylabel("HHI Index")
        ax1.set_title("Campaign Concentration — Impression Takeover Analysis", fontsize=14)
        ax1.legend(frameon=False)

        ax2.bar(daily["report_date"], daily["max_share"] * 100, color="#4CAF50", alpha=0.7)
        ax2.axhline(y=40, color="red", linestyle="--", alpha=0.5, label="40% Threshold")
        ax2.set_ylabel("Top Campaign Share (%)")
        ax2.set_xlabel("Date")
        ax2.legend(frameon=False)

        plt.xticks(rotation=45)
        plt.tight_layout()
        if save_path:
            fig.savefig(save_path, dpi=150, bbox_inches="tight")
        return fig
