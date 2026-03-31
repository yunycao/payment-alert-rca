"""Propensity score drift detection and analysis."""

import pandas as pd
import numpy as np
from scipy import stats
from typing import Optional


class PropensityDriftAnalyzer:
    """Detects and quantifies propensity score distribution drift."""

    def __init__(self, data_path: str = "data/staging/propensity_drift.parquet"):
        self.data_path = data_path
        self._df: Optional[pd.DataFrame] = None

    @property
    def df(self) -> pd.DataFrame:
        if self._df is None:
            self._df = pd.read_parquet(self.data_path)
        return self._df

    def compute_psi(self) -> pd.DataFrame:
        """Compute total PSI per channel from decile-level components."""
        return (
            self.df.groupby("channel")
            .agg(
                total_psi=("psi_component", "sum"),
                max_decile_psi=("psi_component", "max"),
                avg_score_shift=("avg_score_shift", "mean"),
                ref_model_versions=("ref_model_versions", "first"),
                det_model_versions=("det_model_versions", "first"),
            )
            .assign(
                drift_severity=lambda x: pd.cut(
                    x["total_psi"],
                    bins=[-np.inf, 0.1, 0.25, np.inf],
                    labels=["No Drift", "Moderate", "Significant"],
                )
            )
            .reset_index()
        )

    def decile_shift_analysis(self, channel: str) -> pd.DataFrame:
        """Analyze score distribution shifts at the decile level for a channel."""
        ch_data = self.df[self.df["channel"] == channel].copy()
        ch_data["population_shift"] = ch_data["det_pct"] - ch_data["ref_pct"]
        ch_data["relative_shift"] = (
            (ch_data["det_pct"] - ch_data["ref_pct"]) / ch_data["ref_pct"] * 100
        ).round(2)
        return ch_data

    def ks_test(self, channel: str) -> dict:
        """Perform Kolmogorov-Smirnov test on score distributions."""
        ch_data = self.df[self.df["channel"] == channel]

        # Reconstruct approximate distributions from decile summaries
        ref_scores = np.repeat(ch_data["ref_avg_score"].values, ch_data["ref_count"].values.astype(int))
        det_scores = np.repeat(ch_data["det_avg_score"].values, ch_data["det_count"].values.astype(int))

        ks_stat, p_value = stats.ks_2samp(ref_scores, det_scores)

        return {
            "channel": channel,
            "ks_statistic": round(ks_stat, 4),
            "p_value": p_value,
            "significant": p_value < 0.01,
            "ref_mean": round(np.mean(ref_scores), 4),
            "det_mean": round(np.mean(det_scores), 4),
            "mean_shift": round(np.mean(det_scores) - np.mean(ref_scores), 4),
        }

    def model_version_check(self) -> dict:
        """Check if model versions changed between windows."""
        ref_versions = set()
        det_versions = set()

        for _, row in self.df.iterrows():
            if pd.notna(row.get("ref_model_versions")):
                ref_versions.update(str(row["ref_model_versions"]).split(", "))
            if pd.notna(row.get("det_model_versions")):
                det_versions.update(str(row["det_model_versions"]).split(", "))

        return {
            "reference_versions": sorted(ref_versions),
            "detection_versions": sorted(det_versions),
            "version_changed": ref_versions != det_versions,
            "new_versions": sorted(det_versions - ref_versions),
        }

    def generate_report(self) -> str:
        """Generate comprehensive drift analysis report."""
        lines = ["# Propensity Score Drift Analysis\n"]

        psi_summary = self.compute_psi()
        lines.append("## PSI Summary by Channel\n")
        lines.append(psi_summary.to_markdown(index=False))

        version_info = self.model_version_check()
        lines.append(f"\n## Model Version Check\n")
        lines.append(f"- Reference versions: {', '.join(version_info['reference_versions'])}")
        lines.append(f"- Detection versions: {', '.join(version_info['detection_versions'])}")
        lines.append(f"- Version changed: **{version_info['version_changed']}**")

        lines.append("\n## KS Test Results\n")
        for channel in self.df["channel"].unique():
            ks = self.ks_test(channel)
            lines.append(f"- **{channel}**: KS={ks['ks_statistic']}, p={ks['p_value']:.6f}, "
                         f"significant={ks['significant']}, shift={ks['mean_shift']}")

        return "\n".join(lines)
