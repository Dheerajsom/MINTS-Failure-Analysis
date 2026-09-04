from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd
import scipy.stats as st

matplotlib.use("Agg")
import matplotlib.pyplot as plt


from safe.loader import load_pivoted_dataframe
from safe.periods import apply_hard_bounds

CSV_PATH = Path(__file__).resolve().parent / "mintsXU4/data/valo_node_01_full_year.csv"
OUT_DIR = Path(__file__).resolve().parent / "mintsXU4/output"

FIELDS = [
    {"field": "pm1_0", "unit": "ug/m^3", "slug": "pm1_0", "label": "PM1.0"},
    {"field": "temperature", "unit": "C", "slug": "temperature", "label": "Temperature"},
]

COMPARISONS = [
    {
        "name": "Last 2 Days",
        "slug": "last2days",
        "samples": [
            ("Day 1 (2026-05-26)", "2026-05-26", "2026-05-26", "steelblue"),
            ("Day 2 (2026-05-27)", "2026-05-27", "2026-05-27", "darkorange"),
        ],
    },
    {
        "name": "Last 2 Weeks",
        "slug": "last2weeks",
        "samples": [
            ("Week 1 (2026-05-14 to 2026-05-20)", "2026-05-14", "2026-05-20", "steelblue"),
            ("Week 2 (2026-05-21 to 2026-05-27)", "2026-05-21", "2026-05-27", "darkorange"),
        ],
    },
    {
        "name": "Last 2 Months",
        "slug": "last2months",
        "samples": [
            ("Month 1 (2026-03-28 to 2026-04-27)", "2026-03-28", "2026-04-27", "steelblue"),
            ("Month 2 (2026-04-28 to 2026-05-27)", "2026-04-28", "2026-05-27", "darkorange"),
        ],
    },
    {
        "name": "May 2025 vs May 2026",
        "slug": "may2025_vs_may2026",
        "samples": [
            ("May 2025", "2025-05-01", "2025-05-31", "steelblue"),
            ("May 2026", "2026-05-01", "2026-05-31", "darkorange"),
        ],
    },
]


def field_dirs(slug):
    hist_dir = OUT_DIR / slug / "histograms"
    stats_dir = OUT_DIR / slug / "stats"
    hist_dir.mkdir(parents=True, exist_ok=True)
    stats_dir.mkdir(parents=True, exist_ok=True)
    return hist_dir, stats_dir


def zscore(values):
    values = np.asarray(values, dtype=float)
    sd = values.std(ddof=0)
    if sd <= 1e-12:
        return np.zeros_like(values)
    return (values - values.mean()) / sd


def normal_test_rows(field_label, comparison_name, sample_name, values):
    z = zscore(values)
    ks_stat = st.kstest(z, "norm").statistic
    if len(values) >= 8 and np.std(values) > 1e-12:
        normal_stat, normal_p = st.normaltest(values)
    else:
        normal_stat, normal_p = np.nan, np.nan
    return [
        {
            "field": field_label,
            "comparison": comparison_name,
            "sample": sample_name,
            "test": "One-sample KS vs standard normal after z-scoring",
            "statistic": round(float(ks_stat), 6),
            "p_value": None,  # fitted mean/std invalidate the ordinary KS null distribution
            "n": len(values),
            "mean": round(float(np.mean(values)), 6),
            "std": round(float(np.std(values, ddof=0)), 6),
        },
        {
            "field": field_label,
            "comparison": comparison_name,
            "sample": sample_name,
            "test": "D'Agostino-Pearson normality test",
            "statistic": round(float(normal_stat), 6),
            "p_value": float(normal_p) if np.isfinite(normal_p) else None,
            "n": len(values),
            "mean": round(float(np.mean(values)), 6),
            "std": round(float(np.std(values, ddof=0)), 6),
        },
    ]


def plot_standard_normal_comparison(field_label, comparison, period_samples, hist_dir, field_slug):
    fig, ax = plt.subplots(figsize=(10.5, 6))

    x_norm = np.linspace(-4, 4, 600)
    ax.plot(x_norm, st.norm.pdf(x_norm), color="black", linewidth=2.2, label="Standard normal N(0, 1)")

    for period_name, values, color in period_samples:
        z = zscore(values)
        ax.hist(
            z,
            bins="auto",
            density=True,
            alpha=0.42,
            color=color,
            edgecolor="white",
            linewidth=0.4,
            label=f"{period_name} z-scored data",
        )

    ax.set_title("Z-Scored Histogram vs Standard Normal", fontweight="bold")
    ax.set_xlabel("Standardized value (z-score)")
    ax.set_ylabel("Density")
    ax.legend(fontsize=8)
    ax.grid(alpha=0.25)
    fig.suptitle(f"{field_label} Standard Normal Distribution Test - {comparison['name']}", fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94])

    out_path = hist_dir / f"{field_slug}_{comparison['slug']}_standard_normal_test.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {out_path}")


def main():
    raw, metrics = load_pivoted_dataframe(CSV_PATH)
    if raw is None:
        return 1
    if raw["_sensor_name"].nunique() != 1:
        raise ValueError("Distribution visualizer requires a single sensor export")
    raw = apply_hard_bounds(raw, metrics)

    for fdef in FIELDS:
        field = fdef["field"]
        field_slug = fdef["slug"]
        field_label = fdef["label"]
        hist_dir, stats_dir = field_dirs(field_slug)

        if field not in metrics:
            continue
        df = raw[[field]].rename(columns={field: "_value"}).dropna()

        all_rows = []
        for comparison in COMPARISONS:
            period_samples = []
            comparison_rows = []

            for sample_name, start, end, color in comparison["samples"]:
                values = df.loc[start:end, "_value"].to_numpy(dtype=float)
                if len(values) == 0:
                    print(f"Skipping {field_label} {sample_name}: no data")
                    continue
                period_samples.append((sample_name, values, color))
                comparison_rows.extend(normal_test_rows(field_label, comparison["name"], sample_name, values))

            if len(period_samples) < 2:
                continue

            plot_standard_normal_comparison(field_label, comparison, period_samples, hist_dir, field_slug)
            stats_path = stats_dir / f"{field_slug}_{comparison['slug']}_standard_normal_results.csv"
            pd.DataFrame(comparison_rows).to_csv(stats_path, index=False)
            print(f"Saved {stats_path}")
            all_rows.extend(comparison_rows)

        if all_rows:
            combined_path = stats_dir / f"{field_slug}_standard_normal_results.csv"
            pd.DataFrame(all_rows).to_csv(combined_path, index=False)
            print(f"Saved {combined_path}")


if __name__ == "__main__":
    raise SystemExit(main())
