import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as st
from pathlib import Path

CSV_PATH = "mintsXU4/data/valo_node_01_full_year.csv"
OUT_DIR  = Path("mintsXU4/output")

# Field-specific output dirs — created per-field below
def field_dirs(slug):
    hist_dir  = OUT_DIR / slug / "histograms"
    stats_dir = OUT_DIR / slug / "stats"
    hist_dir.mkdir(parents=True, exist_ok=True)
    stats_dir.mkdir(parents=True, exist_ok=True)
    return hist_dir, stats_dir

FIELDS = [
    {"field": "pm1_0",       "unit": "µg/m³", "slug": "pm1_0",       "label": "PM1.0"},
    {"field": "temperature", "unit": "°C",     "slug": "temperature", "label": "Temperature"},
]

CANDIDATES = [
    ("Normal",     st.norm),
    ("Log-Normal", st.lognorm),
    ("Gamma",      st.gamma),
    ("Weibull",    st.weibull_min),
    ("Log-Gamma",  st.loggamma),
    ("Burr",       st.burr),
    ("Inv-Gamma",  st.invgamma),
    ("Gen-Gamma",  st.gengamma),
    ("Exp",        st.expon),
    ("Beta",       st.beta),
]
COLORS = ["steelblue", "darkorange"]


def best_fit(data):
    results = []
    for name, dist in CANDIDATES:
        try:
            params = dist.fit(data)
            ks, p  = st.kstest(data, dist.cdf, args=params)
            results.append((ks, p, name, dist, params))
        except Exception:
            pass
    return sorted(results, key=lambda r: r[0])


def overlay_histogram(ax, s1, s2, label1, label2, title, xlabel):
    for i, (data, color, label) in enumerate(zip([s1, s2], COLORS, [label1, label2])):
        _, bins, _ = ax.hist(data, bins="auto", color=color, edgecolor="white",
                             linewidth=0.4, alpha=0.4, density=True, label=label)
        ks, _, name, dist, params = best_fit(data)[0]
        x = np.linspace(bins[0], bins[-1], 600)
        ax.plot(x, dist.pdf(x, *params), color=color, linewidth=2, alpha=1.0,
                label=f"Period {i+1} best fit: {name} (KS={ks:.4f})")
    ax.set_xlabel(xlabel, fontsize=11)
    ax.set_ylabel("Density", fontsize=11)
    ax.set_title(title, fontsize=13, fontweight="bold")
    ax.legend(fontsize=8)


def run_nonparametric_tests(s1, s2):
    results = {}

    ks_stat, ks_p = st.ks_2samp(s1, s2)
    results["KS"] = {"statistic": round(ks_stat, 6), "p_value": round(ks_p, 6)}

    try:
        ad = st.anderson_ksamp([s1, s2])
        results["AD"] = {"statistic": round(ad.statistic, 6), "p_value": round(ad.pvalue, 6)}
    except Exception as e:
        results["AD"] = {"statistic": None, "p_value": None, "error": str(e)}

    mw_stat, mw_p = st.mannwhitneyu(s1, s2, alternative="two-sided")
    results["MWU"] = {"statistic": round(mw_stat, 6), "p_value": round(mw_p, 6)}

    try:
        es_stat, es_p = st.epps_singleton_2samp(s1, s2)
        results["ES"] = {"statistic": round(es_stat, 6), "p_value": round(es_p, 6)}
    except Exception as e:
        results["ES"] = {"statistic": None, "p_value": None, "error": str(e)}

    return results


def sig_label(p):
    if p is None:   return "N/A"
    if p < 0.001:   return "p<0.001 ***"
    if p < 0.01:    return f"p={p:.4f} **"
    if p < 0.05:    return f"p={p:.4f} *"
    return f"p={p:.4f} (ns)"


def write_md(field_label, unit, period_label, label1, label2, n1, n2, test_results, filepath):
    descriptions = {
        "KS": (
            "## 1. Two-Sample Kolmogorov-Smirnov (KS) Test",
            "Measures the maximum absolute difference between the two empirical CDFs. "
            "Sensitive to any difference in shape, location, or scale. "
            "A small p-value means the two samples are unlikely to come from the same distribution.",
        ),
        "AD": (
            "## 2. Anderson-Darling (AD) Test",
            "Similar to KS but weights differences in the tails more heavily. "
            "More powerful for detecting shifts in extreme readings. "
            "A small p-value indicates the distributions differ, especially in their tails.",
        ),
        "MWU": (
            "## 3. Mann-Whitney U Test",
            f"A rank-based test that checks whether one period tends to have systematically "
            f"higher {field_label} values than the other (stochastic dominance). "
            "Does not assume any distribution shape. "
            "A small p-value means one period had significantly higher/lower values overall.",
        ),
        "ES": (
            "## 4. Epps-Singleton Test",
            "Compares the empirical characteristic functions of the two samples. "
            "Effective even on small samples and can detect differences that KS/AD miss. "
            f"A small p-value indicates the two {field_label} distributions are statistically different.",
        ),
    }

    conclusions = []
    body = []
    for key, (header, desc) in descriptions.items():
        r    = test_results[key]
        stat = r["statistic"]
        p    = r["p_value"]
        body += [header, "",
                 f"**Statistic:** {stat}  ",
                 f"**p-value:** {sig_label(p)}  ",
                 "", f"_{desc}_", ""]
        if p is None:
            verdict = "Could not be computed."
        elif p < 0.05:
            verdict = (f"The two periods are **statistically different** ({key} test, {sig_label(p)}). "
                       "The null hypothesis that both samples come from the same distribution is rejected.")
            conclusions.append(f"- **{key}**: distributions differ significantly")
        else:
            verdict = (f"No statistically significant difference detected ({key} test, {sig_label(p)}). "
                       "Cannot rule out that both periods come from the same distribution.")
            conclusions.append(f"- **{key}**: no significant difference (p≥0.05)")
        body += [f"**Interpretation:** {verdict}", "", "---", ""]

    lines = [
        f"# Non-Parametric Test Results: {field_label} — {period_label}",
        "",
        f"**Field:** {field_label} ({unit})  ",
        f"**Period 1:** {label1} — {n1} data points  ",
        f"**Period 2:** {label2} — {n2} data points  ",
        f"**Significance threshold:** α = 0.05  ",
        "", "---", "",
    ] + body + [
        "## Summary", "",
        f"Comparing **{label1}** vs **{label2}** ({field_label}):", "",
    ] + conclusions + [
        "",
        "> **Note:** All four tests are non-parametric — they make no assumption about the "
        "underlying distribution shape.",
    ]

    filepath.write_text("\n".join(lines))
    print(f"  Saved {filepath.name}")


# ── Load raw CSV once ─────────────────────────────────────────────────────────
raw = pd.read_csv(CSV_PATH, comment="#")
raw["_time"] = pd.to_datetime(raw["_time"], utc=True)

all_csv_rows = []

for fdef in FIELDS:
    field       = fdef["field"]
    unit        = fdef["unit"]
    slug        = fdef["slug"]
    field_label = fdef["label"]

    hist_dir, stats_dir = field_dirs(slug)

    df = raw[raw["_field"] == field].copy()
    df = df.set_index("_time").sort_index()
    df["_value"] = pd.to_numeric(df["_value"], errors="coerce")
    df = df.dropna(subset=["_value"])

    may_2025 = df.loc["2025-05-01":"2025-05-31", "_value"]
    may_2026 = df.loc["2026-05-01":"2026-05-31", "_value"]

    label1 = "May 2025 (2025-05-01 – 2025-05-31)"
    label2 = "May 2026 (2026-05-01 – 2026-05-31)"

    print(f"\n{'='*60}")
    print(f"  {field_label}  |  May 2025 vs May 2026")
    print(f"  May 2025: {len(may_2025)} pts  |  May 2026: {len(may_2026)} pts")
    print(f"{'='*60}")

    if len(may_2025) == 0 or len(may_2026) == 0:
        print("  WARNING: one period has no data — skipping.")
        continue

    # Plot
    fig, ax = plt.subplots(figsize=(11, 6))
    overlay_histogram(
        ax, may_2025, may_2026, label1, label2,
        f"{field_label} Distribution — May 2025 vs May 2026 (vaLo Node 01)",
        f"{field_label} ({unit})",
    )
    plt.tight_layout()
    png_path = hist_dir / f"{slug}_may2025_vs_may2026_histogram.png"
    plt.savefig(png_path, dpi=150)
    plt.close()
    print(f"  Plot saved → {png_path}")

    # Tests
    tr = run_nonparametric_tests(may_2025.values, may_2026.values)

    for test_name, res in tr.items():
        stat = res.get("statistic")
        pval = res.get("p_value")
        sig  = (pval is not None) and (pval < 0.05)
        all_csv_rows.append({
            "field":        field_label,
            "period":       "May 2025 vs May 2026",
            "period_1":     label1,
            "period_2":     label2,
            "n1":           len(may_2025),
            "n2":           len(may_2026),
            "test":         test_name,
            "statistic":    stat,
            "p_value":      pval,
            "significant":  sig,
            "significance": sig_label(pval),
        })
        print(f"  {test_name:<4}  stat={stat}  {sig_label(pval)}")

    # MD + per-field CSV
    write_md(
        field_label, unit, "May 2025 vs May 2026",
        label1, label2, len(may_2025), len(may_2026), tr,
        stats_dir / f"{slug}_may2025_vs_may2026_results.md",
    )
    csv_field = stats_dir / f"{slug}_may2025_vs_may2026_results.csv"
    pd.DataFrame([r for r in all_csv_rows if r["field"] == field_label]).to_csv(csv_field, index=False)
    print(f"  CSV  saved → {csv_field}")

print("\nDone.")
