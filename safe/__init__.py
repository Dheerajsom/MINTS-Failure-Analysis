"""SAFE — Sensor Analysis and Failure Evaluation.

Drift and failure detection for MINTS low-cost air-quality sensor nodes.

Public API:
    SensorDrift          streaming multi-layer drift/failure engine
    sample_comparison    two-sample drift test (effect-size + n_eff gated)
    load_pivoted_dataframe / replay_csv   InfluxDB-export CSV helpers
    run_period_analysis  period-over-period comparisons + CSVs + plots
    HARD_BOUNDS          physical limits per metric
"""

from safe.config import HARD_BOUNDS
from safe.engine import PageHinkley, SensorDrift
from safe.loader import load_pivoted_dataframe, replay_csv
from safe.periods import run_period_analysis
from safe.stats import effective_sample_size, sample_comparison

__version__ = "1.0.0"

__all__ = [
    "HARD_BOUNDS",
    "PageHinkley",
    "SensorDrift",
    "effective_sample_size",
    "load_pivoted_dataframe",
    "replay_csv",
    "run_period_analysis",
    "sample_comparison",
    "__version__",
]
