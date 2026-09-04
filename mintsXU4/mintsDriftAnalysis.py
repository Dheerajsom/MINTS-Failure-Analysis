# ***************************************************************************
#  Compatibility shim — the SAFE engine now lives in the `safe` package at the
#  repository root (safe.engine / safe.stats / safe.loader / safe.config).
#  This module re-exports the old public names so existing scripts keep
#  working, and `python mintsXU4/mintsDriftAnalysis.py` still replays the
#  bundled valo CSV through the engine.
# ***************************************************************************

import logging
import os
import sys

# Make the repo root importable when this file is run directly from mintsXU4/
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from safe.config import (            # noqa: F401,E402
    DEFAULT_FLAT_MEAN_SHIFT,
    FLAT_MEAN_SHIFT_THRESHOLDS,
    HARD_BOUNDS,
    MIN_COHENS_D,
    MIN_STD_RATIO,
    SENSOR_DISPLAY_NAMES,
)
from safe.engine import PageHinkley, SensorDrift, default_alert_handler  # noqa: F401,E402
from safe.loader import load_pivoted_dataframe, parse_and_process_valo_data, replay_csv  # noqa: F401,E402
from safe.stats import sample_comparison  # noqa: F401,E402

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(script_dir, 'data', 'valo_node_01_full_year.csv')

    logger.info(f"Current Working Directory: {os.getcwd()}")
    logger.info(f"Resolved Data File Path: {data_file}")

    sys.exit(0 if replay_csv(data_file) is not None else 1)
