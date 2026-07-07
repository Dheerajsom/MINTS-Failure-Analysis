# ***************************************************************************
#  Compatibility shim — the period plotting now lives in safe.plotting.
#  Re-exports the old public/helper names (mintsPmRegen.py uses several) and
#  running it directly still plots from mintsXU4/output/.
# ***************************************************************************

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from safe.plotting import (   # noqa: F401,E402
    ALPHA,
    LEVENE_COLOR,
    LOG_ALPHA,
    METRIC_INFO,
    METRICS,
    P_FLOOR_LOG,
    _as_bool,
    _neg_log10p,
    generate_category_plots,
    generate_day_to_day_zoomed_significance,
    run_all_plotting,
)


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "output")
    run_all_plotting(output_dir, os.path.join(output_dir, "plots"))


if __name__ == "__main__":
    main()
