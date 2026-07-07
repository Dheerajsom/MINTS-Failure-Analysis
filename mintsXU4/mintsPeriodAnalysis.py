# ***************************************************************************
#  Compatibility shim — the period-over-period analysis now lives in
#  safe.periods. This module re-exports the old public names so existing
#  scripts (e.g. mintsPmRegen.py) keep working, and running it directly still
#  analyzes the bundled valo CSV into mintsXU4/output/.
# ***************************************************************************

import os
import sys

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from safe.periods import (   # noqa: F401,E402
    CSV_COLUMNS,
    GRANULARITIES,
    apply_hard_bounds,
    bucketize,
    comparison_row,
    run_period_analysis,
)

# Old private names, kept for callers that used them
_apply_hard_bounds = apply_hard_bounds
_bucketize = bucketize
_row = comparison_row


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(script_dir, "data", "valo_node_01_full_year.csv")
    output_dir = os.path.join(script_dir, "output")

    print(f"Resolved Data File Path: {data_file}")
    run_period_analysis(data_file, output_dir)
