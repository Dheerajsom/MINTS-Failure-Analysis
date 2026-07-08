# ***************************************************************************
#  Loop the SAFE streaming engine over every day-file in a 1-second data
#  directory and tabulate alert counts per day, so drift-detector behavior
#  can be eyeballed across many days instead of one CLI run at a time.
#
#    python scripts/summarize_daily_drift.py
#    python scripts/summarize_daily_drift.py --variant default --variant no-autocorr
#    python scripts/summarize_daily_drift.py --limit 10 -o /tmp/drift.csv
# ***************************************************************************

import argparse
import collections
import glob
import logging
import os
import sys
import time

import pandas as pd

from safe.engine import SensorDrift
from safe.loader import replay_csv

logging.getLogger("safe").setLevel(logging.ERROR)  # silence per-row loader logs

VARIANT_KWARGS = {
    "default": dict(autocorr_correction=True, enable_page_hinkley=False),
    "no-autocorr": dict(autocorr_correction=False, enable_page_hinkley=False),
    "page-hinkley": dict(autocorr_correction=True, enable_page_hinkley=True),
}


def _day_label(file_path):
    name = os.path.basename(file_path)
    # valo_node_01_20250614_20250615.csv.gz -> 20250614
    parts = name.replace(".csv.gz", "").replace(".csv", "").split("_")
    return parts[-2] if len(parts) >= 2 else name


def summarize_file(file_path, variant):
    engine = SensorDrift(on_alert=lambda *a, **k: None, **VARIANT_KWARGS[variant])
    result = replay_csv(file_path, engine=engine)
    if result is None:
        return None
    counts = collections.Counter(alert["alert"] for _, alert, _ in engine.alerts)
    return counts


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default="mintsXU4/data/valo_node_01_1s",
                        help="Directory of day-files (default: %(default)s)")
    parser.add_argument("--pattern", default="*.csv.gz",
                        help="Glob pattern for day-files (default: %(default)s)")
    parser.add_argument("--variant", action="append", choices=list(VARIANT_KWARGS),
                        help="Engine config to run (repeatable). Default: just 'default'.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only process the first N day-files (sorted by name)")
    parser.add_argument("-o", "--output", default="mintsXU4/output/drift_day_summary.csv",
                        help="Where to write the per-day/variant CSV (default: %(default)s)")
    args = parser.parse_args(argv)

    variants = args.variant or ["default"]

    files = sorted(glob.glob(os.path.join(args.data_dir, args.pattern)))
    if args.limit:
        files = files[:args.limit]
    if not files:
        print(f"No files matched {args.data_dir}/{args.pattern}", file=sys.stderr)
        return 1

    rows = []
    for i, file_path in enumerate(files, 1):
        day = _day_label(file_path)
        for variant in variants:
            t0 = time.time()
            counts = summarize_file(file_path, variant)
            elapsed = time.time() - t0
            if counts is None:
                print(f"[{i}/{len(files)}] {day} ({variant}): FAILED to load")
                continue
            total = sum(counts.values())
            print(f"[{i}/{len(files)}] {day} ({variant}): {total} alert(s) in {elapsed:.1f}s "
                  f"({dict(counts)})")
            row = {"day": day, "variant": variant, "total_alerts": total}
            row.update(counts)
            rows.append(row)

    df = pd.DataFrame(rows).fillna(0)
    out_dir = os.path.dirname(args.output)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    df.to_csv(args.output, index=False)
    print(f"\nWrote {len(df)} row(s) to {args.output}")

    print("\n=== Summary (mean alerts/day by variant) ===")
    print(df.groupby("variant")["total_alerts"].agg(["mean", "min", "max", "count"]))

    return 0


if __name__ == "__main__":
    sys.exit(main())
