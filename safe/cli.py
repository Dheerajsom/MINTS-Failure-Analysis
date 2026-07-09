# ***************************************************************************
#  SAFE — command-line interface
#
#    safe stream  <csv>            replay a CSV through the streaming engine
#    safe periods <csv> -o <dir>   period-over-period analysis + plots
# ***************************************************************************

import argparse
import glob
import logging
import os
import sys

from safe.config import DEFAULT_Z_THRESHOLD


def _add_stream_parser(subparsers):
    p = subparsers.add_parser(
        "stream", help="Replay InfluxDB-export CSV(s) through the streaming drift engine")
    p.add_argument("csv", nargs="+",
                   help="One or more long-format CSV/CSV.GZ paths, or a directory of them "
                        "(replayed in sorted order through a single continuous engine)")
    p.add_argument("--metric", action="append", dest="metrics",
                   help="Restrict processing to this metric (repeatable, e.g. "
                        "--metric pm1_0). Default: all metrics present in the file(s)")
    p.add_argument("--window", type=int, default=200,
                   help="Evaluation window size (default: 200). 1s-resolution data is "
                        "heavily autocorrelated (rho~0.98) — use a much larger window "
                        "(e.g. 7200) for the windowed Welch/Levene drift test to fire")
    p.add_argument("--z-threshold", type=float, default=DEFAULT_Z_THRESHOLD,
                   help="Modified z-score outlier cutoff (default: %(default)s)")
    p.add_argument("--alpha", type=float, default=0.01,
                   help="Significance level for the drift tests (default: 0.01)")
    p.add_argument("--page-hinkley", action="store_true",
                   help="Enable the Page-Hinkley sequential mean-shift layer "
                        "(best for stationary streams; on ambient data it alarms "
                        "on genuine diurnal weather shifts)")
    p.add_argument("--no-autocorr", action="store_true",
                   help="Disable the autocorrelation (n_eff) correction of test p-values")


def _expand_csv_args(paths):
    """Expand directories into their sorted *.csv / *.csv.gz files."""
    files = []
    for path in paths:
        if os.path.isdir(path):
            found = sorted(glob.glob(os.path.join(path, "*.csv.gz")) +
                           glob.glob(os.path.join(path, "*.csv")))
            if not found:
                print(f"No .csv/.csv.gz files found in directory: {path}", file=sys.stderr)
                return None
            files.extend(found)
        else:
            files.append(path)
    return files


def _add_periods_parser(subparsers):
    p = subparsers.add_parser(
        "periods", help="Run period-over-period drift analysis and write CSVs + plots")
    p.add_argument("csv", help="Path to the long-format CSV export")
    p.add_argument("-o", "--output", required=True, help="Output directory for period_*.csv")
    p.add_argument("--alpha", type=float, default=0.01,
                   help="Significance level for the drift tests (default: 0.01)")
    p.add_argument("--no-plots", action="store_true", help="Skip plot generation")


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="safe",
        description="SAFE - Sensor Analysis and Failure Evaluation for MINTS air-quality nodes")
    subparsers = parser.add_subparsers(dest="command", required=True)
    _add_stream_parser(subparsers)
    _add_periods_parser(subparsers)

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    if args.command == "stream":
        from safe.engine import SensorDrift
        from safe.loader import replay_csvs

        files = _expand_csv_args(args.csv)
        if files is None:
            return 1

        engine = SensorDrift(
            window_size=args.window,
            z_threshold=args.z_threshold,
            p_alpha=args.alpha,
            enable_page_hinkley=args.page_hinkley,
            autocorr_correction=not args.no_autocorr,
        )
        result = replay_csvs(files, engine=engine, metrics=args.metrics)
        if result is None:
            return 1
        print(f"\n{len(engine.alerts)} alert(s) raised across {len(files)} file(s).")
        return 0

    if args.command == "periods":
        from safe.periods import run_period_analysis

        ok = run_period_analysis(args.csv, args.output, p_alpha=args.alpha,
                                 make_plots=not args.no_plots)
        return 0 if ok else 1

    return 1


if __name__ == "__main__":
    sys.exit(main())
