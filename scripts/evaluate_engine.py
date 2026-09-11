"""Run from any working directory with the repository's Python environment."""

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from safe.evaluation import evaluate, write_reports
from safe.scenarios import synthetic_scenarios


def main(argv=None):
    parser = argparse.ArgumentParser(description="Evaluate unchanged SAFE defaults on fixed synthetic fixtures")
    parser.add_argument("--seed", type=int, default=1729)
    parser.add_argument("--output", type=Path, default=ROOT / "evaluation_output")
    parser.add_argument("--grace-seconds", type=float, default=0)
    args = parser.parse_args(argv)
    try:
        report = evaluate(synthetic_scenarios(args.seed), grace_seconds=args.grace_seconds, seed=args.seed)
        write_reports(report, args.output)
    except (ValueError, OSError) as exc:
        parser.exit(1, f"Evaluation failed: {exc}\n")
    print(args.output / "summary.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
