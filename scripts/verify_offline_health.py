"""Verify current source against committed input in a fresh isolated directory.

Preserves local deletions and generated artifacts. Never invokes download/live I/O.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="New, nonexisting validation directory")
    args = parser.parse_args(argv)
    destination = args.output.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    paths = ["safe", "mintsXU4/mintsDriftAnalysis.py", "mintsXU4/mintsPeriodAnalysis.py",
             "mintsXU4/mintsPeriodPlotter.py", "mintsXU4/data/valo_node_01_full_year.csv",
             "dataVisualizer.py", "pyproject.toml"]
    archive = destination / "committed-source.zip"
    subprocess.run(["git", "archive", "--format=zip", f"--output={archive}", "HEAD", *paths], cwd=ROOT, check=True)
    with zipfile.ZipFile(archive) as handle:
        handle.extractall(destination)
    shutil.copytree(ROOT / "safe", destination / "safe", dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    shutil.copy2(ROOT / "pyproject.toml", destination / "pyproject.toml")
    source_hashes = {str(p.relative_to(destination)): hashlib.sha256(p.read_bytes()).hexdigest()
                     for p in (destination / "safe").glob("*.py")}
    env = dict(os.environ, PYTHONIOENCODING="utf-8", MPLCONFIGDIR=str(destination / ".mplconfig"),
               PYTHONPATH=str(destination))
    csv = "mintsXU4/data/valo_node_01_full_year.csv"
    commands = [["-m", "safe.cli", "stream", csv],
                ["-m", "safe.cli", "periods", csv, "-o", "mintsXU4/output"],
                ["mintsXU4/mintsDriftAnalysis.py"], ["mintsXU4/mintsPeriodAnalysis.py"],
                ["dataVisualizer.py"],
                ["-m", "safe.cli", "health", csv, "--state-out", "mintsXU4/output/health/state.json"]]
    results = []
    for i, command in enumerate(commands):
        print(f"Running workflow {i + 1}/{len(commands)}: {' '.join(command)}", flush=True)
        with (destination / f"workflow-{i}.log").open("w", encoding="utf-8") as log:
            run = subprocess.run([sys.executable, *command], cwd=destination, env=env, stdout=log, stderr=subprocess.STDOUT)
        results.append(dict(command=command, exit_code=run.returncode))
        if run.returncode:
            raise RuntimeError(f"workflow {i} failed; see {destination / f'workflow-{i}.log'}")
    import numpy as np
    import pandas as pd
    from PIL import Image
    from safe.periods import CSV_COLUMNS

    output = destination / "mintsXU4/output"
    for path in output.glob("period_*.csv"):
        data = pd.read_csv(path)
        assert list(data.columns) == CSV_COLUMNS, path
        for column in ("old_n", "new_n"):
            assert np.isfinite(data[column]).all() and (data[column] >= 2).all(), (path, column)
        for column in ("p_welch", "p_levene"):
            assert np.isfinite(data[column]).all() and data[column].between(0, 1).all(), (path, column)
    images = list(output.rglob("*.png"))
    assert images
    for path in images:
        with Image.open(path) as image:
            image.load()
            assert min(image.size) >= 100, path
    for command in (["-m", "safe.cli", "stream", "missing.csv"],
                    ["-m", "safe.cli", "periods", "missing.csv", "-o", "mintsXU4/output"],
                    ["-m", "safe.cli", "health", "missing.csv"]):
        run = subprocess.run([sys.executable, *command], cwd=destination, env=env, capture_output=True)
        assert run.returncode != 0, command
    record = dict(workflows=results, png_count=len(images), source_hashes=source_hashes,
                  committed_input_sha256=hashlib.sha256((destination / csv).read_bytes()).hexdigest(),
                  generated_output=str(output), failure_exits_checked=True)
    (destination / "verification.json").write_text(json.dumps(record, indent=2) + "\n")
    print(destination / "verification.json", flush=True)


if __name__ == "__main__":
    main()
