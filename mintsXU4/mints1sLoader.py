# ***************************************************************************
#  1-second PM data loader for MINTS valo node 01
#  --------------------------------------------------------------------------
#  Reads the gzipped daily InfluxDB exports under data/valo_node_01_1s/ and
#  returns a wide, time-indexed DataFrame (one column per PM size bin). The
#  full year is ~216M long rows / ~31M timestamps; pivoted to float32 this is
#  ~1.1 GB in RAM, so the result is cached to a pickle for fast re-runs.
#
#  This dataset is PM-only (no temperature / pressure / humidity at 1 s).
# ***************************************************************************

import os
import glob
import logging

import numpy as np
import pandas as pd

from safe.config import PM_BOUNDS

logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "valo_node_01_1s")
CACHE_PATH = os.path.join(DATA_DIR, "_wide_pm_cache.pkl")

# IPS7100 size bins, ascending. All values are µg/m³.
PM_FIELDS = ["pm0_1", "pm0_3", "pm0_5", "pm1_0", "pm2_5", "pm5_0", "pm10_0"]

SENSOR_NAME = "IPS7100_MHC_001"


def _gz_files():
    return sorted(glob.glob(os.path.join(DATA_DIR, "valo_node_01_*.csv.gz")))


def load_wide(use_cache=True, max_files=None, rebuild=False):
    """Return a wide DataFrame: DatetimeIndex (tz-naive UTC, name '_dt') x PM_FIELDS.

    use_cache : read/write the full-year pickle cache (ignored when max_files is set).
    max_files : limit to the first N daily files (for quick tests; bypasses cache).
    rebuild   : force a rebuild even if the cache exists.
    """
    if max_files is not None and (isinstance(max_files, bool) or not isinstance(max_files, int) or max_files < 1):
        raise ValueError("max_files must be a positive integer")
    cacheable = use_cache and max_files is None

    if cacheable and not rebuild and os.path.exists(CACHE_PATH):
        logger.info("Loading wide PM frame from cache %s", CACHE_PATH)
        return pd.read_pickle(CACHE_PATH)

    files = _gz_files()
    if not files:
        raise FileNotFoundError(f"No 1s gz files found in {DATA_DIR}")
    if max_files:
        files = files[:max_files]

    logger.info("Building wide PM frame from %d daily files...", len(files))
    blocks = []
    for i, f in enumerate(files, 1):
        df = pd.read_csv(f, compression="gzip", comment="#", usecols=["_time", "_value", "_field"])
        df["_value"] = pd.to_numeric(df["_value"], errors="coerce")
        df = df[df["_field"].isin(PM_FIELDS)]
        df["_time"] = pd.to_datetime(df["_time"], format="ISO8601", utc=True, errors="coerce")
        df = df.dropna(subset=["_time", "_value"])
        # Pivot this single day (cheap); duplicate timestamps collapsed via 'first'.
        block = df.pivot_table(index="_time", columns="_field", values="_value", aggfunc="first")
        blocks.append(block.astype("float32"))
        if i % 30 == 0 or i == len(files):
            logger.info("  read %d/%d files", i, len(files))

    wide = pd.concat(blocks)
    wide.index = pd.to_datetime(wide.index, utc=True).tz_convert(None)
    wide = wide[~wide.index.duplicated(keep="first")].sort_index()
    wide = wide.reindex(columns=PM_FIELDS)

    # PM is a non-negative mass concentration; treat negatives as bad readings.
    for c in wide.columns:
        wide.loc[~np.isfinite(wide[c]) | (wide[c] < PM_BOUNDS[0]) | (wide[c] > PM_BOUNDS[1]), c] = np.nan
        wide[c] = wide[c].astype("float32")

    wide.index.name = "_dt"

    if cacheable:
        logger.info("Caching wide PM frame -> %s (%.2f GB)",
                    CACHE_PATH, wide.memory_usage(deep=True).sum() / 1e9)
        wide.to_pickle(CACHE_PATH)

    return wide


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    w = load_wide()
    print("Wide PM frame:", w.shape)
    print("Range:", w.index.min(), "->", w.index.max())
    print(w.describe().round(3).to_string())
