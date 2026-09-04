#!/usr/bin/env python3
# ***************************************************************************
#  mintsInfluxDownloader
#   ---------------------------------
#   Bulk downloader for long-span, high-resolution MINTS PM data.
#
#   A single "2 years @ 1 second" Flux query is too large for InfluxDB to
#   return in one response (it times out / overflows the read limit). This
#   script splits the time range into small chunks and runs one query per
#   chunk, streaming each response straight to disk.
#
#   Two backends (pick with --backend):
#     http  (default)  Talks to the InfluxDB v2 HTTP query API using only the
#                      Python standard library. Nothing to install.
#     cli              Shells out to the `influx` CLI once per chunk.
#
#   Features:
#     - Chunked requests              -> each call stays small enough to succeed
#     - Resumable                     -> finished chunks are skipped on re-run
#     - Streamed to disk              -> never holds a whole chunk in memory
#     - Per-chunk CSVs and/or one     -> --merge stitches them into a single CSV
#       merged CSV
#     - Optional gzip                 -> keeps 1 s data from eating your disk
#
#   Credentials come from environment variables so your token never lands in
#   the process list or the repo:
#       INFLUX_TOKEN   (required)  API token with read access to the bucket
#       INFLUX_HOST    (required)  e.g. http://mdash.circ.utdallas.edu:8086
#       INFLUX_ORG     (optional)  defaults to "MINTS"
#       INFLUX_BIN     (optional)  path to the influx CLI (only for --backend cli)
#  ***************************************************************************

import argparse
import csv
import gzip
import io
import os
import shutil
import subprocess
import sys
import time
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# Defaults describing the data to pull. These mirror the original Flux query;
# override any of them from the command line if you want a different node.
# ---------------------------------------------------------------------------
DEFAULT_ORG         = "MINTS"
DEFAULT_BUCKET      = "SharedAirDFW"
DEFAULT_DEVICE_NAME = "vaLo Node 01"
DEFAULT_DEVICE_ID   = "001e064a1520"
DEFAULT_MEASUREMENT = "IPS7100"
DEFAULT_FIELDS      = ["pm0_1", "pm0_3", "pm0_5", "pm10_0", "pm1_0", "pm2_5", "pm5_0"]
DEFAULT_WINDOW      = "1s"
DEFAULT_CHUNK_DAYS  = 1          # one day per request is safe for 1 s data
# Used only if --start is "auto" and the auto-probe can't find the first point.
FALLBACK_START      = "2023-01-01"


# ---------------------------------------------------------------------------
# Flux query builders
# ---------------------------------------------------------------------------
def _field_filter(fields):
    """Build the `r._field == "x" or r._field == "y" ...` predicate."""
    return " or ".join(f'r["_field"] == "{f}"' for f in fields)


def build_query(bucket, device_name, device_id, measurement, fields, window,
                start_iso, stop_iso):
    """The chunked data query: same filters as the original, bounded range."""
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: {start_iso}, stop: {stop_iso})\n'
        f'  |> filter(fn: (r) => r["device_name"] == "{device_name}")\n'
        f'  |> filter(fn: (r) => r["_measurement"] == "{measurement}")\n'
        f'  |> filter(fn: (r) => {_field_filter(fields)})\n'
        f'  |> filter(fn: (r) => r["device_id"] == "{device_id}")\n'
        f'  |> aggregateWindow(every: {window}, fn: mean, createEmpty: false)\n'
        f'  |> yield(name: "mean")\n'
    )


def build_first_query(bucket, device_name, device_id, measurement, fields):
    """Tiny probe query: timestamp of the earliest available point."""
    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: 0)\n'
        f'  |> filter(fn: (r) => r["device_name"] == "{device_name}")\n'
        f'  |> filter(fn: (r) => r["_measurement"] == "{measurement}")\n'
        f'  |> filter(fn: (r) => r["_field"] == "{fields[0]}")\n'
        f'  |> filter(fn: (r) => r["device_id"] == "{device_id}")\n'
        f'  |> first()\n'
        f'  |> keep(columns: ["_time"])\n'
    )


# ---------------------------------------------------------------------------
# Backend: a tiny holder for connection settings + the chosen transport.
# stream_lines(flux) yields decoded text lines from the query response and
# raises RuntimeError on any failure, regardless of backend.
# ---------------------------------------------------------------------------
class Backend:
    def __init__(self, kind, host, org, token, influx_bin, timeout):
        self.kind = kind
        self.host = host.rstrip("/")
        self.org = org
        self.token = token
        self.influx_bin = influx_bin
        self.timeout = timeout

    def stream_lines(self, flux):
        if self.kind == "http":
            yield from self._stream_http(flux)
        else:
            yield from self._stream_cli(flux)

    def _stream_http(self, flux):
        url = f"{self.host}/api/v2/query?org={urllib.parse.quote(self.org)}"
        req = urllib.request.Request(
            url,
            data=flux.encode("utf-8"),
            method="POST",
            headers={
                "Authorization": f"Token {self.token}",
                "Accept": "application/csv",
                "Content-Type": "application/vnd.flux",
            },
        )
        try:
            resp = urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace").strip()
            raise RuntimeError(f"HTTP {e.code} {e.reason}: {body}")
        except urllib.error.URLError as e:
            raise RuntimeError(f"connection error: {e.reason}")
        # TextIOWrapper streams line-by-line; the whole chunk never sits in RAM.
        with io.TextIOWrapper(resp, encoding="utf-8", newline="") as reader:
            for line in reader:
                yield line

    def _stream_cli(self, flux):
        env = os.environ.copy()
        env["INFLUX_TOKEN"] = self.token        # keep the token out of argv
        env["INFLUX_HOST"] = self.host
        env["INFLUX_ORG"] = self.org
        cmd = [self.influx_bin, "query", flux, "--raw",
               "--host", self.host, "--org", self.org]
        # Drain stderr to disk while stdout is streamed; two PIPEs can deadlock
        # when the child fills stderr before closing stdout.
        with tempfile.TemporaryFile(mode="w+t", encoding="utf-8") as errors:
            with subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=errors,
                                  env=env, text=True, encoding="utf-8", bufsize=1) as proc:
                try:
                    yield from proc.stdout
                    proc.wait()
                finally:
                    proc.stdout.close()
                    if proc.poll() is None:
                        proc.terminate()
                rc = proc.wait()
            if rc != 0:
                errors.seek(0)
                raise RuntimeError(f"influx query failed (exit {rc}):\n{errors.read().strip()}")


def resolve_influx_bin(explicit):
    """Find the influx CLI: --influx-bin, then $INFLUX_BIN, then PATH."""
    candidate = explicit or os.environ.get("INFLUX_BIN") or "influx"
    found = shutil.which(candidate) or (candidate if os.path.isfile(candidate) else None)
    if not found:
        sys.exit(
            "ERROR: --backend cli was requested but the 'influx' CLI was not found.\n"
            "  Install it from https://docs.influxdata.com/influxdb/v2/tools/influx-cli/\n"
            "  then add it to PATH, set INFLUX_BIN=/path/to/influx, or pass\n"
            "  --influx-bin /path/to/influx.  (Or just use the default --backend http.)"
        )
    return found


# ---------------------------------------------------------------------------
# Writing a single chunk
# ---------------------------------------------------------------------------
def write_chunk(backend, flux, out_fh, want_header):
    """
    Stream one query response into out_fh, stripping Flux annotation/comment
    lines so the result matches the project's bundled export format. Returns
    the number of data rows written.
    """
    data_rows = 0
    header_done = not want_header
    for line in backend.stream_lines(flux):
        if not line.strip():
            continue            # blank separator lines between Flux tables
        if line.startswith("#"):
            continue            # #group / #datatype / #default annotations
        if _is_header(line):
            if not header_done:
                out_fh.write(line)
                header_done = True
            continue            # drop repeated headers from extra tables
        out_fh.write(line)
        data_rows += 1
    return data_rows


def _is_header(line):
    """True for the Flux CSV column-header row (not a data row)."""
    parts = line.rstrip("\n").split(",")
    return "_time" in parts and "_value" in parts and "_field" in parts


def detect_start(backend, bucket, device_name, device_id, measurement, fields):
    """Probe InfluxDB for the timestamp of the first point; floor to midnight."""
    flux = build_first_query(bucket, device_name, device_id, measurement, fields)
    try:
        text = "".join(backend.stream_lines(flux))
    except RuntimeError as e:
        print(f"  (auto start-detect failed, using fallback {FALLBACK_START}): {e}")
        return _parse_time(FALLBACK_START)

    header = None
    timestamps = []
    for row in csv.reader(io.StringIO(text)):
        if not row or row[0].startswith("#"):
            continue
        if "_time" in row:
            header = row
            continue
        if header:
            t = row[header.index("_time")]
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
            timestamps.append(dt.astimezone(timezone.utc))
    if timestamps:
        return min(timestamps).replace(hour=0, minute=0, second=0, microsecond=0)
    print(f"  (no data found by auto start-detect, using fallback {FALLBACK_START})")
    return _parse_time(FALLBACK_START)


# ---------------------------------------------------------------------------
# Time / file helpers
# ---------------------------------------------------------------------------
def _parse_time(s):
    """Parse an ISO-8601 UTC timestamp or a YYYY-MM-DD UTC date."""
    try:
        parsed = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(
            f"Expected YYYY-MM-DD or an ISO-8601 timestamp, got {s!r}."
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def iso(dt):
    """RFC3339 in UTC, e.g. 2024-08-04T00:00:00Z (Flux-friendly)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def open_text(path, mode, gz):
    if gz:
        return gzip.open(path, mode + "t", encoding="utf-8", newline="")
    return open(path, mode, encoding="utf-8", newline="")


def chunk_path(out_dir, prefix, start, stop, gz):
    name = f"{prefix}_{start:%Y%m%d}_{stop:%Y%m%d}.csv" + (".gz" if gz else "")
    return os.path.join(out_dir, name)


# ---------------------------------------------------------------------------
# Merge step
# ---------------------------------------------------------------------------
def merge_chunks(chunk_files, merged_path, gz):
    """Stream every chunk CSV into one file, keeping the header only once."""
    wrote_header = False
    with open_text(merged_path, "w", gz) as out:
        for path in chunk_files:
            in_gz = path.endswith(".gz")
            with open_text(path, "r", in_gz) as src:
                for line in src:
                    if _is_header(line):
                        if wrote_header:
                            continue
                        wrote_header = True
                    out.write(line)
    return merged_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(
        description="Download long-span MINTS PM data from InfluxDB in "
                    "resumable, chunked CSV files.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--backend", choices=["http", "cli"], default="http",
                   help="http = stdlib HTTP API (no install); cli = influx CLI.")
    p.add_argument("--host", default=os.environ.get("INFLUX_HOST"),
                   help="InfluxDB URL (or set INFLUX_HOST).")
    p.add_argument("--org", default=os.environ.get("INFLUX_ORG", DEFAULT_ORG),
                   help="InfluxDB organization (or set INFLUX_ORG).")
    p.add_argument("--token", default=os.environ.get("INFLUX_TOKEN"),
                   help="API token. Prefer the INFLUX_TOKEN env var over this flag.")
    p.add_argument("--influx-bin", default=None,
                   help="Path to the influx CLI (cli backend only; or set INFLUX_BIN).")
    p.add_argument("--timeout", type=int, default=180,
                   help="Per-request socket timeout in seconds (http backend).")

    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--device-name", default=DEFAULT_DEVICE_NAME)
    p.add_argument("--device-id", default=DEFAULT_DEVICE_ID)
    p.add_argument("--measurement", default=DEFAULT_MEASUREMENT)
    p.add_argument("--fields", nargs="+", default=DEFAULT_FIELDS,
                   help="Field names to pull.")
    p.add_argument("--window", default=DEFAULT_WINDOW,
                   help="aggregateWindow size, e.g. 1s, 1m.")

    p.add_argument("--start", default="auto",
                   help='Start UTC date/timestamp (YYYY-MM-DD or ISO-8601), or "auto" to probe the first point.')
    p.add_argument("--stop", default=None,
                   help="Exclusive UTC date/timestamp (YYYY-MM-DD or ISO-8601). Defaults to now.")
    p.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS,
                   help="Days of data per request.")

    p.add_argument("--out-dir", default="mintsXU4/data/valo_node_01_1s",
                   help="Directory for the per-chunk CSV files.")
    p.add_argument("--prefix", default="valo_node_01",
                   help="Filename prefix for chunk files.")
    p.add_argument("--gzip", action="store_true",
                   help="gzip each chunk (and the merged file).")
    p.add_argument("--merge", action="store_true",
                   help="After downloading, stitch all chunks into one CSV.")
    p.add_argument("--merged-name", default="valo_node_01_all.csv",
                   help="Filename for the merged CSV (inside --out-dir).")
    p.add_argument("--overwrite", action="store_true",
                   help="Re-download chunks even if a file already exists.")
    p.add_argument("--retries", type=int, default=3,
                   help="Retries per chunk before giving up.")

    args = p.parse_args()
    for option in ("chunk_days", "retries", "timeout"):
        if getattr(args, option) < 1:
            p.error(f"--{option.replace('_', '-')} must be positive")

    if not args.host:
        sys.exit("ERROR: no InfluxDB host. Set INFLUX_HOST or pass --host.")
    if not args.token:
        sys.exit("ERROR: no API token. Set INFLUX_TOKEN or pass --token.")

    influx_bin = resolve_influx_bin(args.influx_bin) if args.backend == "cli" else None
    backend = Backend(args.backend, args.host, args.org, args.token,
                      influx_bin, args.timeout)

    os.makedirs(args.out_dir, exist_ok=True)

    # Resolve the time window.
    if args.start == "auto":
        print("Detecting first available timestamp...")
        start = detect_start(backend, args.bucket, args.device_name,
                             args.device_id, args.measurement, args.fields)
    else:
        start = _parse_time(args.start)
    stop = (_parse_time(args.stop) if args.stop
            else datetime.now(timezone.utc))

    if start >= stop:
        sys.exit(f"ERROR: start ({iso(start)}) is not before stop ({iso(stop)}).")

    print(f"Backend:    {args.backend}")
    print(f"Range:      {iso(start)}  ->  {iso(stop)}")
    print(f"Chunk size: {args.chunk_days} day(s)")
    print(f"Output dir: {os.path.abspath(args.out_dir)}")
    print("-" * 60)

    step = timedelta(days=args.chunk_days)
    chunk_files = []
    total_rows = 0
    cursor = start

    while cursor < stop:
        chunk_stop = min(cursor + step, stop)
        path = chunk_path(args.out_dir, args.prefix, cursor, chunk_stop, args.gzip)
        chunk_files.append(path)

        # Resume: skip a chunk that already finished (a non-empty final file).
        if not args.overwrite and os.path.exists(path) and os.path.getsize(path) > 0:
            print(f"skip   {os.path.basename(path)} (already exists)")
            cursor = chunk_stop
            continue

        flux = build_query(args.bucket, args.device_name, args.device_id,
                           args.measurement, args.fields, args.window,
                           iso(cursor), iso(chunk_stop))

        # Write to a .part file first; rename only on success so an interrupted
        # run never leaves a partial file that looks complete to the resume check.
        part = path + ".part"
        rows = None
        for attempt in range(1, args.retries + 1):
            try:
                with open_text(part, "w", args.gzip) as fh:
                    rows = write_chunk(backend, flux, fh, want_header=True)
                break
            except (RuntimeError, OSError) as e:
                wait = 2 ** attempt
                print(f"  attempt {attempt}/{args.retries} failed for "
                      f"{os.path.basename(path)}: {e}")
                if attempt == args.retries:
                    if os.path.exists(part):
                        os.remove(part)
                    sys.exit(f"ERROR: giving up on {os.path.basename(path)}. "
                             f"Re-run to resume from here.")
                time.sleep(wait)

        os.replace(part, path)
        total_rows += rows
        print(f"write  {os.path.basename(path)}  ({rows:,} rows)")
        cursor = chunk_stop

    print("-" * 60)
    print(f"Done. {len(chunk_files)} chunk file(s), {total_rows:,} new rows downloaded.")

    if args.merge:
        merged_name = args.merged_name + (".gz" if args.gzip
                                          and not args.merged_name.endswith(".gz")
                                          else "")
        merged_path = os.path.join(args.out_dir, merged_name)
        print(f"Merging into {merged_path} ...")
        merge_chunks(chunk_files, merged_path, args.gzip)
        size_mb = os.path.getsize(merged_path) / 1e6
        print(f"Merged file written ({size_mb:,.1f} MB).")


if __name__ == "__main__":
    main()
