#!/usr/bin/env python3
"""
batch_minute_extract.py — Run all_minute_extract_data.extract_symbol() for
every symbol listed in watch_list.txt (one symbol per line, # = comment).

Usage:
    python3 batch_minute_extract.py <YYYYMMDDHHmm> [--parallel N]

Arguments:
    YYYYMMDDHHmm   Start datetime (UTC). All symbols are fetched from this
                   point forward. Required.
    --parallel N   Number of symbols to process at the same time (default: 1).
                   Each parallel worker uses a different IB client ID so TWS
                   accepts all connections simultaneously.
                   WARNING: each parallel worker fires its own IB requests.
                   N=3 triples your request rate — stay well under the IB
                   60-requests-per-10-min limit (effective rate with
                   INTER_REQUEST_SLEEP=10 s is 6 req/min per worker, so
                   N ≤ 9 is safe; default N=1 is safest).

Examples:
    # Sequential (safest, one symbol at a time):
    python3 batch_minute_extract.py 202401010930

    # Three symbols in parallel (3× faster, 3× IB request rate):
    python3 batch_minute_extract.py 202401010930 --parallel 3

Output:
    data/<SYMBOL>.csv for each symbol — same format as the single-symbol script.
    A summary table is printed at the end showing new-bar counts and any errors.
"""

import sys
import os
import argparse
import subprocess
import time
import threading
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# Import the core extraction function from the single-symbol script.
# Both files must live in the same directory.
try:
    from all_minute_extract_data import extract_symbol, parse_start, CLIENT_ID
except ImportError:
    sys.exit(
        "ERROR: all_minute_extract_data.py not found in the same directory.\n"
        "Both scripts must be co-located."
    )

# ── Base client ID for batch workers ─────────────────────────────────────────
# Each parallel worker gets CLIENT_ID + worker_index so TWS accepts them all.
# CLIENT_ID itself is not used by the batch runner (that's for single-symbol
# direct invocation), but we start from CLIENT_ID + 1 to leave a gap.
BATCH_CLIENT_ID_BASE = CLIENT_ID + 1   # e.g. 11 if CLIENT_ID=10

# ── ma.py path ────────────────────────────────────────────────────────────────
# Resolved relative to this script's own location so it works regardless of
# the working directory the script is invoked from.
MA_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "ma.py")


# ── Watch list loader ─────────────────────────────────────────────────────────

def load_symbols(path: str = "watch_list.txt") -> list[str]:
    if not os.path.exists(path):
        sys.exit(f"ERROR: {path} not found.")
    symbols = []
    with open(path) as f:
        for line in f:
            s = line.strip()
            if s and not s.startswith("#"):
                symbols.append(s.upper())
    if not symbols:
        sys.exit(f"ERROR: {path} contains no symbols.")
    return symbols


# ── MA runner ─────────────────────────────────────────────────────────────────

def run_ma(symbol: str, start_date: str) -> tuple[bool, str]:
    """
    Run `python3 ../ma.py <SYMBOL> <YYYYMMDD>` and return (success, message).
    Captures stdout/stderr so it doesn't interleave with the batch progress log;
    the combined output is returned in *message* for the summary.
    """
    if not os.path.exists(MA_SCRIPT):
        return False, f"ma.py not found at {MA_SCRIPT}"
    cmd = [sys.executable, MA_SCRIPT, symbol, start_date]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )
        output = (result.stdout + result.stderr).strip()
        if result.returncode != 0:
            return False, f"exit {result.returncode}: {output}"
        return True, output
    except subprocess.TimeoutExpired:
        return False, "timed out after 120s"
    except Exception as exc:
        return False, str(exc)


# ── Worker ────────────────────────────────────────────────────────────────────

def worker(symbol: str, start_dt: datetime, start_date: str, client_id: int,
           results: dict, lock: threading.Lock):
    """
    Thread target. Calls extract_symbol then ma.py, records both outcomes.
    Uses *lock* only for the final dict write (extract_symbol itself is
    thread-safe: each call creates its own IBMinuteExtractor with a distinct
    client_id and its own IB socket connection).
    """
    t0 = time.time()
    try:
        new_bars = extract_symbol(symbol, start_dt, client_id=client_id)

        # Run ma.py immediately after this symbol's bars are on disk
        print(f"  [ma] running ma.py for {symbol} from {start_date} …")
        ma_ok, ma_msg = run_ma(symbol, start_date)
        if ma_ok:
            print(f"  [ma] {symbol}: ok")
        else:
            print(f"  [ma] {symbol}: FAILED — {ma_msg}")

        elapsed = time.time() - t0
        with lock:
            results[symbol] = {
                "status":    "ok",
                "new_bars":  new_bars,
                "elapsed":   elapsed,
                "ma_ok":     ma_ok,
                "ma_msg":    ma_msg,
            }
    except Exception as exc:
        elapsed = time.time() - t0
        with lock:
            results[symbol] = {
                "status":  "error",
                "error":   str(exc),
                "elapsed": elapsed,
                "ma_ok":   False,
                "ma_msg":  "",
            }


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Batch 1-min bar extractor for all symbols in watch_list.txt"
    )
    parser.add_argument(
        "start",
        metavar="YYYYMMDDHHmm",
        help="Start datetime (UTC) — fetch bars from this point forward"
    )
    parser.add_argument(
        "--parallel", "-p",
        metavar="N",
        type=int,
        default=1,
        help="Number of symbols to process in parallel (default: 1)"
    )
    parser.add_argument(
        "--watchlist", "-w",
        metavar="FILE",
        default="watch_list.txt",
        help="Path to watch list file (default: watch_list.txt)"
    )
    args = parser.parse_args()

    if args.parallel < 1:
        sys.exit("ERROR: --parallel must be >= 1")

    start_dt = parse_start(args.start)
    if start_dt >= datetime.now(timezone.utc):
        sys.exit("ERROR: start datetime must be in the past.")

    symbols = load_symbols(args.watchlist)

    # YYYYMMDD date passed to ma.py — the date portion of the start argument
    start_date = args.start[:8]

    print(f"\n{'='*60}")
    print(f"  Batch 1-min extract")
    print(f"  Symbols   : {len(symbols)}")
    print(f"  Start     : {start_dt:%Y-%m-%d %H:%M UTC}")
    print(f"  MA date   : {start_date}")
    print(f"  Parallel  : {args.parallel} worker(s)")
    print(f"  Watch list: {args.watchlist}")
    print(f"  ma.py     : {MA_SCRIPT}")
    print(f"{'='*60}\n")

    results: dict = {}
    lock = threading.Lock()

    if args.parallel == 1:
        # Sequential — simplest, no ThreadPoolExecutor overhead, cleaner logs
        for idx, symbol in enumerate(symbols):
            cid = BATCH_CLIENT_ID_BASE + (idx % args.parallel)
            worker(symbol, start_dt, start_date, cid, results, lock)
    else:
        # Parallel — assign a stable client ID slot to each worker index.
        # With N workers we need N distinct client IDs; we rotate them in
        # round-robin across the symbol list so no two live workers ever
        # share an ID.
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            futures = {}
            for idx, symbol in enumerate(symbols):
                cid = BATCH_CLIENT_ID_BASE + (idx % args.parallel)
                f = pool.submit(worker, symbol, start_dt, start_date, cid, results, lock)
                futures[f] = symbol

            for f in as_completed(futures):
                sym = futures[f]
                exc = f.exception()
                if exc:
                    with lock:
                        results[sym] = {
                            "status": "error", "error": str(exc),
                            "elapsed": 0, "ma_ok": False, "ma_msg": "",
                        }

    # ── Summary table ─────────────────────────────────────────────────────────
    print(f"\n{'='*68}")
    print(f"  BATCH COMPLETE — {len(symbols)} symbol(s)")
    print(f"{'='*68}")
    print(f"  {'SYMBOL':<10}  {'STATUS':<8}  {'NEW BARS':>10}  {'MA':>4}  {'TIME':>8}")
    print(f"  {'-'*10}  {'-'*8}  {'-'*10}  {'-'*4}  {'-'*8}")

    total_new  = 0
    errors     = []
    ma_errors  = []
    for symbol in symbols:
        r = results.get(symbol, {"status": "missing", "new_bars": 0, "elapsed": 0,
                                  "ma_ok": False, "ma_msg": ""})
        if r["status"] == "ok":
            bars_str = str(r["new_bars"])
            total_new += r["new_bars"]
            ma_str = "ok" if r.get("ma_ok") else "FAIL"
            if not r.get("ma_ok"):
                ma_errors.append((symbol, r.get("ma_msg", "")))
        else:
            bars_str = "—"
            ma_str   = "—"
            errors.append((symbol, r.get("error", "unknown")))
        time_str = f"{r['elapsed']:.0f}s"
        print(f"  {symbol:<10}  {r['status']:<8}  {bars_str:>10}  {ma_str:>4}  {time_str:>8}")

    print(f"  {'-'*10}  {'-'*8}  {'-'*10}  {'-'*4}  {'-'*8}")
    print(f"  {'TOTAL':<10}  {'':8}  {total_new:>10}")

    if errors:
        print(f"\n  Extraction errors ({len(errors)}):")
        for sym, msg in errors:
            print(f"    {sym}: {msg}")

    if ma_errors:
        print(f"\n  MA errors ({len(ma_errors)}):")
        for sym, msg in ma_errors:
            print(f"    {sym}: {msg}")

    print(f"{'='*68}\n")


if __name__ == "__main__":
    main()