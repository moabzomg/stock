#!/usr/bin/env python3
"""
compare_backtest.py — Compare live-extracted data files against backtest files.

For each symbol found in both DATA_DIR and the backtest year directory,
compares the four paired files:
    <symbol>_daily.csv       (datetime,open,close,high,low,volume,status)
    <symbol>_minute.csv      (datetime,open,close,high,low,volume)
    <symbol>_ma.csv          (date,ma200,ma150,ma50)
    <symbol>_ma_minute.csv   (datetime,ma200,ma150,ma50)

Live files live at:   data/<symbol>_<type>.csv
Backtest files live at: backtest/data/<year>/<symbol>_<type>_<year>.csv

For every datetime/date key that exists in BOTH files, compares each shared
numeric column and reports any row where values differ beyond a tolerance
(to avoid flagging harmless float rounding noise, e.g. 233.0743 vs 233.0744
from a live-updating MA). Rows only present in one file are reported
separately, since that's a coverage gap rather than a value mismatch.

Usage:
    python3 compare_backtest.py [--tol 0.01] [--symbols GOOGL,AMZN,...] [--year 2026]
    python3 compare_backtest.py --symbols NVDA --at 202607012151
    python3 compare_backtest.py --symbols NVDA --from 202607012140 --to 202607012156

By default scans data/ and backtest/data/<latest-year>/ for whatever
symbols exist in both.

--at / --from / --to restrict which datetime/date keys are examined, across
ALL four file types at once. A 12-digit minute-level value is automatically
truncated to its 8-digit date for the daily/ma files, so one --at value
zooms into the same moment everywhere.
"""

import argparse
import csv
import os
import re
import sys
from collections import OrderedDict

DATA_DIR          = "data"
BACKTEST_ROOT     = "backtest"
BACKTEST_DATA_DIR = os.path.join(BACKTEST_ROOT, "data")

# (type_name, key_field, compare_fields, key_granularity)
# type_name is the token used in both the live suffix (_<type>.csv)
# and the backtest filename (<symbol>_<type>_<year>.csv).
# key_field is the literal CSV header name for the key column (used only
# for display in reports).
# key_granularity is the ACTUAL format of the values in that column:
# "date" for 8-digit YYYYMMDD keys, "datetime" for 12-digit YYYYMMDDHHMM
# keys. This is intentionally separate from key_field's name, because
# _daily.csv's header is literally "datetime" but its values are
# date-only (e.g. 20260701, not 202607010000) — trusting the header name
# for range truncation silently drops every daily-file match.
FILE_SPECS = [
    ("daily",      "datetime", ["open", "close", "high", "low", "volume", "status"], "date"),
    ("minute",     "datetime", ["open", "close", "high", "low", "volume"],           "datetime"),
    ("ma",         "date",     ["ma200", "ma150", "ma50"],                           "date"),
    ("ma_minute",  "datetime", ["ma200", "ma150", "ma50"],                           "datetime"),
]


def load_csv(path):
    """Return OrderedDict keyed by the first column's raw value -> row dict.
    Returns None if the file doesn't exist."""
    if not os.path.isfile(path):
        return None
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return OrderedDict()
        key_field = reader.fieldnames[0]
        rows = OrderedDict()
        for row in reader:
            rows[row[key_field]] = row
        return rows


def values_differ(a, b, field, tol):
    """Compare two raw string cell values for `field`. Numeric fields use a
    tolerance to avoid noise from float rounding or live-updating MAs.
    Non-numeric fields (e.g. 'status') use exact string comparison."""
    if a == b:
        return False
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return a != b
    return abs(fa - fb) > tol


def discover_year(explicit_year):
    """Pick the backtest year directory to use. If not given explicitly,
    use the latest (numerically highest) year folder found under
    backtest/data/."""
    if explicit_year:
        return str(explicit_year)
    if not os.path.isdir(BACKTEST_DATA_DIR):
        return None
    year_dirs = [
        d for d in os.listdir(BACKTEST_DATA_DIR)
        if re.fullmatch(r"\d{4}", d) and os.path.isdir(os.path.join(BACKTEST_DATA_DIR, d))
    ]
    if not year_dirs:
        return None
    return sorted(year_dirs)[-1]


def discover_symbols(explicit, backtest_dir):
    if explicit:
        return sorted(explicit)
    if not os.path.isdir(DATA_DIR) or not backtest_dir or not os.path.isdir(backtest_dir):
        return []
    data_syms = {
        f[: -len("_daily.csv")] for f in os.listdir(DATA_DIR)
        if f.endswith("_daily.csv")
    }
    year_suffix_re = re.compile(r"^(.+)_daily_\d{4}\.csv$")
    backtest_syms = set()
    for f in os.listdir(backtest_dir):
        m = year_suffix_re.match(f)
        if m:
            backtest_syms.add(m.group(1))
    return sorted(data_syms & backtest_syms)


def compare_file(symbol, type_name, key_field, compare_fields, tol, backtest_dir, year,
                  key_from=None, key_to=None):
    """If key_from/key_to are given, only keys with key_from <= key <= key_to
    are considered at all (both for shared-row comparison and for the
    only_live / only_backtest coverage-gap lists). Keys are compared as
    strings, which works for the zero-padded YYYYMMDDHHMM / YYYYMMDD
    formats used in these files."""
    live_path = os.path.join(DATA_DIR, f"{symbol}_{type_name}.csv")
    backtest_path = (
        os.path.join(backtest_dir, f"{symbol}_{type_name}_{year}.csv")
        if backtest_dir else None
    )

    live_rows     = load_csv(live_path)
    backtest_rows = load_csv(backtest_path) if backtest_path else None

    if live_rows is None or backtest_rows is None:
        missing = []
        if live_rows is None:
            missing.append(live_path)
        if backtest_rows is None:
            missing.append(backtest_path or "(backtest year directory not found)")
        return {"skipped": True, "missing_files": missing}

    def in_range(k):
        if key_from is not None and k < key_from:
            return False
        if key_to is not None and k > key_to:
            return False
        return True

    live_keys     = {k for k in live_rows if in_range(k)}
    backtest_keys = {k for k in backtest_rows if in_range(k)}

    shared_keys    = sorted(live_keys & backtest_keys)
    only_live      = sorted(live_keys - backtest_keys)
    only_backtest  = sorted(backtest_keys - live_keys)

    discrepancies = []
    for key in shared_keys:
        lrow, brow = live_rows[key], backtest_rows[key]
        diffs = {}
        for field in compare_fields:
            lv = lrow.get(field, "")
            bv = brow.get(field, "")
            if values_differ(lv, bv, field, tol):
                diffs[field] = (lv, bv)
        if diffs:
            discrepancies.append({"key": key, "diffs": diffs})

    return {
        "skipped": False,
        "key_field": key_field,
        "shared": len(shared_keys),
        "only_live": only_live,
        "only_backtest": only_backtest,
        "discrepancies": discrepancies,
        "live_path": live_path,
        "backtest_path": backtest_path,
    }


def print_report(symbol, type_name, result):
    label = f"{symbol}_{type_name}.csv"
    print(f"\n{'='*70}")
    print(f"{label}")
    print(f"{'='*70}")

    if result["skipped"]:
        for p in result["missing_files"]:
            print(f"  [skip] missing file: {p}")
        return

    print(f"  live:     {result['live_path']}")
    print(f"  backtest: {result['backtest_path']}")
    print(f"  shared keys compared: {result['shared']}")

    if result["shared"] == 0 and not result["only_live"] and not result["only_backtest"]:
        print("  (no keys in the requested range for this file)")
        return

    if result["only_live"]:
        preview = result["only_live"][:10]
        more = f" (+{len(result['only_live'])-10} more)" if len(result["only_live"]) > 10 else ""
        print(f"  keys only in LIVE ({len(result['only_live'])}): {preview}{more}")
    if result["only_backtest"]:
        preview = result["only_backtest"][:10]
        more = f" (+{len(result['only_backtest'])-10} more)" if len(result["only_backtest"]) > 10 else ""
        print(f"  keys only in BACKTEST ({len(result['only_backtest'])}): {preview}{more}")

    if not result["discrepancies"]:
        print("  ✓ no value discrepancies in shared rows")
        return

    print(f"  ✗ {len(result['discrepancies'])} row(s) with discrepancies:\n")
    key_field = result["key_field"]
    for d in result["discrepancies"]:
        field_strs = [f"{f}: live={lv} vs backtest={bv}" for f, (lv, bv) in d["diffs"].items()]
        print(f"    [{key_field}={d['key']}]")
        for fs in field_strs:
            print(f"        {fs}")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--tol", type=float, default=0.01,
                     help="Numeric tolerance for flagging a discrepancy (default: 0.01)")
    ap.add_argument("--symbols", type=str, default=None,
                     help="Comma-separated list of symbols to check (default: auto-discover)")
    ap.add_argument("--year", type=str, default=None,
                     help="Backtest year folder to use, e.g. 2026 "
                          "(default: latest year found under backtest/data/)")
    ap.add_argument("--at", type=str, default=None,
                     help="Only show this single datetime/date key across all files "
                          "(e.g. 202607012151 for minute-level files, or 20260701 "
                          "for daily/ma date-keyed files — matched as a prefix, so "
                          "202607012151 also matches the date key 20260701 in daily/ma files)")
    ap.add_argument("--from", dest="key_from", type=str, default=None,
                     help="Start of key range (inclusive), e.g. 202607012140")
    ap.add_argument("--to", dest="key_to", type=str, default=None,
                     help="End of key range (inclusive), e.g. 202607012156")
    args = ap.parse_args()

    if args.at and (args.key_from or args.key_to):
        sys.exit("Use either --at, or --from/--to, not both.")

    year = discover_year(args.year)
    backtest_dir = os.path.join(BACKTEST_DATA_DIR, year) if year else None

    if not backtest_dir or not os.path.isdir(backtest_dir):
        sys.exit(
            f"Could not find a backtest year directory under '{BACKTEST_DATA_DIR}/'. "
            f"Pass --year YYYY to specify explicitly."
        )

    explicit_symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else None
    symbols = discover_symbols(explicit_symbols, backtest_dir)

    if not symbols:
        sys.exit(
            f"No symbols found in both '{DATA_DIR}/' and '{backtest_dir}/' "
            f"(looked for matching <SYMBOL>_daily.csv / <SYMBOL>_daily_{year}.csv). "
            f"Pass --symbols A,B,C to specify explicitly."
        )

    print(f"Comparing {len(symbols)} symbol(s): {', '.join(symbols)}")
    print(f"Backtest year: {year}  (dir: {backtest_dir})")
    print(f"Tolerance: {args.tol}")
    if args.at:
        print(f"Filtering to key: {args.at}")
    elif args.key_from or args.key_to:
        print(f"Filtering to key range: {args.key_from or '(start)'} .. {args.key_to or '(end)'}")

    total_discrepancy_rows = 0
    for symbol in symbols:
        for type_name, key_field, compare_fields, key_granularity in FILE_SPECS:
            # date-keyed files (daily, ma) use an 8-digit YYYYMMDD key;
            # datetime-keyed files (minute, ma_minute) use a 12-digit
            # YYYYMMDDHHMM key. When --at is given with a 12-digit value,
            # truncate to 8 digits for date-granularity files so the same
            # --at value selects the matching day everywhere. This is
            # keyed off key_granularity, NOT key_field's name — the daily
            # file's header is literally "datetime" but its values are
            # date-only.
            if args.at:
                if key_granularity == "date" and len(args.at) > 8:
                    key_from = key_to = args.at[:8]
                else:
                    key_from = key_to = args.at
            else:
                key_from, key_to = args.key_from, args.key_to
                if key_granularity == "date":
                    if key_from and len(key_from) > 8:
                        key_from = key_from[:8]
                    if key_to and len(key_to) > 8:
                        key_to = key_to[:8]

            result = compare_file(symbol, type_name, key_field, compare_fields,
                                   args.tol, backtest_dir, year,
                                   key_from=key_from, key_to=key_to)
            print_report(symbol, type_name, result)
            if not result["skipped"]:
                total_discrepancy_rows += len(result["discrepancies"])

    print(f"\n{'='*70}")
    print(f"TOTAL rows with discrepancies across all files: {total_discrepancy_rows}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()