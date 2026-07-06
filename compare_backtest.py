#!/usr/bin/env python3
"""
compare_backtest.py — Compare live-extracted data files against backtest files.

For each symbol found in both DATA_DIR and the backtest year directory,
compares the four paired files:
    <symbol>_daily.csv       (datetime,open,close,high,low,volume,status)
    <symbol>_minute.csv      (datetime,open,close,high,low,volume)
    <symbol>_ma.csv          (date,ma200,ma150,ma50)

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

--categorize additionally buckets every discrepancy row found into likely
root causes (extended-hours coverage gaps, timestamp/minute-alignment
shifts, isolated volume spikes vs. the steady vendor-volume ratio, etc.)
and writes a second report (default: categorise.txt) alongside the normal
per-file report.
"""

import argparse
import csv
import os
import re
import sys
from collections import OrderedDict, defaultdict, Counter


class Tee:
    """Writes to both the original stream and a log file simultaneously,
    so output still appears in the terminal while also being saved."""
    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)

    def flush(self):
        for s in self.streams:
            s.flush()

DATA_DIR          = "data"
BACKTEST_ROOT     = "backtest"
BACKTEST_DATA_DIR = os.path.join(BACKTEST_ROOT, "data")

# Placeholder/sentinel values used by the backtest (or live) pipeline to mean
# "not computed yet" (e.g. not enough historical daily bars for a 200-day MA).
# These should never be flagged as a live-vs-backtest discrepancy — they're
# a coverage gap, not a wrong value.
SENTINEL_VALUES = {"-1", "-1.0"}

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
]

# ---------------------------------------------------------------------------
# Categorization thresholds/config (used only when --categorize is passed)
# ---------------------------------------------------------------------------
# Regular session in this dataset runs roughly 08:13-20:00 exchange-local
# (per observed minute-key coverage). Anything with a minute-of-day clearly
# outside that window is treated as pre/post market for the
# extended_hours_missing bucket.
SESSION_START_HHMM = 813
SESSION_END_HHMM = 2000

# Ratio thresholds for the outlier_spike bucket: live/backtest volume ratio
# beyond this band (or its reciprocal) is treated as an isolated spike rather
# than the steady vendor-volume-convention gap.
OUTLIER_RATIO_HIGH = 5.0
OUTLIER_RATIO_LOW = 1.0 / OUTLIER_RATIO_HIGH

# How close two OHLC 'open' values need to be (in price units) to be
# considered "the same print" when checking for a timestamp/minute shift.
SHIFT_MATCH_TOL = 0.005

CATEGORY_ORDER = [
    "daily_bar",
    "extended_hours_missing",
    "timestamp_shift",
    "outlier_spike",
    "volume_ratio",
    "other",
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
    sentinel_skipped = 0
    for key in shared_keys:
        lrow, brow = live_rows[key], backtest_rows[key]
        diffs = {}
        for field in compare_fields:
            lv = lrow.get(field, "")
            bv = brow.get(field, "")
            if lv in SENTINEL_VALUES or bv in SENTINEL_VALUES:
                sentinel_skipped += 1
                continue
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
        "sentinel_skipped": sentinel_skipped,
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

    if result.get("sentinel_skipped"):
        print(f"  (skipped {result['sentinel_skipped']} field value(s) that were "
              f"-1 placeholders, not counted as discrepancies)")

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


# ---------------------------------------------------------------------------
# Categorization: bucket every discrepancy row (already in-memory, no log
# parsing needed) into likely root causes.
#
# Categories:
#   1. extended_hours_missing : live volume==0 while backtest has real
#                                volume, at a minute-bar key outside the
#                                regular session window.
#   2. timestamp_shift        : the live 'open' value exactly matches the
#                                backtest 'open' value of an ADJACENT minute
#                                key (+-1 or +-2 min) — classic off-by-a-
#                                few-seconds/minute alignment bug.
#   3. volume_ratio           : remaining volume-involving rows during
#                                regular session — bucketed to show the
#                                systematic vendor-volume-convention ratio.
#   4. outlier_spike          : live/backtest volume ratio (or its
#                                reciprocal) exceeds OUTLIER_RATIO_HIGH —
#                                an isolated spike inconsistent with the
#                                steady ratio, not counted with volume_ratio.
#   5. daily_bar              : any discrepancy from a *_daily.csv section
#                                (systematic vendor-level differences, not
#                                minute-level noise).
#   6. other                  : ma discrepancies, and anything not captured
#                                above.
# ---------------------------------------------------------------------------

def try_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def minute_of_day(key12):
    """202607010813 -> 813 (HHMM as int), for 12-digit minute-granularity keys."""
    return int(key12[-4:])


def is_extended_hours(key12):
    """Flag minute keys clearly outside the observed regular session window."""
    try:
        mod = minute_of_day(key12)
    except (TypeError, ValueError):
        return False
    return mod < SESSION_START_HHMM or mod > SESSION_END_HHMM


def adjacent_key(key12, delta_minutes):
    """Return the 12-digit datetime key shifted by delta_minutes (same day,
    no rollover — fine since we only ever look +-1/+-2)."""
    if len(key12) != 12:
        return None
    date_part = key12[:8]
    hh = int(key12[8:10])
    mm = int(key12[10:12])
    total = hh * 60 + mm + delta_minutes
    if total < 0 or total >= 24 * 60:
        return None
    nh, nm = divmod(total, 60)
    return f"{date_part}{nh:02d}{nm:02d}"


def categorize_discrepancies(all_results):
    """all_results: list of (symbol, type_name, key_field, result_dict) for
    every non-skipped compare_file() call. Returns (categories, counts) where
    categories maps category name -> list of (symbol, type_name, key, diffs)."""

    # Index every minute-file discrepancy by (symbol, key) -> diffs, so the
    # timestamp_shift check can look up adjacent-minute backtest values.
    minute_diffs_by_symkey = {}
    for symbol, type_name, key_field, result in all_results:
        if type_name != "minute":
            continue
        for d in result["discrepancies"]:
            minute_diffs_by_symkey[(symbol, d["key"])] = d["diffs"]

    categories = defaultdict(list)
    counts = Counter()

    for symbol, type_name, key_field, result in all_results:
        for d in result["discrepancies"]:
            key, diffs = d["key"], d["diffs"]

            if type_name == "daily":
                categories["daily_bar"].append((symbol, type_name, key, diffs))
                counts["daily_bar"] += 1
                continue

            if type_name == "ma":
                categories["other"].append((symbol, type_name, key, diffs))
                counts["other"] += 1
                continue

            # type_name == "minute" from here on
            only_volume = set(diffs.keys()) == {"volume"}
            has_ohlc = any(f in diffs for f in ("open", "close", "high", "low"))

            # --- extended hours missing ---
            if only_volume and is_extended_hours(key):
                lv, bv = diffs["volume"]
                lvf, bvf = try_float(lv), try_float(bv)
                if lvf == 0.0 and bvf and bvf > 0:
                    categories["extended_hours_missing"].append((symbol, type_name, key, diffs))
                    counts["extended_hours_missing"] += 1
                    continue

            # --- timestamp shift ---
            if has_ohlc:
                shifted = False
                live_open = diffs.get("open", (None, None))[0]
                lof = try_float(live_open) if live_open is not None else None
                if lof is not None:
                    for delta in (-2, -1, 1, 2):
                        other_key = adjacent_key(key, delta)
                        if other_key is None:
                            continue
                        other_diffs = minute_diffs_by_symkey.get((symbol, other_key))
                        if not other_diffs:
                            continue
                        bof = try_float(other_diffs.get("open", (None, None))[1])
                        if bof is not None and abs(lof - bof) < SHIFT_MATCH_TOL:
                            shifted = True
                            break
                if shifted:
                    categories["timestamp_shift"].append((symbol, type_name, key, diffs))
                    counts["timestamp_shift"] += 1
                    continue

            # --- outlier spike ---
            if "volume" in diffs:
                lv, bv = diffs["volume"]
                lvf, bvf = try_float(lv), try_float(bv)
                if lvf is not None and bvf is not None and lvf > 0 and bvf > 0:
                    ratio = lvf / bvf
                    if ratio > OUTLIER_RATIO_HIGH or ratio < OUTLIER_RATIO_LOW:
                        categories["outlier_spike"].append((symbol, type_name, key, diffs))
                        counts["outlier_spike"] += 1
                        continue

            # --- steady volume ratio bucket ---
            if "volume" in diffs:
                categories["volume_ratio"].append((symbol, type_name, key, diffs))
                counts["volume_ratio"] += 1
                continue

            categories["other"].append((symbol, type_name, key, diffs))
            counts["other"] += 1

    return categories, counts


def print_categorized_report(categories, counts):
    total = sum(counts.values())
    print(f"\n{'#'*70}")
    print("CATEGORIZED DISCREPANCY REPORT")
    print(f"{'#'*70}")
    print(f"\nTotal discrepancy rows: {total}\n")
    print(f"{'Category':<26}{'Count':>8}{'% of total':>12}")
    print("-" * 46)
    for cat in CATEGORY_ORDER:
        c = counts.get(cat, 0)
        pct = (100.0 * c / total) if total else 0
        print(f"{cat:<26}{c:>8}{pct:>11.1f}%")
    print("-" * 46)
    print(f"{'TOTAL':<26}{total:>8}")

    ratios = []
    for symbol, type_name, key, diffs in categories.get("volume_ratio", []):
        lv, bv = diffs["volume"]
        lvf, bvf = try_float(lv), try_float(bv)
        if lvf and bvf:
            ratios.append(bvf / lvf)
    if ratios:
        ratios.sort()
        n = len(ratios)
        median = ratios[n // 2]
        print(f"\nvolume_ratio bucket: backtest/live ratio median = {median:.2f}x "
              f"(n={n}, min={ratios[0]:.2f}x, max={ratios[-1]:.2f}x)")

    print("\n--- Sample rows per category ---")
    for cat in CATEGORY_ORDER:
        items = categories.get(cat, [])
        if not items:
            continue
        print(f"\n[{cat}] ({len(items)} total, showing up to 5)")
        for symbol, type_name, key, diffs in items[:5]:
            fstr = ", ".join(f"{f}: live={lv} vs bt={bv}" for f, (lv, bv) in diffs.items())
            print(f"  {symbol}_{type_name} [{key}]  {fstr}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                  formatter_class=argparse.RawDescriptionHelpFormatter)
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
    ap.add_argument("--log", type=str, default="log.txt",
                     help="File to write a copy of the per-file comparison output to, "
                          "in addition to printing to the terminal (default: log.txt). "
                          "Pass --log '' to disable file logging.")
    ap.add_argument("--categorize", action="store_true",
                     help="Also bucket every discrepancy row into likely root causes "
                          "(extended-hours gaps, timestamp shifts, outlier spikes, "
                          "the steady vendor volume ratio, etc.) and print/save a "
                          "second report.")
    ap.add_argument("--cat-out", type=str, default="categorise.txt",
                     help="File to write the categorized report to, when --categorize "
                          "is passed (default: categorise.txt). Pass --cat-out '' to "
                          "disable file output for the categorized report only.")
    args = ap.parse_args()

    if args.at and (args.key_from or args.key_to):
        sys.exit("Use either --at, or --from/--to, not both.")

    log_file = None
    if args.log:
        log_file = open(args.log, "w")
        sys.stdout = Tee(sys.stdout, log_file)

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
    all_results = []  # (symbol, type_name, key_field, result) for --categorize

    for symbol in symbols:
        for type_name, key_field, compare_fields, key_granularity in FILE_SPECS:
            # date-keyed files (daily, ma) use an 8-digit YYYYMMDD key;
            # datetime-keyed files (minute) use a 12-digit
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
                all_results.append((symbol, type_name, key_field, result))

    print(f"\n{'='*70}")
    print(f"TOTAL rows with discrepancies across all files: {total_discrepancy_rows}")
    print(f"{'='*70}")

    if log_file:
        sys.stdout = sys.__stdout__
        log_file.close()

    if args.categorize:
        cat_out_file = None
        if args.cat_out:
            cat_out_file = open(args.cat_out, "w")
            sys.stdout = Tee(sys.stdout, cat_out_file)

        categories, counts = categorize_discrepancies(all_results)
        print_categorized_report(categories, counts)

        if cat_out_file:
            sys.stdout = sys.__stdout__
            cat_out_file.close()
            print(f"\nWrote categorized report to {args.cat_out}")


if __name__ == "__main__":
    main()