#!/usr/bin/env python3
"""
ma_minute.py — Compute daily SMA (pure daily-close rolling window) and
minute-level "hybrid" SMA.

Files are year-partitioned under data/<year>/:
    data/<year>/<SYMBOL>_daily_<year>.csv        (source, written by minute_extract.py)
    data/<year>/<SYMBOL>_minute_<year>.csv       (source, written by minute_extract.py)
    data/<year>/<SYMBOL>_ma_<year>.csv           (output: daily MA)
    data/<year>/<SYMBOL>_ma_minute_<year>.csv    (output: minute MA)

Daily MA (compute_daily_ma):
    Standard rolling window of N daily closes, unchanged from before.

Minute MA (compute_minute_ma):
    For a minute timestamp T on trading day D, the window is:
        (N-1) most recent daily closes strictly BEFORE D (from daily.csv,
        never including D itself)
      + the single minute close AT T (from minute.csv)
    = N values total.

    This is evaluated independently per minute row — it is NOT a running
    intraday accumulation. A row is only written if that exact timestamp
    exists in minute.csv; there is no gap-fill for missing minutes.

    Example (N=50): minute 202607011005 uses daily closes from
    20260428..20260630 (49 values, all days strictly before 20260701) plus
    the single minute close at 202607011005 (1 value) = 50 total.

Usage:
    python3 ma_minute.py <SYMBOL> [start_yyyymmdd]                # daily MA only
    python3 ma_minute.py <SYMBOL> --minute [start_yyyymmddhhmm]    # minute MA only
    python3 ma_minute.py <SYMBOL> --both [start_yyyymmdd]          # both

NOTE ON THE BACKTEST-VS-LIVE DISCREPANCY INVESTIGATION
--------------------------------------------------------
No bug was found in this file. It was checked and confirmed to be
unaffected by the extractor issues fixed in minute_extract.py because:
  - It reads daily.csv/minute.csv via csv.DictReader, i.e. by column
    *name*, not position — so it's immune to any column-ordering issues in
    the source files, and doesn't care that DAILY_FIELDS gained a
    bar_count column.
  - It has no independent notion of "which days are complete" — it simply
    trusts whatever close values exist in the source files. Once
    minute_extract.py's gap-detection/finalization fixes let a
    previously-incomplete day get corrected, re-running compute_daily_ma /
    compute_minute_ma from that date forward (which extract_symbol()
    already does automatically) picks up the corrected values with no
    changes needed here.
"""

import sys, os, csv
from bisect import bisect_left

DATA_DIR     = 'data'
MA_PERIODS   = (200, 150, 50)
DAILY_FIELDS  = ['date', 'ma200', 'ma150', 'ma50']
MINUTE_FIELDS = ['datetime', 'ma200', 'ma150', 'ma50']


def _year_dir(year):
    return os.path.join(DATA_DIR, str(year))


def _list_years():
    if not os.path.isdir(DATA_DIR):
        return []
    return sorted(d for d in os.listdir(DATA_DIR)
                  if d.isdigit() and os.path.isdir(os.path.join(DATA_DIR, d)))


def _daily_src_path(symbol, year):  return os.path.join(_year_dir(year), f'{symbol}_daily_{year}.csv')
def _minute_src_path(symbol, year): return os.path.join(_year_dir(year), f'{symbol}_minute_{year}.csv')
def _ma_path(symbol, year):         return os.path.join(_year_dir(year), f'{symbol}_ma_{year}.csv')
def _ma_min_path(symbol, year):     return os.path.join(_year_dir(year), f'{symbol}_ma_minute_{year}.csv')


def _read_series(symbol, src_path_fn, dt_col):
    """Read a dt_col/close series merged across every year folder, sorted ascending."""
    rows = []
    for year in _list_years():
        path = src_path_fn(symbol, year)
        if not os.path.isfile(path):
            continue
        with open(path, newline='') as f:
            rows.extend(csv.DictReader(f))
    rows.sort(key=lambda r: r[dt_col])
    return [r[dt_col] for r in rows], [float(r['close']) for r in rows]


# ── Daily MA: unchanged pure rolling window over daily closes ────────────────

def _compute_daily_ma(dates, closes, start=None, end=None):
    """Return list of {dt: ..., ma200, ma150, ma50} dicts using a standard
    trailing window of N daily closes ending at (and including) each date."""
    n = len(dates)
    if start is None and end is None:
        idxs = [n - 1]
    elif end is None:
        idxs = [i for i, d in enumerate(dates) if d >= start]
    else:
        idxs = [i for i, d in enumerate(dates) if start <= d <= end]
    results = []
    for i in idxs:
        results.append({
            'dt': dates[i],
            **{f'ma{p}': (round(sum(closes[i+1-p:i+1])/p, 4) if i+1 >= p else -1)
               for p in MA_PERIODS}
        })
    return results


# ── Minute MA: (N-1) prior daily closes (strictly before that day) + the ────
# ── single minute close at that timestamp ────────────────────────────────────

def _compute_minute_ma(daily_dates, daily_closes, minute_dates, minute_closes,
                        start=None, end=None):
    """
    For each minute row in range, find how many daily closes exist strictly
    before that minute's trading day (via bisect on daily_dates), then take
    the most recent (N-1) of them plus this single minute close.

    If fewer than (N-1) prior daily closes exist for a given period N, that
    period's value is -1 (mirrors the daily-MA insufficient-history guard).
    """
    n = len(minute_dates)
    if start is None and end is None:
        idxs = [n - 1]
    elif end is None:
        idxs = [i for i, d in enumerate(minute_dates) if d >= start]
    else:
        idxs = [i for i, d in enumerate(minute_dates) if start <= d <= end]

    results = []
    for i in idxs:
        mdt = minute_dates[i]
        day = mdt[:8]
        # index of first daily date >= day == count of daily closes strictly before `day`
        prior_count = bisect_left(daily_dates, day)
        row = {'dt': mdt}
        for p in MA_PERIODS:
            need = p - 1  # prior daily closes needed
            if prior_count >= need:
                prior_slice = daily_closes[prior_count - need:prior_count] if need > 0 else []
                window = prior_slice + [minute_closes[i]]
                row[f'ma{p}'] = round(sum(window) / p, 4)
            else:
                row[f'ma{p}'] = -1
        results.append(row)
    return results


def _upsert(symbol, ma_path_fn, fields, dt_col, results):
    """Upsert results into the appropriate year-partitioned MA CSV(s), keyed
    by dt_col. Each result's year (dt[:4]) determines which file it lands
    in; only years present in `results` get read+rewritten."""
    if not results:
        return 0
    by_year = {}
    for r in results:
        by_year.setdefault(r['dt'][:4], []).append(r)

    total = 0
    for year, recs in by_year.items():
        d = _year_dir(year)
        os.makedirs(d, exist_ok=True)
        path = ma_path_fn(symbol, year)
        existing = {}
        if os.path.isfile(path):
            with open(path, newline='') as f:
                rr = csv.DictReader(f)
                if rr.fieldnames == fields:
                    existing = {row[dt_col]: row for row in rr}
        for r in recs:
            existing[r['dt']] = {dt_col: r['dt'], 'ma200': r['ma200'],
                                  'ma150': r['ma150'], 'ma50': r['ma50']}
        with open(path, 'w', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for dd in sorted(existing):
                w.writerow(existing[dd])
        total += len(recs)
    return total


def compute_daily_ma(symbol, start_date=None, end_date=None):
    """Compute daily SMA (pure daily rolling window) and upsert into the
    relevant year's _ma_<year>.csv. Returns rows written."""
    dates, closes = _read_series(symbol, _daily_src_path, 'datetime')
    if not dates:
        return 0
    results = _compute_daily_ma(dates, closes, start_date, end_date)
    return _upsert(symbol, _ma_path, DAILY_FIELDS, 'date', results)


def compute_minute_ma(symbol, start_dt=None, end_dt=None):
    """Compute minute SMA — (N-1) prior daily closes + this minute's close —
    and upsert into the relevant year's _ma_minute_<year>.csv. Returns rows
    written. Only timestamps that exist in minute.csv are ever written."""
    daily_dates, daily_closes = _read_series(symbol, _daily_src_path, 'datetime')
    minute_dates, minute_closes = _read_series(symbol, _minute_src_path, 'datetime')
    if not minute_dates:
        return 0
    results = _compute_minute_ma(daily_dates, daily_closes, minute_dates, minute_closes,
                                  start_dt, end_dt)
    return _upsert(symbol, _ma_min_path, MINUTE_FIELDS, 'datetime', results)


if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        sys.exit('Usage: ma_minute.py <SYMBOL> [--minute|--both] [start]')
    symbol  = args[0].upper()
    mode    = 'daily'
    start   = None
    for a in args[1:]:
        if a == '--minute': mode = 'minute'
        elif a == '--both': mode = 'both'
        else: start = a
    if mode in ('daily', 'both'):
        n = compute_daily_ma(symbol, start)
        print(f'Daily MA: {n} date(s) written')
    if mode in ('minute', 'both'):
        n = compute_minute_ma(symbol, start)
        print(f'Minute MA: {n} datetime(s) written')