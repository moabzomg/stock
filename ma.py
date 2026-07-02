#!/usr/bin/env python3
"""
ma.py — Compute daily and/or minute SMAs.

Daily MA  (200/150/50 days):    reads _daily.csv,  writes _ma.csv
Minute MA (200/150/50 periods): reads _daily.csv + _minute.csv, writes _ma_minute.csv
                                 Each minute MA blends (period-1) prior *completed*
                                 daily closes with the single minute's own close,
                                 so it stays stable across days regardless of how
                                 much minute history is retained on disk.

Usage:
    python3 ma.py <SYMBOL> [start_yyyymmdd]                # daily MA only
    python3 ma.py <SYMBOL> --minute [start_yyyymmddhhmm]   # minute MA only
    python3 ma.py <SYMBOL> --both [start_yyyymmdd]         # both
"""

import sys, os, csv, bisect

DATA_DIR      = 'data'
MA_PERIODS    = (200, 150, 50)
DAILY_FIELDS  = ['date', 'ma200', 'ma150', 'ma50']
MINUTE_FIELDS = ['datetime', 'ma200', 'ma150', 'ma50']

def _daily_path(symbol):  return os.path.join(DATA_DIR, f'{symbol}_daily.csv')
def _ma_path(symbol):     return os.path.join(DATA_DIR, f'{symbol}_ma.csv')
def _minute_path(symbol): return os.path.join(DATA_DIR, f'{symbol}_minute.csv')
def _ma_min_path(symbol): return os.path.join(DATA_DIR, f'{symbol}_ma_minute.csv')


def _read_series(path, dt_col):
    """Read a CSV and return (datetimes, closes) sorted ascending."""
    if not os.path.isfile(path):
        return [], []
    with open(path, newline='') as f:
        rows = sorted(csv.DictReader(f), key=lambda r: r[dt_col])
    return [r[dt_col] for r in rows], [float(r['close']) for r in rows]


def _compute_ma(dates, closes, start=None, end=None):
    """Return list of {dt_col: ..., ma200, ma150, ma50} dicts.

    Pure rolling average over `dates`/`closes` themselves — used for the
    daily MA, where the series is self-contained.
    """
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


def _upsert(path, fields, dt_col, results):
    """Upsert results into the MA CSV, keyed by dt_col."""
    if not results:
        return 0
    os.makedirs(DATA_DIR, exist_ok=True)
    existing = {}
    if os.path.isfile(path):
        with open(path, newline='') as f:
            r = csv.DictReader(f)
            if r.fieldnames == fields:
                existing = {row[dt_col]: row for row in r}
    for r in results:
        existing[r['dt']] = {dt_col: r['dt'], 'ma200': r['ma200'],
                              'ma150': r['ma150'], 'ma50': r['ma50']}
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for d in sorted(existing):
            w.writerow(existing[d])
    return len(results)


def compute_daily_ma(symbol, start_date=None, end_date=None):
    """Compute daily SMA and upsert into _ma.csv. Returns rows written."""
    dates, closes = _read_series(_daily_path(symbol), 'datetime')
    if not dates:
        return 0
    results = _compute_ma(dates, closes, start_date, end_date)
    return _upsert(_ma_path(symbol), DAILY_FIELDS, 'date', results)


def compute_minute_ma(symbol, start_dt=None, end_dt=None):
    """Compute minute SMA and upsert into _ma_minute.csv. Returns rows written.

    For a minute timestamp `dt` (e.g. 202607011005), the moving average of
    period `p` is built from the (p-1) most recent *completed* daily closes
    strictly before `dt`'s calendar day, plus that minute's own close, e.g.
    ma200 @ 202607011005 = avg(199 daily closes ending 20260630, plus the
    202607011005 minute close).

    This is deliberately NOT a pure rolling average over minute.csv rows:
    minute.csv is pruned to a short rolling window (see extract_data.py's
    MINUTE_DAYS), so a self-contained minute-only average would be unstable
    and mostly meaningless for period=200. Anchoring to the daily close
    series keeps the average stable across restarts/pruning.

    Only timestamps already present in minute.csv are ever emitted (idxs is
    derived from minute_dates itself), so e.g. a missing 202607011007 simply
    produces no output row for that minute — it is never invented.

    Day lookup uses bisect_left on the daily date list, which lands exactly
    on `day` if a same-day row exists there (e.g. today's in-progress bar
    written by extract_data.py's live collapse) and thus correctly excludes
    it — only *prior* days ever contribute.
    """
    daily_dates, daily_closes   = _read_series(_daily_path(symbol),  'datetime')
    minute_dates, minute_closes = _read_series(_minute_path(symbol), 'datetime')
    if not minute_dates:
        return 0

    n = len(minute_dates)
    if start_dt is None and end_dt is None:
        idxs = [n - 1]
    elif end_dt is None:
        idxs = [i for i, d in enumerate(minute_dates) if d >= start_dt]
    else:
        idxs = [i for i, d in enumerate(minute_dates) if start_dt <= d <= end_dt]

    results = []
    for i in idxs:
        dt  = minute_dates[i]
        day = dt[:8]
        # Index of the last daily date strictly before `day`. bisect_left
        # returns the insertion point to the left of any existing equal
        # entry, so if `day` itself is present in daily_dates (e.g. a
        # same-day 'live' row), pos ends up pointing at the day *before* it.
        pos = bisect.bisect_left(daily_dates, day) - 1

        row = {'dt': dt}
        for p in MA_PERIODS:
            need = p - 1  # prior daily closes required
            if pos + 1 >= need:
                prior = daily_closes[pos - need + 1: pos + 1]
                row[f'ma{p}'] = round((sum(prior) + minute_closes[i]) / p, 4)
            else:
                row[f'ma{p}'] = -1
        results.append(row)

    return _upsert(_ma_min_path(symbol), MINUTE_FIELDS, 'datetime', results)


if __name__ == '__main__':
    args = sys.argv[1:]
    if not args:
        sys.exit('Usage: ma.py <SYMBOL> [--minute|--both] [start]')
    symbol  = args[0].upper()
    mode    = 'daily'
    start   = None
    for a in args[1:]:
        if a == '--minute': mode = 'minute'
        elif a == '--both': mode = 'both'
        else: start = a
    if mode in ('daily', 'both'):
        n = compute_daily_ma(symbol, start)
        print(f'Daily MA: {_ma_path(symbol)} ({n} date(s))')
    if mode in ('minute', 'both'):
        n = compute_minute_ma(symbol, start)
        print(f'Minute MA: {_ma_min_path(symbol)} ({n} datetime(s))')