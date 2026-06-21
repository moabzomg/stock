import sys
import os
import csv

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR   = 'data'
MA_PERIODS = (200, 150, 50)   # trading-day lookback windows
NA         = -1               # used whenever there isn't enough history

FIELDNAMES = ['date', 'symbol', 'ma200', 'ma150', 'ma50', 'latest price', 'latest volume']


def csv_path(symbol: str) -> str:
    return os.path.join(DATA_DIR, f'{symbol}.csv')


def ma_csv_path(symbol: str) -> str:
    return os.path.join(DATA_DIR, f'{symbol}_ma.csv')


def _read_rows(fpath: str) -> list:
    with open(fpath, newline='') as f:
        r = csv.reader(f)
        next(r, None)   # header: datetime,open,close,high,low,volume
        return [row for row in r if row]


def _is_minute_row(row: list) -> bool:
    """True when the timestamp is minute-resolution (YYYYMMDDHHmm, len==12)."""
    return len(row[0]) == 12


def _daily_rows(rows: list) -> dict:
    """
    Collapse rows into one representative row per calendar day, handling a
    file that mixes two resolutions:

      • Day rows    – timestamp is 'YYYYMMDD' (len 8).  One row per day.
      • Minute rows – timestamp is 'YYYYMMDDHHmm' (len 12).  Many per day.

    Rules
    -----
    1. If a day has *any* minute rows, those are used exclusively (the daily
       row for that same date, if present, is ignored — minute data is
       authoritative for live/recent days).
    2. The **close** used for that day is the close of the *last* minute bar
       (highest timestamp), matching standard MA convention.
    3. The **volume** for a minute-data day is the *sum* of all minute bars'
       volumes, giving the true day total rather than a single bar's volume.
    4. For pure day-resolution days, the single daily row is used as-is.

    Returns {yyyymmdd: row} where row[2] is the day's close and row[5] is
    the day's total volume.
    """
    minute_by_day = {}   # day -> list of minute rows
    daily_by_day  = {}   # day -> single daily row

    for row in rows:
        day = row[0][:8]
        if _is_minute_row(row):
            minute_by_day.setdefault(day, []).append(row)
        else:
            # Keep the latest if somehow multiple daily rows exist for one day
            if day not in daily_by_day or row[0] > daily_by_day[day][0]:
                daily_by_day[day] = row

    by_day = {}

    # Days that have only a daily row
    for day, row in daily_by_day.items():
        if day not in minute_by_day:
            by_day[day] = row

    # Days that have minute bars — minute data takes precedence
    for day, mrows in minute_by_day.items():
        mrows_sorted = sorted(mrows, key=lambda r: r[0])
        last_bar     = mrows_sorted[-1]   # latest minute bar → provides close

        total_volume = sum(float(r[5]) for r in mrows_sorted)

        # Copy the last bar's columns; replace volume with the day total
        synthetic    = list(last_bar)
        synthetic[5] = str(int(total_volume))
        by_day[day]  = synthetic

    return by_day


def compute_ma_series(symbol: str, start_date: str = None, end_date: str = None):
    """
    Returns a list of per-day result dicts, or None if no data is available
    for *symbol* at all.

      • start_date is None and end_date is None -> just the latest day
      • start_date given, end_date is None       -> start_date .. latest day
      • both given                               -> start_date .. end_date (inclusive)

    Dates are 'YYYYMMDD'. Only days that actually have data are returned —
    there's no fabricated row for a day with no underlying data.
    """
    fpath = csv_path(symbol)
    if not os.path.isfile(fpath):
        return None

    rows = _read_rows(fpath)
    if not rows:
        return None

    by_day = _daily_rows(rows)
    ordered_days = sorted(by_day)
    if not ordered_days:
        return None

    closes = [float(by_day[d][2]) for d in ordered_days]   # index 2 = close

    if start_date is None and end_date is None:
        target_idxs = [len(ordered_days) - 1]
    elif end_date is None:
        target_idxs = [i for i, d in enumerate(ordered_days) if d >= start_date]
    else:
        target_idxs = [i for i, d in enumerate(ordered_days)
                        if start_date <= d <= end_date]

    results = []
    for i in target_idxs:
        d = ordered_days[i]
        row = by_day[d]

        mas = {}
        for period in MA_PERIODS:
            if i + 1 >= period:
                window = closes[i + 1 - period: i + 1]
                mas[period] = round(sum(window) / period, 4)
            else:
                mas[period] = NA

        results.append({
            'date':          d,        # YYYYMMDD
            'symbol':        symbol,
            'ma200':         mas[200],
            'ma150':         mas[150],
            'ma50':          mas[50],
            'latest price':  row[2],   # index 2 = close (last minute bar on minute days)
            'latest volume': row[5],   # index 5 = volume (summed across bars on minute days)
        })
    return results


def write_ma_rows(symbol: str, results: list):
    """Append new dates / overwrite existing dates in <symbol>_ma.csv, then
    rewrite the whole file sorted by date."""
    if not results:
        return

    fpath = ma_csv_path(symbol)
    os.makedirs(DATA_DIR, exist_ok=True)

    existing = {}
    if os.path.isfile(fpath):
        with open(fpath, newline='') as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != FIELDNAMES:
                print(f"[warn] {fpath} has a different/old column layout "
                      f"{reader.fieldnames} — rebuilding it with the current "
                      f"layout {FIELDNAMES}.")
            else:
                for row in reader:
                    existing[row['date']] = row

    for r in results:
        existing[r['date']] = r   # append if new date, overwrite if it exists

    with open(fpath, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for d in sorted(existing):          # keep the file sorted by date
            w.writerow(existing[d])


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        print('Usage: python3 ma.py <SYMBOL> [start_yyyymmdd] [end_yyyymmdd]')
        sys.exit(1)

    symbol_arg = sys.argv[1].upper()
    start_arg  = sys.argv[2] if len(sys.argv) > 2 else None
    end_arg    = sys.argv[3] if len(sys.argv) > 3 else None

    series = compute_ma_series(symbol_arg, start_arg, end_arg)

    if series is None:
        print(f"No data available for {symbol_arg} "
              f"(data/{symbol_arg}.csv missing or empty) — nothing written")
        sys.exit(0)

    write_ma_rows(symbol_arg, series)
    print(f"Updated {ma_csv_path(symbol_arg)} with {len(series)} date(s):")
    for r in series:
        print(f"  {r['date']}  ma200={r['ma200']}  ma150={r['ma150']}  "
              f"ma50={r['ma50']}  price={r['latest price']}  volume={r['latest volume']}")