#!/usr/bin/env python3
import sys, os, csv

DATA_DIR   = 'data'
MA_PERIODS = (200, 150, 50)
FIELDNAMES = ['date', 'ma200', 'ma150', 'ma50']

def _daily_path(symbol): return os.path.join(DATA_DIR, f'{symbol}_daily.csv')
def _ma_path(symbol):    return os.path.join(DATA_DIR, f'{symbol}_ma.csv')

def _read_closes(symbol):
    path = _daily_path(symbol)
    if not os.path.isfile(path):
        return [], []
    with open(path, newline='') as f:
        rows = sorted(csv.DictReader(f), key=lambda r: r['datetime'])
    return [r['datetime'] for r in rows], [float(r['close']) for r in rows]

def compute_ma_series(symbol, start_date=None, end_date=None):
    dates, closes = _read_closes(symbol)
    if not dates:
        return None
    n = len(dates)
    if start_date is None and end_date is None:
        idxs = [n - 1]
    elif end_date is None:
        idxs = [i for i, d in enumerate(dates) if d >= start_date]
    else:
        idxs = [i for i, d in enumerate(dates) if start_date <= d <= end_date]
    results = []
    for i in idxs:
        results.append({'date': dates[i],
            **{f'ma{p}': (round(sum(closes[i+1-p:i+1])/p, 4) if i+1 >= p else -1)
               for p in MA_PERIODS}})
    return results

def write_ma_rows(symbol, results):
    if not results:
        return
    path = _ma_path(symbol)
    os.makedirs(DATA_DIR, exist_ok=True)
    existing = {}
    if os.path.isfile(path):
        with open(path, newline='') as f:
            r = csv.DictReader(f)
            if r.fieldnames == FIELDNAMES:
                existing = {row['date']: row for row in r}
    for r in results:
        existing[r['date']] = r
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES)
        w.writeheader()
        for d in sorted(existing):
            w.writerow(existing[d])

if __name__ == '__main__':
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        sys.exit('Usage: ma.py <SYMBOL> [start_yyyymmdd] [end_yyyymmdd]')
    symbol = sys.argv[1].upper()
    start  = sys.argv[2] if len(sys.argv) > 2 else None
    end    = sys.argv[3] if len(sys.argv) > 3 else None
    series = compute_ma_series(symbol, start, end)
    if series is None:
        print(f'No data for {symbol}')
        sys.exit(0)
    write_ma_rows(symbol, series)
    print(f'Updated {_ma_path(symbol)} with {len(series)} date(s)')