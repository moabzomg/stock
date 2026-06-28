#!/usr/bin/env python3
import sys, os, csv

DATA_DIR   = 'data'
FIELDNAMES = ['datetime', 'open', 'close', 'high', 'low', 'volume']

def _path(symbol, kind):
    return os.path.join(DATA_DIR, f'{symbol}_{kind}.csv')

def _load(path):
    if not os.path.exists(path):
        return {}
    with open(path, newline='') as f:
        return {r['datetime']: {'datetime': r['datetime'], 'open': float(r['open']),
                'close': float(r['close']), 'high': float(r['high']),
                'low': float(r['low']), 'volume': float(r['volume'])}
                for r in csv.DictReader(f)}

def _save(path, rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(path, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction='ignore')
        w.writeheader()
        w.writerows(sorted(rows.values(), key=lambda r: r['datetime']))

def _collapse(day, bars):
    return {'datetime': day, 'open': bars[0]['open'], 'close': bars[-1]['close'],
            'high': max(b['high'] for b in bars), 'low': min(b['low'] for b in bars),
            'volume': sum(b['volume'] for b in bars)}

def main(argv=None):
    args = (argv or sys.argv[1:])
    if not args:
        sys.exit('Usage: collapse_to_daily.py <SYMBOL> [--force-today]')
    symbol      = args[0].upper()
    force_today = '--force-today' in args[1:]

    minute_rows = _load(_path(symbol, 'minute'))
    if not minute_rows:
        print(f'[{symbol}] no minute data')
        return

    daily_rows  = _load(_path(symbol, 'daily'))
    minute_days = sorted({k[:8] for k in minute_rows if len(k) == 12})
    last_day    = minute_days[-1]

    to_write = [d for d in minute_days if d != last_day]
    if force_today or last_day not in daily_rows:
        to_write.append(last_day)

    written = []
    for day in to_write:
        bars = sorted((r for k, r in minute_rows.items()
                       if len(k) == 12 and k[:8] == day), key=lambda r: r['datetime'])
        if not bars:
            continue
        daily_rows[day] = _collapse(day, bars)
        written.append(day)

    if written:
        _save(_path(symbol, 'daily'), daily_rows)
        print(f'[{symbol}] collapsed {len(written)} day(s) to daily CSV')
    else:
        print(f'[{symbol}] already up to date')

if __name__ == '__main__':
    main()