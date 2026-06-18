import sys
import os
import csv
import time
from datetime import datetime, timedelta

import yfinance as yf
from ib_insync import IB, Stock

DATA_DIR = 'data'
POLL_SECONDS = 30
MINUTE_DAYS = 3
BOOTSTRAP_DAYS = 730

IB_HOST = '127.0.0.1'
IB_PORT = 7497
IB_CLIENT_ID = 2


def _to_naive(dt):
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def get_historical_data(symbol, start_date):
    start = datetime.strptime(start_date, '%Y%m%d')
    now = datetime.now()
    cutoff = now - timedelta(days=MINUTE_DAYS)

    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)

    contract = Stock(symbol, 'SMART', 'USD')

    rows = []

    if start < cutoff:
        duration_days = (cutoff - start).days + 1
        duration_str = f'{duration_days} D' if duration_days<365 else f'{duration_days//365} Y'
        day_bars = ib.reqHistoricalData(
            contract,
            endDateTime=cutoff,
            durationStr=duration_str,
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1,
        )
        for b in day_bars:
            d = b.date if isinstance(b.date, datetime) else datetime.combine(b.date, datetime.min.time())
            rows.append([d.strftime('%Y%m%d%H%M'), b.open, b.high, b.low, b.close, b.close, b.volume])
        print(f"fetched {len(day_bars)} daily bars up to {cutoff.strftime('%Y-%m-%d')}")
        minute_start = cutoff
    else:
        minute_start = start

    day_cursor = minute_start.date()
    today = now.date()

    while day_cursor <= today:
        end_of_day = datetime.combine(day_cursor, datetime.min.time()) + timedelta(days=1)

        minute_bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_of_day,
            durationStr='1 D',
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1,
        )
        for b in minute_bars:
            bd = _to_naive(b.date)
            if bd >= minute_start:
                rows.append([bd.strftime('%Y%m%d%H%M'), b.open, b.high, b.low, b.close, b.close, b.volume])

        print(f"fetched {len(minute_bars)} minute bars for {day_cursor}")
        day_cursor += timedelta(days=1)
        ib.sleep(2)

    ib.disconnect()

    rows.sort(key=lambda r: r[0])

    os.makedirs(DATA_DIR, exist_ok=True)
    fpath = os.path.join(DATA_DIR, f"{symbol}.csv")
    with open(fpath, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'open', 'high', 'low', 'close', 'price', 'volume'])
        for r in rows:
            w.writerow(r)

    print(f"saved {fpath} ({len(rows)} rows)")
    return fpath


def append_live_rows(symbol):
    fpath = os.path.join(DATA_DIR, f"{symbol}.csv")
    file_exists = os.path.isfile(fpath)

    last_date_written = None
    if file_exists:
        with open(fpath, 'r', newline='') as f:
            lines = f.readlines()
            if len(lines) > 1:
                last_date_written = lines[-1].split(',')[0]

    ticker = yf.Ticker(symbol)
    bars = ticker.history(period='7d', interval='1m')
    if bars.empty:
        return

    new_rows = []
    for idx, row in bars.iterrows():
        date_str = idx.strftime('%Y%m%d%H%M')
        if last_date_written is not None and date_str <= last_date_written:
            continue
        new_rows.append([
            date_str, row['Open'], row['High'], row['Low'], row['Close'],
            row['Close'], int(row['Volume']),
        ])

    if not new_rows:
        return

    with open(fpath, 'a', newline='') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(['date', 'open', 'high', 'low', 'close', 'price', 'volume'])
        for r in new_rows:
            w.writerow(r)
            print(r)


def run_live_loop(symbol):
    fpath = os.path.join(DATA_DIR, f"{symbol}.csv")
    if not os.path.isfile(fpath):
        bootstrap_start = (datetime.now() - timedelta(days=BOOTSTRAP_DAYS)).strftime('%Y%m%d')
        get_historical_data(symbol, bootstrap_start)

    print(f"Polling {symbol} via yfinance every {POLL_SECONDS}s. Ctrl+C to stop.")
    while True:
        append_live_rows(symbol)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 last_ticker.py <symbol> [yyyymmdd]")
        sys.exit(1)

    symbol_arg = sys.argv[1].upper()

    if len(sys.argv) > 2:
        get_historical_data(symbol_arg, sys.argv[2])
    else:
        run_live_loop(symbol_arg)