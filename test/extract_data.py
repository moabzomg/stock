import sys
import os
import csv
import time
from datetime import datetime

import yfinance as yf
from ib_insync import IB, Stock

DATA_DIR = 'data'
POLL_SECONDS = 30

IB_HOST = '127.0.0.1'
IB_PORT = 7496
IB_CLIENT_ID = 2
CHUNK_DAYS = 10


def _to_naive(dt):
    return dt.replace(tzinfo=None) if dt.tzinfo else dt


def get_historical_data(symbol, start_date):
    start = datetime.strptime(start_date, '%Y%m%d')

    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)

    contract = Stock(symbol, 'SMART', 'USD')
    ib.qualifyContracts(contract)

    all_bars = {}
    end_dt = ''

    while True:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=end_dt,
            durationStr=f'{CHUNK_DAYS} D',
            barSizeSetting='1 min',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1,
        )
        if not bars:
            break

        for b in bars:
            all_bars[_to_naive(b.date)] = b

        oldest = _to_naive(bars[0].date)
        if oldest <= start:
            break

        end_dt = bars[0].date
        ib.sleep(2)

    ib.disconnect()

    ordered = sorted(
        (b for dt, b in all_bars.items() if dt >= start),
        key=lambda b: _to_naive(b.date),
    )

    os.makedirs(DATA_DIR, exist_ok=True)
    fpath = os.path.join(DATA_DIR, f"{symbol}.csv")

    with open(fpath, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'open', 'high', 'low', 'close', 'price', 'volume'])
        for b in ordered:
            w.writerow([
                _to_naive(b.date).strftime('%Y%m%d%H%M'),
                b.open, b.high, b.low, b.close, b.close, b.volume,
            ])

    print(f"saved {fpath} ({len(ordered)} rows, source=IB)")
    return fpath


def get_last_ticker(symbol):
    ticker = yf.Ticker(symbol)
    bars = ticker.history(period='1d', interval='1m')
    if bars.empty:
        return None
    last = bars.iloc[-1]
    return {
        'date': bars.index[-1].strftime('%Y%m%d%H%M'),
        'open': last['Open'],
        'high': last['High'],
        'low': last['Low'],
        'close': last['Close'],
        'price': last['Close'],
        'volume': int(last['Volume']),
    }


def append_live_row(symbol):
    row = get_last_ticker(symbol)
    if row is None:
        return

    os.makedirs(DATA_DIR, exist_ok=True)
    fpath = os.path.join(DATA_DIR, f"{symbol}.csv")

    file_exists = os.path.isfile(fpath)
    last_date_written = None
    if file_exists:
        with open(fpath, 'r', newline='') as f:
            lines = f.readlines()
            if len(lines) > 1:
                last_date_written = lines[-1].split(',')[0]

    if row['date'] == last_date_written:
        return

    with open(fpath, 'a', newline='') as f:
        w = csv.writer(f)
        if not file_exists:
            w.writerow(['date', 'open', 'high', 'low', 'close', 'price', 'volume'])
        w.writerow([row['date'], row['open'], row['high'], row['low'], row['close'], row['price'], row['volume']])

    print(row)


def run_live_loop(symbol):
    print(f"Polling {symbol} via yfinance every {POLL_SECONDS}s. Ctrl+C to stop.")
    while True:
        append_live_row(symbol)
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 last_ticker.py <symbol> [yyyymmdd]")
        sys.exit(1)

    symbol_arg = sys.argv[1].upper()

    if len(sys.argv) > 2:
        get_historical_data(symbol_arg, sys.argv[2])

    run_live_loop(symbol_arg)