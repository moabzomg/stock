import sys
import csv
from datetime import datetime
from ib_insync import IB, Stock

PORT      = 7496
HOST      = '127.0.0.1'
CLIENT_ID = 2

ticker_symbol = sys.argv[1].upper() if len(sys.argv) > 1 else print("Usage: python3 scrap_stock.py <ticker>") or sys.exit(1)

ib = IB()
ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)

contract = Stock(ticker_symbol, 'SMART', 'USD')
ib.qualifyContracts(contract)

now = datetime.now()

bars_min = ib.reqHistoricalData(contract, endDateTime='', durationStr='1 D',
                                barSizeSetting='1 min', whatToShow='TRADES',
                                useRTH=True, formatDate=1)

fname_min = f"{ticker_symbol}_{now.strftime('%Y%m%d')}.csv"
with open(fname_min, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['date', 'price', 'volume'])
    for b in bars_min:
        w.writerow([b.date.strftime('%Y%m%d%H%M%S'), b.close, b.volume])
print(f"saved {fname_min} ({len(bars_min)} rows)")

bars_day = ib.reqHistoricalData(contract, endDateTime='', durationStr='2 Y',
                                barSizeSetting='1 day', whatToShow='TRADES',
                                useRTH=True, formatDate=1)

start = bars_day[0].date.strftime('%Y%m%d')
end   = now.strftime('%Y%m%d')
fname_day = f"{ticker_symbol}_{start}_{end}.csv"
with open(fname_day, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow(['date', 'open', 'high', 'low', 'close', 'volume', 'average', 'barCount'])
    for b in bars_day:
        w.writerow([b.date.strftime('%Y%m%d'), b.open, b.high, b.low, b.close, b.volume, b.average, b.barCount])
print(f"saved {fname_day} ({len(bars_day)} rows)")

ib.disconnect()