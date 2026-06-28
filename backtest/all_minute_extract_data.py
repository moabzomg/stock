#!/usr/bin/env python3
"""
Usage:
    python3 all_minute_extract_data.py <SYMBOL> <YYYYMMDDHHmm>
"""

import sys, os, csv, time, threading, subprocess
from datetime import datetime, timedelta, timezone
from collections import deque

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

IB_HOST   = "127.0.0.1"
IB_PORT   = 7496
CLIENT_ID = 10

INTER_REQUEST_SLEEP  = 10
MAX_REQUESTS_PER_10M = 55
REQUEST_WINDOW_SECS  = 600
CHUNK_DURATION       = "1 D"
CHUNK_STEP_DAYS      = 1


class IBExtractor(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)
        self._req_id     = 1
        self._bars       = []
        self._done       = threading.Event()
        self._error      = False
        self._request_ts = deque()

    def historicalData(self, reqId, bar):
        raw = bar.date.strip()
        try:
            dt = datetime.fromtimestamp(int(raw), tz=timezone.utc)
        except ValueError:
            dt = datetime.strptime(raw, "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
        self._bars.append({
            "datetime": dt.strftime("%Y%m%d%H%M"),
            "open": bar.open, "close": bar.close,
            "high": bar.high, "low":   bar.low, "volume": bar.volume,
        })

    def historicalDataEnd(self, reqId, start, end):
        self._done.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in (2104, 2106, 2107, 2108, 2119, 2150, 2158):
            return
        if errorCode in (162, 321, 1100, 1101, 1102, 2110):
            print(f"  [warn] {errorCode}: {errorString}")
            self._done.set()
        else:
            print(f"  [error] {errorCode}: {errorString}")
            self._error = True
            self._done.set()

    def connectAck(self):
        print(f"  [ib] connected to {IB_HOST}:{IB_PORT}")

    def _pace(self):
        now = time.time()
        while self._request_ts and now - self._request_ts[0] > REQUEST_WINDOW_SECS:
            self._request_ts.popleft()
        if len(self._request_ts) >= MAX_REQUESTS_PER_10M:
            wait = REQUEST_WINDOW_SECS - (now - self._request_ts[0]) + 1
            print(f"  [pace] sleeping {wait:.0f}s")
            time.sleep(wait)
            now = time.time()
            while self._request_ts and now - self._request_ts[0] > REQUEST_WINDOW_SECS:
                self._request_ts.popleft()
        if self._request_ts:
            elapsed = time.time() - self._request_ts[-1]
            if elapsed < INTER_REQUEST_SLEEP:
                time.sleep(INTER_REQUEST_SLEEP - elapsed)
        self._request_ts.append(time.time())

    def fetch(self, contract, end_dt):
        self._bars, self._error = [], False
        self._done.clear()
        self._pace()
        end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")
        print(f"  → end={end_str} ...", end="", flush=True)
        self.reqHistoricalData(
            self._req_id, contract, end_str, CHUNK_DURATION,
            "1 min", "TRADES", 0, 2, 0, [],
        )
        self._req_id += 1
        if not self._done.wait(timeout=120):
            print(" TIMEOUT")
            self.cancelHistoricalData(self._req_id - 1)
            return []
        bars = list(self._bars)
        print(f" {len(bars)} bars" + (f"  ({bars[0]['datetime']} → {bars[-1]['datetime']})" if bars else ""))
        return bars


def make_contract(symbol):
    c = Contract()
    c.symbol, c.secType, c.exchange, c.currency = symbol, "STK", "SMART", "USD"
    return c


def is_valid(bar):
    try:
        o, c, h, l, v = float(bar["open"]), float(bar["close"]), float(bar["high"]), float(bar["low"]), float(bar["volume"])
        return o > 0 and c > 0 and h > 0 and l > 0 and h >= l and v >= 0
    except (ValueError, TypeError, KeyError):
        return False


def load_csv(symbol):
    path = os.path.join("data", f"{symbol}_minute.csv")
    if not os.path.exists(path):
        return [], set()
    with open(path, newline="") as f:
        bars = [{"datetime": r["datetime"], "open": float(r["open"]), "close": float(r["close"]),
                 "high": float(r["high"]), "low": float(r["low"]), "volume": float(r["volume"])}
                for r in csv.DictReader(f)]
    bars = [b for b in bars if is_valid(b)]
    seen = {b["datetime"] for b in bars}
    if bars:
        print(f"  [resume] {len(bars)} existing bars ({min(seen)} → {max(seen)})")
    return bars, seen


def save_csv(symbol, bars):
    os.makedirs("data", exist_ok=True)
    path = os.path.join("data", f"{symbol}_minute.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["datetime","open","close","high","low","volume"], extrasaction="ignore")
        w.writeheader()
        w.writerows(bars)
    print(f"  [saved] {len(bars)} bars → {path}")
    return path


def compute_gaps(seen, start_dt, now_utc):
    midnight = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if not seen:
        return [(start_dt, midnight)]
    days      = {dt[:8] for dt in seen}
    earliest  = datetime.strptime(min(days), "%Y%m%d").replace(tzinfo=timezone.utc)
    latest    = datetime.strptime(max(days), "%Y%m%d").replace(tzinfo=timezone.utc)
    gaps = []
    if start_dt < earliest:
        # Include earliest day itself — it may be incomplete.
        gaps.append((start_dt, earliest + timedelta(days=1)))
    # Include latest day itself — it may be incomplete.
    if latest < midnight:
        gaps.append((latest, midnight))
    return gaps


def run_collapse_and_ma(symbol, earliest_date):
    base = os.path.dirname(__file__)
    for cmd in [
        [sys.executable, os.path.join(base, "collapse_to_daily.py"), symbol],
        [sys.executable, os.path.join(base, "ma.py"), symbol, earliest_date],
    ]:
        r = subprocess.run(cmd)
        if r.returncode != 0:
            print(f"  [warn] {os.path.basename(cmd[1])} exited {r.returncode}")


def main():
    if len(sys.argv) != 3:
        sys.exit("Usage: all_minute_extract_data.py <SYMBOL> <YYYYMMDDHHmm>")

    symbol   = sys.argv[1].upper()
    try:
        start_dt = datetime.strptime(sys.argv[2], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    except ValueError:
        sys.exit(f"ERROR: bad datetime '{sys.argv[2]}', expected YYYYMMDDHHmm")

    now_utc = datetime.now(timezone.utc)
    if start_dt >= now_utc:
        sys.exit("ERROR: start must be in the past")

    print(f"[{symbol}] {start_dt:%Y-%m-%d %H:%M} → {now_utc:%Y-%m-%d %H:%M} UTC")

    app = IBExtractor()
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)
    threading.Thread(target=app.run, daemon=True).start()
    time.sleep(2)
    if not app.isConnected():
        sys.exit("ERROR: could not connect to IB")

    all_bars, seen = load_csv(symbol)
    before         = len(all_bars)
    gaps           = compute_gaps(seen, start_dt, now_utc)

    if not gaps:
        print(f"[{symbol}] already up to date")
    else:
        for gap_start, gap_end in gaps:
            print(f"  [gap] {gap_start:%Y-%m-%d} → {gap_end:%Y-%m-%d}")

    for gap_start, gap_end in gaps:
        chunk_end  = gap_end
        last_first = None
        while chunk_end > gap_start:
            bars = app.fetch(make_contract(symbol), chunk_end)
            if app._error:
                print(f"  [error] stopping {symbol}")
                break
            if bars:
                this_first = bars[0]["datetime"]
                if this_first == last_first:
                    chunk_end -= timedelta(days=CHUNK_STEP_DAYS)
                    continue
                last_first = this_first
            for b in bars:
                bdt = datetime.strptime(b["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                if bdt >= start_dt and b["datetime"] not in seen and is_valid(b):
                    seen.add(b["datetime"])
                    all_bars.append(b)
            chunk_end -= timedelta(days=CHUNK_STEP_DAYS)
        else:
            continue
        break

    app.disconnect()

    if not all_bars:
        sys.exit(f"[{symbol}] no bars — nothing saved")

    all_bars.sort(key=lambda b: b["datetime"])
    save_csv(symbol, all_bars)
    print(f"[{symbol}] +{len(all_bars) - before} new bars, {len(all_bars)} total")

    run_collapse_and_ma(symbol, all_bars[0]["datetime"][:8])


if __name__ == "__main__":
    main()