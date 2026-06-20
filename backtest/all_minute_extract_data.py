#!/usr/bin/env python3
"""
Extract 1-minute bar historical data from IB TWS/Gateway.

Usage:
    python3 all_minute_extract_data.py <SYMBOL> <YYYYMMDDHHmm>

Example:
    python3 all_minute_extract_data.py AAPL 202401010930

Pacing rules observed (per IB docs):
  - No identical request within 15 s
  - No more than 6 requests for same contract/exchange/ticktype in 2 s
  - No more than 60 requests in any 10-minute rolling window
  - Each chunk uses "1 D" duration / "1 min" bar size (≤ ~390 bars/day RTH,
    up to ~960 bars/day when useRTH=0 and extended hours are included)
  - 10-second sleep between requests (safe, well under all limits)

Timezone handling:
  - All requests use endDateTime in the unambiguous "YYYYMMDD-HH:MM:SS UTC"
    format, so IB does not fall back to interpreting it in TWS's local
    timezone setting.
  - formatDate=2 is used, which makes IB return bar.date as a Unix epoch
    (seconds), which is inherently UTC and unambiguous — avoiding the
    exchange-local-time strings returned by formatDate=1.
  - All datetimes in the output CSV are therefore UTC.
"""

import sys
import time
import csv
import os
import threading
from datetime import datetime, timedelta, timezone
from collections import deque

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData

# ── Pacing constants ──────────────────────────────────────────────────────────
INTER_REQUEST_SLEEP   = 10          # seconds between each reqHistoricalData call
MAX_REQUESTS_PER_10M  = 55          # hard IB limit is 60; we use 55 for safety
REQUEST_WINDOW_SECS   = 600         # 10-minute rolling window
CHUNK_DURATION        = "1 D"       # one day per request (fits 1-min bars)
BAR_SIZE              = "1 min"
WHAT_TO_SHOW          = "TRADES"
USE_RTH               = 0           # 0 = include extended hours
KEEP_UP_TO_DATE       = 0

# Expected bar counts, used only for the sanity-check warning (not enforced)
EXPECTED_BARS_RTH       = 390       # ~6.5h regular trading hours
EXPECTED_BARS_EXTENDED  = 960       # ~16h with useRTH=0 (pre/post market included)

# ── IB connection defaults ────────────────────────────────────────────────────
IB_HOST = "127.0.0.1"
IB_PORT = 7496          
CLIENT_ID = 10


class IBMinuteExtractor(EWrapper, EClient):
    """Single-threaded IB app that pulls 1-min bars chunk by chunk."""

    def __init__(self):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self._req_id       = 1
        self._bars         = []          # accumulates bars for current request
        self._done_event   = threading.Event()
        self._error_flag   = False
        self._request_ts   = deque()     # timestamps of recent requests (rolling window)

    # ── EWrapper callbacks ────────────────────────────────────────────────────

    def historicalData(self, reqId: int, bar: BarData):
        # With formatDate=2, bar.date is a Unix epoch string (always UTC).
        # Keep a fallback for formatDate=1 style strings just in case.
        raw = bar.date.strip()
        try:
            dt = datetime.fromtimestamp(int(raw), tz=timezone.utc).replace(tzinfo=None)
        except ValueError:
            try:
                dt = datetime.strptime(raw, "%Y%m%d  %H:%M:%S")
            except ValueError:
                dt = datetime.strptime(raw, "%Y%m%d %H:%M:%S")
        fmt_dt = dt.strftime("%Y%m%d%H%M")

        self._bars.append({
            "datetime": fmt_dt,
            "open":     bar.open,
            "close":    bar.close,
            "high":     bar.high,
            "low":      bar.low,
            "volume":   bar.volume,
        })

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        self._done_event.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        # 162 = "Historical Market Data Service error" (no data for period) – non-fatal
        # 200 = no security definition – fatal
        if errorCode in (162, 321):
            print(f"  [warn] reqId={reqId} code={errorCode}: {errorString}")
            self._done_event.set()          # treat as empty result and continue
        elif errorCode in (1100, 1101, 1102, 2110):
            print(f"  [warn] connectivity code={errorCode}: {errorString}")
        elif errorCode < 1000:              # actual errors (not informational)
            print(f"  [ERROR] reqId={reqId} code={errorCode}: {errorString}")
            self._error_flag = True
            self._done_event.set()
        else:
            # informational (e.g. 2104 market data farm connected)
            pass

    def connectAck(self):
        print(f"Connected to IB TWS/Gateway at {IB_HOST}:{IB_PORT}")

    # ── Pacing helper ─────────────────────────────────────────────────────────

    def _pace(self):
        """
        Block until it is safe to fire the next request, enforcing:
          • ≤ MAX_REQUESTS_PER_10M requests in any 600-second window
          • INTER_REQUEST_SLEEP seconds since the last request
        """
        now = time.time()

        # Trim timestamps older than 10 minutes
        while self._request_ts and now - self._request_ts[0] > REQUEST_WINDOW_SECS:
            self._request_ts.popleft()

        # If we've hit the rolling-window cap, sleep until the oldest drops off
        if len(self._request_ts) >= MAX_REQUESTS_PER_10M:
            wait = REQUEST_WINDOW_SECS - (now - self._request_ts[0]) + 1
            print(f"  [pace] rolling window full – sleeping {wait:.1f}s …")
            time.sleep(wait)
            # Re-trim after sleep
            now = time.time()
            while self._request_ts and now - self._request_ts[0] > REQUEST_WINDOW_SECS:
                self._request_ts.popleft()

        # Enforce minimum gap between successive requests
        if self._request_ts:
            elapsed = time.time() - self._request_ts[-1]
            if elapsed < INTER_REQUEST_SLEEP:
                time.sleep(INTER_REQUEST_SLEEP - elapsed)

        self._request_ts.append(time.time())

    # ── Core fetch ────────────────────────────────────────────────────────────

    def fetch_chunk(self, contract: Contract, end_dt: datetime) -> list:
        """
        Request one day of 1-min bars ending at *end_dt* (must be UTC-aware).
        Returns list of bar dicts (may be empty).
        """
        # IB unambiguous UTC end-datetime format: "YYYYMMDD-HH:MM:SS UTC"
        # (dash between date/time, plus explicit "UTC" suffix). This avoids
        # IB silently falling back to TWS's local timezone setting, which is
        # what caused inconsistent/truncated chunks previously.
        end_str = end_dt.strftime("%Y%m%d-%H:%M:%S") + " UTC"

        self._bars = []
        self._done_event.clear()
        self._error_flag = False

        self._pace()

        print(f"  → requesting bars up to {end_str} …", end="", flush=True)

        self.reqHistoricalData(
            reqId          = self._req_id,
            contract       = contract,
            endDateTime    = end_str,
            durationStr    = CHUNK_DURATION,
            barSizeSetting = BAR_SIZE,
            whatToShow     = WHAT_TO_SHOW,
            useRTH         = USE_RTH,
            formatDate     = 2,     # 2 = Unix epoch seconds, always UTC, unambiguous
            keepUpToDate   = KEEP_UP_TO_DATE,
            chartOptions   = [],
        )
        self._req_id += 1

        # Wait for historicalDataEnd (or error) with a generous timeout
        finished = self._done_event.wait(timeout=120)
        if not finished:
            print(f" TIMEOUT")
            self.cancelHistoricalData(self._req_id - 1)
            return []

        bars = list(self._bars)

        # ── Sanity check ────────────────────────────────────────────────────
        # Flag chunks that look truncated so a bad day is visible immediately
        # instead of being silently merged into the output CSV.
        if bars:
            first_dt, last_dt = bars[0]["datetime"], bars[-1]["datetime"]
            expected = EXPECTED_BARS_RTH if USE_RTH else EXPECTED_BARS_EXTENDED
            note = ""
            if len(bars) < expected * 0.5:
                note = "  [!] looks truncated vs expected for a full weekday session"
            print(f" {len(bars)} bars  ({first_dt} → {last_dt}){note}")
        else:
            print(f" 0 bars")

        return bars


# ── Helpers ───────────────────────────────────────────────────────────────────

def make_contract(symbol: str) -> Contract:
    c = Contract()
    c.symbol   = symbol.upper()
    c.secType  = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


def parse_start(ts_str: str) -> datetime:
    try:
        dt = datetime.strptime(ts_str, "%Y%m%d%H%M")
    except ValueError:
        sys.exit(f"ERROR: cannot parse datetime '{ts_str}'. Expected YYYYMMDDHHmm.")
    # Return as timezone-aware UTC for safe comparisons
    return dt.replace(tzinfo=timezone.utc)


def bar_dt(bar: dict) -> datetime:
    """Parse bar['datetime'] (already YYYYMMDDHHmm, UTC) to aware datetime."""
    return datetime.strptime(bar["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def save_csv(symbol: str, all_bars: list, start_dt: datetime):
    os.makedirs("data", exist_ok=True)
    filename = os.path.join("data", f"{symbol}_{start_dt.strftime('%Y%m%d%H%M')}.csv")
    fieldnames = ["datetime", "open", "close", "high", "low", "volume"]
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_bars)
    print(f"\n✓ Saved {len(all_bars)} bars → {filename}  (all datetimes are UTC)")
    return filename


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) != 3:
        sys.exit("Usage: python3 all_minute_extract_data.py <SYMBOL> <YYYYMMDDHHmm>")

    symbol   = sys.argv[1].upper()
    start_dt = parse_start(sys.argv[2])
    now_utc  = datetime.now(timezone.utc)

    if start_dt >= now_utc:
        sys.exit("ERROR: start datetime must be in the past.")

    print(f"\nExtracting 1-min bars for {symbol}")
    print(f"  From : {start_dt.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  To   : {now_utc.strftime('%Y-%m-%d %H:%M UTC')} (latest)\n")

    contract = make_contract(symbol)

    app = IBMinuteExtractor()
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)

    # Run the EClient message loop in a background thread
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    time.sleep(2)   # give the connection a moment to establish

    if not app.isConnected():
        sys.exit("ERROR: Could not connect to IB TWS/Gateway. "
                 "Make sure TWS or IB Gateway is running and API connections are enabled.")

    all_bars   = []
    seen_dts   = set()

    # Walk backwards from now to start_dt, one day at a time.
    # IB "1 D" requests return the day *ending* at endDateTime.
    chunk_end = now_utc

    while chunk_end > start_dt:
        bars = app.fetch_chunk(contract, chunk_end)

        if app._error_flag:
            print("Fatal IB error – aborting.")
            break

        if bars:
            for b in bars:
                bdt = bar_dt(b)
                if bdt >= start_dt and b["datetime"] not in seen_dts:
                    seen_dts.add(b["datetime"])
                    all_bars.append(b)

            # Move end pointer back by 1 day for next chunk
            chunk_end = chunk_end - timedelta(days=1)
        else:
            # Empty result – might be a weekend/holiday; step back anyway
            chunk_end = chunk_end - timedelta(days=1)

        # Safety: if we've gone past start, we're done
        if chunk_end <= start_dt:
            break

    app.disconnect()

    if not all_bars:
        print("\nNo bars retrieved. Check symbol, connectivity, and market data subscriptions.")
        return

    # Sort chronologically
    all_bars.sort(key=lambda b: b["datetime"])

    save_csv(symbol, all_bars, start_dt)


if __name__ == "__main__":
    main()