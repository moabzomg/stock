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
  - Each chunk uses "1 D" duration / "1 min" bar size. This is IB's
    documented step-size limit for 1-min bars (see
    https://interactivebrokers.github.io/tws-api/historical_limitations.html
    step-size table: "1 D -> 1 min - 1 day" is the largest duration
    officially paired with 1-min granularity). Although IB's API will
    *accept* larger durations like "10 D" without an error, it silently
    caps the response at an internal bar-count limit and shifts/clips the
    returned window instead of honoring the full requested calendar span
    — i.e. you get fewer bars than requested AND they don't start where
    you expect, with no warning. "1 D" is the only size IB guarantees
    will return the full, correctly-bounded window.
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

CHUNK_DURATION         = "1 D"
CHUNK_STEP_DAYS        = 1
BAR_SIZE              = "1 min"
WHAT_TO_SHOW          = "TRADES"
USE_RTH               = 0
KEEP_UP_TO_DATE       = 0

EXPECTED_BARS_RTH       = 390
EXPECTED_BARS_EXTENDED  = 960

# ── IB connection defaults ────────────────────────────────────────────────────
IB_HOST   = "127.0.0.1"
IB_PORT   = 7496
CLIENT_ID = 10          # used only when running single-symbol directly


class IBMinuteExtractor(EWrapper, EClient):
    """Single-threaded IB app that pulls 1-min bars chunk by chunk."""

    def __init__(self, client_id: int = CLIENT_ID):
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self._client_id   = client_id
        self._req_id      = 1
        self._bars        = []
        self._done_event  = threading.Event()
        self._error_flag  = False
        self._request_ts  = deque()
        self._reported_start     = None
        self._reported_end       = None
        self._last_chunk_window  = None

    # ── EWrapper callbacks ────────────────────────────────────────────────────

    def historicalData(self, reqId: int, bar: BarData):
        raw = bar.date.strip()
        try:
            dt = datetime.fromtimestamp(int(raw), tz=timezone.utc).replace(tzinfo=None)
        except ValueError:
            try:
                dt = datetime.strptime(raw, "%Y%m%d  %H:%M:%S")
            except ValueError:
                dt = datetime.strptime(raw, "%Y%m%d %H:%M:%S")
        self._bars.append({
            "datetime": dt.strftime("%Y%m%d%H%M"),
            "open":     bar.open,
            "close":    bar.close,
            "high":     bar.high,
            "low":      bar.low,
            "volume":   bar.volume,
        })

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        self._reported_start = start
        self._reported_end   = end
        self._done_event.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in (162, 321):
            print(f"  [warn] reqId={reqId} code={errorCode}: {errorString}")
            self._done_event.set()
        elif errorCode in (1100, 1101, 1102, 2110):
            print(f"  [warn] connectivity code={errorCode}: {errorString}")
        elif errorCode < 1000:
            print(f"  [ERROR] reqId={reqId} code={errorCode}: {errorString}")
            self._error_flag = True
            self._done_event.set()
        elif errorCode in (2104, 2106, 2107, 2108, 2119, 2150, 2158):
            pass
        else:
            print(f"  [ERROR] reqId={reqId} code={errorCode}: {errorString}")
            self._error_flag = True
            self._done_event.set()

    def connectAck(self):
        print(f"  [ib] connected (client_id={self._client_id}) to {IB_HOST}:{IB_PORT}")

    # ── Pacing helper ─────────────────────────────────────────────────────────

    def _pace(self):
        now = time.time()
        while self._request_ts and now - self._request_ts[0] > REQUEST_WINDOW_SECS:
            self._request_ts.popleft()
        if len(self._request_ts) >= MAX_REQUESTS_PER_10M:
            wait = REQUEST_WINDOW_SECS - (now - self._request_ts[0]) + 1
            print(f"  [pace] rolling window full – sleeping {wait:.1f}s …")
            time.sleep(wait)
            now = time.time()
            while self._request_ts and now - self._request_ts[0] > REQUEST_WINDOW_SECS:
                self._request_ts.popleft()
        if self._request_ts:
            elapsed = time.time() - self._request_ts[-1]
            if elapsed < INTER_REQUEST_SLEEP:
                time.sleep(INTER_REQUEST_SLEEP - elapsed)
        self._request_ts.append(time.time())

    # ── Core fetch ────────────────────────────────────────────────────────────

    def fetch_chunk(self, contract: Contract, end_dt: datetime) -> list:
        end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")

        self._bars = []
        self._done_event.clear()
        self._error_flag    = False
        self._reported_start = None
        self._reported_end   = None

        self._pace()

        this_req_id = self._req_id
        print(f"  → [reqId={this_req_id}] end={end_str} dur={CHUNK_DURATION} …", end="", flush=True)

        self.reqHistoricalData(
            reqId          = this_req_id,
            contract       = contract,
            endDateTime    = end_str,
            durationStr    = CHUNK_DURATION,
            barSizeSetting = BAR_SIZE,
            whatToShow     = WHAT_TO_SHOW,
            useRTH         = USE_RTH,
            formatDate     = 2,
            keepUpToDate   = KEEP_UP_TO_DATE,
            chartOptions   = [],
        )
        self._req_id += 1

        finished = self._done_event.wait(timeout=120)
        if not finished:
            print(" TIMEOUT")
            self.cancelHistoricalData(this_req_id)
            return []

        bars = list(self._bars)

        if bars:
            first_dt, last_dt = bars[0]["datetime"], bars[-1]["datetime"]
            per_day_expected  = EXPECTED_BARS_RTH if USE_RTH else EXPECTED_BARS_EXTENDED
            expected_weekdays = CHUNK_STEP_DAYS * 5 / 7
            expected          = per_day_expected * expected_weekdays
            note = ""
            if len(bars) < expected * 0.5:
                note = "  [!] looks truncated"
            this_window = (first_dt, last_dt)
            if self._last_chunk_window is not None and this_window == self._last_chunk_window:
                note += "  [!!] DUPLICATE WINDOW"
            self._last_chunk_window = this_window
            print(f" {len(bars)} bars  ({first_dt} → {last_dt}){note}")
        else:
            self._last_chunk_window = None
            print(" 0 bars")

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
    return dt.replace(tzinfo=timezone.utc)


def bar_dt(bar: dict) -> datetime:
    return datetime.strptime(bar["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def load_existing_csv(symbol: str) -> tuple[list, set]:
    filename = os.path.join("data", f"{symbol}.csv")
    if not os.path.exists(filename):
        return [], set()
    bars = []
    with open(filename, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bars.append({
                "datetime": row["datetime"],
                "open":     float(row["open"]),
                "close":    float(row["close"]),
                "high":     float(row["high"]),
                "low":      float(row["low"]),
                "volume":   float(row["volume"]),
            })
    seen = {b["datetime"] for b in bars}
    if bars:
        print(f"  [resume] {filename}: {len(bars)} existing bars ({min(seen)} → {max(seen)})")
    else:
        print(f"  [resume] found empty {filename}")
    return bars, seen


def covered_days(seen_dts: set, return_diagnostics: bool = False):
    by_day: dict[str, set] = {}
    for dt_str in seen_dts:
        by_day.setdefault(dt_str[:8], set()).add(dt_str[8:])

    if not by_day:
        return (set(), {}) if return_diagnostics else set()

    per_day_expected = EXPECTED_BARS_EXTENDED if USE_RTH == 0 else EXPECTED_BARS_RTH
    min_bars = int(per_day_expected * 0.80)

    full, incomplete = set(), {}
    for day, times in by_day.items():
        if len(times) >= min_bars:
            full.add(day)
        elif return_diagnostics:
            incomplete[day] = {
                "bar_count":  len(times),
                "first_time": min(times),
                "last_time":  max(times),
            }

    return (full, incomplete) if return_diagnostics else full


def save_csv(symbol: str, all_bars: list):
    os.makedirs("data", exist_ok=True)
    filename = os.path.join("data", f"{symbol}.csv")
    fieldnames = ["datetime", "open", "close", "high", "low", "volume"]
    with open(filename, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_bars)
    print(f"  ✓ saved {len(all_bars)} bars → {filename}")
    return filename


# ── Per-symbol extraction (reusable by batch runner) ─────────────────────────

def extract_symbol(symbol: str, start_dt: datetime, client_id: int = CLIENT_ID) -> int:
    """
    Connect, fetch all missing 1-min bars for *symbol* from *start_dt* to now,
    and save. Returns the number of NEW bars added this run (0 on failure).

    client_id should be unique per concurrent call to avoid IB rejecting
    duplicate connections.
    """
    now_utc = datetime.now(timezone.utc)
    print(f"\n[{symbol}] extracting from {start_dt:%Y-%m-%d %H:%M UTC} → {now_utc:%Y-%m-%d %H:%M UTC}")

    contract = make_contract(symbol)

    app = IBMinuteExtractor(client_id=client_id)
    app.connect(IB_HOST, IB_PORT, client_id)
    api_thread = threading.Thread(target=app.run, daemon=True)
    api_thread.start()
    time.sleep(2)

    if not app.isConnected():
        print(f"  [ERROR] could not connect to IB for {symbol} (client_id={client_id})")
        return 0

    all_bars, seen_dts     = load_existing_csv(symbol)
    starting_count         = len(all_bars)
    already_full, incomplete = covered_days(seen_dts, return_diagnostics=True)

    if already_full:
        print(f"  [resume] {len(already_full)} day(s) already fully covered — will skip")
    if incomplete:
        print(f"  [resume] {len(incomplete)} incomplete day(s) — will re-fetch:")
        for day in sorted(incomplete):
            d = incomplete[day]
            print(f"    {day}: {d['bar_count']} bars  {d['first_time']}–{d['last_time']}")

    # Anchor to next midnight so each chunk covers a clean UTC day
    next_midnight_utc = (now_utc + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    chunk_end = next_midnight_utc

    while chunk_end > start_dt:
        chunk_start_dt = chunk_end - timedelta(days=CHUNK_STEP_DAYS)

        # Skip pure-weekend chunks with no possible trading data
        if all(
            (chunk_start_dt + timedelta(days=i)).weekday() >= 5
            for i in range(CHUNK_STEP_DAYS)
        ):
            chunk_end -= timedelta(days=CHUNK_STEP_DAYS)
            continue

        # Build the list of calendar days this chunk spans
        days_in_chunk = [
            (chunk_start_dt + timedelta(days=i)).strftime("%Y%m%d")
            for i in range(CHUNK_STEP_DAYS)
        ]

        if all(d in already_full for d in days_in_chunk):
            chunk_end -= timedelta(days=CHUNK_STEP_DAYS)
            continue

        bars = app.fetch_chunk(contract, chunk_end)

        if app._error_flag:
            print(f"  [ERROR] fatal IB error for {symbol} — stopping")
            break

        for b in bars:
            if bar_dt(b) >= start_dt and b["datetime"] not in seen_dts:
                seen_dts.add(b["datetime"])
                all_bars.append(b)

        chunk_end -= timedelta(days=CHUNK_STEP_DAYS)

    app.disconnect()

    if not all_bars:
        print(f"  [warn] no bars for {symbol}")
        return 0

    new_count = len(all_bars) - starting_count
    all_bars.sort(key=lambda b: b["datetime"])
    save_csv(symbol, all_bars)
    print(f"  [summary] {symbol}: +{new_count} new bars this run, {len(all_bars)} total")
    return new_count


# ── Entry point (single-symbol) ───────────────────────────────────────────────

def main():
    if len(sys.argv) != 3:
        sys.exit("Usage: python3 all_minute_extract_data.py <SYMBOL> <YYYYMMDDHHmm>")

    symbol   = sys.argv[1].upper()
    start_dt = parse_start(sys.argv[2])

    if start_dt >= datetime.now(timezone.utc):
        sys.exit("ERROR: start datetime must be in the past.")

    extract_symbol(symbol, start_dt, client_id=CLIENT_ID)


if __name__ == "__main__":
    main()