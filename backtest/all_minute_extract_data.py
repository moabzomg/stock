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

# IB's step-size table only guarantees a full, correctly-bounded response
# for 1-min bars at "1 D". Larger durations ("10 D", "50 D", etc.) are
# accepted without error but get silently capped/shifted by IB — you get
# back a fixed bar count from an unpredictable window instead of the full
# calendar span you asked for. Keep this at 1 D / 1 day unless you've
# re-verified against IB that a larger step returns the full window.
CHUNK_DURATION         = "1 D"      # 1 day per request (IB's documented max for 1-min bars)
CHUNK_STEP_DAYS        = 1          # must match the number in CHUNK_DURATION above
BAR_SIZE              = "1 min"
WHAT_TO_SHOW          = "TRADES"
USE_RTH               = 0           # 0 = include extended hours
KEEP_UP_TO_DATE       = 0

# Expected bar counts, used only for the sanity-check warning (not enforced)
EXPECTED_BARS_RTH       = 390       # ~6.5h regular trading hours
EXPECTED_BARS_EXTENDED  = 960       # ~16h with useRTH=0 (pre/post market included)

# ── IB connection defaults ────────────────────────────────────────────────────
IB_HOST = "127.0.0.1"
IB_PORT = 7496          # TWS paper: 7497 | TWS live: 7496 | Gateway paper: 4002
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
        self._last_request_params = None # (endDateTime, durationStr) sent for current reqId, for debug logging
        self._reported_start = None      # IB's own "start" from historicalDataEnd callback
        self._reported_end   = None      # IB's own "end" from historicalDataEnd callback
        self._last_chunk_window = None   # (first_bar_dt, last_bar_dt) of the previous fetch_chunk call

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
        self._reported_start = start
        self._reported_end   = end
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
        elif errorCode in (2104, 2106, 2107, 2108, 2119, 2150, 2158):
            # purely informational (e.g. "market data farm connected")
            pass
        else:
            # Any other code (e.g. 10314 invalid date/time format) is
            # unexpected — print it and unblock the wait instead of hanging
            # silently for the full 120s timeout.
            print(f"  [ERROR] reqId={reqId} code={errorCode}: {errorString}")
            self._error_flag = True
            self._done_event.set()

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
        # IB unambiguous UTC end-datetime format: "YYYYMMDD-HH:MM:SS"
        # (dash between date/time = UTC notation, per IB error 10314's own
        # format spec). No trailing " UTC" suffix — that combination is
        # rejected by IB with error 10314 ("invalid date/time/timezone").
        # This avoids IB silently falling back to TWS's local timezone
        # setting, which is what caused inconsistent/truncated chunks
        # previously.
        end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")

        self._bars = []
        self._done_event.clear()
        self._error_flag = False
        self._reported_start = None
        self._reported_end   = None

        self._pace()

        this_req_id = self._req_id
        self._last_request_params = {
            "reqId": this_req_id,
            "endDateTime": end_str,
            "durationStr": CHUNK_DURATION,
            "barSizeSetting": BAR_SIZE,
            "useRTH": USE_RTH,
        }
        print(f"  → [reqId={this_req_id}] requesting endDateTime={end_str} "
              f"duration={CHUNK_DURATION} bar={BAR_SIZE} useRTH={USE_RTH} …", end="", flush=True)

        self.reqHistoricalData(
            reqId          = this_req_id,
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
            self.cancelHistoricalData(this_req_id)
            return []

        # IB's own reported window for THIS reqId, straight from the
        # historicalDataEnd callback — ground truth independent of what we
        # infer from the bars themselves. If this doesn't match the
        # requested endDateTime, IB itself is telling us it served a
        # different window than asked.
        print(f"\n      [ib-reported] reqId={this_req_id} start={self._reported_start!r} "
              f"end={self._reported_end!r}")

        bars = list(self._bars)

        # ── Sanity check ────────────────────────────────────────────────────
        # Flag chunks that look truncated so a bad period is visible
        # immediately instead of being silently merged into the output CSV.
        # Scaled by CHUNK_STEP_DAYS since a chunk can in principle span
        # multiple days; only ~5/7 of those days are weekdays on average,
        # so we scale the per-day expectation down accordingly rather than
        # assuming every day in the chunk is a full trading day.
        #
        # IMPORTANT: this check can only catch a SHORTFALL in bar *count*.
        # It cannot detect IB returning the "right" bar count from the
        # WRONG calendar window (which is what happens if CHUNK_DURATION is
        # increased beyond "1 D" — see module docstring). If you ever
        # change CHUNK_DURATION, also verify (first_dt, last_dt) below
        # actually matches the requested [end_dt - duration, end_dt] window
        # before trusting multi-day chunks.
        if bars:
            first_dt, last_dt = bars[0]["datetime"], bars[-1]["datetime"]
            per_day_expected = EXPECTED_BARS_RTH if USE_RTH else EXPECTED_BARS_EXTENDED
            expected_weekdays = CHUNK_STEP_DAYS * 5 / 7
            expected = per_day_expected * expected_weekdays
            note = ""
            if len(bars) < expected * 0.5:
                note = "  [!] looks truncated vs expected for this period"

            # Cross-request duplicate check: if this chunk's bar window is
            # IDENTICAL to the previous chunk's, despite the two requests
            # using different endDateTime values, IB served stale/repeated
            # data instead of the newly requested window. This is the exact
            # failure mode to watch for — flag it loudly rather than
            # silently merging it in (the dedup-by-datetime-string logic
            # downstream will no-op on it, but it's worth knowing WHY no
            # new bars showed up).
            this_window = (first_dt, last_dt)
            if self._last_chunk_window is not None and this_window == self._last_chunk_window:
                note += (f"  [!!] DUPLICATE WINDOW vs previous request "
                         f"(prev endDateTime≠this endDateTime but same bars returned — "
                         f"IB likely didn't have data for the newly requested window "
                         f"and silently repeated the last available session)")
            self._last_chunk_window = this_window
            print(f" {len(bars)} bars  ({first_dt} → {last_dt}){note}")
        else:
            self._last_chunk_window = None
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


def load_existing_csv(symbol: str) -> tuple[list, set]:
    """
    Load any previously saved bars for this symbol, if the CSV exists.
    Returns (list_of_bar_dicts, set_of_datetime_strings_already_present).
    """
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
    print(f"  [resume] found existing {filename} with {len(bars)} bars "
          f"({min(seen)} → {max(seen)})" if bars else f"  [resume] found empty {filename}")
    return bars, seen


def covered_days(seen_dts: set, return_diagnostics: bool = False):
    """
    Given a set of 'YYYYMMDDHHmm' strings already on disk, return the set of
    'YYYYMMDD' calendar days that are *fully* covered.

    "Fully covered" is determined relative to the data actually on disk,
    not a hardcoded 00:00–23:59 assumption — that assumption only holds for
    24-hour FX symbols. A stock with useRTH=0 might run e.g. 08:00–23:59
    UTC and never have a 00:00 bar at all, which would make a fixed-00:00
    check permanently reject every day as "incomplete" even when it's
    genuinely complete (this was a real bug: see conversation history).

    Approach: look at the earliest and latest time-of-day seen across ALL
    days in the dataset (a reasonable proxy for "this symbol's normal
    session start/end"), then a day is considered fully covered if it has
    a bar at/near that session's start and end. This self-calibrates per
    symbol instead of assuming any fixed clock time.

    If return_diagnostics is True, also returns a dict mapping every
    INCOMPLETE day -> {"bar_count", "first_time", "last_time"} so callers
    can print a human-readable report of exactly which days are partial
    and by how much, instead of just a bare count.
    """
    by_day = {}
    all_times = []
    for dt_str in seen_dts:
        day, hhmm = dt_str[:8], dt_str[8:]
        by_day.setdefault(day, set()).add(hhmm)
        all_times.append(hhmm)

    if not all_times:
        return (set(), {}) if return_diagnostics else set()

    # Use the 5th/95th percentile of observed start/end times per day as the
    # "normal session" reference, rather than the single global min/max
    # (which could be skewed by one unusually long or short day).
    daily_starts = sorted(min(times) for times in by_day.values())
    daily_ends   = sorted(max(times) for times in by_day.values())

    def percentile(sorted_list, pct):
        if not sorted_list:
            return None
        idx = min(len(sorted_list) - 1, int(len(sorted_list) * pct))
        return sorted_list[idx]

    session_start_ref = percentile(daily_starts, 0.10)   # typical earliest start
    session_end_ref   = percentile(daily_ends, 0.90)     # typical latest end

    # Tolerance window: a day's first/last bar should be within ~5 minutes
    # of the typical session start/end to count as "fully covered".
    def hhmm_to_minutes(hhmm):
        return int(hhmm[:2]) * 60 + int(hhmm[2:])

    start_ref_min = hhmm_to_minutes(session_start_ref)
    end_ref_min   = hhmm_to_minutes(session_end_ref)
    tolerance_min = 5

    full = set()
    incomplete = {}
    for day, times in by_day.items():
        day_start_min = hhmm_to_minutes(min(times))
        day_end_min   = hhmm_to_minutes(max(times))
        has_start = day_start_min <= start_ref_min + tolerance_min
        has_end   = day_end_min   >= end_ref_min - tolerance_min
        if has_start and has_end:
            full.add(day)
        elif return_diagnostics:
            incomplete[day] = {
                "bar_count": len(times),
                "first_time": min(times),
                "last_time": max(times),
            }

    if return_diagnostics:
        return full, incomplete
    return full


def save_csv(symbol: str, all_bars: list, start_dt: datetime):
    os.makedirs("data", exist_ok=True)
    filename = os.path.join("data", f"{symbol}.csv")
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

    all_bars, seen_dts = load_existing_csv(symbol)
    starting_count     = len(all_bars)
    already_full_days, incomplete_days = covered_days(seen_dts, return_diagnostics=True)
    if already_full_days:
        print(f"  [resume] {len(already_full_days)} day(s) already fully covered – will skip re-requesting them")
    if incomplete_days:
        print(f"  [resume] {len(incomplete_days)} day(s) have SOME data but are incomplete "
              f"– will re-fetch these:")
        for day in sorted(incomplete_days):
            diag = incomplete_days[day]
            print(f"      {day}: {diag['bar_count']} bars, "
                  f"{diag['first_time']}–{diag['last_time']}")
    print()

    # Walk backwards from the next UTC midnight boundary down to start_dt,
    # CHUNK_STEP_DAYS at a time (matching CHUNK_DURATION). Anchoring to
    # midnight (rather than the live "now" timestamp) means each chunk's
    # boundary aligns to a full UTC day, instead of being truncated to
    # whatever time-of-day the script happened to be run at.
    # IB requests return the period *ending* at endDateTime, so we ask
    # for "tomorrow 00:00 UTC" to get all of today's data.
    next_midnight_utc = (now_utc + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    chunk_end = next_midnight_utc

    while chunk_end > start_dt:
        chunk_start_dt = chunk_end - timedelta(days=CHUNK_STEP_DAYS)
        days_in_chunk = [
            (chunk_start_dt + timedelta(days=i)).strftime("%Y%m%d")
            for i in range(CHUNK_STEP_DAYS)
        ]

        # Resume support: if every day in this chunk is already fully
        # covered on disk, skip the request entirely — no point asking IB
        # for data we already have.
        if all(d in already_full_days for d in days_in_chunk):
            print(f"  [skip] {days_in_chunk[0]}–{days_in_chunk[-1]} already fully in {symbol}.csv – skipping")
            chunk_end = chunk_end - timedelta(days=CHUNK_STEP_DAYS)
            continue

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

        # Move end pointer back for next chunk regardless of whether this
        # one returned data (weekend-only chunks will legitimately return
        # fewer/no bars, which is expected, not an error).
        chunk_end = chunk_end - timedelta(days=CHUNK_STEP_DAYS)

        # Safety: if we've gone past start, we're done
        if chunk_end <= start_dt:
            break

    app.disconnect()

    if not all_bars:
        print("\nNo bars retrieved. Check symbol, connectivity, and market data subscriptions.")
        return

    new_count = len(all_bars) - starting_count
    print(f"\n  [summary] {new_count} new bar(s) fetched this run, {len(all_bars)} total in file")

    # Sort chronologically
    all_bars.sort(key=lambda b: b["datetime"])

    save_csv(symbol, all_bars, start_dt)


if __name__ == "__main__":
    main()