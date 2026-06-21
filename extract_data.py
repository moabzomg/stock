#!/usr/bin/env python3
"""
extract_data.py — Combined historical (Interactive Brokers) + live (yfinance)
1-minute / daily bar extractor with a rolling minute-resolution window.

═══════════════════════════════════════════════════════════════════════════
USAGE
═══════════════════════════════════════════════════════════════════════════
    python3 extract_data.py <SYMBOL> <YYYYMMDD>   # historical mode (one-shot)
    python3 extract_data.py <SYMBOL>               # live mode (runs forever)

═══════════════════════════════════════════════════════════════════════════
DATA MODEL  —  data/<SYMBOL>.csv
═══════════════════════════════════════════════════════════════════════════
Single CSV per symbol, columns: datetime,open,close,high,low,volume

The `datetime` field's LENGTH determines the row's resolution:
    len == 8   ("YYYYMMDD")        -> a DAILY bar
    len == 12  ("YYYYMMDDHHMM")    -> a MINUTE bar (always UTC)

Only the most recent MINUTE_DAYS *trading* days are kept at 1-minute
resolution. Everything older is collapsed into a single daily bar
(open = first minute's open, close = last minute's close, high = max high,
low = min low, volume = sum of volume).

═══════════════════════════════════════════════════════════════════════════
HISTORICAL MODE  (Interactive Brokers)
═══════════════════════════════════════════════════════════════════════════
Invoked either:
  (a) directly from the CLI:  `extract_data.py SYMBOL YYYYMMDD`, or
  (b) internally by live mode, HISTORICAL_DELAY_HOURS after the market
      closes, with no CLI date (it reconciles using whatever start date
      makes sense from the existing CSV / BOOTSTRAP_DAYS).

Behavior:
  1. If data/<SYMBOL>.csv does NOT exist yet:
       -> fetch MINUTE bars from the given date through the latest
          completed trading day via IB (no day-data shortcut, since
          there's nothing to seed a daily collapse from).
  2. If it DOES exist:
       a. Reconciliation pass (CSV-only, no fetching): for every daily
          bar already on disk, and for every trading day that is fully
          covered by minute bars, recompute OHLCV from the underlying
          minute rows (open = first minute, close = last minute,
          high = max, low = min, volume = sum) and compare against the
          stored daily bar (or against itself, as a self-consistency
          check). Mismatches are logged as warnings; nothing is
          auto-corrected (best-effort, per spec).
       b. Gap-fill pass (uses IB): any trading day between the effective
          start date and "today" that is completely absent gets fetched.
          The effective start date is whichever is EARLIER of: the CSV's
          earliest existing data, or MIN_RECONCILE_DAYS trading days back
          from today — so reconciliation always maintains at least
          MIN_RECONCILE_DAYS trading days of history, even for a symbol
          whose CSV so far only has a handful of days of live data. Days
          older than MINUTE_DAYS trading days are fetched/stored as a
          single DAILY bar; the most recent MINUTE_DAYS trading days are
          fetched/stored as MINUTE bars.
       c. Collapse pass: any day that currently has minute-resolution
          data but has fallen outside the MINUTE_DAYS window gets
          collapsed down to a single daily bar (computed from its own
          minute rows — no IB call needed for this part).

═══════════════════════════════════════════════════════════════════════════
LIVE MODE  (yfinance)
═══════════════════════════════════════════════════════════════════════════
Runs forever, cycling between two states:

  OPEN  — the symbol's session is currently active (per the exchange
          calendar). This now includes the PRE-MARKET session, not just
          regular trading hours (RTH) — see INCLUDE_PREMARKET /
          PREMARKET_OFFSET below. Post-market is intentionally NOT
          included yet:
          poll yfinance every POLL_SECONDS, upsert 1-min bars for *today*
          into the CSV (minute resolution). yfinance is called with
          prepost=True, so pre-market bars are tagged and stored exactly
          like RTH bars, just with earlier UTC timestamps.

  CLOSED — the session is currently inactive (after RTH close, before the
          next day's pre-market open):
          a. Once, after the market closes, wait until
             HISTORICAL_DELAY_HOURS after the close, then:
               1. yfinance reconciliation: fetch the last MINUTE_DAYS
                  trading days of 1-min bars from yfinance. If the CSV's
                  existing minute data for those exact MINUTE_DAYS does
                  NOT match (missing days, wrong count, etc.), purge ALL
                  minute rows for days outside that fresh MINUTE_DAYS set
                  and re-pull them from yfinance.
               2. Run historical-mode reconciliation (IB) as described
                  above, to backfill/collapse daily bars further back.
          b. Idle-poll every IDLE_POLL_SECONDS, waiting for the next
             session open (pre-market open), then switch back to OPEN.

═══════════════════════════════════════════════════════════════════════════
DEPENDENCIES
═══════════════════════════════════════════════════════════════════════════
    pip install yfinance pandas_market_calendars --break-system-packages
    pip install ibapi --break-system-packages   # (Interactive Brokers API)

pandas_market_calendars is used for accurate exchange trading-hours and
holiday/half-day awareness (NYSE by default — see EXCHANGE_CALENDAR below).
"""

import sys
import os
import csv
import time
import threading
from datetime import datetime, timedelta, timezone
from collections import deque

# ─────────────────────────────────────────────────────────────────────────
# Configuration constants
# ─────────────────────────────────────────────────────────────────────────
DATA_DIR                = 'data'
POLL_SECONDS             = 30         # live-mode polling interval while market is open
MINUTE_DAYS              = 3          # how many of the most-recent trading days are kept
                                       # at 1-minute resolution; older days get collapsed
                                       # into a single daily bar by historical mode
BOOTSTRAP_DAYS           = 730        # how far back to go on first run (CLI-less mode)
MIN_RECONCILE_DAYS       = 200        # the historical reconciliation/gap-fill pass always
                                       # looks back at least this many *trading* days from
                                       # today, even if the existing CSV's earliest row is
                                       # more recent than that (e.g. a symbol only just
                                       # started in live mode shouldn't end up with just a
                                       # few days of backfilled history).
HISTORICAL_DELAY_HOURS   = 1          # run historical-mode reconciliation this many
                                       # hours after the market closes (tweak freely)
IDLE_POLL_SECONDS        = 60         # how often the idle loop wakes to re-check the
                                       # clock while waiting for the next open/trigger

EXCHANGE_CALENDAR        = 'NYSE'     # pandas_market_calendars exchange code for the
                                       # symbol being tracked; change per-symbol if needed

INCLUDE_PREMARKET        = True       # if True, the live "OPEN" state begins at the
                                       # pre-market session open instead of the RTH open,
                                       # so pre-market minute bars get polled live instead
                                       # of only showing up later via yfinance_reconcile.
PREMARKET_OFFSET         = timedelta(hours=5, minutes=30)
                                       # US pre-market conventionally starts at 4:00 ET;
                                       # NYSE RTH opens at 9:30 ET, so pre-market open is
                                       # always 5.5h before the RTH open on any given
                                       # trading day (DST-safe, since both times sit in
                                       # the same exchange-local day/offset).
                                       # NOTE: post-market (16:00-20:00 ET) is NOT yet
                                       # included — the live "OPEN" state still ends at
                                       # the RTH close. Add a POSTMARKET_OFFSET the same
                                       # way if/when that's wanted too.

# ── IB connection defaults (historical mode) ────────────────────────────────
IB_HOST   = "127.0.0.1"
IB_PORT   = 7496           # TWS paper: 7497 | TWS live: 7496 | Gateway paper: 4002
CLIENT_ID = 11

# ── IB pacing constants (reused from all_minute_extract_data.py) ───────────
INTER_REQUEST_SLEEP   = 10
MAX_REQUESTS_PER_10M  = 55
REQUEST_WINDOW_SECS   = 600
MINUTE_CHUNK_DURATION = "10 D"        # 1-min bar chunk size per IB request
MINUTE_CHUNK_STEP_DAYS = 10
DAILY_CHUNK_DURATION  = "1 Y"         # 1-day bar chunk size per IB request
WHAT_TO_SHOW          = "TRADES"
USE_RTH               = 0             # 0 = include extended hours

FIELDNAMES = ["datetime", "open", "close", "high", "low", "volume"]


# ═══════════════════════════════════════════════════════════════════════════
# CSV helpers
# ═══════════════════════════════════════════════════════════════════════════

def csv_path(symbol: str) -> str:
    return os.path.join(DATA_DIR, f"{symbol}.csv")


def is_daily_row(dt_str: str) -> bool:
    return len(dt_str) == 8


def is_minute_row(dt_str: str) -> bool:
    return len(dt_str) == 12


def row_day(dt_str: str) -> str:
    """Return the YYYYMMDD day component regardless of row resolution."""
    return dt_str[:8]


def load_csv(symbol: str) -> dict:
    """
    Load data/<SYMBOL>.csv into {datetime_str: row_dict}.
    Returns an empty dict if the file doesn't exist.
    """
    path = csv_path(symbol)
    rows = {}
    if not os.path.exists(path):
        return rows
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows[r["datetime"]] = {
                "datetime": r["datetime"],
                "open":     float(r["open"]),
                "close":    float(r["close"]),
                "high":     float(r["high"]),
                "low":      float(r["low"]),
                "volume":   float(r["volume"]),
            }
    return rows


def save_csv(symbol: str, rows: dict):
    """
    Write {datetime_str: row_dict} back out, sorted chronologically.
    Daily bars (YYYYMMDD) and minute bars (YYYYMMDDHHMM) sort correctly
    together as plain strings since daily bars' 8-char keys are always
    "before" any minute bar sharing the same day prefix... EXCEPT plain
    lexicographic sort would interleave them oddly (8-char string sorts
    before its own 12-char extensions, which is fine, but a daily bar for
    day N would sort before ALL minute bars of day N, which is the
    desired behavior since a daily bar represents the whole day "as of
    its close"). We sort using an explicit key for safety/clarity.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    path = csv_path(symbol)

    def sort_key(dt_str):
        # (day_as_int, 0 if daily-bar else 1, minute_part_as_int)
        day = int(row_day(dt_str))
        if is_daily_row(dt_str):
            return (day, 0, 0)
        return (day, 1, int(dt_str[8:]))

    ordered = sorted(rows.values(), key=lambda r: sort_key(r["datetime"]))
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ordered)


def minute_rows_for_day(rows: dict, day: str) -> list:
    """All minute-resolution rows for a given YYYYMMDD day, sorted by time."""
    matches = [r for k, r in rows.items() if is_minute_row(k) and row_day(k) == day]
    matches.sort(key=lambda r: r["datetime"])
    return matches


def collapse_minutes_to_daily(minute_bars: list) -> dict:
    """
    Given a chronologically-sorted list of minute-bar dicts for one day,
    compute the equivalent single daily bar:
        open   = first minute's open
        close  = last minute's close
        high   = max of all highs
        low    = min of all lows
        volume = sum of all volumes
    """
    day = row_day(minute_bars[0]["datetime"])
    return {
        "datetime": day,
        "open":     minute_bars[0]["open"],
        "close":    minute_bars[-1]["close"],
        "high":     max(b["high"] for b in minute_bars),
        "low":      min(b["low"] for b in minute_bars),
        "volume":   sum(b["volume"] for b in minute_bars),
    }


# ═══════════════════════════════════════════════════════════════════════════
# Exchange calendar helpers (pandas_market_calendars)
# ═══════════════════════════════════════════════════════════════════════════

def _get_calendar():
    try:
        import pandas_market_calendars as mcal
    except ImportError:
        sys.exit(
            "ERROR: pandas_market_calendars is required.\n"
            "Install with: pip install pandas_market_calendars --break-system-packages"
        )
    return mcal.get_calendar(EXCHANGE_CALENDAR)


def trading_days_between(start_date, end_date):
    """
    Return a sorted list of 'YYYYMMDD' strings for trading days in
    [start_date, end_date] (inclusive), per the exchange calendar.
    start_date/end_date may be date, datetime, or 'YYYYMMDD' string.
    """
    cal = _get_calendar()
    schedule = cal.schedule(start_date=start_date, end_date=end_date)
    return [d.strftime("%Y%m%d") for d in schedule.index]


def last_n_trading_days(n: int, as_of=None) -> list:
    """Return the last n trading days (YYYYMMDD, ascending) up to/including
    `as_of` (defaults to today, UTC date)."""
    if as_of is None:
        as_of = datetime.now(timezone.utc).date()
    cal = _get_calendar()
    # Look back generously (n trading days is at most ~1.5x n calendar days,
    # but holidays can stretch this further over long weekends — pad safely)
    lookback_start = as_of - timedelta(days=int(n * 2.5) + 10)
    schedule = cal.schedule(start_date=lookback_start, end_date=as_of)
    days = [d.strftime("%Y%m%d") for d in schedule.index]
    return days[-n:]


def market_session_today(as_of_utc=None):
    """
    Return (is_trading_day, open_utc, close_utc) for "today" (in exchange
    local terms) — open_utc/close_utc are None if today is not a trading day.
    These are REGULAR TRADING HOURS (RTH) only — see session_open_close_today()
    for the pre-market-aware version used by the live state machine.
    """
    if as_of_utc is None:
        as_of_utc = datetime.now(timezone.utc)
    cal = _get_calendar()
    today_date = as_of_utc.date()
    schedule = cal.schedule(start_date=today_date, end_date=today_date)
    if schedule.empty:
        return False, None, None
    open_utc  = schedule.iloc[0]["market_open"].tz_convert("UTC").to_pydatetime()
    close_utc = schedule.iloc[0]["market_close"].tz_convert("UTC").to_pydatetime()
    return True, open_utc, close_utc


def session_open_close_today(as_of_utc=None):
    """
    Like market_session_today(), but if INCLUDE_PREMARKET is set, the
    returned "open" is the pre-market session open (PREMARKET_OFFSET
    before the RTH open) rather than the RTH open itself. The returned
    "close" is still the RTH close — post-market is intentionally out of
    scope for now (see PREMARKET_OFFSET comment above).

    This is what the live OPEN/CLOSED state machine (is_market_open,
    next_market_open) should use, so that pre-market bars get polled live
    instead of only showing up later via the post-close yfinance
    reconciliation pass.
    """
    is_trading_day, rth_open_utc, rth_close_utc = market_session_today(as_of_utc)
    if not is_trading_day:
        return False, None, None
    session_open_utc = rth_open_utc - PREMARKET_OFFSET if INCLUDE_PREMARKET else rth_open_utc
    return True, session_open_utc, rth_close_utc


def is_market_open(as_of_utc=None) -> bool:
    """
    True whenever the live polling loop should be active. Includes the
    pre-market session (per session_open_close_today), not just RTH.
    """
    if as_of_utc is None:
        as_of_utc = datetime.now(timezone.utc)
    is_trading_day, session_open_utc, session_close_utc = session_open_close_today(as_of_utc)
    if not is_trading_day:
        return False
    return session_open_utc <= as_of_utc < session_close_utc


def next_market_open(as_of_utc=None):
    """Return the UTC datetime of the next session open at/after as_of_utc
    — i.e. the next pre-market open if INCLUDE_PREMARKET, else the next
    RTH open."""
    if as_of_utc is None:
        as_of_utc = datetime.now(timezone.utc)
    cal = _get_calendar()
    horizon = as_of_utc.date() + timedelta(days=14)
    schedule = cal.schedule(start_date=as_of_utc.date(), end_date=horizon)
    for _, row in schedule.iterrows():
        rth_open_utc = row["market_open"].tz_convert("UTC").to_pydatetime()
        session_open_utc = rth_open_utc - PREMARKET_OFFSET if INCLUDE_PREMARKET else rth_open_utc
        if session_open_utc >= as_of_utc:
            return session_open_utc
    sys.exit("ERROR: could not find next market open within 14 days — check exchange calendar.")


def most_recent_close(as_of_utc=None):
    """Return the UTC datetime of the most recently completed market close
    at/before as_of_utc (i.e. the close we should react to). This stays
    pinned to the RTH close (post-market reconciliation is out of scope)."""
    if as_of_utc is None:
        as_of_utc = datetime.now(timezone.utc)
    cal = _get_calendar()
    lookback = as_of_utc.date() - timedelta(days=14)
    schedule = cal.schedule(start_date=lookback, end_date=as_of_utc.date())
    last_close = None
    for _, row in schedule.iterrows():
        close_utc = row["market_close"].tz_convert("UTC").to_pydatetime()
        if close_utc <= as_of_utc:
            last_close = close_utc
    return last_close


# ═══════════════════════════════════════════════════════════════════════════
# Interactive Brokers client (historical mode)
# ═══════════════════════════════════════════════════════════════════════════

def _ib_imports():
    try:
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
        from ibapi.contract import Contract
        from ibapi.common import BarData
        return EClient, EWrapper, Contract, BarData
    except ImportError:
        sys.exit(
            "ERROR: ibapi is required for historical mode.\n"
            "Install with: pip install ibapi --break-system-packages"
        )


def make_ib_app():
    EClient, EWrapper, Contract, BarData = _ib_imports()

    class IBExtractor(EWrapper, EClient):
        def __init__(self):
            EWrapper.__init__(self)
            EClient.__init__(self, wrapper=self)
            self._req_id     = 1
            self._bars       = []
            self._done_event = threading.Event()
            self._error_flag = False
            self._request_ts = deque()

        def historicalData(self, reqId, bar):
            raw = bar.date.strip()
            try:
                dt = datetime.fromtimestamp(int(raw), tz=timezone.utc).replace(tzinfo=None)
                is_daily = False
            except ValueError:
                # Daily bars come back as plain "YYYYMMDD" even with formatDate=2
                try:
                    dt = datetime.strptime(raw, "%Y%m%d")
                    is_daily = True
                except ValueError:
                    dt = datetime.strptime(raw, "%Y%m%d %H:%M:%S")
                    is_daily = False
            fmt_dt = dt.strftime("%Y%m%d") if is_daily else dt.strftime("%Y%m%d%H%M")
            self._bars.append({
                "datetime": fmt_dt,
                "open":     bar.open,
                "close":    bar.close,
                "high":     bar.high,
                "low":      bar.low,
                "volume":   bar.volume,
            })

        def historicalDataEnd(self, reqId, start, end):
            self._done_event.set()

        def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
            if errorCode in (162, 321):
                print(f"  [warn] reqId={reqId} code={errorCode}: {errorString}")
                self._done_event.set()
            elif errorCode in (2104, 2106, 2107, 2108, 2119, 2150, 2158):
                pass  # informational
            elif errorCode in (1100, 1101, 1102, 2110):
                print(f"  [warn] connectivity code={errorCode}: {errorString}")
            else:
                print(f"  [ERROR] reqId={reqId} code={errorCode}: {errorString}")
                self._error_flag = True
                self._done_event.set()

        def connectAck(self):
            print(f"  [ib] connected to {IB_HOST}:{IB_PORT}")

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

        def fetch(self, contract, end_dt, duration, bar_size):
            end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")
            self._bars = []
            self._done_event.clear()
            self._error_flag = False
            self._pace()
            print(f"  [ib] requesting {bar_size} bars, duration={duration}, up to {end_str} …", end="", flush=True)
            self.reqHistoricalData(
                reqId=self._req_id, contract=contract, endDateTime=end_str,
                durationStr=duration, barSizeSetting=bar_size,
                whatToShow=WHAT_TO_SHOW, useRTH=USE_RTH, formatDate=2,
                keepUpToDate=0, chartOptions=[],
            )
            self._req_id += 1
            finished = self._done_event.wait(timeout=180)
            if not finished:
                print(" TIMEOUT")
                self.cancelHistoricalData(self._req_id - 1)
                return []
            print(f" {len(self._bars)} bars")
            return list(self._bars)

    return IBExtractor()


def make_contract(symbol: str):
    _, _, Contract, _ = _ib_imports()
    c = Contract()
    c.symbol   = symbol.upper()
    c.secType  = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


class IBSession:
    """Context manager wrapping connect/disconnect + background message loop."""
    def __init__(self):
        self.app = make_ib_app()

    def __enter__(self):
        self.app.connect(IB_HOST, IB_PORT, CLIENT_ID)
        t = threading.Thread(target=self.app.run, daemon=True)
        t.start()
        time.sleep(2)
        if not self.app.isConnected():
            sys.exit("ERROR: could not connect to IB TWS/Gateway. Is it running with API enabled?")
        return self.app

    def __exit__(self, exc_type, exc, tb):
        self.app.disconnect()


def ib_fetch_minute_range(app, contract, start_dt, end_dt_exclusive):
    """
    Fetch 1-min bars from IB covering [start_dt, end_dt_exclusive), walking
    backward in MINUTE_CHUNK_STEP_DAYS chunks. Returns {datetime_str: row}.
    """
    result = {}
    next_midnight = (end_dt_exclusive + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    if end_dt_exclusive.time() != datetime.min.time():
        # If end_dt_exclusive isn't already a midnight boundary, use it directly
        next_midnight = end_dt_exclusive

    chunk_end = next_midnight
    while chunk_end > start_dt:
        bars = app.fetch(contract, chunk_end, MINUTE_CHUNK_DURATION, "1 min")
        if app._error_flag:
            print("  [ib] fatal error during minute fetch – stopping this fetch.")
            break
        for b in bars:
            bdt = datetime.strptime(b["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            if bdt >= start_dt:
                result[b["datetime"]] = b
        chunk_end = chunk_end - timedelta(days=MINUTE_CHUNK_STEP_DAYS)
    return result


def ib_fetch_daily_range(app, contract, start_dt, end_dt_exclusive):
    """
    Fetch 1-day bars from IB covering [start_dt, end_dt_exclusive). Returns
    {datetime_str(YYYYMMDD): row}.
    """
    result = {}
    end_str_dt = end_dt_exclusive
    bars = app.fetch(contract, end_str_dt, DAILY_CHUNK_DURATION, "1 day")
    if app._error_flag:
        print("  [ib] fatal error during daily fetch.")
        return result
    for b in bars:
        if len(b["datetime"]) == 8:
            day_dt = datetime.strptime(b["datetime"], "%Y%m%d").replace(tzinfo=timezone.utc)
            if day_dt >= start_dt:
                result[b["datetime"]] = b
    return result


# ═══════════════════════════════════════════════════════════════════════════
# yfinance client (live mode)
# ═══════════════════════════════════════════════════════════════════════════

def _yf_import():
    try:
        import yfinance as yf
        return yf
    except ImportError:
        sys.exit(
            "ERROR: yfinance is required for live mode.\n"
            "Install with: pip install yfinance --break-system-packages"
        )


def yf_fetch_minute_bars(symbol: str, period_days: int) -> dict:
    """
    Fetch the last `period_days` of 1-minute bars from yfinance.
    Returns {datetime_str(YYYYMMDDHHMM, UTC): row}. yfinance only supports
    a max ~7-8 day window for 1m bars, so period_days should stay small
    (this is fine — only ever called with MINUTE_DAYS-scale windows here).
    """
    yf = _yf_import()
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period=f"{period_days}d", interval="1m", prepost=True)
    result = {}
    for idx, row in hist.iterrows():
        dt_utc = idx.tz_convert("UTC") if idx.tzinfo else idx.tz_localize("UTC")
        dt_str = dt_utc.strftime("%Y%m%d%H%M")
        result[dt_str] = {
            "datetime": dt_str,
            "open":     float(row["Open"]),
            "close":    float(row["Close"]),
            "high":     float(row["High"]),
            "low":      float(row["Low"]),
            "volume":   float(row["Volume"]),
        }
    return result


def yf_fetch_today_minute_bars(symbol: str) -> dict:
    """Fetch just today's 1-min bars (for live polling)."""
    return yf_fetch_minute_bars(symbol, period_days=1)


# ═══════════════════════════════════════════════════════════════════════════
# Reconciliation logic (CSV-only checks, no fetching)
# ═══════════════════════════════════════════════════════════════════════════

def reconcile_check(symbol: str, rows: dict):
    """
    Best-effort consistency check (per spec): for any day that has BOTH a
    daily bar AND minute bars on disk, or that has a full set of minute
    bars, recompute OHLCV from the minute rows and compare against the
    stored daily bar / against itself. Mismatches are logged only —
    nothing is corrected automatically.
    """
    days_with_minutes = sorted({row_day(k) for k in rows if is_minute_row(k)})
    mismatches = 0
    for day in days_with_minutes:
        minute_bars = minute_rows_for_day(rows, day)
        if not minute_bars:
            continue
        computed = collapse_minutes_to_daily(minute_bars)
        daily_key = day
        if daily_key in rows and is_daily_row(daily_key):
            stored = rows[daily_key]
            issues = []
            if abs(stored["open"] - computed["open"]) > 1e-6:
                issues.append(f"open {stored['open']} vs computed {computed['open']}")
            if abs(stored["close"] - computed["close"]) > 1e-6:
                issues.append(f"close {stored['close']} vs computed {computed['close']}")
            if abs(stored["volume"] - computed["volume"]) > max(1.0, stored["volume"] * 0.01):
                issues.append(f"volume {stored['volume']} vs computed {computed['volume']}")
            if issues:
                mismatches += 1
                print(f"  [reconcile][!] {symbol} {day}: " + "; ".join(issues))
    if mismatches == 0:
        print(f"  [reconcile] {symbol}: no misalignments found in {len(days_with_minutes)} day(s) checked")
    return mismatches


# ═══════════════════════════════════════════════════════════════════════════
# Historical mode
# ═══════════════════════════════════════════════════════════════════════════

def run_historical_mode(symbol: str, cli_start_date: str = None):
    """
    One-shot historical reconciliation/backfill using Interactive Brokers.
    cli_start_date: 'YYYYMMDD' if invoked from the CLI with an explicit
    start date; None if invoked internally by live mode (in which case we
    infer the start from existing CSV data, or BOOTSTRAP_DAYS if no CSV
    exists yet).
    """
    print(f"\n[historical] {symbol} — starting reconciliation")
    rows = load_csv(symbol)
    now_utc = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y%m%d")
    recent_days = set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date()))

    contract = make_contract(symbol)

    # ── Case 1: no CSV at all yet ───────────────────────────────────────
    if not rows:
        start_date = cli_start_date or (now_utc - timedelta(days=BOOTSTRAP_DAYS)).strftime("%Y%m%d")
        start_dt = datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=timezone.utc)
        print(f"  [historical] no existing data — fetching minute bars from {start_date} to latest trading day")
        with IBSession() as app:
            fetched = ib_fetch_minute_range(app, contract, start_dt, now_utc)
        rows.update(fetched)
        save_csv(symbol, rows)
        print(f"  [historical] bootstrap complete: {len(fetched)} minute bars saved")
        return

    # ── Case 2: CSV exists — reconcile, gap-fill, collapse ──────────────
    # 2a. Reconciliation pass (no fetching)
    reconcile_check(symbol, rows)

    # 2b. Determine the effective start date for gap-filling
    existing_days = {row_day(k) for k in rows}
    if cli_start_date:
        start_date = cli_start_date
    else:
        earliest_existing = min(existing_days) if existing_days else None
        # Floor: never reconcile a shallower window than MIN_RECONCILE_DAYS
        # trading days, regardless of how little history happens to be on
        # disk already (e.g. a symbol whose CSV only has a few days of live
        # data so far).
        floor_days = last_n_trading_days(MIN_RECONCILE_DAYS, as_of=now_utc.date())
        floor_start_date = floor_days[0] if floor_days else (
            (now_utc - timedelta(days=BOOTSTRAP_DAYS)).strftime("%Y%m%d")
        )
        start_date = min(earliest_existing, floor_start_date) if earliest_existing else floor_start_date
    start_dt = datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=timezone.utc)

    all_trading_days = trading_days_between(start_dt.date(), now_utc.date())
    missing_days = [d for d in all_trading_days if d not in existing_days]

    if missing_days:
        print(f"  [historical] {len(missing_days)} missing trading day(s) found — backfilling via IB")
        missing_minute_days  = [d for d in missing_days if d in recent_days]
        missing_daily_days   = [d for d in missing_days if d not in recent_days]

        with IBSession() as app:
            if missing_minute_days:
                lo = min(missing_minute_days)
                lo_dt = datetime.strptime(lo, "%Y%m%d").replace(tzinfo=timezone.utc)
                fetched = ib_fetch_minute_range(app, contract, lo_dt, now_utc)
                # Only keep bars for the days we actually identified as missing
                fetched = {k: v for k, v in fetched.items() if row_day(k) in missing_minute_days}
                rows.update(fetched)
                print(f"  [historical] backfilled {len(fetched)} minute bars across {len(missing_minute_days)} day(s)")

            if missing_daily_days:
                lo = min(missing_daily_days)
                hi = max(missing_daily_days)
                lo_dt = datetime.strptime(lo, "%Y%m%d").replace(tzinfo=timezone.utc)
                hi_dt = datetime.strptime(hi, "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
                fetched = ib_fetch_daily_range(app, contract, lo_dt, hi_dt)
                fetched = {k: v for k, v in fetched.items() if k in missing_daily_days}
                rows.update(fetched)
                print(f"  [historical] backfilled {len(fetched)} daily bar(s)")
    else:
        print(f"  [historical] no missing trading days in range {start_date}–{today_str}")

    # 2c. Collapse pass: any minute-resolution day that's now outside the
    # MINUTE_DAYS window gets collapsed to a single daily bar.
    days_with_minutes = sorted({row_day(k) for k in rows if is_minute_row(k)})
    collapsed = 0
    for day in days_with_minutes:
        if day in recent_days:
            continue  # still within the rolling minute window — leave as-is
        minute_bars = minute_rows_for_day(rows, day)
        if not minute_bars:
            continue
        daily_bar = collapse_minutes_to_daily(minute_bars)
        for b in minute_bars:
            del rows[b["datetime"]]
        rows[daily_bar["datetime"]] = daily_bar
        collapsed += 1
    if collapsed:
        print(f"  [historical] collapsed {collapsed} day(s) from minute → daily resolution")

    save_csv(symbol, rows)
    print(f"  [historical] reconciliation complete — {len(rows)} total row(s) saved")


# ═══════════════════════════════════════════════════════════════════════════
# Live mode
# ═══════════════════════════════════════════════════════════════════════════

def yfinance_reconcile(symbol: str, rows: dict):
    """
    Per spec: after market close, check whether the CSV's minute data for
    the last MINUTE_DAYS trading days matches a fresh yfinance pull. If
    not, purge ALL minute rows outside that fresh MINUTE_DAYS set and
    re-pull them from yfinance.
    """
    print(f"  [live->reconcile] checking last {MINUTE_DAYS} trading day(s) of minute data via yfinance")
    fresh = yf_fetch_minute_bars(symbol, period_days=MINUTE_DAYS + 2)
    fresh_days = sorted({row_day(k) for k in fresh})
    target_days = set(fresh_days[-MINUTE_DAYS:]) if fresh_days else set()

    existing_minute_days = {row_day(k) for k in rows if is_minute_row(k)}

    needs_refresh = (existing_minute_days != target_days)
    if not needs_refresh:
        print(f"  [live->reconcile] CSV already has correct {MINUTE_DAYS}-day minute window — no action needed")
        return rows

    print(f"  [live->reconcile] mismatch found (have {sorted(existing_minute_days)}, "
          f"want {sorted(target_days)}) — purging and re-pulling")

    # Remove ALL existing minute rows for days NOT in the target set
    # (per spec: "remove all minute data for the days other than the last
    # 3 days with live stock")
    for k in list(rows.keys()):
        if is_minute_row(k) and row_day(k) not in target_days:
            del rows[k]

    # Also remove any stale daily bars for days that are now supposed to be
    # minute-resolution (target_days), so they don't coexist with the fresh
    # minute data we're about to insert.
    for k in list(rows.keys()):
        if is_daily_row(k) and k in target_days:
            del rows[k]

    # Re-pull minute data for the target days from yfinance and insert
    new_minute_rows = {k: v for k, v in fresh.items() if row_day(k) in target_days}
    rows.update(new_minute_rows)
    print(f"  [live->reconcile] inserted {len(new_minute_rows)} fresh minute bars "
          f"across {len(target_days)} day(s)")
    return rows


def run_live_poll_loop(symbol: str):
    """While the market is open, poll yfinance every POLL_SECONDS and
    upsert today's minute bars into the CSV."""
    print(f"[live] {symbol} — market open, polling every {POLL_SECONDS}s")
    while True:
        if not is_market_open():
            print("[live] market has closed — exiting poll loop")
            return

        try:
            today_bars = yf_fetch_today_minute_bars(symbol)
            rows = load_csv(symbol)
            before = len(rows)
            rows.update(today_bars)
            save_csv(symbol, rows)
            added = len(rows) - before
            print(f"  [live] poll: {len(today_bars)} bar(s) fetched, {added} new row(s) saved "
                  f"({datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC)")
        except Exception as e:
            print(f"  [live][!] poll error: {e}")

        time.sleep(POLL_SECONDS)


def run_idle_loop_until_open(symbol: str):
    """
    While the market is closed: run the post-close historical/yfinance
    reconciliation exactly once per close event, then idle-poll until the
    next market open.
    """
    reconciled_for_close = None  # tracks which close event we've already handled

    while True:
        if is_market_open():
            print("[idle] market is now open — switching to live polling")
            return

        last_close = most_recent_close()
        if last_close is not None:
            trigger_time = last_close + timedelta(hours=HISTORICAL_DELAY_HOURS)
            now_utc = datetime.now(timezone.utc)

            if now_utc >= trigger_time and reconciled_for_close != last_close:
                print(f"\n[idle] {HISTORICAL_DELAY_HOURS}h post-close trigger reached "
                      f"(close was {last_close.strftime('%Y-%m-%d %H:%M UTC')}) — reconciling")
                try:
                    rows = load_csv(symbol)
                    rows = yfinance_reconcile(symbol, rows)
                    save_csv(symbol, rows)
                except Exception as e:
                    print(f"  [idle][!] yfinance reconcile error: {e}")

                try:
                    run_historical_mode(symbol, cli_start_date=None)
                except SystemExit as e:
                    print(f"  [idle][!] historical mode aborted: {e}")
                except Exception as e:
                    print(f"  [idle][!] historical mode error: {e}")

                reconciled_for_close = last_close
                print(f"[idle] reconciliation complete for this close — "
                      f"idling until next open ({next_market_open().strftime('%Y-%m-%d %H:%M UTC')})\n")

        time.sleep(IDLE_POLL_SECONDS)


def run_live_mode(symbol: str):
    """
    Runs forever, cycling between live polling (market open) and idle/
    reconciliation (market closed).
    """
    print(f"\n[live] {symbol} — starting live mode (Ctrl+C to stop)")
    while True:
        if is_market_open():
            run_live_poll_loop(symbol)
        else:
            run_idle_loop_until_open(symbol)


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) not in (2, 3):
        sys.exit(
            "Usage:\n"
            "  python3 extract_data.py <SYMBOL> <YYYYMMDD>   # historical mode\n"
            "  python3 extract_data.py <SYMBOL>               # live mode (runs forever)"
        )

    symbol = sys.argv[1].upper()

    if len(sys.argv) == 3:
        start_date = sys.argv[2]
        try:
            datetime.strptime(start_date, "%Y%m%d")
        except ValueError:
            sys.exit(f"ERROR: cannot parse date '{start_date}'. Expected YYYYMMDD.")
        run_historical_mode(symbol, cli_start_date=start_date)
    else:
        run_live_mode(symbol)


if __name__ == "__main__":
    main()