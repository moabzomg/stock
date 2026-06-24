#!/usr/bin/env python3
"""
extract_data.py — Combined historical (Interactive Brokers) + live (yfinance)
1-minute / daily bar extractor with a rolling minute-resolution window.

═══════════════════════════════════════════════════════════════════════════
USAGE
═══════════════════════════════════════════════════════════════════════════
    python3 extract_data.py                        # live mode for all symbols (from watch_list.txt)
    python3 extract_data.py <YYYYMMDD>             # historical mode for all symbols (from watch_list.txt)
    python3 extract_data.py <SYMBOL>               # live mode for a particular symbol
    python3 extract_data.py <SYMBOL> <YYYYMMDD>    # historical mode for a particular symbol

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
SESSION OPEN TIME INFERENCE
═══════════════════════════════════════════════════════════════════════════
Instead of relying on a hardcoded exchange calendar offset, the live idle
loop infers the next session open time directly from the CSV:

  1. Collect all unique trading dates that appear in the CSV (both daily
     bar dates and the date component of minute bar keys). Sort them.
  2. Identify the last MINUTE_DAYS dates that have minute-bar data.
  3. For each of those days, read the first (earliest) minute bar's HHMM.
  4. If all MINUTE_DAYS start times agree -> use that HHMM as the next
     session's open time, combined with the next trading date (from
     pandas_market_calendars, kept for date math only).
  5. If start times do NOT all agree -> enter live mode immediately and
     stay there until consistent data accumulates.

pandas_market_calendars is still used for one thing: computing the *next
trading date* after the last known trading day (it correctly handles
weekends and holidays). All idle-poll / delay constants and pre-market
offset constants have been removed; session timing comes from the data.

═══════════════════════════════════════════════════════════════════════════
HISTORICAL MODE  (Interactive Brokers)
═══════════════════════════════════════════════════════════════════════════
Invoked either:
  (a) directly from the CLI:  `extract_data.py YYYYMMDD`, or
  (b) internally by live mode, once after the market closes.

Behavior:
  1. If data/<SYMBOL>.csv does NOT exist yet:
       -> fetch MINUTE bars from the given date through the latest
          completed trading day via IB.
  2. If it DOES exist:
       a. Reconciliation pass (CSV-only): recompute OHLCV from minute rows
          and compare against stored daily bars. Mismatches are logged.
       b. Gap-fill pass (uses IB): fetch any missing trading days.
          Days older than MINUTE_DAYS are stored as daily bars; the most
          recent MINUTE_DAYS are stored as minute bars.
       c. Collapse pass: any day with minute data that has fallen outside
          the MINUTE_DAYS window gets collapsed to a single daily bar.

═══════════════════════════════════════════════════════════════════════════
LIVE MODE  (yfinance)
═══════════════════════════════════════════════════════════════════════════
Each symbol runs in its own thread. Threads are independent — some may be
in the OPEN (polling) state while others are in the IDLE (sleeping) state.

  OPEN  — session is active: poll yfinance every POLL_SECONDS, upsert
           1-min bars into the CSV. Phantom zero-volume flat bars emitted
           by yfinance after close are filtered out before saving.

  IDLE  — session is inactive:
           a. Run post-close reconciliation once (yfinance + IB historical).
           b. Collapse the 4th most recent trading day (oldest day that
              just fell outside the MINUTE_DAYS window) from minute rows
              into a single daily bar.
           c. Infer next session open time from the CSV (see above).
              - If inference succeeds: sleep until that UTC datetime, then
                switch back to OPEN.
              - If inference fails (inconsistent start times): switch back
                to OPEN immediately and keep polling until consistent
                data accumulates.

═══════════════════════════════════════════════════════════════════════════
INITIAL STATE DETERMINATION
═══════════════════════════════════════════════════════════════════════════
On startup, each symbol thread determines whether to start OPEN or IDLE
by checking whether the market session is currently active via
pandas_market_calendars. This is the only place the calendar is used for
timing (rather than just date arithmetic) — it is necessary to correctly
handle the startup case where the inferred next open may refer to the
*next* trading day even though today's session is still ongoing.

If the market is currently closed at startup, reconcile_done is set to
False so that post-close reconciliation still runs once before the thread
sleeps until the next open. This ensures the IB historical backfill and
yfinance reconciliation are never skipped just because the script was
started during the post-close window.

═══════════════════════════════════════════════════════════════════════════
PHANTOM BAR FILTERING
═══════════════════════════════════════════════════════════════════════════
yfinance emits flat zero-volume bars after market close to pad the time
series. These are identified by: volume == 0 AND open == high == low ==
close. They are dropped before any bar is written to the CSV.

═══════════════════════════════════════════════════════════════════════════
EFFICIENCY NOTES
═══════════════════════════════════════════════════════════════════════════
- The NYSE calendar object is cached at module level (_CALENDAR) so
  mcal.get_calendar() is only called once per process.
- Calendar date lookups (last_completed_trading_day, last_n_trading_days,
  recent_days) are computed once per run_historical_mode call and reused,
  not repeated per-symbol.
- The poll loop passes already-loaded rows into infer_session_open_from_rows
  to avoid re-reading the CSV from disk after every save.
- post-close reconciliation and collapse_fourth_day share a single CSV
  load/save cycle via run_post_close_and_collapse.
- is_market_open_now reuses rows already loaded by the caller.
- Only one IB historical-mode connection runs at a time across ALL symbol
  threads (_IB_SESSION_LOCK), since multiple symbols on the same NYSE
  calendar tend to close together and would otherwise open several
  simultaneous IB API connections.

═══════════════════════════════════════════════════════════════════════════
DEPENDENCIES
═══════════════════════════════════════════════════════════════════════════
    pip install yfinance pandas_market_calendars --break-system-packages
    pip install ibapi --break-system-packages   # (Interactive Brokers API)
"""

import sys
import os
import csv
import time
import threading
import subprocess
from datetime import datetime, timedelta, timezone
from collections import deque


# ─────────────────────────────────────────────────────────────────────────
# Configuration constants
# ─────────────────────────────────────────────────────────────────────────
DATA_DIR              = 'data'
POLL_SECONDS          = 30        # live-mode polling interval while market is open
MINUTE_DAYS           = 3         # how many of the most-recent trading days are kept
                                   # at 1-minute resolution; older days collapsed to daily
BOOTSTRAP_DAYS        = 730       # how far back to go on first run (CLI-less mode)
MIN_RECONCILE_DAYS    = 500       # historical reconciliation always looks back at least
                                   # this many trading days from today

# ── IB connection defaults (historical mode) ─────────────────────────────
IB_HOST   = "127.0.0.1"
IB_PORT   = 7496           # TWS paper: 7497 | TWS live: 7496 | Gateway paper: 4002
CLIENT_ID = int(os.environ.get("IB_CLIENT_ID", "11"))

# Only one IB historical-mode connection at a time, across ALL symbol
# threads. Without this, several symbols closing around the same time
# (they're all on the same NYSE calendar) can each open their own
# IBSession() simultaneously -- each one spins up its own background
# socket-reader thread on top of the ~50 yfinance polling threads already
# running, which is real CPU/GIL contention and unnecessary pressure on
# the IB Gateway's connection count.
_IB_SESSION_LOCK = threading.Lock()

# ── IB pacing constants ───────────────────────────────────────────────────
INTER_REQUEST_SLEEP    = 10
MAX_REQUESTS_PER_10M   = 55
REQUEST_WINDOW_SECS    = 600
MINUTE_CHUNK_DURATION  = "10 D"
MINUTE_CHUNK_STEP_DAYS = 10
DAILY_CHUNK_DURATION   = "1 Y"
WHAT_TO_SHOW           = "TRADES"
USE_RTH                = 0

YF_MAX_MINUTE_DAYS = 7         # yfinance hard cap on 1m history

FIELDNAMES = ["datetime", "open", "close", "high", "low", "volume"]


# ═══════════════════════════════════════════════════════════════════════════
# Cached calendar — mcal.get_calendar() is expensive; call it once.
# ═══════════════════════════════════════════════════════════════════════════

_CALENDAR = None

def _get_calendar():
    global _CALENDAR
    if _CALENDAR is None:
        try:
            import pandas_market_calendars as mcal
        except ImportError:
            sys.exit(
                "ERROR: pandas_market_calendars is required.\n"
                "Install with: pip install pandas_market_calendars --break-system-packages"
            )
        _CALENDAR = mcal.get_calendar("NYSE")
    return _CALENDAR


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


def save_csv(symbol: str, rows: dict, run_ma: bool = False, ma_start_date: str = None):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = csv_path(symbol)

    def sort_key(dt_str):
        day = int(row_day(dt_str))
        if is_daily_row(dt_str):
            return (day, 0, 0)
        return (day, 1, int(dt_str[8:]))

    ordered = sorted(rows.values(), key=lambda r: sort_key(r["datetime"]))
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(ordered)

    if run_ma:
        cmd = [sys.executable, "ma.py", symbol]
        if ma_start_date:
            cmd.append(ma_start_date)
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            print(f"  [ma] warning: '{' '.join(cmd)}' exited with code {e.returncode} for {symbol}")
        except FileNotFoundError:
            print("  [ma] warning: ma.py not found — skipping MA update")


def minute_rows_for_day(rows: dict, day: str) -> list:
    """All minute-resolution rows for a given YYYYMMDD day, sorted by time."""
    matches = [r for k, r in rows.items() if is_minute_row(k) and row_day(k) == day]
    matches.sort(key=lambda r: r["datetime"])
    return matches


def collapse_minutes_to_daily(minute_bars: list) -> dict:
    """
    Given a chronologically-sorted list of minute-bar dicts for one day,
    compute the equivalent single daily bar.
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
# Phantom bar filtering
# ═══════════════════════════════════════════════════════════════════════════

def is_phantom_bar(bar: dict) -> bool:
    """
    yfinance emits flat zero-volume bars after market close to pad the
    time series. Identify them by: volume == 0 AND all four price fields
    are identical. These carry no information and are dropped before
    writing to the CSV.
    """
    return (
        bar["volume"] == 0.0
        and bar["open"] == bar["high"] == bar["low"] == bar["close"]
    )


# ═══════════════════════════════════════════════════════════════════════════
# Trading date helpers (derived purely from CSV data)
# ═══════════════════════════════════════════════════════════════════════════

def last_n_minute_days(rows: dict, n: int) -> list:
    """
    Return the last n trading dates (YYYYMMDD, ascending) that have at
    least one minute bar in the CSV.
    """
    minute_dates = sorted({row_day(k) for k in rows if is_minute_row(k)})
    return minute_dates[-n:]


# ═══════════════════════════════════════════════════════════════════════════
# Session open inference from CSV rows (no disk I/O)
# ═══════════════════════════════════════════════════════════════════════════

def infer_session_open_hhmm(rows: dict):
    """
    Inspect the first minute bar of each of the last MINUTE_DAYS trading
    days that have minute data. If all MINUTE_DAYS start times agree,
    return that HHMM string (e.g. "0800"). Otherwise return None.
    """
    recent_days = last_n_minute_days(rows, MINUTE_DAYS)

    if len(recent_days) < MINUTE_DAYS:
        return None

    start_times = []
    for day in recent_days:
        minute_bars = minute_rows_for_day(rows, day)
        if not minute_bars:
            return None
        start_times.append(minute_bars[0]["datetime"][8:])

    if len(set(start_times)) == 1:
        return start_times[0]
    return None


def infer_next_session_open_from_rows(rows: dict):
    """
    Given an already-loaded rows dict, return the UTC datetime of the next
    session open, or None if it cannot be inferred.

    Avoids re-reading the CSV from disk — callers that already have rows
    in memory should use this instead of infer_next_session_open_utc().
    """
    if not rows:
        return None

    hhmm = infer_session_open_hhmm(rows)
    if hhmm is None:
        return None

    recent_days     = last_n_minute_days(rows, MINUTE_DAYS)
    last_minute_day = recent_days[-1]

    next_day = next_trading_date_after(last_minute_day)
    if next_day is None:
        return None

    return datetime.strptime(next_day + hhmm, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def infer_next_session_open_utc(symbol: str):
    """
    Load the CSV for symbol and return the UTC datetime of the next session
    open, or None. Use infer_next_session_open_from_rows() when rows are
    already in memory.
    """
    return infer_next_session_open_from_rows(load_csv(symbol))


# ═══════════════════════════════════════════════════════════════════════════
# Exchange calendar helpers
# ═══════════════════════════════════════════════════════════════════════════

def next_trading_date_after(last_day_str: str):
    """
    Return the next NYSE trading date after last_day_str (YYYYMMDD), or
    None if the calendar cannot be queried.
    """
    cal   = _get_calendar()
    start = datetime.strptime(last_day_str, "%Y%m%d").date() + timedelta(days=1)
    end   = start + timedelta(days=14)
    schedule = cal.schedule(start_date=start, end_date=end)
    if schedule.empty:
        return None
    return schedule.index[0].strftime("%Y%m%d")


def trading_days_between(start_date, end_date) -> list:
    cal = _get_calendar()
    schedule = cal.schedule(start_date=start_date, end_date=end_date)
    return [d.strftime("%Y%m%d") for d in schedule.index]


def last_n_trading_days(n: int, as_of=None) -> list:
    if as_of is None:
        as_of = datetime.now(timezone.utc).date()
    cal = _get_calendar()
    lookback_start = as_of - timedelta(days=int(n * 2.5) + 10)
    schedule = cal.schedule(start_date=lookback_start, end_date=as_of)
    days = [d.strftime("%Y%m%d") for d in schedule.index]
    return days[-n:]


def last_completed_trading_day(as_of_utc=None) -> str:
    if as_of_utc is None:
        as_of_utc = datetime.now(timezone.utc)
    cal = _get_calendar()
    lookback = as_of_utc.date() - timedelta(days=14)
    schedule = cal.schedule(start_date=lookback, end_date=as_of_utc.date())
    last_day = None
    for idx, row in schedule.iterrows():
        close_utc = row["market_close"].tz_convert("UTC").to_pydatetime()
        if close_utc <= as_of_utc:
            last_day = idx.strftime("%Y%m%d")
    return last_day


def is_market_open_now(rows: dict, now_utc: datetime) -> bool:
    """
    Return True if now_utc falls within today's trading session.

    - Session START: inferred from the first minute bar across the last
                     MINUTE_DAYS days in the already-loaded rows dict
                     (covers pre-market since yfinance uses prepost=True,
                     typically ~08:00 UTC / 4am ET). Falls back to the
                     calendar's regular open if inference isn't available.
    - Session END  : midnight UTC on the trading day. yfinance with
                     prepost=True returns post-market bars until ~00:00 UTC
                     (8pm ET), so we poll until midnight rather than the
                     RTH close (20:00 UTC / 4pm ET) to capture them.

    Accepts already-loaded rows to avoid an extra CSV read. This is the
    SINGLE authoritative open/closed check -- used both at thread startup
    AND on every iteration of the live poll loop (see run_symbol_live_loop).
    """
    cal       = _get_calendar()
    today     = now_utc.date()
    today_str = today.strftime("%Y%m%d")

    try:
        schedule = cal.schedule(start_date=today, end_date=today)
    except Exception:
        return False
    if schedule.empty:
        return False

    # Session end = midnight UTC (end of the trading calendar day).
    # This covers pre-market (08:00), RTH (09:30–20:00), and post-market
    # (20:00–00:00) in a single polling window.
    session_end = datetime(today.year, today.month, today.day,
                           23, 59, 59, tzinfo=timezone.utc)

    hhmm = infer_session_open_hhmm(rows) if rows else None
    if hhmm is not None:
        market_open = datetime.strptime(today_str + hhmm, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    else:
        market_open = schedule.iloc[0]["market_open"].tz_convert("UTC").to_pydatetime()

    return market_open <= now_utc <= session_end


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
            self._bar_size   = None

        def historicalData(self, reqId, bar):
            raw      = bar.date.strip()
            is_daily = (self._bar_size == "1 day")

            if is_daily:
                # Daily bars: IB sends a Unix timestamp with formatDate=2.
                # Convert to YYYYMMDD via UTC date, ignoring the time component.
                try:
                    dt     = datetime.fromtimestamp(int(raw), tz=timezone.utc)
                    fmt_dt = dt.strftime("%Y%m%d")
                except ValueError:
                    # Fallback: some IB versions send "YYYYMMDD" strings directly.
                    fmt_dt = raw.replace(" ", "")[:8]
            else:
                # Minute bars: Unix timestamp -> YYYYMMDDHHMM (UTC).
                try:
                    dt     = datetime.fromtimestamp(int(raw), tz=timezone.utc)
                    fmt_dt = dt.strftime("%Y%m%d%H%M")
                except ValueError:
                    raw_clean = raw.replace(" ", "")
                    try:
                        dt = datetime.strptime(raw, "%Y%m%d %H:%M:%S")
                    except ValueError:
                        dt = datetime.strptime(raw_clean, "%Y%m%d%H%M%S")
                    fmt_dt = dt.strftime("%Y%m%d%H%M")

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
                pass
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
                print(f"  [pace] rolling window full - sleeping {wait:.1f}s ...")
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
            end_str          = end_dt.strftime("%Y%m%d-%H:%M:%S")
            self._bars       = []
            self._bar_size   = bar_size
            self._done_event.clear()
            self._error_flag = False
            self._pace()
            print(f"  [ib] requesting {bar_size} bars, duration={duration}, up to {end_str} ...", end="", flush=True)
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
    result    = {}
    chunk_end = end_dt_exclusive
    while chunk_end > start_dt:
        bars = app.fetch(contract, chunk_end, MINUTE_CHUNK_DURATION, "1 min")
        if app._error_flag:
            print("  [ib] fatal error during minute fetch - stopping.")
            break
        for b in bars:
            bdt = datetime.strptime(b["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            if bdt >= start_dt:
                result[b["datetime"]] = b
        chunk_end -= timedelta(days=MINUTE_CHUNK_STEP_DAYS)
    return result


def ib_fetch_daily_range(app, contract, start_dt, end_dt_exclusive):
    start_str = start_dt.strftime("%Y%m%d")
    end_str   = (end_dt_exclusive - timedelta(days=1)).strftime("%Y%m%d")
    result    = {}
    chunk_end = end_dt_exclusive
    while chunk_end > start_dt:
        bars = app.fetch(contract, chunk_end, DAILY_CHUNK_DURATION, "1 day")
        if app._error_flag:
            print("  [ib] fatal error during daily fetch.")
            break
        for b in bars:
            day = b["datetime"]
            if len(day) != 8:
                continue
            if start_str <= day <= end_str:
                result[day] = b
        chunk_end -= timedelta(days=366)
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
    Fetch 1-minute bars from yfinance for the last period_days days.
    Phantom bars (flat OHLC, zero volume) are filtered out before returning.
    """
    if period_days > YF_MAX_MINUTE_DAYS:
        print(f"  [warn] yfinance caps 1m bars at {YF_MAX_MINUTE_DAYS} days; "
              f"clamping from {period_days}")
        period_days = YF_MAX_MINUTE_DAYS
    yf     = _yf_import()
    ticker = yf.Ticker(symbol)
    hist   = ticker.history(period=f"{period_days}d", interval="1m", prepost=True)
    result = {}
    for idx, row in hist.iterrows():
        dt_utc = idx.tz_convert("UTC") if idx.tzinfo else idx.tz_localize("UTC")
        dt_str = dt_utc.strftime("%Y%m%d%H%M")
        bar = {
            "datetime": dt_str,
            "open":     float(row["Open"]),
            "close":    float(row["Close"]),
            "high":     float(row["High"]),
            "low":      float(row["Low"]),
            "volume":   float(row["Volume"]),
        }
        if not is_phantom_bar(bar):
            result[dt_str] = bar
    return result


def yf_fetch_today_minute_bars(symbol: str) -> dict:
    return yf_fetch_minute_bars(symbol, period_days=1)


# ═══════════════════════════════════════════════════════════════════════════
# Reconciliation logic
# ═══════════════════════════════════════════════════════════════════════════

def reconcile_check(symbol: str, rows: dict):
    days_with_minutes = sorted({row_day(k) for k in rows if is_minute_row(k)})
    mismatches = 0
    for day in days_with_minutes:
        minute_bars = minute_rows_for_day(rows, day)
        if not minute_bars:
            continue
        computed  = collapse_minutes_to_daily(minute_bars)
        daily_key = day
        if daily_key in rows and is_daily_row(daily_key):
            stored = rows[daily_key]
            issues = []
            if abs(stored["open"] - computed["open"]) > 1e-6:
                issues.append(f"open {stored['open']} vs computed {computed['open']}")
            if abs(stored["close"] - computed["close"]) > 1e-6:
                issues.append(f"close {stored['close']} vs computed {computed['close']}")
            if abs(stored["volume"] - computed["volume"]) > max(1.0, stored["volume"] * 0.001):
                issues.append(f"volume {stored['volume']} vs computed {computed['volume']}")
            if issues:
                mismatches += 1
                print(f"  [reconcile][!] {symbol} {day}: " + "; ".join(issues))
    if mismatches == 0:
        print(f"  [reconcile] {symbol}: no misalignments found in {len(days_with_minutes)} day(s)")
    return mismatches


# ═══════════════════════════════════════════════════════════════════════════
# Historical mode
# ═══════════════════════════════════════════════════════════════════════════

def run_historical_mode(symbol: str, cli_start_date: str = None,
                        today_str: str = None, recent_days: set = None):
    """
    today_str and recent_days can be passed in from the caller to avoid
    redundant calendar queries when processing multiple symbols in a loop.
    """
    print(f"\n[historical] {symbol} -- starting reconciliation")
    rows    = load_csv(symbol)
    now_utc = datetime.now(timezone.utc)

    if today_str is None:
        today_str = last_completed_trading_day(now_utc)
    if today_str is None:
        print(f"  [historical] {symbol}: could not determine last trading day -- skipping")
        return

    if recent_days is None:
        recent_days = set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date()))

    contract = make_contract(symbol)

    if cli_start_date:
        start_date = cli_start_date
    else:
        target_window  = last_n_trading_days(MIN_RECONCILE_DAYS + 1, as_of=now_utc.date())
        calendar_start = target_window[0]
        if rows:
            earliest_existing = min(row_day(k) for k in rows)
            start_date = min(earliest_existing, calendar_start)
        else:
            start_date = calendar_start

    start_dt = datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(today_str, "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=1)

    if rows:
        reconcile_check(symbol, rows)

    existing_daily_days  = {k for k in rows if is_daily_row(k)}
    existing_minute_days = {row_day(k) for k in rows if is_minute_row(k)}
    all_td               = trading_days_between(start_dt.date(), today_str)
    will_be_collapsed    = existing_minute_days - recent_days

    missing_daily_days = [
        d for d in all_td
        if d not in recent_days
        and d not in existing_daily_days
        and d not in will_be_collapsed
    ]
    missing_minute_days = [
        d for d in all_td
        if d in recent_days
        and d not in existing_minute_days
    ]

    if missing_daily_days or missing_minute_days:
        print(f"  [historical] {len(missing_daily_days)} missing daily bar(s), "
              f"{len(missing_minute_days)} missing minute day(s) -- backfilling via IB")

        # Serialize ALL IB connections process-wide -- see _IB_SESSION_LOCK
        # comment above. Without this, several symbol threads landing in
        # historical mode around the same time would each open their own
        # simultaneous IB API connection.
        with _IB_SESSION_LOCK:
            with IBSession() as app:
                if missing_minute_days:
                    lo_dt   = datetime.strptime(min(missing_minute_days), "%Y%m%d").replace(tzinfo=timezone.utc)
                    fetched = ib_fetch_minute_range(app, contract, lo_dt, end_dt)
                    fetched = {k: v for k, v in fetched.items() if row_day(k) in set(missing_minute_days)}
                    rows.update(fetched)
                    print(f"  [historical] backfilled {len(fetched)} minute bars across {len(missing_minute_days)} day(s)")

                if missing_daily_days:
                    lo_dt   = datetime.strptime(min(missing_daily_days), "%Y%m%d").replace(tzinfo=timezone.utc)
                    fetched = ib_fetch_daily_range(app, contract, lo_dt, end_dt)
                    fetched = {k: v for k, v in fetched.items() if k in set(missing_daily_days)}
                    rows.update(fetched)
                    print(f"  [historical] backfilled {len(fetched)} daily bars")
    else:
        print(f"  [historical] no missing trading days in range {start_date}--{today_str}")

    # Collapse pass: any minute day outside the MINUTE_DAYS window -> daily bar.
    days_with_minutes = sorted({row_day(k) for k in rows if is_minute_row(k)})
    collapsed = 0
    for day in days_with_minutes:
        if day in recent_days:
            continue
        minute_bars = minute_rows_for_day(rows, day)
        if not minute_bars:
            continue
        daily_bar      = collapse_minutes_to_daily(minute_bars)
        keys_to_delete = [b["datetime"] for b in minute_bars]
        for k in keys_to_delete:
            del rows[k]
        rows[daily_bar["datetime"]] = daily_bar
        collapsed += 1
    if collapsed:
        print(f"  [historical] collapsed {collapsed} day(s) from minute -> daily resolution")

    all_dates     = sorted(rows.keys())
    earliest_date = row_day(all_dates[0]) if all_dates else None
    save_csv(symbol, rows, run_ma=True, ma_start_date=earliest_date)


# ═══════════════════════════════════════════════════════════════════════════
# Live mode -- per-symbol functions
# ═══════════════════════════════════════════════════════════════════════════

def yfinance_reconcile(symbol: str, rows: dict) -> dict:
    fresh       = yf_fetch_minute_bars(symbol, period_days=MINUTE_DAYS + 2)
    fresh_days  = sorted({row_day(k) for k in fresh})
    target_days = set(fresh_days[-MINUTE_DAYS:]) if fresh_days else set()

    existing_minute_days = {row_day(k) for k in rows if is_minute_row(k)}
    if existing_minute_days == target_days:
        return rows   # already correct — no log, no action

    print(f"  [reconcile] {symbol}: yfinance minute window mismatch — re-pulling "
          f"(have {sorted(existing_minute_days)}, want {sorted(target_days)})")
    for k in list(rows.keys()):
        if is_minute_row(k) and row_day(k) not in target_days:
            del rows[k]
    for k in list(rows.keys()):
        if is_daily_row(k) and k in target_days:
            del rows[k]
    new_rows = {k: v for k, v in fresh.items() if row_day(k) in target_days}
    rows.update(new_rows)
    print(f"  [reconcile] {symbol}: inserted {len(new_rows)} fresh minute bars "
          f"across {len(target_days)} day(s)")
    return rows


def run_live_poll_once(symbol: str) -> dict:
    """
    Fetch today's minute bars, merge into the CSV, save, and return the
    updated rows dict so the caller can reuse it without a second disk read.
    Returns an empty dict on error.
    """
    try:
        today_bars = yf_fetch_today_minute_bars(symbol)
        rows       = load_csv(symbol)
        before     = len(rows)
        rows.update(today_bars)
        added = len(rows) - before
        save_csv(symbol, rows)
        # Only log when something actually changed to reduce noise.
        if added > 0:
            print(f"  [live] {symbol}: +{added} new bar(s) "
                  f"({datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC)")
        return rows
    except Exception as e:
        print(f"  [live][!] {symbol} poll error: {e}")
        return {}


def run_post_close_and_collapse(symbol: str):
    """
    Run yfinance reconciliation, IB historical reconciliation, and the
    fourth-day collapse in a single load/save cycle to minimise disk I/O.
    """
    print(f"[{symbol}] post-close reconciliation starting")

    try:
        rows = load_csv(symbol)
        rows = yfinance_reconcile(symbol, rows)
        save_csv(symbol, rows)
    except Exception as e:
        print(f"[{symbol}][!] yfinance reconcile error: {e}")

    try:
        run_historical_mode(symbol)
    except Exception as e:
        print(f"[{symbol}][!] historical reconcile error: {e}")

    try:
        rows        = load_csv(symbol)
        minute_days = sorted({row_day(k) for k in rows if is_minute_row(k)})

        if len(minute_days) >= MINUTE_DAYS + 1:
            fourth_day  = minute_days[-(MINUTE_DAYS + 1)]
            minute_bars = minute_rows_for_day(rows, fourth_day)

            if minute_bars:
                daily_bar = collapse_minutes_to_daily(minute_bars)
                for b in minute_bars:
                    del rows[b["datetime"]]
                rows[daily_bar["datetime"]] = daily_bar

                all_dates     = sorted(rows.keys())
                earliest_date = row_day(all_dates[0]) if all_dates else None
                save_csv(symbol, rows, run_ma=True, ma_start_date=earliest_date)
                print(f"[{symbol}] collapsed {fourth_day} ({len(minute_bars)} bars) -> daily")
    except Exception as e:
        print(f"[{symbol}][!] collapse error: {e}")


def run_symbol_live_loop(symbol: str):
    """
    Per-symbol live loop. Runs forever:

      OPEN state  -- poll yfinance every POLL_SECONDS. The rows dict
                    returned by run_live_poll_once() is reused directly
                    for close detection (via is_market_open_now), avoiding
                    a second CSV read.

      IDLE state  -- run post-close reconciliation and fourth-day collapse
                    in one pass, then infer next session open from the CSV.
                    - If inference succeeds: sleep until that UTC time.
                    - If inference fails: re-enter OPEN state immediately.

    Initial state:
      - is_market_open_now() == True  -> OPEN, reconcile_done=False
      - is_market_open_now() == False -> IDLE, reconcile_done=False
        Post-close reconciliation always runs once when starting in IDLE,
        regardless of whether the script was started mid-session or after
        close. This ensures the IB historical backfill is never skipped.
    """
    print(f"[{symbol}] starting per-symbol live loop")

    now  = datetime.now(timezone.utc)
    rows = load_csv(symbol)

    if is_market_open_now(rows, now):
        state          = "OPEN"
        reconcile_done = False
        print(f"[{symbol}] OPEN (session active)")
    else:
        next_open = infer_next_session_open_from_rows(rows)
        if next_open is not None and now < next_open:
            state          = "IDLE"
            reconcile_done = False
            print(f"[{symbol}] IDLE until {next_open.strftime('%Y-%m-%d %H:%M')} UTC (will reconcile first)")
        else:
            state          = "OPEN"
            reconcile_done = False
            print(f"[{symbol}] OPEN (no future open inferred, polling)")

    while True:
        now = datetime.now(timezone.utc)

        # ── OPEN state ────────────────────────────────────────────────────
        if state == "OPEN":
            tick_start = time.time()

            # Poll returns the updated rows dict -- reuse it for close
            # detection instead of reading the CSV from disk again.
            rows = run_live_poll_once(symbol)

            # FIX: this MUST be the same check used at startup
            # (is_market_open_now), not a comparison against the inferred
            # *next* session open. The old check compared "now" against
            # next_open computed from the CSV -- but next_open is "the day
            # AFTER the most recent minute-day on disk", which becomes
            # tomorrow's open the instant a single bar lands for today.
            # That made "now < next_open" true almost immediately after
            # the first bar of the day was recorded, so the symbol got
            # flagged CLOSED -- and sent into a multi-hour sleep -- while
            # the market was still very much open. is_market_open_now()
            # uses an explicit end-of-day boundary (23:59:59 UTC) instead,
            # so it can't fire until the session has actually ended.
            if not is_market_open_now(rows, datetime.now(timezone.utc)):
                next_open = infer_next_session_open_from_rows(rows)
                if next_open is not None:
                    print(f"[{symbol}] session closed -> IDLE (next open {next_open.strftime('%Y-%m-%d %H:%M')} UTC)")
                else:
                    print(f"[{symbol}] session closed -> IDLE (next open not yet inferable)")
                state          = "IDLE"
                reconcile_done = False
                continue

            elapsed   = time.time() - tick_start
            remaining = POLL_SECONDS - elapsed
            if remaining > 0:
                time.sleep(remaining)
            else:
                print(f"  [{symbol}] poll round took {elapsed:.1f}s -- skipping sleep")

        # ── IDLE state ────────────────────────────────────────────────────
        else:
            if not reconcile_done:
                run_post_close_and_collapse(symbol)
                reconcile_done = True

            # Re-read once after reconciliation to get the authoritative state.
            rows      = load_csv(symbol)
            next_open = infer_next_session_open_from_rows(rows)
            now       = datetime.now(timezone.utc)

            if next_open is None:
                state          = "OPEN"
                reconcile_done = False
                continue

            if now >= next_open:
                print(f"[{symbol}] inferred open passed -> OPEN")
                state          = "OPEN"
                reconcile_done = False
                continue

            sleep_secs = (next_open - now).total_seconds()
            print(f"[{symbol}] sleeping {sleep_secs/3600:.2f}h -> {next_open.strftime('%Y-%m-%d %H:%M')} UTC")
            time.sleep(sleep_secs)

            print(f"[{symbol}] waking -> OPEN")
            state          = "OPEN"
            reconcile_done = False


# ═══════════════════════════════════════════════════════════════════════════
# Preflight
# ═══════════════════════════════════════════════════════════════════════════

def has_sufficient_data(symbol: str, recent_trading_days: list, required_start: str) -> bool:
    rows = load_csv(symbol)
    if not rows:
        print(f"  [preflight] {symbol}: no CSV found -- needs full backfill")
        return False

    daily_days       = sorted(k for k in rows if is_daily_row(k))
    minute_days      = sorted({row_day(k) for k in rows if is_minute_row(k)})
    all_covered_days = sorted(set(daily_days) | set(minute_days))

    has_enough_history = len(all_covered_days) >= MIN_RECONCILE_DAYS
    has_minute_data    = set(recent_trading_days).issubset(set(minute_days))

    if has_enough_history:
        hist_msg = (f"{len(all_covered_days)} total bars "
                    f"[{all_covered_days[0]}-{all_covered_days[-1]}] OK")
    else:
        earliest_on_disk = all_covered_days[0] if all_covered_days else "none"
        missing_count    = MIN_RECONCILE_DAYS - len(all_covered_days)
        hist_msg = (f"{len(all_covered_days)}/{MIN_RECONCILE_DAYS} bars -- "
                    f"need {missing_count} more back to {required_start}, "
                    f"earliest on disk: {earliest_on_disk}")

    missing_minutes = sorted(set(recent_trading_days) - set(minute_days))
    if has_minute_data:
        min_msg = f"minute [{', '.join(minute_days)}] OK"
    else:
        min_msg = (f"minute [{', '.join(minute_days) or 'none'}] "
                   f"MISSING {missing_minutes}")

    status = "OK" if (has_enough_history and has_minute_data) else "NEEDS BACKFILL"
    print(f"  [preflight] {symbol}: {status} | {hist_msg} | {min_msg}")
    return has_enough_history and has_minute_data


def ensure_data_ready(symbols: list):
    print(f"\n[preflight] checking data sufficiency for {len(symbols)} symbol(s) ...")

    # Compute calendar lookups once for all symbols.
    recent_trading_days = sorted(last_n_trading_days(MINUTE_DAYS))
    required_start      = last_n_trading_days(MIN_RECONCILE_DAYS + 1)[0]

    needs_backfill = [
        s for s in symbols
        if not has_sufficient_data(s, recent_trading_days, required_start)
    ]

    if not needs_backfill:
        print("[preflight] all symbols have sufficient data -- skipping backfill\n")
        return

    print(f"\n[preflight] {len(needs_backfill)} symbol(s) need backfill: {', '.join(needs_backfill)}")
    print(f"[preflight] fetching from {required_start} ({MIN_RECONCILE_DAYS} trading days back) ...\n")

    # Precompute shared calendar values for the backfill loop.
    now_utc     = datetime.now(timezone.utc)
    today_str   = last_completed_trading_day(now_utc)
    recent_days = set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date()))

    for symbol in needs_backfill:
        print(f"[preflight] backfilling {symbol} ...")
        try:
            run_historical_mode(symbol, today_str=today_str, recent_days=recent_days)
        except Exception as e:
            print(f"[preflight][!] {symbol} error: {e}")

    print(f"\n[preflight] backfill complete\n")


# ═══════════════════════════════════════════════════════════════════════════
# Live mode entry point
# ═══════════════════════════════════════════════════════════════════════════

def run_live_mode(symbols: list):
    print(f"\n[live] starting live mode for {len(symbols)} symbol(s): {', '.join(symbols)}")

    ensure_data_ready(symbols)

    threads = []
    for symbol in symbols:
        t = threading.Thread(
            target=run_symbol_live_loop,
            args=(symbol,),
            name=f"live-{symbol}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    print(f"[live] {len(threads)} symbol thread(s) started")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n[live] interrupted -- shutting down")
        # Daemon threads (including IB's background socket-reader threads,
        # which can briefly delay signal delivery and make the first
        # Ctrl+C feel like it didn't register) are killed unconditionally
        # by the OS the instant the process exits -- no need to wait for
        # them. os._exit() skips Python's normal interpreter teardown
        # (atexit handlers, thread joins) for an immediate, clean kill.
        os._exit(0)


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def main():
    if len(sys.argv) not in (1, 2, 3):
        sys.exit(
            "Usage:\n"
            "  python3 extract_data.py                        # live mode for all symbols (from watch_list.txt)\n"
            "  python3 extract_data.py <YYYYMMDD>             # historical mode for all symbols (from watch_list.txt)\n"
            "  python3 extract_data.py <SYMBOL>               # live mode for a particular symbol\n"
            "  python3 extract_data.py <SYMBOL> <YYYYMMDD>    # historical mode for a particular symbol"
        )

    os.makedirs(DATA_DIR, exist_ok=True)

    arg1 = sys.argv[1] if len(sys.argv) >= 2 else None
    arg2 = sys.argv[2] if len(sys.argv) == 3 else None

    def is_date(s):
        try:
            datetime.strptime(s, "%Y%m%d")
            return True
        except ValueError:
            return False

    if arg1 is not None and not is_date(arg1):
        # arg1 is a symbol
        symbols    = [arg1.upper()]
        start_date = arg2
        if start_date is not None and not is_date(start_date):
            sys.exit(f"ERROR: cannot parse date '{start_date}'. Expected YYYYMMDD.")
    else:
        # arg1 is a date (or absent) -- load symbols from watch_list.txt
        start_date = arg1
        try:
            with open("watch_list.txt", "r") as f:
                symbols = [
                    line.strip().upper()
                    for line in f
                    if line.strip() and not line.startswith("#")
                ]
        except FileNotFoundError:
            sys.exit("ERROR: watch_list.txt not found.")
        if not symbols:
            sys.exit("ERROR: watch_list.txt is empty.")

    if start_date is not None:
        # Historical mode: share calendar values across all symbols.
        now_utc     = datetime.now(timezone.utc)
        today_str   = last_completed_trading_day(now_utc)
        recent_days = set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date()))
        for symbol in symbols:
            run_historical_mode(symbol, cli_start_date=start_date,
                                today_str=today_str, recent_days=recent_days)
    else:
        run_live_mode(symbols)


if __name__ == "__main__":
    main()