import sys
import os
import csv
import time
from collections import deque
from datetime import datetime, timedelta, date, time as dtime

import yfinance as yf
from ib_insync import IB, Stock

try:
    from zoneinfo import ZoneInfo
except ImportError:                                   # pragma: no cover
    from backports.zoneinfo import ZoneInfo            # py < 3.9 fallback

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR       = 'data'
POLL_SECONDS   = 30        # live-mode polling interval, while market is open
MINUTE_DAYS    = 3         # how many of the most-recent trading days are kept
                            # at 1-minute resolution; older days get collapsed
                            # into a single daily bar by historical mode
BOOTSTRAP_DAYS = 730        # how far back to go on first run (CLI-less mode)

HISTORICAL_DELAY_HOURS = 1  # run historical-mode reconciliation this many
                             # hours after the market closes (tweak freely)
IDLE_POLL_SECONDS      = 60 # how often the idle loop wakes to re-check the
                             # clock while waiting for the next open/trigger

IB_HOST      = '127.0.0.1'
IB_PORT      = 7497
IB_CLIENT_ID = 2

MARKET_TZ_NAME = 'America/New_York'
MARKET_TZ      = ZoneInfo(MARKET_TZ_NAME)
MARKET_OPEN_T  = dtime(9, 30)
MARKET_CLOSE_T = dtime(16, 0)

# ── IB pacing limits (see TWS API docs) ──────────────────────────────────────
#   • Max 60 historical requests in any rolling 10-minute window
#   • Max 6 requests for the same contract/exchange/type within 2 seconds
#   • No identical requests within 15 seconds
MAX_REQ_PER_10_MIN  = 57    # stay safely below the hard cap of 60
WINDOW_SECONDS      = 600   # 10-minute rolling window (seconds)
SAME_CONTRACT_GAP   = 2.2   # seconds to sleep between same-contract requests
                             # (> 2 s enforces the "6-in-2-s" rule for single-
                             #  threaded sequential requests)
PACING_BACKOFF_BASE = 65    # sleep after the first code-162 error;
                             # multiplied by attempt number for back-off
MAX_RETRIES         = 5     # retries per request before giving up


# ─────────────────────────────────────────────────────────────────────────────
# Market calendar helpers
# ─────────────────────────────────────────────────────────────────────────────
# We try to use pandas_market_calendars for proper NYSE holiday / early-close
# awareness. If it isn't installed we fall back to a plain weekday + fixed
# 9:30-16:00 ET check — that fallback will NOT know about holidays or half
# days, so installing the real calendar is recommended:
#
#     pip install pandas_market_calendars
#
try:
    import pandas_market_calendars as mcal
    _NYSE = mcal.get_calendar('NYSE')
    _HAS_MCAL = True
except ImportError:
    _NYSE = None
    _HAS_MCAL = False
    print("[warn] pandas_market_calendars not installed — falling back to "
          "weekday-only market-hours logic (holidays/half-days will NOT be "
          "detected). Install with: pip install pandas_market_calendars")


def now_et() -> datetime:
    return datetime.now(MARKET_TZ)


def market_session(d: date):
    """Return (open_dt, close_dt) in ET for trading day *d*, or None if *d*
    is not a trading day."""
    if _HAS_MCAL:
        sched = _NYSE.schedule(start_date=d, end_date=d)
        if sched.empty:
            return None
        o = sched.iloc[0]['market_open'].tz_convert(MARKET_TZ).to_pydatetime()
        c = sched.iloc[0]['market_close'].tz_convert(MARKET_TZ).to_pydatetime()
        return o, c

    if d.weekday() >= 5:
        return None
    o = datetime.combine(d, MARKET_OPEN_T, tzinfo=MARKET_TZ)
    c = datetime.combine(d, MARKET_CLOSE_T, tzinfo=MARKET_TZ)
    return o, c


def is_trading_day(d: date) -> bool:
    return market_session(d) is not None


def is_market_open(now: datetime = None) -> bool:
    now = now or now_et()
    sess = market_session(now.date())
    if sess is None:
        return False
    o, c = sess
    return o <= now <= c


def last_close(now: datetime = None) -> datetime:
    """Most recent market-close at or before *now*."""
    now = now or now_et()
    d = now.date()
    while True:
        sess = market_session(d)
        if sess is not None:
            o, c = sess
            if now >= c:
                return c
        d -= timedelta(days=1)


def next_open(now: datetime = None) -> datetime:
    """Next market-open at or after *now*."""
    now = now or now_et()
    d = now.date()
    while True:
        sess = market_session(d)
        if sess is not None:
            o, c = sess
            if now < o:
                return o
        d += timedelta(days=1)


def _previous_trading_day(d: date) -> date:
    d -= timedelta(days=1)
    while not is_trading_day(d):
        d -= timedelta(days=1)
    return d


def _next_trading_day(d: date) -> date:
    d += timedelta(days=1)
    while not is_trading_day(d):
        d += timedelta(days=1)
    return d


def _last_n_trading_days(ref_date: date, n: int) -> list:
    """The *n* most-recent trading days at or before ref_date (inclusive)."""
    days = []
    d = ref_date
    while len(days) < n:
        if is_trading_day(d):
            days.append(d)
        d -= timedelta(days=1)
    return sorted(days)


def _expected_trading_days(start_d: date, end_d: date) -> list:
    days = []
    d = start_d
    while d <= end_d:
        if is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def _consecutive_ranges(days: list) -> list:
    """Group a sorted list of dates into (start, end) ranges of *consecutive
    trading days* — used so a run of missing days becomes one IB request."""
    if not days:
        return []
    days = sorted(days)
    ranges = []
    start = prev = days[0]
    for d in days[1:]:
        if d == _next_trading_day(prev):
            prev = d
        else:
            ranges.append((start, prev))
            start = prev = d
    ranges.append((start, prev))
    return ranges


# ─────────────────────────────────────────────────────────────────────────────
# Rolling-window rate limiter
# ─────────────────────────────────────────────────────────────────────────────
class RateLimiter:
    """
    Blocks until there is capacity to send a request without exceeding
    MAX_REQ_PER_10_MIN in any rolling 10-minute window.
    """

    def __init__(self, max_req: int = MAX_REQ_PER_10_MIN,
                 window: int = WINDOW_SECONDS):
        self.max_req = max_req
        self.window  = window
        self._ts: deque = deque()   # monotonic timestamps of past requests

    def acquire(self):
        now = time.monotonic()

        # Evict entries that have rolled out of the window
        while self._ts and self._ts[0] < now - self.window:
            self._ts.popleft()

        if len(self._ts) >= self.max_req:
            # Sleep until the oldest entry exits the window
            sleep_for = self._ts[0] + self.window - now + 1.0   # +1 s buffer
            print(f"  [rate-limit] {len(self._ts)}/{self.max_req} requests in "
                  f"last 10 min — sleeping {sleep_for:.1f} s …")
            time.sleep(sleep_for)

            # Re-evict after sleeping
            now = time.monotonic()
            while self._ts and self._ts[0] < now - self.window:
                self._ts.popleft()

        self._ts.append(time.monotonic())


# ─────────────────────────────────────────────────────────────────────────────
# Generic helpers
# ─────────────────────────────────────────────────────────────────────────────
def csv_path(symbol: str) -> str:
    return os.path.join(DATA_DIR, f'{symbol}.csv')


def _naive(dt: datetime) -> datetime:
    """Strip timezone info so datetime comparisons don't raise."""
    return dt.replace(tzinfo=None) if getattr(dt, 'tzinfo', None) else dt


def _dedupe_rows(rows: list) -> list:
    """
    Remove rows with duplicate timestamps (keep the last one) and sort
    chronologically. Useful when daily and minute chunks overlap slightly.
    """
    seen: dict = {}
    for r in rows:
        seen[r[0]] = r
    return [seen[k] for k in sorted(seen)]


def _read_csv_rows(fpath: str) -> list:
    if not os.path.isfile(fpath):
        return []
    with open(fpath, newline='') as f:
        r = csv.reader(f)
        next(r, None)   # header
        return [row for row in r if row]


def _write_csv_rows(fpath: str, rows: list):
    os.makedirs(os.path.dirname(fpath) or '.', exist_ok=True)
    rows = _dedupe_rows(rows)
    with open(fpath, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['date', 'open', 'high', 'low', 'close', 'price', 'volume'])
        for r in rows:
            w.writerow(r)


def _row_date(row) -> date:
    return datetime.strptime(row[0][:8], '%Y%m%d').date()


def _row_is_daily(row) -> bool:
    """A row is 'daily resolution' if its timestamp has no intraday
    component (HHMM == '0000'). Real intraday bars never land exactly on
    midnight since the market isn't open then, so this is a safe marker."""
    return row[0][8:] == '0000'


def _minute_resolution_days(rows: list) -> set:
    """Trading days that currently have at least one minute-resolution row."""
    return {_row_date(r) for r in rows if not _row_is_daily(r)}


def _aggregate_minute_rows_to_daily(rows_for_day: list) -> list:
    """Collapse a sorted list of minute rows for one trading day into a
    single daily bar: open = first minute's open, close = last minute's
    close, high/low = max/min across the day, volume = sum."""
    opens  = [float(r[1]) for r in rows_for_day]
    highs  = [float(r[2]) for r in rows_for_day]
    lows   = [float(r[3]) for r in rows_for_day]
    closes = [float(r[5]) for r in rows_for_day]
    vols   = [float(r[6]) for r in rows_for_day]
    d = _row_date(rows_for_day[0])
    day_ts = d.strftime('%Y%m%d') + '0000'
    o, h, l, c, v = opens[0], max(highs), min(lows), closes[-1], sum(vols)
    return [day_ts, o, h, l, c, c, v]


def _check_alignment(rows: list) -> list:
    """Cheap local sanity check (no network calls): for every day in *rows*,
    confirm open & close fall within that day's [low, high] range. Returns a
    list of human-readable issues (empty list == looks fine)."""
    issues = []
    by_day: dict = {}
    for r in rows:
        by_day.setdefault(_row_date(r), []).append(r)

    for d, day_rows in by_day.items():
        minute_rows = sorted([r for r in day_rows if not _row_is_daily(r)],
                              key=lambda r: r[0])
        if minute_rows:
            o = float(minute_rows[0][1])
            c = float(minute_rows[-1][5])
            h = max(float(r[2]) for r in minute_rows)
            l = min(float(r[3]) for r in minute_rows)
        else:
            daily_rows = [r for r in day_rows if _row_is_daily(r)]
            if not daily_rows:
                continue
            r = daily_rows[-1]
            o, h, l, c = float(r[1]), float(r[2]), float(r[3]), float(r[5])

        if not (l <= o <= h and l <= c <= h):
            issues.append(f"{d}: open/close fall outside [low, high]")
    return issues


# ─────────────────────────────────────────────────────────────────────────────
# Core IB fetch wrapper — rate-limited + pacing-violation retry
# ─────────────────────────────────────────────────────────────────────────────
def fetch_bars(ib: IB, limiter: RateLimiter, contract,
               end_dt: datetime, duration: str, bar_size: str,
               what: str = 'TRADES', use_rth: bool = True) -> list:
    """
    Wraps ib.reqHistoricalData with:
      1. Rolling-window rate limiting (blocks before the request if needed).
      2. Subscription to ib.errorEvent to detect code-162 pacing violations.
      3. Exponential back-off retry (PACING_BACKOFF_BASE * attempt seconds).

    Caller is responsible for sleeping SAME_CONTRACT_GAP seconds *between*
    successive calls for the same contract (enforces the "6-in-2-s" rule).
    """
    pacing_flag: list = []   # non-empty → pacing violation was signalled

    def _on_error(reqId, code, msg, contract_):
        if code == 162:
            pacing_flag.append(True)
            print(f"  [IB 162] pacing violation: {msg}")
        elif code not in {2104, 2106, 2158, 2119, 10182}:
            # 2104/2106/2158/2119 are normal connectivity info messages.
            # 10182 fires on non-trading days ("no data").
            print(f"  [IB {code}] {msg}")

    ib.errorEvent += _on_error
    try:
        for attempt in range(1, MAX_RETRIES + 1):
            pacing_flag.clear()
            limiter.acquire()          # block here if near the 60-req cap

            bars = ib.reqHistoricalData(
                contract,
                endDateTime=end_dt,
                durationStr=duration,
                barSizeSetting=bar_size,
                whatToShow=what,
                useRTH=use_rth,
                formatDate=1,
                timeout=60,            # don't hang forever on a stalled request
            )

            if pacing_flag:
                backoff = PACING_BACKOFF_BASE * attempt
                print(f"  [retry {attempt}/{MAX_RETRIES}] "
                      f"pacing back-off {backoff} s …")
                time.sleep(backoff)
                continue               # retry

            return bars                # success

        print(f"  [ERROR] {MAX_RETRIES} retries exhausted for "
              f"duration={duration} barSize={bar_size} end={end_dt} "
              "— returning empty list")
        return []

    finally:
        ib.errorEvent -= _on_error    # always unsubscribe the handler


# ─────────────────────────────────────────────────────────────────────────────
# Self-contained daily / minute fetch helpers (each opens its own IB session)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_daily_bars(symbol: str, start_d: date, end_d: date) -> list:
    """Fetch 1-day bars for [start_d, end_d] inclusive. Chunked into ≤1-year
    slices per the IB step-size table ('1 Y' duration <-> '1 day' bar size)."""
    if start_d > end_d:
        return []

    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)
    contract = Stock(symbol, 'SMART', 'USD')
    limiter  = RateLimiter()
    rows: list = []

    try:
        end_dt   = datetime.combine(end_d + timedelta(days=1), datetime.min.time())
        start_dt = datetime.combine(start_d, datetime.min.time())

        chunks: list = []
        chunk_end = end_dt
        while chunk_end > start_dt:
            chunk_start = max(start_dt, chunk_end - timedelta(days=365))
            chunks.append((chunk_start, chunk_end))
            chunk_end = chunk_start
        chunks.reverse()   # oldest first

        total = len(chunks)
        for i, (cs, ce) in enumerate(chunks, start=1):
            days = (ce - cs).days
            dur  = '1 Y' if days >= 365 else f'{max(days, 1)} D'
            print(f"  [daily {i}/{total}] {cs.date()} → {ce.date()} ({dur})")

            bars = fetch_bars(ib, limiter, contract,
                               end_dt=ce, duration=dur, bar_size='1 day')

            for b in bars:
                d = (b.date if isinstance(b.date, datetime)
                     else datetime.combine(b.date, datetime.min.time()))
                rows.append([d.strftime('%Y%m%d%H%M'),
                             b.open, b.high, b.low, b.close, b.close, b.volume])

            print(f"    → {len(bars)} bars")
            if i < total:
                time.sleep(SAME_CONTRACT_GAP)
    finally:
        ib.disconnect()

    return rows


def fetch_minute_bars(symbol: str, start_d: date, end_d: date) -> list:
    """Fetch 1-minute bars for every *trading* day in [start_d, end_d]. IB's
    step-size table only allows '1 D' duration <-> '1 min' bar size, so this
    issues one request per trading day."""
    if start_d > end_d:
        return []

    ib = IB()
    ib.connect(IB_HOST, IB_PORT, clientId=IB_CLIENT_ID, timeout=10)
    contract = Stock(symbol, 'SMART', 'USD')
    limiter  = RateLimiter()
    rows: list = []

    try:
        day_cursor = start_d
        is_first   = True
        while day_cursor <= end_d:
            if not is_trading_day(day_cursor):
                day_cursor += timedelta(days=1)
                continue

            end_of_day = datetime.combine(day_cursor + timedelta(days=1),
                                           datetime.min.time())
            if not is_first:
                time.sleep(SAME_CONTRACT_GAP)
            is_first = False

            print(f"  [1-min] {day_cursor}")
            bars = fetch_bars(ib, limiter, contract,
                              end_dt=end_of_day, duration='1 D',
                              bar_size='1 min')

            for b in bars:
                bd = _naive(b.date)
                rows.append([bd.strftime('%Y%m%d%H%M'),
                             b.open, b.high, b.low, b.close, b.close, b.volume])

            print(f"    → {len(bars)} bars")
            day_cursor += timedelta(days=1)
    finally:
        ib.disconnect()

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# CLI-triggered historical extraction
# ─────────────────────────────────────────────────────────────────────────────
def get_historical_data(symbol: str, start_date: str) -> str:
    """
    Download historical data for *symbol* starting from *start_date*
    ('YYYYMMDD') and write it to data/<symbol>.csv. The resulting CSV always
    starts exactly at *start_date* (older rows are dropped).

    Behaviour depends on what's already on disk:

      • CSV already exists AND already holds minute-resolution rows for one
        or more of the latest MINUTE_DAYS trading days
            → those minute rows are left untouched; only DAILY bars are
              fetched to cover [start_date, day before the earliest
              preserved minute day].

      • CSV doesn't exist yet, or has no minute-resolution rows in that
        window
            → DAILY bars are fetched for the *entire* range
              [start_date, latest trading day]. No minute fetch happens
              here — minute-level data is populated separately by live mode.
    """
    start_d = datetime.strptime(start_date, '%Y%m%d').date()
    fpath = csv_path(symbol)

    today = now_et().date()
    latest_trading_day = today if is_trading_day(today) else _previous_trading_day(today)
    minute_window = set(_last_n_trading_days(latest_trading_day, MINUTE_DAYS))

    existing_rows = _read_csv_rows(fpath)
    preserved_minute_days = {
        d for d in _minute_resolution_days(existing_rows) if d in minute_window
    }

    if preserved_minute_days:
        daily_end = min(preserved_minute_days) - timedelta(days=1)
        print(f"[historical-fetch] {symbol}: preserving existing minute data "
              f"for {sorted(preserved_minute_days)}; fetching daily bars "
              f"{start_d} → {daily_end}")
        preserved_rows = [r for r in existing_rows
                           if _row_date(r) in preserved_minute_days]
    else:
        daily_end = latest_trading_day
        preserved_rows = []
        print(f"[historical-fetch] {symbol}: no recent minute data to "
              f"preserve; fetching daily bars {start_d} → {daily_end}")

    daily_rows = fetch_daily_bars(symbol, start_d, daily_end) if daily_end >= start_d else []

    # Keep any pre-existing daily rows in range as a fallback in case a
    # particular day's fetch failed; freshly fetched rows win on dedupe.
    merged = [r for r in existing_rows
              if start_d <= _row_date(r) <= daily_end and _row_is_daily(r)]
    merged += daily_rows
    merged += preserved_rows
    merged = [r for r in merged if _row_date(r) >= start_d]

    _write_csv_rows(fpath, merged)
    print(f"Saved {fpath}  ({len(_dedupe_rows(merged))} rows)")
    return fpath


# ─────────────────────────────────────────────────────────────────────────────
# Live polling via yfinance
# ─────────────────────────────────────────────────────────────────────────────
def append_live_rows(symbol: str):
    fpath       = csv_path(symbol)
    file_exists = os.path.isfile(fpath)

    last_date_written = None
    if file_exists:
        with open(fpath, 'r', newline='') as f:
            lines = f.readlines()
            if len(lines) > 1:
                last_date_written = lines[-1].split(',')[0]

    ticker = yf.Ticker(symbol)
    bars   = ticker.history(period='7d', interval='1m')
    if bars.empty:
        return

    new_rows = []
    for idx, row in bars.iterrows():
        date_str = idx.strftime('%Y%m%d%H%M')
        if last_date_written is not None and date_str <= last_date_written:
            continue
        new_rows.append([
            date_str,
            row['Open'], row['High'], row['Low'], row['Close'],
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


def ensure_live_data_fresh(symbol: str):
    """
    Run once, right as we transition into live mode (market just opened).
    Confirms the CSV holds minute-resolution rows for each of the latest
    MINUTE_DAYS trading days. If any of those days are missing/stale (e.g.
    collapsed into a daily row by historical mode, or never populated),
    drop whatever rows exist for those days and re-pull live data — yfinance
    returns up to 7 days of 1-minute history per call, comfortably covering
    the MINUTE_DAYS window.
    """
    fpath = csv_path(symbol)
    rows = _read_csv_rows(fpath)
    if not rows:
        return   # nothing to check yet — bootstrap/first live poll will populate it

    today = now_et().date()
    target_days = set(_last_n_trading_days(today, MINUTE_DAYS))
    have_minute_days = {d for d in _minute_resolution_days(rows) if d in target_days}

    if have_minute_days == target_days:
        print(f"[live-check] {symbol}: last {MINUTE_DAYS} trading days "
              "already at minute resolution")
        return

    missing = target_days - have_minute_days
    print(f"[live-check] {symbol}: minute data missing for {sorted(missing)} "
          f"— dropping stale rows for those days and re-pulling live data")

    earliest_missing = min(missing)
    rows = [r for r in rows if _row_date(r) < earliest_missing]
    _write_csv_rows(fpath, rows)

    append_live_rows(symbol)   # backfills from earliest_missing forward


def run_live_mode(symbol: str):
    print(f"[live] {symbol}: market open — polling every {POLL_SECONDS} s")
    while is_market_open():
        append_live_rows(symbol)
        time.sleep(POLL_SECONDS)
    print(f"[live] {symbol}: market closed")


# ─────────────────────────────────────────────────────────────────────────────
# Historical mode — scheduled reconciliation after market close
# ─────────────────────────────────────────────────────────────────────────────
def run_historical_mode(symbol: str):
    """
    Runs HISTORICAL_DELAY_HOURS after the close. Two jobs:

      1. Locally (no network calls) collapse minute-resolution data for any
         trading day that has rolled OUT of the latest-MINUTE_DAYS window
         into a single daily bar (sum volume, max high, min low, first
         minute's open, last minute's close).

      2. Locally check the CSV for missing trading days and for basic
         open/close-within-[low,high] alignment. If everything looks
         complete and aligned, log it and do nothing further. Otherwise,
         fetch ONLY the missing day-ranges via the daily-bar IB path and
         merge them in.
    """
    fpath = csv_path(symbol)
    if not os.path.isfile(fpath):
        print(f"[historical] {symbol}: no CSV yet — bootstrapping")
        bootstrap_start = (datetime.now() - timedelta(days=BOOTSTRAP_DAYS)).strftime('%Y%m%d')
        get_historical_data(symbol, bootstrap_start)
        return

    rows = _read_csv_rows(fpath)
    if not rows:
        return

    lc = last_close(now_et())
    keep_minute_days = set(_last_n_trading_days(lc.date(), MINUTE_DAYS))

    # ── Step 1: collapse minute rows that rolled out of the window ─────────
    by_day: dict = {}
    for r in rows:
        by_day.setdefault(_row_date(r), []).append(r)

    new_rows = []
    collapsed_days = []
    for d, day_rows in by_day.items():
        if d in keep_minute_days:
            new_rows.extend(day_rows)
            continue
        minute_rows = sorted([r for r in day_rows if not _row_is_daily(r)],
                              key=lambda r: r[0])
        if len(minute_rows) > 1:
            new_rows.append(_aggregate_minute_rows_to_daily(minute_rows))
            collapsed_days.append(d)
        else:
            new_rows.extend(day_rows)   # already daily (or a single stray row)

    if collapsed_days:
        print(f"[historical] {symbol}: collapsed minute data into daily bars "
              f"for {len(collapsed_days)} day(s): {sorted(collapsed_days)}")
        _write_csv_rows(fpath, new_rows)
        rows = new_rows
        by_day = {}
        for r in rows:
            by_day.setdefault(_row_date(r), []).append(r)

    # ── Step 2: local completeness / alignment check — no network calls ────
    present_days = sorted(by_day.keys())
    first_day, last_day = present_days[0], present_days[-1]
    expected_days = _expected_trading_days(first_day, last_day)
    missing_days  = [d for d in expected_days if d not in by_day]
    misaligned    = _check_alignment(rows)

    if not missing_days and not misaligned:
        print(f"[historical] {symbol}: data looks complete and aligned "
              f"({first_day} → {last_day}) — nothing to do")
        return

    if missing_days:
        print(f"[historical] {symbol}: missing trading day(s): {missing_days}")
    if misaligned:
        print(f"[historical] {symbol}: alignment check flagged: {misaligned}")

    # ── Step 3: fetch only what's missing ───────────────────────────────────
    for gap_start, gap_end in _consecutive_ranges(missing_days):
        print(f"[historical] {symbol}: fetching daily bars {gap_start} → {gap_end}")
        new_daily = fetch_daily_bars(symbol, gap_start, gap_end)
        existing = _read_csv_rows(fpath)
        _write_csv_rows(fpath, existing + new_daily)


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestration loop — alternates live mode / historical mode forever
# ─────────────────────────────────────────────────────────────────────────────
def main_loop(symbol: str):
    fpath = csv_path(symbol)
    if not os.path.isfile(fpath):
        bootstrap_start = (datetime.now() - timedelta(days=BOOTSTRAP_DAYS)).strftime('%Y%m%d')
        get_historical_data(symbol, bootstrap_start)

    historical_done_for = None   # trading-day date already reconciled today
    print(f"Starting {symbol} live/historical loop (Ctrl+C to stop)")

    while True:
        now = now_et()

        if is_market_open(now):
            ensure_live_data_fresh(symbol)
            run_live_mode(symbol)        # blocks until market closes
            continue

        lc = last_close(now)
        trigger_at = lc + timedelta(hours=HISTORICAL_DELAY_HOURS)

        if now >= trigger_at and historical_done_for != lc.date():
            run_historical_mode(symbol)
            historical_done_for = lc.date()
            continue

        nxt_open = next_open(now)
        wake_at = nxt_open if now >= trigger_at else min(trigger_at, nxt_open)
        sleep_s = max(1.0, min((wake_at - now).total_seconds(), IDLE_POLL_SECONDS))
        time.sleep(sleep_s)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python3 extract_data.py <SYMBOL> [yyyymmdd]')
        sys.exit(1)

    symbol_arg = sys.argv[1].upper()

    if len(sys.argv) > 2:
        get_historical_data(symbol_arg, sys.argv[2])
    else:
        main_loop(symbol_arg)