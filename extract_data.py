#!/usr/bin/env python3
"""
Usage:
    python3 extract_data.py                        # live mode
    python3 extract_data.py <YYYYMMDD>             # historical mode, all symbols
    python3 extract_data.py <SYMBOL>               # live mode, one symbol
    python3 extract_data.py <SYMBOL> <YYYYMMDD>    # historical mode, one symbol
"""

import sys, os, csv, math, time, threading, subprocess
from datetime import datetime, timedelta, timezone
from collections import deque

DATA_DIR           = 'data'
POLL_SECONDS       = 30
MINUTE_DAYS        = 3
MIN_RECONCILE_DAYS = 500
WATCH_ACTIVE_FILE  = 'watch_active.txt'
WATCH_PASSIVE_FILE = 'watch_passive.txt'
IB_HOST            = "127.0.0.1"
IB_PORT            = 7496
CLIENT_ID          = int(os.environ.get("IB_CLIENT_ID", "11"))
_IB_SESSION_LOCK   = threading.Lock()
INTER_REQUEST_SLEEP    = 10
MAX_REQUESTS_PER_10M   = 55
REQUEST_WINDOW_SECS    = 600
MINUTE_CHUNK_DURATION  = "1 D"
MINUTE_CHUNK_STEP_DAYS = 1
DAILY_CHUNK_DURATION   = "1 Y"
WHAT_TO_SHOW           = "TRADES"
USE_RTH                = 0
YF_MAX_MINUTE_DAYS     = 7
FIELDNAMES             = ["datetime", "open", "close", "high", "low", "volume"]
_CALENDAR              = None


def _get_calendar():
    global _CALENDAR
    if _CALENDAR is None:
        try:
            import pandas_market_calendars as mcal
        except ImportError:
            sys.exit("ERROR: pip install pandas_market_calendars --break-system-packages")
        _CALENDAR = mcal.get_calendar("NYSE")
    return _CALENDAR


# ── CSV paths / predicates ────────────────────────────────────────────────────

def minute_csv_path(symbol): return os.path.join(DATA_DIR, f"{symbol}_minute.csv")
def daily_csv_path(symbol):  return os.path.join(DATA_DIR, f"{symbol}_daily.csv")
def is_minute_row(dt_str):   return len(dt_str) == 12
def row_day(dt_str):         return dt_str[:8]


# ── CSV I/O ───────────────────────────────────────────────────────────────────

def _load_csv(path):
    if not os.path.exists(path):
        return {}
    with open(path, newline="") as f:
        return {r["datetime"]: {k: (float(r[k]) if k != "datetime" else r[k])
                                for k in FIELDNAMES}
                for r in csv.DictReader(f)}

def load_minute_csv(symbol): return _load_csv(minute_csv_path(symbol))
def load_daily_csv(symbol):  return _load_csv(daily_csv_path(symbol))


def _write_csv(path, rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        w.writeheader()
        w.writerows(sorted(rows.values(), key=lambda r: r["datetime"]))


def save_minute_csv(symbol, rows):
    if not rows:
        _write_csv(minute_csv_path(symbol), rows)
        return
    all_days  = sorted({row_day(k) for k in rows if is_minute_row(k)})
    keep_days = set(all_days[-MINUTE_DAYS:])
    _write_csv(minute_csv_path(symbol), {k: v for k, v in rows.items() if row_day(k) in keep_days})


def save_daily_csv(symbol, rows, run_ma=False, ma_start_date=None):
    _write_csv(daily_csv_path(symbol), rows)
    if run_ma:
        cmd = [sys.executable, "ma.py", symbol]
        if ma_start_date:
            cmd.append(ma_start_date)
        try:
            subprocess.run(cmd, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(f"  [ma] warning: {e}")


def _run_collapse(symbol, force_today=False):
    cmd = [sys.executable, "collapse_to_daily.py", symbol]
    if force_today:
        cmd.append("--force-today")
    print(f"  [collapse] {symbol}")
    r = subprocess.run(cmd)
    if r.returncode != 0:
        print(f"  [collapse] warning: exited {r.returncode} for {symbol}")


# ── Bar helpers ───────────────────────────────────────────────────────────────

def minute_rows_for_day(rows, day):
    return sorted((r for k, r in rows.items() if is_minute_row(k) and row_day(k) == day),
                  key=lambda r: r["datetime"])


def collapse_minutes_to_daily(bars):
    day = row_day(bars[0]["datetime"])
    return {"datetime": day, "open": bars[0]["open"], "close": bars[-1]["close"],
            "high": max(b["high"] for b in bars), "low": min(b["low"] for b in bars),
            "volume": sum(b["volume"] for b in bars)}


def is_valid_bar(bar):
    for f in ("open", "close", "high", "low"):
        v = bar.get(f)
        if v is None or not isinstance(v, float) or math.isnan(v) or math.isinf(v) or v <= 0:
            return False
    vol = bar.get("volume")
    if vol is None or (isinstance(vol, float) and (math.isnan(vol) or vol < 0)):
        return False
    if bar.get("volume", 0) == 0 and bar["open"] == bar["high"] == bar["low"] == bar["close"]:
        return False  # phantom bar
    return True


# ── Calendar helpers ──────────────────────────────────────────────────────────

def next_trading_date_after(day_str):
    cal   = _get_calendar()
    start = datetime.strptime(day_str, "%Y%m%d").date() + timedelta(days=1)
    sched = cal.schedule(start_date=start, end_date=start + timedelta(days=14))
    return sched.index[0].strftime("%Y%m%d") if not sched.empty else None


def trading_days_between(start_date, end_date):
    cal   = _get_calendar()
    sched = cal.schedule(start_date=start_date, end_date=end_date)
    return [d.strftime("%Y%m%d") for d in sched.index]


def last_n_trading_days(n, as_of=None):
    if as_of is None:
        as_of = datetime.now(timezone.utc).date()
    cal   = _get_calendar()
    sched = cal.schedule(start_date=as_of - timedelta(days=int(n * 2.5) + 10), end_date=as_of)
    return [d.strftime("%Y%m%d") for d in sched.index][-n:]


def last_completed_trading_day(as_of_utc=None):
    if as_of_utc is None:
        as_of_utc = datetime.now(timezone.utc)
    cal   = _get_calendar()
    sched = cal.schedule(start_date=as_of_utc.date() - timedelta(days=14), end_date=as_of_utc.date())
    last  = None
    for idx, row in sched.iterrows():
        if row["market_close"].tz_convert("UTC").to_pydatetime() <= as_of_utc:
            last = idx.strftime("%Y%m%d")
    return last


def _calendar_session_for_today(now_utc):
    """Return (midnight_utc, market_close_utc) for today if it's a trading day, else (None, None).
    Pre-market starts at 00:00 UTC."""
    cal = _get_calendar()
    try:
        sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    except Exception:
        return None, None
    if sched.empty:
        return None, None
    market_close = sched.iloc[0]["market_close"].tz_convert("UTC").to_pydatetime()
    midnight     = market_close.replace(hour=0, minute=0, second=0, microsecond=0)
    return midnight, market_close


def next_session_open_from_calendar(now_utc):
    """Return midnight UTC of the next trading day (pre-market open)."""
    cal   = _get_calendar()
    sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date() + timedelta(days=10))
    for _, row in sched.iterrows():
        midnight = row["market_open"].tz_convert("UTC").to_pydatetime().replace(
            hour=0, minute=0, second=0, microsecond=0)
        if midnight > now_utc:
            return midnight
    return None


# ── Session open/close inference ──────────────────────────────────────────────

def _infer_session_open_hhmm(rows):
    recent = sorted({row_day(k) for k in rows if is_minute_row(k)})[-MINUTE_DAYS:]
    if len(recent) < MINUTE_DAYS:
        return None
    starts = []
    for day in recent:
        bars = minute_rows_for_day(rows, day)
        if not bars:
            return None
        starts.append(bars[0]["datetime"][8:])
    return starts[0] if len(set(starts)) == 1 else None


def _infer_next_open_from_rows(minute_rows):
    hhmm = _infer_session_open_hhmm(minute_rows)
    if hhmm is None:
        return None
    recent   = sorted({row_day(k) for k in minute_rows if is_minute_row(k)})[-MINUTE_DAYS:]
    next_day = next_trading_date_after(recent[-1])
    if next_day is None:
        return None
    return datetime.strptime(next_day + hhmm, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)


def is_market_open_now(minute_rows_or_none, now_utc):
    market_open, market_close = _calendar_session_for_today(now_utc)
    if market_open is None:
        return False
    if minute_rows_or_none is None:
        return market_open <= now_utc <= market_close
    today_str   = now_utc.date().strftime("%Y%m%d")
    session_end = datetime(now_utc.year, now_utc.month, now_utc.day, 23, 59, 59, tzinfo=timezone.utc)
    hhmm = _infer_session_open_hhmm(minute_rows_or_none)
    if hhmm is not None:
        market_open = datetime.strptime(today_str + hhmm, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    return market_open <= now_utc <= session_end


def infer_next_session_open(minute_rows_or_none, now_utc):
    if minute_rows_or_none is None:
        return next_session_open_from_calendar(now_utc)
    return _infer_next_open_from_rows(minute_rows_or_none) or next_session_open_from_calendar(now_utc)


# ── IB client ─────────────────────────────────────────────────────────────────

def _ib_imports():
    try:
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
        from ibapi.contract import Contract
        return EClient, EWrapper, Contract
    except ImportError:
        sys.exit("ERROR: pip install ibapi --break-system-packages")


def make_ib_app():
    EClient, EWrapper, Contract = _ib_imports()

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
            try:
                ts = datetime.fromtimestamp(int(raw), tz=timezone.utc)
                fmt_dt = ts.strftime("%Y%m%d" if is_daily else "%Y%m%d%H%M")
            except ValueError:
                raw_clean = raw.replace(" ", "")
                fmt_dt = raw_clean[:8] if is_daily else datetime.strptime(
                    raw_clean, "%Y%m%d%H%M%S").strftime("%Y%m%d%H%M")
            self._bars.append({"datetime": fmt_dt, "open": bar.open, "close": bar.close,
                                "high": bar.high, "low": bar.low, "volume": bar.volume})

        def historicalDataEnd(self, reqId, start, end):
            self._done_event.set()

        def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
            if errorCode in (2104, 2106, 2107, 2108, 2119, 2150, 2158):
                return
            if errorCode in (162, 321, 1100, 1101, 1102, 2110):
                print(f"  [warn] {errorCode}: {errorString}")
                self._done_event.set()
            else:
                print(f"  [ERROR] {errorCode}: {errorString}")
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
                print(f"  [pace] window full, sleeping {wait:.0f}s")
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
            self._bars, self._bar_size = [], bar_size
            self._done_event.clear()
            self._error_flag = False
            self._pace()
            end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")
            print(f"  [ib] {bar_size} end={end_str} ...", end="", flush=True)
            self.reqHistoricalData(self._req_id, contract, end_str, duration,
                                   bar_size, WHAT_TO_SHOW, USE_RTH, 2, 0, [])
            self._req_id += 1
            if not self._done_event.wait(timeout=180):
                print(" TIMEOUT")
                self.cancelHistoricalData(self._req_id - 1)
                return []
            print(f" {len(self._bars)} bars")
            return list(self._bars)

    return IBExtractor()


def make_contract(symbol):
    _, _, Contract = _ib_imports()
    c = Contract()
    c.symbol, c.secType, c.exchange, c.currency = symbol.upper(), "STK", "SMART", "USD"
    return c


class IBSession:
    def __init__(self):
        self.app = make_ib_app()

    def __enter__(self):
        self.app.connect(IB_HOST, IB_PORT, CLIENT_ID)
        threading.Thread(target=self.app.run, daemon=True).start()
        time.sleep(2)
        if not self.app.isConnected():
            sys.exit("ERROR: could not connect to IB TWS/Gateway.")
        return self.app

    def __exit__(self, *_):
        self.app.disconnect()


def ib_fetch_minute_range(app, contract, start_dt, end_dt):
    result = {}
    chunk_end = end_dt
    while chunk_end > start_dt:
        bars = app.fetch(contract, chunk_end, MINUTE_CHUNK_DURATION, "1 min")
        if app._error_flag:
            break
        for b in bars:
            bdt = datetime.strptime(b["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            if bdt >= start_dt:
                result[b["datetime"]] = b
        chunk_end -= timedelta(days=MINUTE_CHUNK_STEP_DAYS)
    return result


def ib_fetch_daily_range(app, contract, start_dt, end_dt):
    start_str = start_dt.strftime("%Y%m%d")
    end_str   = (end_dt - timedelta(days=1)).strftime("%Y%m%d")
    result    = {}
    chunk_end = end_dt
    while chunk_end > start_dt:
        bars = app.fetch(contract, chunk_end, DAILY_CHUNK_DURATION, "1 day")
        if app._error_flag:
            break
        for b in bars:
            if len(b["datetime"]) == 8 and start_str <= b["datetime"] <= end_str:
                result[b["datetime"]] = b
        chunk_end -= timedelta(days=366)
    return result


# ── yfinance ──────────────────────────────────────────────────────────────────

def _yf_import():
    try:
        import yfinance as yf
        return yf
    except ImportError:
        sys.exit("ERROR: pip install yfinance --break-system-packages")


def _parse_yf_df(df, symbols):
    result = {sym: {} for sym in symbols}
    if df.empty:
        return result
    multi = getattr(df.columns, "nlevels", 1) > 1
    for sym in symbols:
        sym_df = df[sym] if multi else df
        if sym_df is None or sym_df.empty:
            continue
        for idx, row in sym_df.iterrows():
            dt_utc = idx.tz_convert("UTC") if idx.tzinfo else idx.tz_localize("UTC")
            dt_str = dt_utc.strftime("%Y%m%d%H%M")
            try:
                bar = {"datetime": dt_str, "open": float(row["Open"]), "close": float(row["Close"]),
                       "high": float(row["High"]), "low": float(row["Low"]), "volume": float(row["Volume"])}
            except (ValueError, KeyError):
                continue
            if is_valid_bar(bar):
                result[sym][dt_str] = bar
    return result


def yf_batch_fetch_minute_bars(symbols):
    if not symbols:
        return {}
    yf = _yf_import()
    df = yf.download(tickers=symbols, period="1d", interval="1m", prepost=True,
                     group_by="ticker", auto_adjust=True, progress=False)
    return _parse_yf_df(df, symbols)


def yf_batch_fetch_minute_bars_window(symbols, period_days):
    if not symbols:
        return {}
    yf = _yf_import()
    df = yf.download(tickers=symbols, period=f"{min(period_days, YF_MAX_MINUTE_DAYS)}d",
                     interval="1m", prepost=True, group_by="ticker", auto_adjust=True, progress=False)
    return _parse_yf_df(df, symbols)


# ── Bar distribution ──────────────────────────────────────────────────────────

def apply_active_bars(symbol, today_bars, minute_rows):
    before = len(minute_rows)
    minute_rows.update(today_bars)
    added = len(minute_rows) - before
    if added > 0:
        save_minute_csv(symbol, minute_rows)
        print(f"  [active] {symbol}: +{added} bar(s) ({datetime.now(timezone.utc):%H:%M:%S} UTC)")
    return minute_rows


def apply_passive_bars(symbol, today_bars):
    if not today_bars:
        return
    today_str       = row_day(next(iter(today_bars)))
    old_minute_rows = load_minute_csv(symbol)
    if old_minute_rows:
        old_today  = {k: v for k, v in old_minute_rows.items()
                      if row_day(k) == today_str and is_minute_row(k)}
        today_bars = {**old_today, **today_bars}
        _write_csv(minute_csv_path(symbol), {})
    sorted_bars = sorted(today_bars.values(), key=lambda b: b["datetime"])
    inprogress  = collapse_minutes_to_daily(sorted_bars)
    daily_rows  = load_daily_csv(symbol)
    is_new      = today_str not in daily_rows
    daily_rows[today_str] = inprogress
    save_daily_csv(symbol, daily_rows, run_ma=False)
    if is_new:
        print(f"  [passive] {symbol}: new bar {today_str} close={inprogress['close']:.2f} "
              f"({datetime.now(timezone.utc):%H:%M:%S} UTC)")


# ── Reconciliation ────────────────────────────────────────────────────────────

def reconcile_check(symbol, minute_rows, daily_rows):
    days = sorted({row_day(k) for k in minute_rows if is_minute_row(k)})
    mismatches = 0
    for day in days:
        bars = minute_rows_for_day(minute_rows, day)
        if not bars or day not in daily_rows:
            continue
        computed = collapse_minutes_to_daily(bars)
        stored   = daily_rows[day]
        issues   = []
        if abs(stored["open"]   - computed["open"])   > 1e-6:
            issues.append(f"open {stored['open']} vs {computed['open']}")
        if abs(stored["close"]  - computed["close"])  > 1e-6:
            issues.append(f"close {stored['close']} vs {computed['close']}")
        if abs(stored["volume"] - computed["volume"]) > max(1.0, stored["volume"] * 0.001):
            issues.append(f"volume {stored['volume']} vs {computed['volume']}")
        if issues:
            mismatches += 1
            print(f"  [reconcile][!] {symbol} {day}: {'; '.join(issues)}")
    if mismatches == 0:
        print(f"  [reconcile] {symbol}: OK ({len(days)} day(s))")
    return mismatches


def yfinance_reconcile_active(symbol, minute_rows, fresh_batch=None):
    if fresh_batch is None:
        fresh_batch = yf_batch_fetch_minute_bars_window([symbol], MINUTE_DAYS + 2).get(symbol, {})
    fresh_days    = sorted({row_day(k) for k in fresh_batch})
    target_days   = set(fresh_days[-MINUTE_DAYS:]) if fresh_days else set()
    existing_days = {row_day(k) for k in minute_rows if is_minute_row(k)}
    if existing_days == target_days:
        return minute_rows
    print(f"  [reconcile] {symbol}: window mismatch — re-pulling "
          f"(have {sorted(existing_days)}, want {sorted(target_days)})")
    for k in list(minute_rows.keys()):
        if row_day(k) not in target_days:
            del minute_rows[k]
    new_rows = {k: v for k, v in fresh_batch.items() if row_day(k) in target_days}
    minute_rows.update(new_rows)
    print(f"  [reconcile] {symbol}: inserted {len(new_rows)} bars")
    return minute_rows


# ── Historical / gap-fill ─────────────────────────────────────────────────────

def _compute_gaps(symbol, is_passive, today_str, recent_days, start_date):
    daily_rows  = load_daily_csv(symbol)
    minute_rows = {} if is_passive else load_minute_csv(symbol)
    start_dt    = datetime.strptime(start_date, "%Y%m%d").replace(tzinfo=timezone.utc)
    all_td      = trading_days_between(start_dt.date(), today_str)

    if is_passive:
        if len(daily_rows) >= MIN_RECONCILE_DAYS:
            return [], []
        return [d for d in all_td if d not in daily_rows], []

    existing_daily  = set(daily_rows.keys())
    existing_minute = {row_day(k) for k in minute_rows if is_minute_row(k)}
    will_collapse   = existing_minute - recent_days
    missing_daily   = [d for d in all_td
                       if d not in recent_days and d not in existing_daily and d not in will_collapse]
    missing_minute  = [d for d in all_td if d in recent_days and d not in existing_minute]
    return missing_daily, missing_minute


def run_historical_mode_batch(symbol_modes, cli_start_date=None, today_str=None, recent_days=None):
    if not symbol_modes:
        return
    now_utc = datetime.now(timezone.utc)
    if today_str is None:
        today_str = last_completed_trading_day(now_utc)
    if today_str is None:
        print("[historical-batch] could not determine last trading day -- skipping")
        return
    if recent_days is None:
        recent_days = set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date()))

    end_dt         = datetime.strptime(today_str, "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    calendar_start = last_n_trading_days(MIN_RECONCILE_DAYS + 1, as_of=now_utc.date())[0]

    sym_info = {}
    for symbol, is_passive in symbol_modes:
        daily_rows  = load_daily_csv(symbol)
        minute_rows = {} if is_passive else load_minute_csv(symbol)
        all_existing = {**daily_rows, **minute_rows}
        start_date   = cli_start_date or (
            min(min(row_day(k) for k in all_existing), calendar_start)
            if all_existing else calendar_start)
        missing_daily, missing_minute = _compute_gaps(symbol, is_passive, today_str, recent_days, start_date)
        if not is_passive and (minute_rows or daily_rows):
            reconcile_check(symbol, minute_rows, daily_rows)
        sym_info[symbol] = {"is_passive": is_passive, "start_date": start_date,
                            "missing_daily": missing_daily, "missing_minute": missing_minute,
                            "daily_rows": daily_rows, "minute_rows": minute_rows}

    # yfinance reconcile for active symbols
    active_need = [s for s, i in sym_info.items() if not i["is_passive"] and i["missing_minute"]]
    if active_need:
        print(f"[historical-batch] yfinance reconcile: {active_need}")
        fresh = yf_batch_fetch_minute_bars_window(active_need, MINUTE_DAYS + 2)
        for symbol in active_need:
            info = sym_info[symbol]
            info["minute_rows"] = yfinance_reconcile_active(symbol, info["minute_rows"],
                                                             fresh.get(symbol, {}))
            save_minute_csv(symbol, info["minute_rows"])

    # IB backfill
    needs_ib = [(s, i) for s, i in sym_info.items() if i["missing_daily"] or i["missing_minute"]]
    if needs_ib:
        total_d = sum(len(i["missing_daily"])  for _, i in needs_ib)
        total_m = sum(len(i["missing_minute"]) for _, i in needs_ib)
        print(f"[historical-batch] IB backfill: {len(needs_ib)} symbol(s), "
              f"{total_d} daily, {total_m} minute day(s)")
        with _IB_SESSION_LOCK:
            with IBSession() as app:
                for symbol, info in needs_ib:
                    contract = make_contract(symbol)
                    if info["missing_minute"]:
                        lo_dt   = datetime.strptime(min(info["missing_minute"]), "%Y%m%d").replace(tzinfo=timezone.utc)
                        fetched = {k: v for k, v in ib_fetch_minute_range(app, contract, lo_dt, end_dt).items()
                                   if row_day(k) in set(info["missing_minute"])}
                        info["minute_rows"].update(fetched)
                        print(f"  [ib] {symbol}: +{len(fetched)} minute bars")
                    if info["missing_daily"]:
                        lo_dt   = datetime.strptime(min(info["missing_daily"]), "%Y%m%d").replace(tzinfo=timezone.utc)
                        fetched = {k: v for k, v in ib_fetch_daily_range(app, contract, lo_dt, end_dt).items()
                                   if k in set(info["missing_daily"])}
                        info["daily_rows"].update(fetched)
                        print(f"  [ib] {symbol}: +{len(fetched)} daily bars")
    else:
        print("[historical-batch] no IB backfill needed")

    # Save + post-process
    for symbol, info in sym_info.items():
        if not info["is_passive"]:
            save_minute_csv(symbol, info["minute_rows"])
            save_daily_csv(symbol, info["daily_rows"])
            _run_collapse(symbol)
        else:
            all_dates = sorted(info["daily_rows"].keys())
            # Only recompute MA from the latest date, not the full history
            latest_date = all_dates[-1] if all_dates else None
            save_daily_csv(symbol, info["daily_rows"], run_ma=True, ma_start_date=latest_date)


# ── Post-close worker ─────────────────────────────────────────────────────────

def _post_close_worker(states):
    if not states:
        return
    now_utc = datetime.now(timezone.utc)
    print(f"[post-close] reconciling {len(states)} symbol(s): {[s.symbol for s in states]}")

    active_states  = [s for s in states if not s.is_passive]
    passive_states = [s for s in states if s.is_passive]

    if active_states:
        syms  = [s.symbol for s in active_states]
        print(f"[post-close] yfinance minute reconcile: {syms}")
        fresh = yf_batch_fetch_minute_bars_window(syms, MINUTE_DAYS + 2)
        for st in active_states:
            st.minute_rows = yfinance_reconcile_active(
                st.symbol, st.minute_rows or load_minute_csv(st.symbol),
                fresh.get(st.symbol, {}))
            save_minute_csv(st.symbol, st.minute_rows)

    if passive_states:
        syms = [s.symbol for s in passive_states]
        print(f"[post-close] yfinance daily reconcile: {syms}")
        yf = _yf_import()
        df = yf.download(tickers=syms, period="5d", interval="1d",
                         group_by="ticker", auto_adjust=True, progress=False)
        multi = getattr(df.columns, "nlevels", 1) > 1
        for st in passive_states:
            sym_df = df[st.symbol] if multi else df
            if sym_df is None or sym_df.empty:
                print(f"  [yf] warning: no daily data for {st.symbol}")
                continue
            daily_rows = load_daily_csv(st.symbol)
            latest_new = None
            for idx, row in sym_df.iterrows():
                dt_utc  = idx.tz_convert("UTC") if idx.tzinfo else idx.tz_localize("UTC")
                day_str = dt_utc.strftime("%Y%m%d")
                bar = {"datetime": day_str, "open": float(row["Open"]), "close": float(row["Close"]),
                       "high": float(row["High"]), "low": float(row["Low"]), "volume": float(row["Volume"])}
                if is_valid_bar(bar):
                    daily_rows[day_str] = bar
                    latest_new = day_str
                else:
                    print(f"  [yf] {st.symbol}: skipping invalid bar {day_str}")
            # Only run MA from the latest new date
            save_daily_csv(st.symbol, daily_rows, run_ma=bool(latest_new), ma_start_date=latest_new)

    symbol_modes = [(s.symbol, False) for s in active_states] + [(s.symbol, True) for s in passive_states]
    run_historical_mode_batch(symbol_modes)

    for st in active_states:
        st.minute_rows = load_minute_csv(st.symbol)

    now_utc = datetime.now(timezone.utc)
    for st in states:
        st.next_open      = infer_next_session_open(st.minute_rows, now_utc)
        st.reconcile_done = True
        label = f"{st.next_open:%Y-%m-%d %H:%M} UTC" if st.next_open else "unknown"
        print(f"[post-close] {st.symbol}: next open {label}")


# ── Symbol state + poll coordinator ──────────────────────────────────────────

class SymbolState:
    __slots__ = ("symbol", "is_passive", "state", "reconcile_done", "minute_rows", "next_open")

    def __init__(self, symbol, is_passive, minute_rows, now_utc):
        self.symbol         = symbol
        self.is_passive     = is_passive
        self.minute_rows    = None if is_passive else minute_rows
        self.reconcile_done = False
        label               = "passive" if is_passive else "active"

        if is_market_open_now(self.minute_rows, now_utc):
            self.state     = "OPEN"
            self.next_open = None
            print(f"[{symbol}] OPEN ({label})")
        else:
            next_open = infer_next_session_open(self.minute_rows, now_utc)
            if next_open and now_utc < next_open:
                self.state     = "IDLE"
                self.next_open = next_open
                print(f"[{symbol}] IDLE until {next_open:%Y-%m-%d %H:%M} UTC ({label})")
            else:
                self.state     = "OPEN"
                self.next_open = None
                print(f"[{symbol}] OPEN (no future open inferred) ({label})")


class PollCoordinator:
    def __init__(self, active_symbols, passive_symbols):
        now_utc      = datetime.now(timezone.utc)
        self.states  = {}
        self._recon_thread = None
        for sym in active_symbols:
            self.states[sym] = SymbolState(sym, False, load_minute_csv(sym), now_utc)
        for sym in passive_symbols:
            self.states[sym] = SymbolState(sym, True, {}, now_utc)

    def run(self):
        while True:
            tick_start   = time.time()
            now_utc      = datetime.now(timezone.utc)
            open_active  = [s for s in self.states.values() if s.state == "OPEN" and not s.is_passive]
            open_passive = [s for s in self.states.values() if s.state == "OPEN" and s.is_passive]

            if open_active or open_passive:
                all_syms = [s.symbol for s in open_active + open_passive]
                print(f"[poll] fetching {len(all_syms)} symbol(s)")
                batch = yf_batch_fetch_minute_bars(all_syms)
                for st in open_active:
                    st.minute_rows = apply_active_bars(st.symbol, batch.get(st.symbol, {}), st.minute_rows)
                for st in open_passive:
                    apply_passive_bars(st.symbol, batch.get(st.symbol, {}))

            now_utc = datetime.now(timezone.utc)
            for st in list(self.states.values()):
                if st.state == "OPEN":
                    if not is_market_open_now(st.minute_rows, now_utc):
                        st.state          = "IDLE"
                        st.reconcile_done = False
                        st.next_open      = infer_next_session_open(st.minute_rows, now_utc)
                        label = "passive" if st.is_passive else "active"
                        next_str = f"{st.next_open:%Y-%m-%d %H:%M} UTC" if st.next_open else "unknown"
                        print(f"[{st.symbol}] OPEN→IDLE (next {next_str}, {label})")
                elif st.state == "IDLE":
                    if st.reconcile_done and st.next_open and now_utc >= st.next_open:
                        st.state          = "OPEN"
                        st.reconcile_done = False
                        print(f"[{st.symbol}] IDLE→OPEN ({'passive' if st.is_passive else 'active'})")

            idle_unrecon = [s for s in self.states.values() if s.state == "IDLE" and not s.reconcile_done]
            if idle_unrecon and (self._recon_thread is None or not self._recon_thread.is_alive()):
                for st in idle_unrecon:
                    st.reconcile_done = True
                self._recon_thread = threading.Thread(
                    target=_post_close_worker, args=(idle_unrecon,),
                    name="post-close-worker", daemon=True)
                self._recon_thread.start()

            elapsed = time.time() - tick_start
            if (remaining := POLL_SECONDS - elapsed) > 0:
                time.sleep(remaining)
            else:
                print(f"[poll] tick took {elapsed:.1f}s -- skipping sleep")


# ── Preflight ─────────────────────────────────────────────────────────────────

def has_sufficient_data(symbol, recent_trading_days, required_start, is_passive=False):
    daily_rows = load_daily_csv(symbol)
    if is_passive:
        if not daily_rows:
            print(f"  [preflight] {symbol} (passive): no daily CSV -- needs backfill")
            return False
        days   = sorted(daily_rows.keys())
        enough = len(days) >= MIN_RECONCILE_DAYS
        status = "OK" if enough else "NEEDS BACKFILL"
        detail = (f"{len(days)} bars [{days[0]}-{days[-1]}]" if enough else
                  f"{len(days)}/{MIN_RECONCILE_DAYS} bars, need {MIN_RECONCILE_DAYS - len(days)} more")
        print(f"  [preflight] {symbol} (passive): {status} | {detail}")
        return enough

    minute_rows = load_minute_csv(symbol)
    if not minute_rows and not daily_rows:
        print(f"  [preflight] {symbol} (active): no data -- needs backfill")
        return False
    daily_days  = sorted(daily_rows.keys())
    minute_days = sorted({row_day(k) for k in minute_rows if is_minute_row(k)})
    all_covered = sorted(set(daily_days) | set(minute_days))
    has_history = len(all_covered) >= MIN_RECONCILE_DAYS
    has_minutes = set(recent_trading_days).issubset(set(minute_days))
    missing_min = sorted(set(recent_trading_days) - set(minute_days))
    status  = "OK" if (has_history and has_minutes) else "NEEDS BACKFILL"
    hist_ok = f"{len(all_covered)} bars [{all_covered[0]}-{all_covered[-1]}] OK" if has_history else \
              f"{len(all_covered)}/{MIN_RECONCILE_DAYS} bars, need {MIN_RECONCILE_DAYS - len(all_covered)} more"
    min_ok  = f"minute [{', '.join(minute_days)}] OK" if has_minutes else \
              f"minute MISSING {missing_min}"
    print(f"  [preflight] {symbol} (active): {status} | {hist_ok} | {min_ok}")
    return has_history and has_minutes


def ensure_data_ready(active_symbols, passive_symbols):
    total = len(active_symbols) + len(passive_symbols)
    print(f"\n[preflight] checking {total} symbol(s) ({len(active_symbols)} active, {len(passive_symbols)} passive) ...")
    recent = sorted(last_n_trading_days(MINUTE_DAYS))
    req    = last_n_trading_days(MIN_RECONCILE_DAYS + 1)[0]
    needs_active  = [s for s in active_symbols  if not has_sufficient_data(s, recent, req, False)]
    needs_passive = [s for s in passive_symbols if not has_sufficient_data(s, recent, req, True)]
    if not needs_active and not needs_passive:
        print("[preflight] all symbols have sufficient data -- skipping backfill\n")
        return
    print(f"\n[preflight] backfill needed: {len(needs_active)} active, {len(needs_passive)} passive\n")
    now_utc = datetime.now(timezone.utc)
    run_historical_mode_batch(
        [(s, False) for s in needs_active] + [(s, True) for s in needs_passive],
        today_str=last_completed_trading_day(now_utc),
        recent_days=set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date())))
    print("\n[preflight] backfill complete\n")


# ── Live mode ─────────────────────────────────────────────────────────────────

def run_live_mode(active_symbols, passive_symbols):
    print(f"\n[live] starting: {len(active_symbols)} active, {len(passive_symbols)} passive")
    ensure_data_ready(active_symbols, passive_symbols)
    coordinator = PollCoordinator(active_symbols, passive_symbols)
    print(f"[live] coordinator started — polling every {POLL_SECONDS}s")
    try:
        coordinator.run()
    except KeyboardInterrupt:
        print("\n[live] interrupted -- shutting down")
        os._exit(0)


# ── Entry point ───────────────────────────────────────────────────────────────

def _load_watch_file(path):
    try:
        with open(path) as f:
            return [l.strip().upper() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        return []


def _is_date(s):
    try:
        datetime.strptime(s, "%Y%m%d")
        return True
    except ValueError:
        return False


def main():
    if len(sys.argv) not in (1, 2, 3):
        sys.exit("Usage: extract_data.py [SYMBOL] [YYYYMMDD]")
    os.makedirs(DATA_DIR, exist_ok=True)

    arg1 = sys.argv[1] if len(sys.argv) >= 2 else None
    arg2 = sys.argv[2] if len(sys.argv) == 3 else None

    if arg1 and not _is_date(arg1):
        symbol     = arg1.upper()
        start_date = arg2
        if start_date and not _is_date(start_date):
            sys.exit(f"ERROR: bad date '{start_date}', expected YYYYMMDD")
        passive_syms = _load_watch_file(WATCH_PASSIVE_FILE)
        is_passive   = symbol in passive_syms
        if start_date:
            now_utc = datetime.now(timezone.utc)
            run_historical_mode_batch([(symbol, is_passive)], cli_start_date=start_date,
                today_str=last_completed_trading_day(now_utc),
                recent_days=set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date())))
        else:
            run_live_mode([] if not is_passive else [], [symbol] if is_passive else [])
    else:
        start_date      = arg1
        active_symbols  = _load_watch_file(WATCH_ACTIVE_FILE)
        passive_symbols = [s for s in _load_watch_file(WATCH_PASSIVE_FILE) if s not in set(active_symbols)]
        if not active_symbols and not passive_symbols:
            sys.exit(f"ERROR: no symbols found in {WATCH_ACTIVE_FILE} / {WATCH_PASSIVE_FILE}")
        if start_date:
            now_utc = datetime.now(timezone.utc)
            run_historical_mode_batch(
                [(s, False) for s in active_symbols] + [(s, True) for s in passive_symbols],
                cli_start_date=start_date, today_str=last_completed_trading_day(now_utc),
                recent_days=set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date())))
        else:
            run_live_mode(active_symbols, passive_symbols)


if __name__ == "__main__":
    main()