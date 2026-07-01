#!/usr/bin/env python3
"""
extract_data.py — Live minute + daily data extractor.

Polls all symbols in watch.txt every 30s via yfinance during market hours
(including post-market/extended hours). After close, reconciles daily bars
via yfinance and runs IB backfill if needed. Today's in-progress daily bar
is built from minute data (IB won't have it yet), and is finalized at the
regular-session close (not the extended-hours close).

Usage:
    python3 extract_data.py                  # live mode
    python3 extract_data.py <YYYYMMDD>       # historical backfill, all symbols
"""

import sys, os, csv, math, time, threading, subprocess, importlib.util
from datetime import datetime, timedelta, timezone
from collections import deque

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR           = 'data'
POLL_SECONDS       = 30
MINUTE_DAYS        = 3          # how many days of minute bars to keep
MIN_DAILY_BARS     = 500        # minimum daily bars required before live mode
WATCH_FILE         = 'watch.txt'
PORTFOLIO_FILE     = 'portfolio.txt'
IB_HOST            = "127.0.0.1"
IB_PORT            = 7496
CLIENT_ID          = int(os.environ.get("IB_CLIENT_ID", "11"))
IB_CONNECT_TIMEOUT = 10          # seconds to wait for nextValidId handshake
INTER_REQUEST_SLEEP  = 10
MAX_REQUESTS_PER_10M = 55
REQUEST_WINDOW_SECS  = 600
DAILY_CHUNK_DURATION = "1 Y"
MINUTE_CHUNK_DURATION= "1 D"
WHAT_TO_SHOW         = "TRADES"
USE_RTH              = 0
YF_MAX_MINUTE_DAYS   = 7
POST_MARKET_HOURS    = 4          # keep polling this many hours past RTH close (extended hours)
PRE_MARKET_HOURS     = 4          # start polling this many hours before RTH open (extended hours)
DAILY_FIELDS  = ["datetime","open","close","high","low","volume","status"]
MINUTE_FIELDS = ["datetime","open","close","high","low","volume"]
_IB_SESSION_LOCK = threading.Lock()
_CALENDAR        = None
_MA_MOD          = None          # cached ma module


def _get_calendar():
    global _CALENDAR
    if _CALENDAR is None:
        try:
            import pandas_market_calendars as mcal
        except ImportError:
            sys.exit("pip install pandas_market_calendars --break-system-packages")
        _CALENDAR = mcal.get_calendar("NYSE")
    return _CALENDAR


def _get_ma_mod():
    global _MA_MOD
    if _MA_MOD is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ma.py")
        spec = importlib.util.spec_from_file_location("ma", path)
        _MA_MOD = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_MA_MOD)
    return _MA_MOD


# ── CSV helpers ───────────────────────────────────────────────────────────────

def _path(symbol, kind):
    return os.path.join(DATA_DIR, f"{symbol}_{kind}.csv")


def _load_csv(path, fields):
    if not os.path.exists(path):
        return {}
    with open(path, newline="") as f:
        rows = {}
        for r in csv.DictReader(f):
            row = {}
            for k in fields:
                if k in ("datetime", "status"):
                    row[k] = r.get(k, "")
                else:
                    try:
                        row[k] = float(r[k])
                    except (KeyError, ValueError):
                        row[k] = 0.0
            rows[r["datetime"]] = row
        return rows


def _write_csv(path, rows, fields):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in sorted(rows.values(), key=lambda r: r["datetime"]):
            if "status" in fields and "status" not in r:
                r = {**r, "status": ""}
            w.writerow(r)


def load_daily(symbol):  return _load_csv(_path(symbol, "daily"),  DAILY_FIELDS)
def load_minute(symbol): return _load_csv(_path(symbol, "minute"), MINUTE_FIELDS)

def save_daily(symbol, rows):  _write_csv(_path(symbol, "daily"),  rows, DAILY_FIELDS)
def save_minute(symbol, rows):
    # Prune to last MINUTE_DAYS trading days before writing
    if not rows:
        _write_csv(_path(symbol, "minute"), rows, MINUTE_FIELDS)
        return
    days      = sorted({k[:8] for k in rows})
    keep_days = set(days[-MINUTE_DAYS:])
    pruned    = {k: v for k, v in rows.items() if k[:8] in keep_days}
    _write_csv(_path(symbol, "minute"), pruned, MINUTE_FIELDS)


# ── Bar helpers ───────────────────────────────────────────────────────────────

def is_valid(bar):
    try:
        o, c, h, l = float(bar["open"]), float(bar["close"]), float(bar["high"]), float(bar["low"])
        v = float(bar.get("volume", 0))
        if not (o > 0 and c > 0 and h >= l and h > 0 and l > 0 and v >= 0):
            return False
        if any(math.isnan(x) or math.isinf(x) for x in (o, c, h, l, v)):
            return False
        # A bar with zero volume means no trades occurred, so it cannot have
        # any real intrabar range — high must equal low (and open/close should
        # sit on that same price). Zero-volume bars with a wide h/l spread are
        # a known bad-tick pattern from yfinance during illiquid extended-hours
        # trading (stale/bogus prints), e.g. a 5%+ range on 0 volume.
        if v == 0 and (h - l) > 1e-6:
            return False
        # Sanity check: high/low shouldn't be wildly detached from open/close.
        # A >10% spread between the bar's range and its open/close midpoint is
        # implausible for a single 1-minute bar and usually indicates a bad
        # print rather than a real move.
        mid = (o + c) / 2
        if mid > 0 and (h - l) / mid > 0.10:
            return False
        return True
    except (ValueError, TypeError):
        return False


def collapse_to_daily(symbol, minute_rows, status="live"):
    """Build today's daily bar from minute rows. Returns bar dict or None."""
    today = datetime.now(timezone.utc).strftime("%Y%m%d")
    bars  = sorted((v for k, v in minute_rows.items() if k[:8] == today),
                   key=lambda b: b["datetime"])
    if not bars:
        return None
    return {"datetime": today, "open": bars[0]["open"], "close": bars[-1]["close"],
            "high": max(b["high"] for b in bars), "low": min(b["low"] for b in bars),
            "volume": sum(b["volume"] for b in bars), "status": status}


def run_daily_ma(symbol, start_date=None):
    try:
        _get_ma_mod().compute_daily_ma(symbol, start_date)
    except Exception as e:
        print(f"  [ma] {symbol}: {e}")


def run_minute_ma(symbol, start_dt=None):
    try:
        _get_ma_mod().compute_minute_ma(symbol, start_dt)
    except Exception as e:
        print(f"  [ma_min] {symbol}: {e}")


# ── Calendar helpers ──────────────────────────────────────────────────────────

def trading_days_between(start, end):
    cal   = _get_calendar()
    sched = cal.schedule(start_date=start, end_date=end)
    return [d.strftime("%Y%m%d") for d in sched.index]


def last_n_trading_days(n, as_of=None):
    as_of = as_of or datetime.now(timezone.utc).date()
    cal   = _get_calendar()
    sched = cal.schedule(start_date=as_of - timedelta(days=int(n*2.5)+10), end_date=as_of)
    return [d.strftime("%Y%m%d") for d in sched.index][-n:]


def last_completed_trading_day(now_utc=None):
    now_utc = now_utc or datetime.now(timezone.utc)
    cal     = _get_calendar()
    sched   = cal.schedule(start_date=now_utc.date() - timedelta(days=14), end_date=now_utc.date())
    last    = None
    for idx, row in sched.iterrows():
        if row["market_close"].tz_convert("UTC").to_pydatetime() <= now_utc:
            last = idx.strftime("%Y%m%d")
    return last


def session_times(now_utc):
    """
    Return (poll_open_utc, rth_close_utc, poll_close_utc) for today, or (None, None, None).

    poll_open_utc  — start of the extended (pre-market) polling window
    rth_close_utc  — the *regular trading hours* close (used to finalize the daily bar)
    poll_close_utc — end of the extended (post-market) polling window; poll_tick
                      keeps running until this time even though the daily bar was
                      already finalized at rth_close_utc
    """
    cal = _get_calendar()
    try:
        sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date())
    except Exception:
        return None, None, None
    if sched.empty:
        return None, None, None
    row       = sched.iloc[0]
    rth_open  = row["market_open"].tz_convert("UTC").to_pydatetime()
    rth_close = row["market_close"].tz_convert("UTC").to_pydatetime()
    poll_open  = rth_open  - timedelta(hours=PRE_MARKET_HOURS)
    poll_close = rth_close + timedelta(hours=POST_MARKET_HOURS)
    return poll_open, rth_close, poll_close


def next_session_open(now_utc):
    """Return the start of the next extended (pre-market) polling window."""
    cal   = _get_calendar()
    sched = cal.schedule(start_date=now_utc.date(), end_date=now_utc.date() + timedelta(days=10))
    for _, row in sched.iterrows():
        rth_open  = row["market_open"].tz_convert("UTC").to_pydatetime()
        poll_open = rth_open - timedelta(hours=PRE_MARKET_HOURS)
        if poll_open > now_utc:
            return poll_open
    return None


def is_session_open(now_utc):
    """True if we're inside the extended polling window (pre-market through post-market)."""
    poll_open, _, poll_close = session_times(now_utc)
    return poll_open is not None and poll_open <= now_utc <= poll_close


# ── IB client ─────────────────────────────────────────────────────────────────

def _ib_imports():
    try:
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
        from ibapi.contract import Contract
        return EClient, EWrapper, Contract
    except ImportError:
        sys.exit("pip install ibapi --break-system-packages")


def make_ib_app():
    EClient, EWrapper, Contract = _ib_imports()

    class App(EWrapper, EClient):
        def __init__(self):
            EWrapper.__init__(self); EClient.__init__(self, wrapper=self)
            self._req_id = 1; self._bars = []; self._bar_size = None
            self._done = threading.Event(); self._error = False
            self._ts   = deque()
            self._connected = threading.Event()   # set once nextValidId arrives

        def nextValidId(self, orderId):
            """Called by IB once the API handshake is fully complete. This is the
            correct signal that the connection is actually usable — isConnected()
            only tells you the socket is up, not that the handshake finished."""
            self._req_id = orderId
            self._connected.set()

        def historicalData(self, reqId, bar):
            raw = bar.date.strip()
            if self._bar_size == "1 day":
                # IB always returns daily bar.date as a plain "YYYYMMDD" string
                # (optionally with trailing " 00:00:00" on some server versions),
                # never as epoch seconds. Parsing it with int()+fromtimestamp()
                # silently produces a garbage 1970-era date (e.g. "20260701" is
                # interpreted as 20,260,701 seconds since epoch), which then fails
                # every downstream date-range/membership filter with no error.
                fmt_dt = raw.replace(" ", "")[:8]
            else:
                # Minute/intraday bars: IB sends either raw epoch seconds or a
                # "YYYYMMDD HH:MM:SS[ TZ]" string depending on server version.
                try:
                    ts     = datetime.fromtimestamp(int(raw), tz=timezone.utc)
                    fmt_dt = ts.strftime("%Y%m%d%H%M")
                except ValueError:
                    raw2   = raw.replace(" ", "")
                    fmt_dt = datetime.strptime(raw2[:14], "%Y%m%d%H%M%S").strftime("%Y%m%d%H%M")
            self._bars.append({"datetime": fmt_dt, "open": bar.open, "close": bar.close,
                                "high": bar.high, "low": bar.low, "volume": bar.volume})

        def historicalDataEnd(self, reqId, start, end): self._done.set()

        def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
            if errorCode in (2104,2105,2106,2107,2108,2119,2150,2158): return
            if errorCode in (162,321,1100,1101,1102,2110):
                print(f"  [ib warn] {errorCode}: {errorString}"); self._done.set()
            else:
                print(f"  [ib error] {errorCode}: {errorString}")
                self._error = True; self._done.set()

        def connectAck(self): print(f"  [ib] socket connected to {IB_HOST}:{IB_PORT}, waiting for handshake ...")

        def _pace(self):
            now = time.time()
            while self._ts and now - self._ts[0] > REQUEST_WINDOW_SECS: self._ts.popleft()
            if len(self._ts) >= MAX_REQUESTS_PER_10M:
                wait = REQUEST_WINDOW_SECS - (now - self._ts[0]) + 1
                print(f"  [ib] pacing: sleep {wait:.0f}s"); time.sleep(wait)
                now = time.time()
                while self._ts and now - self._ts[0] > REQUEST_WINDOW_SECS: self._ts.popleft()
            if self._ts:
                elapsed = time.time() - self._ts[-1]
                if elapsed < INTER_REQUEST_SLEEP: time.sleep(INTER_REQUEST_SLEEP - elapsed)
            self._ts.append(time.time())

        def fetch(self, contract, end_dt, duration, bar_size):
            self._bars, self._error, self._bar_size = [], False, bar_size
            self._done.clear(); self._pace()
            end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")
            print(f"  [ib] {bar_size} end={end_str} ...", end="", flush=True)
            self.reqHistoricalData(self._req_id, contract, end_str, duration,
                                   bar_size, WHAT_TO_SHOW, USE_RTH, 2, 0, [])
            self._req_id += 1
            if not self._done.wait(timeout=180):
                print(" TIMEOUT"); self.cancelHistoricalData(self._req_id-1); return []
            print(f" {len(self._bars)} bars")
            return list(self._bars)

    return App()


def make_contract(symbol):
    _, _, Contract = _ib_imports()
    c = Contract()
    c.symbol, c.secType, c.exchange, c.currency = symbol.upper(), "STK", "SMART", "USD"
    return c


class IBSession:
    def __enter__(self):
        self.app = make_ib_app()
        self.app.connect(IB_HOST, IB_PORT, CLIENT_ID)
        threading.Thread(target=self.app.run, daemon=True).start()

        # Wait for the *handshake* to complete (nextValidId), not just the socket.
        # isConnected() can be True while the API session is still mid-setup, which
        # is what caused "504: Not connected" errors on the very first request.
        if not self.app._connected.wait(timeout=IB_CONNECT_TIMEOUT):
            self.app.disconnect()
            sys.exit(
                f"ERROR: IB handshake did not complete within {IB_CONNECT_TIMEOUT}s "
                f"(nextValidId never received). Check that TWS/IB Gateway is running, "
                f"API access is enabled for {IB_HOST}:{IB_PORT}, and that client id "
                f"{CLIENT_ID} isn't already in use by another session."
            )
        if not self.app.isConnected():
            sys.exit("ERROR: cannot connect to IB")
        return self.app

    def __exit__(self, *_): self.app.disconnect()


def ib_fetch_daily(app, contract, start_dt, end_dt):
    """Fetch daily bars from IB, filtered to valid NYSE trading days."""
    start_str    = start_dt.strftime("%Y%m%d")
    end_str      = (end_dt - timedelta(days=1)).strftime("%Y%m%d")
    trading_days = set(trading_days_between(start_dt.date(), end_str))
    result       = {}
    chunk_end    = end_dt
    while chunk_end > start_dt:
        bars = app.fetch(contract, chunk_end, DAILY_CHUNK_DURATION, "1 day")
        if app._error: break
        for b in bars:
            day = b["datetime"]
            if len(day) == 8 and start_str <= day <= end_str and day in trading_days:
                b["status"] = "final"
                result[day] = b
        chunk_end -= timedelta(days=366)
    return result


def ib_fetch_minute(app, contract, start_dt, end_dt):
    """Fetch 1-min bars from IB for the given range."""
    result    = {}
    chunk_end = end_dt
    last_first = None
    while chunk_end > start_dt:
        bars = app.fetch(contract, chunk_end, MINUTE_CHUNK_DURATION, "1 min")
        if app._error: break
        if bars:
            this_first = bars[0]["datetime"]
            if this_first == last_first:
                print(f"  [ib] duplicate window, skipping")
                chunk_end -= timedelta(days=1); continue
            last_first = this_first
        for b in bars:
            bdt = datetime.strptime(b["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
            if bdt >= start_dt:
                result[b["datetime"]] = b
        chunk_end -= timedelta(days=1)
    return result


# ── yfinance ──────────────────────────────────────────────────────────────────

def _yf():
    try:
        import yfinance as yf; return yf
    except ImportError:
        sys.exit("pip install yfinance --break-system-packages")


def yf_fetch_minute_batch(symbols):
    """Fetch today's 1-min bars for all symbols in one call (includes pre/post market)."""
    if not symbols: return {}
    df = _yf().download(tickers=symbols, period="1d", interval="1m",
                        prepost=True, group_by="ticker", auto_adjust=True, progress=False)
    return _parse_yf(df, symbols, "%Y%m%d%H%M")


def yf_fetch_minute_window(symbols, days):
    """Fetch last N days of 1-min bars for a list of symbols (includes pre/post market)."""
    if not symbols: return {}
    days = min(days, YF_MAX_MINUTE_DAYS)
    df   = _yf().download(tickers=symbols, period=f"{days}d", interval="1m",
                          prepost=True, group_by="ticker", auto_adjust=True, progress=False)
    return _parse_yf(df, symbols, "%Y%m%d%H%M")


def yf_fetch_daily_batch(symbols, period="5d"):
    """Fetch recent daily bars for all symbols in one call."""
    if not symbols: return {}
    df = _yf().download(tickers=symbols, period=period, interval="1d",
                        group_by="ticker", auto_adjust=True, progress=False)
    return _parse_yf(df, symbols, "%Y%m%d")


def _parse_yf(df, symbols, dt_fmt):
    result = {sym: {} for sym in symbols}
    if df is None or df.empty: return result
    multi  = getattr(df.columns, "nlevels", 1) > 1
    for sym in symbols:
        try:
            sym_df = df[sym] if multi else df
        except KeyError:
            continue
        if sym_df is None or sym_df.empty: continue
        for idx, row in sym_df.iterrows():
            dt_utc = idx.tz_convert("UTC") if idx.tzinfo else idx.tz_localize("UTC")
            dt_str = dt_utc.strftime(dt_fmt)
            try:
                bar = {"datetime": dt_str, "open": float(row["Open"]),
                       "close": float(row["Close"]), "high": float(row["High"]),
                       "low": float(row["Low"]),    "volume": float(row["Volume"])}
            except (ValueError, KeyError):
                continue
            if is_valid(bar):
                result[sym][dt_str] = bar
    return result


# ── Gap computation ───────────────────────────────────────────────────────────

def compute_daily_gaps(symbol, today_str, start_date):
    """Return list of missing daily trading days (excluding today — built from minutes).
    Any bar already marked final is skipped. Non-final bars are re-checked."""
    daily_rows = load_daily(symbol)
    final_days = {k for k, v in daily_rows.items() if v.get("status") == "final"}
    start_dt   = datetime.strptime(start_date, "%Y%m%d")
    all_td     = trading_days_between(start_dt.date(), today_str)
    missing    = [d for d in all_td if d != today_str and d not in final_days]
    return missing


def compute_minute_gaps(symbol, recent_days):
    """Return list of recent trading days missing from minute CSV.
    Only checks the most recent MINUTE_DAYS days — older minute data not needed."""
    minute_rows   = load_minute(symbol)
    existing_days = {k[:8] for k in minute_rows}
    target_days   = sorted(recent_days)[-MINUTE_DAYS:]
    return [d for d in target_days if d not in existing_days]


# ── Historical backfill ───────────────────────────────────────────────────────

def backfill_symbols(symbols, today_str, recent_days, start_date=None):
    """Gap-fill daily + minute data for all symbols using IB."""
    now_utc    = datetime.now(timezone.utc)
    end_dt     = datetime.strptime(today_str, "%Y%m%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    cal_start  = last_n_trading_days(MIN_DAILY_BARS + 1)[0]
    start_date = start_date or cal_start

    sym_gaps = {}
    for sym in symbols:
        daily_rows  = load_daily(sym)
        minute_rows = load_minute(sym)
        all_ex      = {**daily_rows, **minute_rows}
        s_date      = start_date if start_date else (
            min(k[:8] for k in all_ex) if all_ex else cal_start)
        s_date      = min(s_date, cal_start)

        miss_daily  = compute_daily_gaps(sym, today_str, s_date)
        miss_minute = compute_minute_gaps(sym, list(recent_days))
        sym_gaps[sym] = {"miss_daily": miss_daily, "miss_minute": miss_minute,
                         "daily_rows": daily_rows, "minute_rows": minute_rows}

    needs_ib = [(s, g) for s, g in sym_gaps.items() if g["miss_daily"] or g["miss_minute"]]
    if not needs_ib:
        print("[backfill] all symbols up to date — no IB fetch needed")
        return

    total_d = sum(len(g["miss_daily"])  for _, g in needs_ib)
    total_m = sum(len(g["miss_minute"]) for _, g in needs_ib)
    print(f"[backfill] {len(needs_ib)} symbol(s) need IB: "
          f"{total_d} daily day(s) to fill, {total_m} minute day(s) to fill")

    # Step A: yfinance batch — fills recent daily + minute gaps instantly
    # This covers last 5-7 days for all symbols in one HTTP call each.
    # NOTE: yfinance's daily "7d" window only has ~5 *trading* days in it, so
    # this step can only ever close a handful of the gaps for a first-run/deep
    # backfill (where miss_daily has ~500 entries) — the vast majority of a
    # first-run backfill still has to come from IB in step 2. That's expected,
    # not a bug: yfinance here is just a fast top-up for the last few days so
    # IB has less to do, not a substitute for deep history.
    all_syms = [s for s, _ in needs_ib]
    print(f"[backfill] step 1/2: yfinance batch for recent bars ...")
    yf_daily  = yf_fetch_daily_batch(all_syms, period="7d")
    yf_minute = yf_fetch_minute_window(all_syms, MINUTE_DAYS + 2)

    for sym, gap in needs_ib:
        before_d, before_m = len(gap["miss_daily"]), len(gap["miss_minute"])

        # Fill recent daily gaps from yfinance
        for day, bar in yf_daily.get(sym, {}).items():
            if day in set(gap["miss_daily"]):
                bar["status"] = "final"
                gap["daily_rows"][day] = bar
                gap["miss_daily"] = [d for d in gap["miss_daily"] if d != day]

        # Fill minute gaps from yfinance
        fresh_days = sorted({k[:8] for k in yf_minute.get(sym, {})})
        target     = set(fresh_days[-MINUTE_DAYS:])
        for k, v in yf_minute.get(sym, {}).items():
            if k[:8] in target:
                gap["minute_rows"][k] = v
        gap["miss_minute"] = [d for d in gap["miss_minute"] if d not in target]

        filled_d = before_d - len(gap["miss_daily"])
        filled_m = before_m - len(gap["miss_minute"])
        yf_daily_got  = len(yf_daily.get(sym, {}))
        yf_minute_got = len(yf_minute.get(sym, {}))
        print(f"  [yf] {sym}: fetched {yf_daily_got} daily / {yf_minute_got} minute bars "
              f"from yfinance -> filled {filled_d} daily gap(s), {filled_m} minute-day gap(s) "
              f"| {len(gap['miss_daily'])} daily, {len(gap['miss_minute'])} minute-day(s) still missing")

    # Step B: IB — only for deep historical gaps yfinance can't cover
    still_needs_ib = [(s, g) for s, g in needs_ib if g["miss_daily"] or g["miss_minute"]]
    if still_needs_ib:
        total_d2 = sum(len(g["miss_daily"])  for _, g in still_needs_ib)
        total_m2 = sum(len(g["miss_minute"]) for _, g in still_needs_ib)
        print(f"[backfill] step 2/2: IB for {len(still_needs_ib)} symbol(s) — "
              f"{total_d2} daily, {total_m2} minute day(s) remaining")
        with _IB_SESSION_LOCK:
            with IBSession() as app:
                for sym, gap in still_needs_ib:
                    contract = make_contract(sym)
                    if gap["miss_daily"]:
                        lo = datetime.strptime(min(gap["miss_daily"]), "%Y%m%d").replace(tzinfo=timezone.utc)
                        fetched  = ib_fetch_daily(app, contract, lo, end_dt)
                        filtered = {k: v for k, v in fetched.items() if k in set(gap["miss_daily"])}
                        gap["daily_rows"].update(filtered)
                        print(f"  [ib] {sym}: +{len(filtered)} daily bars")
                    if gap["miss_minute"]:
                        lo = datetime.strptime(min(gap["miss_minute"]), "%Y%m%d").replace(tzinfo=timezone.utc)
                        fetched = {k: v for k, v in ib_fetch_minute(app, contract, lo, end_dt).items()
                                   if k[:8] in set(gap["miss_minute"])}
                        gap["minute_rows"].update(fetched)
                        print(f"  [ib] {sym}: +{len(fetched)} minute bars")
    else:
        print("[backfill] step 2/2: no IB fetch needed — yfinance covered all gaps")

    # Save and compute MAs
    for sym, gap in sym_gaps.items():
        save_daily(sym, gap["daily_rows"])
        save_minute(sym, gap["minute_rows"])
        if gap["daily_rows"]:
            run_daily_ma(sym, sorted(gap["daily_rows"])[0])
        if gap["minute_rows"]:
            run_minute_ma(sym, sorted(gap["minute_rows"])[0])
        print(f"  [saved] {sym}: {len(gap['daily_rows'])} daily, "
              f"{len(gap['minute_rows'])} minute bars")


# ── Preflight ─────────────────────────────────────────────────────────────────

def preflight(symbols):
    """
    Startup check and reconciliation. Handles:
    - First run: full IB backfill from calendar_start
    - Restart after days stopped: fills daily + minute gaps, yfinance reconcile
    - Restart mid-session: fills any minute gap since last poll
    - All paths: logs clearly what is missing and what action is taken
    Returns True if post-close reconcile was already run (so live loop skips it).
    """
    now_utc     = datetime.now(timezone.utc)
    today_str   = last_completed_trading_day(now_utc)
    recent_days = set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date()))
    market_is_open = is_session_open(now_utc)

    print(f"\n[preflight] {len(symbols)} symbol(s) | "
          f"last trading day: {today_str} | "
          f"market: {'OPEN' if market_is_open else 'CLOSED'}")

    # ── Step 1: check each symbol, collect gaps ───────────────────────────────
    needs_backfill  = []
    all_ok          = True

    for sym in symbols:
        daily_rows  = load_daily(sym)
        minute_rows = load_minute(sym)
        daily_days  = sorted(daily_rows)
        minute_days = sorted({k[:8] for k in minute_rows})
        n_daily     = len(daily_days)
        has_daily   = n_daily >= MIN_DAILY_BARS
        has_minute  = recent_days.issubset(set(minute_days))

        # Check for daily gaps even if we have MIN_DAILY_BARS
        # (e.g. stopped for 5 days — recent days missing from daily CSV)
        missing_recent_daily = [d for d in sorted(recent_days)
                                 if d != today_str
                                 and daily_rows.get(d, {}).get("status") != "final"]

        ok = has_daily and has_minute and not missing_recent_daily

        range_str = f"[{daily_days[0]}-{daily_days[-1]}]" if daily_days else "[none]"
        miss_min  = sorted(recent_days - set(minute_days))
        status    = "OK" if ok else "NEEDS BACKFILL"
        detail    = f"{n_daily} daily {range_str}"
        if not has_daily:
            detail += f" (need {MIN_DAILY_BARS - n_daily} more)"
        if miss_min:
            detail += f" | minute MISSING {miss_min}"
        if missing_recent_daily:
            detail += f" | daily gaps {missing_recent_daily}"
        print(f"  [{sym}] {status} | {detail}")

        if not ok:
            needs_backfill.append(sym)
            all_ok = False

    if all_ok:
        print("[preflight] all symbols ready")
    else:
        print(f"\n[preflight] backfilling {len(needs_backfill)} symbol(s) ...")
        backfill_symbols(needs_backfill, today_str, recent_days)
        print("[preflight] backfill complete")

    # ── Step 2: if market is closed on startup, run post-close reconcile now ──
    # This covers: stopped overnight, stopped for multiple days, first run after close.
    # Avoids running it again in the live loop for today.
    if not market_is_open:
        print("\n[preflight] market closed on startup — running post-close reconcile ...")
        post_close_reconcile(symbols, {sym: load_minute(sym) for sym in symbols})
        return True   # signal to live loop: already reconciled today

    print("[preflight] market open on startup — entering live mode\n")
    return False


# ── Live poll ─────────────────────────────────────────────────────────────────

def poll_tick(symbols, minute_cache, now_utc, rth_close):
    """One poll cycle: fetch minute bars, update CSVs + MAs.

    rth_close is the *regular trading hours* close — used only to decide
    whether today's in-progress daily bar should be marked 'final'. Polling
    itself continues through the extended post-market window regardless.
    """
    if not symbols:
        return

    batch = yf_fetch_minute_batch(symbols)
    added_any = False

    for sym in symbols:
        new_bars = batch.get(sym, {})
        if not new_bars:
            continue
        before = len(minute_cache[sym])
        minute_cache[sym].update(new_bars)
        added  = len(minute_cache[sym]) - before
        if added > 0:
            save_minute(sym, minute_cache[sym])
            added_any = True
            print(f"  [poll] {sym}: +{added} bar(s) ({now_utc:%H:%M:%S} UTC)")

            # Update today's in-progress daily bar from minutes.
            # Only keep updating/writing it while we're still inside RTH —
            # post-market prints keep updating the minute cache/file but should
            # not still be flowing into today's daily OHLC after RTH close
            # (post_close_reconcile marks the bar 'final' from yfinance instead).
            is_final = now_utc >= rth_close
            if not is_final:
                bar = collapse_to_daily(sym, minute_cache[sym], "live")
                if bar:
                    daily_rows = load_daily(sym)
                    daily_rows[bar["datetime"]] = bar
                    save_daily(sym, daily_rows)

            # Update MAs in background
            today_str = now_utc.strftime("%Y%m%d")
            threading.Thread(target=lambda s=sym, t=today_str: (
                run_daily_ma(s, t), run_minute_ma(s, t)
            ), daemon=True).start()

    if not added_any:
        print(f"  [poll] no new bars ({now_utc:%H:%M:%S} UTC)")


# ── Post-close reconciliation ─────────────────────────────────────────────────

def post_close_reconcile(symbols, minute_cache):
    """After market close: yfinance daily reconcile + IB backfill for any gaps."""
    now_utc   = datetime.now(timezone.utc)
    today_str = last_completed_trading_day(now_utc)
    print(f"\n[post-close] reconciling {len(symbols)} symbol(s) for {today_str}")

    # 1. yfinance minute reconcile — make sure we have the right 3-day window
    print(f"[post-close] yfinance minute reconcile ...")
    fresh = yf_fetch_minute_window(symbols, MINUTE_DAYS + 2)
    for sym in symbols:
        new_bars   = fresh.get(sym, {})
        if not new_bars: continue
        fresh_days = sorted({k[:8] for k in new_bars})
        target     = set(fresh_days[-MINUTE_DAYS:])
        existing   = {k[:8] for k in minute_cache.get(sym, {})}
        if existing != target:
            print(f"  [reconcile] {sym}: window {sorted(existing)} → {sorted(target)}")
            minute_cache[sym] = {k: v for k, v in new_bars.items() if k[:8] in target}
        else:
            minute_cache[sym].update(new_bars)
        save_minute(sym, minute_cache[sym])

    # 2. yfinance daily reconcile — get final daily bars for recent days
    print(f"[post-close] yfinance daily reconcile ...")
    daily_batch = yf_fetch_daily_batch(symbols, period="5d")
    for sym in symbols:
        daily_rows = load_daily(sym)
        updated    = []
        for dt_str, bar in daily_batch.get(sym, {}).items():
            if len(dt_str) == 8:
                bar["status"] = "final"
                daily_rows[dt_str] = bar
                updated.append(dt_str)
        if updated:
            save_daily(sym, daily_rows)
            print(f"  [yf] {sym}: updated daily bars {updated}")

    # 3. Final MAs for all symbols (no IB backfill here — preflight handles that)
    print(f"[post-close] running final MAs ...")
    for sym in symbols:
        run_daily_ma(sym, today_str)
        run_minute_ma(sym, today_str)

    print(f"[post-close] done — {len(symbols)} symbol(s) reconciled")


# ── Main live loop ────────────────────────────────────────────────────────────

def run_live(symbols):
    print(f"\n[live] starting — {len(symbols)} symbol(s), polling every {POLL_SECONDS}s "
          f"(+/-{PRE_MARKET_HOURS}h pre / {POST_MARKET_HOURS}h post market)")
    already_reconciled = preflight(symbols)

    # Load minute cache into memory after preflight (data may have been updated)
    minute_cache = {sym: load_minute(sym) for sym in symbols}

    # recon_date tracks whether post-close has run for the current calendar day.
    # post-close now fires right at RTH close (not at the end of the extended poll
    # window), so daily reconciliation isn't delayed by hours of post-market polling.
    recon_date   = datetime.now(timezone.utc).strftime("%Y%m%d") if already_reconciled else None
    recon_thread = None
    _logged_idle = False   # suppress repeated "market closed" log

    print("[live] entering poll loop ...")
    while True:
        tick_start = time.time()
        now_utc    = datetime.now(timezone.utc)
        poll_open, rth_close, poll_close = session_times(now_utc)
        is_polling     = poll_open is not None and poll_open <= now_utc <= poll_close
        past_rth_close = rth_close is not None and now_utc >= rth_close
        today          = now_utc.strftime("%Y%m%d")

        if is_polling:
            _logged_idle = False
            # Reset recon flag so post-close fires after today's session
            if recon_date != today:
                recon_date = None
            poll_tick(symbols, minute_cache, now_utc, rth_close)

            # Fire post-close reconcile once RTH closes, even though polling
            # continues into the post-market window. This keeps daily bars
            # finalized promptly instead of waiting for post-market to end.
            already_done  = (recon_date == today)
            recon_running = recon_thread is not None and recon_thread.is_alive()
            if past_rth_close and not already_done and not recon_running:
                recon_date   = today
                recon_thread = threading.Thread(
                    target=post_close_reconcile,
                    args=(symbols, minute_cache),
                    name="post-close", daemon=True)
                recon_thread.start()
        else:
            # Outside the extended polling window entirely (deep overnight).
            already_done  = (recon_date == today)
            recon_running = recon_thread is not None and recon_thread.is_alive()

            if not already_done and not recon_running:
                recon_date   = today
                recon_thread = threading.Thread(
                    target=post_close_reconcile,
                    args=(symbols, minute_cache),
                    name="post-close", daemon=True)
                recon_thread.start()
                _logged_idle = False
            elif not recon_running and not _logged_idle:
                next_open    = next_session_open(now_utc)
                nxt_str      = f"{next_open:%Y-%m-%d %H:%M} UTC" if next_open else "unknown"
                print(f"[live] market closed — next session {nxt_str}")
                _logged_idle = True   # only log once until market reopens

        elapsed = time.time() - tick_start
        sleep   = max(0, POLL_SECONDS - elapsed)
        if elapsed > POLL_SECONDS:
            print(f"[live] tick took {elapsed:.1f}s")
        time.sleep(sleep)


# ── Entry point ───────────────────────────────────────────────────────────────

def load_watch():
    try:
        with open(WATCH_FILE) as f:
            return [l.strip().upper() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        sys.exit(f"ERROR: {WATCH_FILE} not found")


def main():
    if len(sys.argv) > 2:
        sys.exit("Usage: extract_data.py [YYYYMMDD]")
    os.makedirs(DATA_DIR, exist_ok=True)
    symbols = load_watch()
    if not symbols:
        sys.exit(f"ERROR: {WATCH_FILE} is empty")
    print(f"[start] {len(symbols)} symbol(s) from {WATCH_FILE}")

    if len(sys.argv) == 2:
        arg = sys.argv[1]
        try:
            datetime.strptime(arg, "%Y%m%d")
        except ValueError:
            sys.exit(f"ERROR: expected YYYYMMDD, got '{arg}'")
        now_utc   = datetime.now(timezone.utc)
        today_str = last_completed_trading_day(now_utc)
        recent    = set(last_n_trading_days(MINUTE_DAYS, as_of=now_utc.date()))
        backfill_symbols(symbols, today_str, recent, start_date=arg)
    else:
        try:
            run_live(symbols)
        except KeyboardInterrupt:
            print("\n[live] interrupted")
            os._exit(0)


if __name__ == "__main__":
    main()