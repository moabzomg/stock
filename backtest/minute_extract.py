#!/usr/bin/env python3
"""
minute_extract.py — Fetch 1-min historical bars from IB.

Usage:
    python3 minute_extract.py <SYMBOL> <YYYYMMDDHHmm>      # single symbol
    python3 minute_extract.py <YYYYMMDDHHmm> [-w FILE]     # all symbols in watchlist
"""

import sys, os, csv, time, threading, subprocess, importlib.util, argparse
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
DATA_DIR  = "data"
DAILY_FIELDS  = ["datetime","open","close","high","low","volume","status"]
MINUTE_FIELDS = ["datetime","open","close","high","low","volume"]
_MA_MOD = None


def _get_ma_mod():
    """
    Load ma.py from the parent directory (../ma.py relative to this file),
    matching where the file actually lives.

    IMPORTANT: we only assign to the module-level cache (_MA_MOD) *after*
    exec_module succeeds. Previously module_from_spec() populated _MA_MOD
    immediately, so if exec_module() then raised (e.g. wrong path), the
    global was left pointing at an empty, half-initialized module. Every
    later call saw "_MA_MOD is not None" and skipped reloading, silently
    reusing that broken module for the rest of the run (this is why the
    first symbol failed with FileNotFoundError but every symbol after it
    failed with a different, misleading "module 'ma' has no attribute
    ..." error).
    """
    global _MA_MOD
    if _MA_MOD is None:
        here = os.path.dirname(os.path.abspath(__file__))
        path = os.path.normpath(os.path.join(here, "..", "ma.py"))
        spec = importlib.util.spec_from_file_location("ma", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)   # if this throws, _MA_MOD stays None
        _MA_MOD = mod
    return _MA_MOD


# ── IB extractor ──────────────────────────────────────────────────────────────

class IBExtractor(EWrapper, EClient):
    def __init__(self):
        EWrapper.__init__(self); EClient.__init__(self, wrapper=self)
        self._req_id = 1; self._bars = []; self._done = threading.Event()
        self._error  = False; self._ts = deque()

    def historicalData(self, reqId, bar):
        raw = bar.date.strip()
        try:
            dt = datetime.fromtimestamp(int(raw), tz=timezone.utc)
        except ValueError:
            dt = datetime.strptime(raw, "%Y%m%d %H:%M:%S").replace(tzinfo=timezone.utc)
        self._bars.append({"datetime": dt.strftime("%Y%m%d%H%M"), "open": bar.open,
                           "close": bar.close, "high": bar.high, "low": bar.low,
                           "volume": bar.volume})

    def historicalDataEnd(self, reqId, start, end): self._done.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in (2104,2105,2106,2107,2108,2119,2150,2158): return
        if errorCode in (162,321,1100,1101,1102,2110):
            print(f"  [warn] {errorCode}: {errorString}"); self._done.set()
        else:
            print(f"  [error] {errorCode}: {errorString}")
            self._error = True; self._done.set()

    def connectAck(self): print(f"  [ib] connected to {IB_HOST}:{IB_PORT}")

    def _pace(self):
        now = time.time()
        while self._ts and now - self._ts[0] > REQUEST_WINDOW_SECS: self._ts.popleft()
        if len(self._ts) >= MAX_REQUESTS_PER_10M:
            wait = REQUEST_WINDOW_SECS - (now - self._ts[0]) + 1
            print(f"  [pace] sleeping {wait:.0f}s"); time.sleep(wait)
            now = time.time()
            while self._ts and now - self._ts[0] > REQUEST_WINDOW_SECS: self._ts.popleft()
        if self._ts:
            elapsed = time.time() - self._ts[-1]
            if elapsed < INTER_REQUEST_SLEEP: time.sleep(INTER_REQUEST_SLEEP - elapsed)
        self._ts.append(time.time())

    def fetch(self, contract, end_dt):
        self._bars, self._error = [], False; self._done.clear(); self._pace()
        end_str = end_dt.strftime("%Y%m%d-%H:%M:%S")
        print(f"  → end={end_str} ...", end="", flush=True)
        self.reqHistoricalData(self._req_id, contract, end_str, "1 D",
                               "1 min", "TRADES", 0, 2, 0, [])
        self._req_id += 1
        if not self._done.wait(timeout=120):
            print(" TIMEOUT"); self.cancelHistoricalData(self._req_id-1); return []
        bars = list(self._bars)
        print(f" {len(bars)} bars" +
              (f"  ({bars[0]['datetime']} → {bars[-1]['datetime']})" if bars else ""))
        return bars


def make_contract(symbol):
    c = Contract()
    c.symbol, c.secType, c.exchange, c.currency = symbol, "STK", "SMART", "USD"
    return c


# ── CSV helpers ───────────────────────────────────────────────────────────────

def is_valid(bar):
    try:
        o, c, h, l = float(bar["open"]), float(bar["close"]), float(bar["high"]), float(bar["low"])
        v = float(bar.get("volume", 0))
        return o > 0 and c > 0 and h >= l and v >= 0
    except (ValueError, TypeError, KeyError):
        return False


def load_minute(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_minute.csv")
    if not os.path.exists(path): return [], set()
    with open(path, newline="") as f:
        bars = [{"datetime": r["datetime"], "open": float(r["open"]),
                 "close": float(r["close"]), "high": float(r["high"]),
                 "low": float(r["low"]), "volume": float(r["volume"])}
                for r in csv.DictReader(f)]
    bars = [b for b in bars if is_valid(b)]
    seen = {b["datetime"] for b in bars}
    if bars: print(f"  [resume] {symbol}: {len(bars)} bars ({min(seen)} → {max(seen)})")
    return bars, seen


def save_minute(symbol, bars):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{symbol}_minute.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MINUTE_FIELDS, extrasaction="ignore")
        w.writeheader(); w.writerows(bars)
    print(f"  [saved] {symbol}: {len(bars)} minute bars")


def load_daily(symbol):
    path = os.path.join(DATA_DIR, f"{symbol}_daily.csv")
    if not os.path.exists(path): return {}
    with open(path, newline="") as f:
        rows = {}
        for r in csv.DictReader(f):
            rows[r["datetime"]] = {"datetime": r["datetime"], "open": float(r["open"]),
                "close": float(r["close"]), "high": float(r["high"]),
                "low": float(r["low"]), "volume": float(r["volume"]),
                "status": r.get("status", "")}
    return rows


def save_daily(symbol, rows):
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{symbol}_daily.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=DAILY_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in sorted(rows.values(), key=lambda r: r["datetime"]):
            if "status" not in r: r = {**r, "status": ""}
            w.writerow(r)


def market_close_utc():
    try:
        import pandas_market_calendars as mcal
        cal   = mcal.get_calendar("NYSE")
        today = datetime.now(timezone.utc).date()
        sched = cal.schedule(start_date=today, end_date=today)
        if sched.empty: return None
        return sched.iloc[0]["market_close"].tz_convert("UTC").to_pydatetime()
    except Exception:
        return None


def _aggregate_day(date_str, day_bars, is_final):
    return {"datetime": date_str, "open": day_bars[0]["open"],
            "close": day_bars[-1]["close"],
            "high":  max(b["high"]   for b in day_bars),
            "low":   min(b["low"]    for b in day_bars),
            "volume": sum(b["volume"] for b in day_bars),
            "status": "final" if is_final else "live"}


def update_daily_bars(symbol, all_bars):
    """
    Build/refresh daily bars for every date that has minute data but does
    not yet have a *finalized* daily bar on disk.

    This is intentionally driven by "what's on disk missing a final daily
    bar", not "what did we fetch this run" — otherwise a small incremental
    fetch (e.g. just today's few new minutes) never goes back and backfills
    older days whose daily/MA aggregation failed or was skipped previously.
    Historical days (before the current UTC calendar day) are always
    final, since the trading day is over. Today's bar is final only once
    we're past NYSE close.

    Returns the sorted list of date strings written, or [] if nothing
    changed.
    """
    now_utc   = datetime.now(timezone.utc)
    today_str = now_utc.strftime("%Y%m%d")
    close     = market_close_utc()
    today_is_final = close is not None and now_utc >= close

    daily = load_daily(symbol)

    bars_by_day = {}
    for b in all_bars:
        bars_by_day.setdefault(b["datetime"][:8], []).append(b)

    written = []
    for date_str in sorted(bars_by_day):
        if daily.get(date_str, {}).get("status") == "final":
            continue  # already finalized, nothing to do
        day_bars = sorted(bars_by_day[date_str], key=lambda b: b["datetime"])
        is_final = today_is_final if date_str == today_str else True
        daily[date_str] = _aggregate_day(date_str, day_bars, is_final)
        written.append(date_str)

    if written:
        save_daily(symbol, daily)
    return written


# ── Gap computation ───────────────────────────────────────────────────────────

def compute_gaps(seen, start_dt, now_utc):
    midnight = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    if not seen: return [(start_dt, midnight)]
    days     = {dt[:8] for dt in seen}
    earliest = datetime.strptime(min(days), "%Y%m%d").replace(tzinfo=timezone.utc)
    latest   = datetime.strptime(max(days), "%Y%m%d").replace(tzinfo=timezone.utc)
    gaps     = []
    if start_dt < earliest:
        gaps.append((start_dt, earliest + timedelta(days=1)))
    if latest < midnight:
        gaps.append((latest, midnight))
    return gaps


# ── Per-symbol extraction ─────────────────────────────────────────────────────

def extract_symbol(app, symbol, start_dt, now_utc):
    all_bars, seen = load_minute(symbol)
    before         = len(all_bars)
    gaps           = compute_gaps(seen, start_dt, now_utc)

    if not gaps:
        print(f"  [{symbol}] already up to date")
    else:
        for gs, ge in gaps:
            print(f"  [{symbol}] gap: {gs:%Y-%m-%d} → {ge:%Y-%m-%d}")

    contract = make_contract(symbol)
    for gap_start, gap_end in gaps:
        chunk_end  = gap_end
        last_first = None
        while chunk_end > gap_start:
            bars = app.fetch(contract, chunk_end)
            if app._error:
                print(f"  [{symbol}] IB error — stopping")
                break
            if bars:
                this_first = bars[0]["datetime"]
                if this_first == last_first:
                    print(f"  [{symbol}] duplicate window, advancing")
                    chunk_end -= timedelta(days=1); continue
                last_first = this_first
            for b in bars:
                bdt = datetime.strptime(b["datetime"], "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
                if bdt >= start_dt and b["datetime"] not in seen and is_valid(b):
                    seen.add(b["datetime"]); all_bars.append(b)
            chunk_end -= timedelta(days=1)
        else:
            continue
        break

    new_count = len(all_bars) - before
    if all_bars:
        all_bars.sort(key=lambda b: b["datetime"])
        save_minute(symbol, all_bars)
        print(f"  [{symbol}] +{new_count} new bars, {len(all_bars)} total")

        # Aggregate daily bars for every date that has minute data but no
        # finalized daily bar yet — this backfills any historical days left
        # over from earlier failed runs, not just what was fetched now.
        updated_dates = update_daily_bars(symbol, all_bars)

        new_dates = sorted({b["datetime"][:8] for b in all_bars[len(all_bars) - new_count:]}) \
                    if new_count else []

        # MA needs to be (re)computed starting at the earliest date that
        # either got new minute bars or a fresh/updated daily bar this run,
        # so a small incremental fetch still catches up any MA backlog.
        candidates = updated_dates + new_dates
        start_ma = min(candidates) if candidates else all_bars[0]["datetime"][:8]
        try:
            ma = _get_ma_mod()
            ma.compute_daily_ma(symbol, start_ma)
            ma.compute_minute_ma(symbol, start_ma)
        except Exception as e:
            print(f"  [{symbol}] MA warning: {e}")
    return new_count


# ── Entry point ───────────────────────────────────────────────────────────────

def load_symbols(path):
    try:
        with open(path) as f:
            return [l.strip().upper() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        sys.exit(f"ERROR: {path} not found")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("first",  help="SYMBOL or YYYYMMDDHHmm")
    parser.add_argument("second", nargs="?", help="YYYYMMDDHHmm (if first is SYMBOL)")
    parser.add_argument("--watchlist", "-w", default="watch.txt")
    args = parser.parse_args()

    is_single = not args.first.isdigit()
    if is_single:
        if args.second is None:
            sys.exit("Usage: minute_extract.py <SYMBOL> <YYYYMMDDHHmm>")
        symbols, ts = [args.first.upper()], args.second
    else:
        symbols, ts = load_symbols(args.watchlist), args.first

    try:
        start_dt = datetime.strptime(ts, "%Y%m%d%H%M").replace(tzinfo=timezone.utc)
    except ValueError:
        sys.exit(f"ERROR: bad datetime '{ts}', expected YYYYMMDDHHmm")
    now_utc = datetime.now(timezone.utc)
    if start_dt >= now_utc:
        sys.exit("ERROR: start must be in the past")

    print(f"[extract] {len(symbols)} symbol(s) from {start_dt:%Y-%m-%d %H:%M} UTC")
    app = IBExtractor()
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)
    threading.Thread(target=app.run, daemon=True).start()
    time.sleep(2)
    if not app.isConnected(): sys.exit("ERROR: cannot connect to IB")

    results = {}
    for sym in symbols:
        t0 = time.time()
        try:
            n = extract_symbol(app, sym, start_dt, now_utc)
            results[sym] = (n, time.time()-t0, None)
        except Exception as e:
            print(f"  [{sym}] error: {e}")
            results[sym] = (0, time.time()-t0, str(e))

    app.disconnect()

    if len(symbols) > 1:
        print(f"\n{'─'*52}")
        print(f"  {'SYMBOL':<10}  {'NEW BARS':>10}  {'TIME':>7}  STATUS")
        total = 0
        for sym in symbols:
            n, t, err = results[sym]
            total += n
            print(f"  {sym:<10}  {n:>10}  {t:>6.0f}s  {err or 'ok'}")
        print(f"  {'TOTAL':<10}  {total:>10}\n{'─'*52}\n")


if __name__ == "__main__":
    main()