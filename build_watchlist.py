#!/usr/bin/env python3
"""
build_watchlist.py — Build watch.txt from TradingView, validated against
yfinance and/or Interactive Brokers.

Why this exists: TradingView's screener returns class shares with a dot
("BRK.A"), but yfinance expects a hyphen ("BRK-A"). Writing the TradingView
symbol straight to watch.txt (as the original script does) means every
yfinance request for those symbols fails with a bogus "possibly delisted"
error — it's a formatting problem, not a real data-availability problem.

This script:
  1. Pulls the full TradingView universe.
  2. Normalizes symbols to this codebase's convention: dot -> slash
     (e.g. "BRK.A" -> "BRK/A"), matching extract_data.py's own comment that
     "IB (and this codebase's watch.txt) writes preferred/class shares with
     a slash". extract_data.py's _yahoo_symbol() already converts that slash
     to a hyphen for Yahoo requests, so this alignment is what makes the
     existing pipeline work correctly for class shares.
  3. Validates every symbol against yfinance in chunked, non-threaded
     batches (same chunking rationale as extract_data.py: yfinance's
     threaded download() exhausts OS threads on large ticker lists).
  4. For symbols yfinance couldn't confirm, validates against IB via
     reqContractDetails (a cheap, non-historical-data request — no
     10-minute pacing window needed, just a small per-request delay to
     stay well under IB's message-rate limits).
  5. Writes every symbol confirmed by yfinance and/or IB to watch.txt,
     backing up any existing watch.txt first.

Usage:
    python3 build_watchlist.py                # yfinance + IB fallback
    python3 build_watchlist.py --yf-only       # skip IB entirely
    python3 build_watchlist.py --require-both  # keep only symbols BOTH confirm
"""

import sys, os, time, json, argparse, threading
from datetime import datetime

WATCH_FILE      = "watch.txt"
YF_CHUNK_SIZE   = 150
YF_CHUNK_SLEEP  = 1.0
IB_HOST         = "127.0.0.1"
IB_PORT         = 7496
CLIENT_ID       = int(os.environ.get("IB_CLIENT_ID", "12"))  # different from extract_data.py's 11
IB_CONNECT_TIMEOUT   = 10
IB_REQUEST_TIMEOUT   = 5
IB_INTER_REQUEST_SLEEP = 0.05   # contractDetails is cheap; no 10-min pacing needed


# ── TradingView fetch ─────────────────────────────────────────────────────────

def fetch_tradingview_symbols():
    """Pull the full US common/preferred/DR/fund universe from TradingView's
    screener endpoint. Returns raw symbols exactly as TradingView emits them
    (dots for class shares, e.g. 'BRK.A') — normalize_symbol() below is what
    converts them to this codebase's convention."""
    import requests

    url = "https://scanner.tradingview.com/america/scan?label-product=screener-stock"
    headers = {
        "accept": "application/json",
        "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
        "content-type": "text/plain;charset=UTF-8",
        "origin": "https://www.tradingview.com",
        "referer": "https://www.tradingview.com/",
        "user-agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"),
    }
    payload = {
        "columns": ["ticker-view", "close", "type", "typespecs"],
        "filter": [{"left": "close", "operation": "egreater", "right": 1}],
        "ignore_unknown_fields": False,
        "options": {"lang": "en"},
        "range": [0, 10000],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "markets": ["america"],
        "filter2": {
            "operator": "and",
            "operands": [
                {"operation": {"operator": "or", "operands": [
                    {"operation": {"operator": "and", "operands": [
                        {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                        {"expression": {"left": "typespecs", "operation": "has", "right": ["common"]}}]}},
                    {"operation": {"operator": "and", "operands": [
                        {"expression": {"left": "type", "operation": "equal", "right": "stock"}},
                        {"expression": {"left": "typespecs", "operation": "has", "right": ["preferred"]}}]}},
                    {"operation": {"operator": "and", "operands": [
                        {"expression": {"left": "type", "operation": "equal", "right": "dr"}}]}},
                    {"operation": {"operator": "and", "operands": [
                        {"expression": {"left": "type", "operation": "equal", "right": "fund"}},
                        {"expression": {"left": "typespecs", "operation": "has_none_of", "right": ["etf", "mutual"]}}]}},
                ]}},
                {"expression": {"left": "typespecs", "operation": "has_none_of", "right": ["pre-ipo"]}},
            ],
        },
    }

    print("[tv] requesting TradingView universe ...")
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    rows = resp.json().get("data", [])

    symbols = []
    for item in rows:
        ticker_id = item.get("s")
        if ticker_id and isinstance(ticker_id, str):
            if ":" in ticker_id:
                ticker_id = ticker_id.split(":", 1)[1]
            symbols.append(ticker_id)
    print(f"[tv] got {len(symbols)} symbols")
    return symbols


def normalize_symbol(sym):
    """TradingView -> this codebase's watch.txt convention: dot -> slash.
    'BRK.A' -> 'BRK/A', matching how extract_data.py already expects class
    shares to be written (and translates '/' -> '-' for Yahoo internally)."""
    return sym.strip().upper().replace(".", "/")


# ── yfinance validation ────────────────────────────────────────────────────────

def _chunked(seq, size):
    seq = list(seq)
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def _yahoo_symbol(sym):
    """Same translation extract_data.py uses: '/' -> '-' for Yahoo requests."""
    return sym.replace("/", "-")


def validate_yfinance(symbols):
    """Return the subset of `symbols` (already in this codebase's slash
    convention) that yfinance returns usable data for. Chunked and
    non-threaded for the same reason as extract_data.py's batch downloader:
    yfinance's threaded download() exhausts the OS thread limit on large
    ticker lists."""
    try:
        import yfinance as yf
    except ImportError:
        sys.exit("pip install yfinance --break-system-packages")

    confirmed = set()
    total = len(symbols)
    for i, chunk in enumerate(_chunked(symbols, YF_CHUNK_SIZE)):
        yahoo_map = {_yahoo_symbol(s): s for s in chunk}
        yahoo_tix = list(yahoo_map.keys())
        try:
            df = yf.download(tickers=yahoo_tix, period="5d", interval="1d",
                              group_by="ticker", auto_adjust=True,
                              progress=False, threads=False)
        except Exception as e:
            print(f"  [yf] chunk {i} failed ({len(chunk)} symbols): {e}")
            continue

        multi = getattr(df.columns, "nlevels", 1) > 1
        for yahoo_sym, orig_sym in yahoo_map.items():
            try:
                sub = df[yahoo_sym] if multi else df
            except KeyError:
                continue
            if sub is None or sub.empty:
                continue
            if sub["Close"].notna().any():
                confirmed.add(orig_sym)

        done = min((i + 1) * YF_CHUNK_SIZE, total)
        print(f"  [yf] {done}/{total} checked, {len(confirmed)} confirmed so far")
        time.sleep(YF_CHUNK_SLEEP)

    return confirmed


# ── IB validation ──────────────────────────────────────────────────────────────

def _ib_imports():
    try:
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
        from ibapi.contract import Contract
        return EClient, EWrapper, Contract
    except ImportError:
        sys.exit("pip install ibapi --break-system-packages")


def make_contract(symbol):
    _, _, Contract = _ib_imports()
    c = Contract()
    c.symbol, c.secType, c.exchange, c.currency = symbol.upper(), "STK", "SMART", "USD"
    return c


def validate_ib(symbols):
    """Return the subset of `symbols` that resolve to a valid IB contract via
    reqContractDetails. Much cheaper than historical-data requests — no
    10-minute pacing window applies, just a small per-request delay to stay
    well under IB's message-rate limit."""
    if not symbols:
        return set()

    EClient, EWrapper, Contract = _ib_imports()

    class App(EWrapper, EClient):
        def __init__(self):
            EWrapper.__init__(self); EClient.__init__(self, wrapper=self)
            self._req_id = 1
            self._connected = threading.Event()
            self._done = threading.Event()
            self._found = False

        def nextValidId(self, orderId):
            self._req_id = orderId
            self._connected.set()

        def contractDetails(self, reqId, contractDetails):
            self._found = True

        def contractDetailsEnd(self, reqId):
            self._done.set()

        def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
            if errorCode in (2104, 2105, 2106, 2107, 2108, 2119, 2150, 2158):
                return
            if errorCode == 200:   # "No security definition has been found"
                self._done.set()
                return
            print(f"  [ib error] {errorCode}: {errorString}")
            self._done.set()

        def check(self, contract):
            self._found = False
            self._done.clear()
            self.reqContractDetails(self._req_id, contract)
            self._req_id += 1
            self._done.wait(timeout=IB_REQUEST_TIMEOUT)
            return self._found

    app = App()
    app.connect(IB_HOST, IB_PORT, CLIENT_ID)
    threading.Thread(target=app.run, daemon=True).start()

    if not app._connected.wait(timeout=IB_CONNECT_TIMEOUT):
        app.disconnect()
        print("[ib] WARNING: could not connect to TWS/IB Gateway within "
              f"{IB_CONNECT_TIMEOUT}s — skipping IB validation entirely.")
        return set()

    confirmed = set()
    total = len(symbols)
    for i, sym in enumerate(symbols):
        try:
            if app.check(make_contract(sym)):
                confirmed.add(sym)
        except Exception as e:
            print(f"  [ib] {sym}: {e}")
        if (i + 1) % 200 == 0:
            print(f"  [ib] {i + 1}/{total} checked, {len(confirmed)} confirmed so far")
        time.sleep(IB_INTER_REQUEST_SLEEP)

    app.disconnect()
    return confirmed


# ── watch.txt I/O ──────────────────────────────────────────────────────────────

def write_watchlist(symbols, path=WATCH_FILE):
    symbols = sorted(set(symbols))
    if os.path.exists(path):
        backup = f"{path}.bak.{datetime.now():%Y%m%d%H%M%S}"
        os.replace(path, backup)
        print(f"[write] backed up existing {path} -> {backup}")
    with open(path, "w") as f:
        f.write("\n".join(symbols) + "\n")
    print(f"[write] wrote {len(symbols)} symbols to {path}")


# ── Main ────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--yf-only", action="store_true",
                     help="Skip IB validation; keep only yfinance-confirmed symbols.")
    ap.add_argument("--require-both", action="store_true",
                     help="Keep only symbols confirmed by BOTH yfinance and IB "
                          "(runs IB against all symbols, not just yfinance's rejects).")
    ap.add_argument("--out", default=WATCH_FILE, help="Output path (default: watch.txt)")
    args = ap.parse_args()

    raw = fetch_tradingview_symbols()
    normalized = sorted({normalize_symbol(s) for s in raw if s.strip()})
    print(f"[normalize] {len(normalized)} unique symbols after dot->slash normalization")

    yf_confirmed = validate_yfinance(normalized)
    print(f"[yf] {len(yf_confirmed)}/{len(normalized)} confirmed by yfinance")

    if args.yf_only:
        final = yf_confirmed
    else:
        if args.require_both:
            ib_targets = normalized
        else:
            ib_targets = [s for s in normalized if s not in yf_confirmed]
        print(f"[ib] validating {len(ib_targets)} symbol(s) via IB ...")
        ib_confirmed = validate_ib(ib_targets)
        print(f"[ib] {len(ib_confirmed)}/{len(ib_targets)} confirmed by IB")

        if args.require_both:
            final = yf_confirmed & ib_confirmed
        else:
            final = yf_confirmed | ib_confirmed

    rejected = set(normalized) - final
    if rejected:
        print(f"[summary] {len(rejected)} symbol(s) confirmed by neither source, dropping:")
        for s in sorted(rejected)[:50]:
            print(f"    {s}")
        if len(rejected) > 50:
            print(f"    ... and {len(rejected) - 50} more")

    write_watchlist(final, path=args.out)


if __name__ == "__main__":
    main()