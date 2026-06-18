"""
MA200 + MA500 Screener — Major Stocks + FX Pairs
=================================================
• Async API with hard per-request timeout (fixes Python 3.12 hang)
• No qualifyContracts (was the original hang culprit)
• FX uses BID bars (MIDPOINT needs paid subscription → Error 162)
• BUY / SELL / NEUTRAL signal per instrument
• Last 5 closes shown in terminal
• Auto-saves timestamped CSV

RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  pip install "ib_insync>=0.9.86" pandas
  python3 ma200_ma500.py
"""

import asyncio, sys
from datetime import datetime
import pandas as pd
from ib_insync import IB, Stock, Forex, util

util.logToConsole(False)   # silence ib_insync debug noise

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG — tweak these to your needs
# ══════════════════════════════════════════════════════════════════════════════

TWS_HOST        = "127.0.0.1"
TWS_PORT        = 7497          # 7497 = TWS  |  4001 = IB Gateway
CLIENT_ID       = 15
REQUEST_TIMEOUT = 25            # seconds before skipping one instrument
SLEEP_BETWEEN   = 1.5           # seconds between requests (IBKR pacing)
HISTORY_PERIOD  = "3 Y"         # how far back to fetch

# ── Major stocks ──────────────────────────────────────────────────────────────
# (symbol, exchange, currency, friendly_name)
STOCKS = [
    # ── US Mega-cap Tech ──────────────────────────────────────────────────────
    ("AAPL",  "SMART", "USD", "Apple"),
    ("MSFT",  "SMART", "USD", "Microsoft"),
    ("GOOGL", "SMART", "USD", "Alphabet"),
    ("AMZN",  "SMART", "USD", "Amazon"),
    ("NVDA",  "SMART", "USD", "NVIDIA"),
    ("META",  "SMART", "USD", "Meta"),
    ("TSLA",  "SMART", "USD", "Tesla"),
    ("AVGO",  "SMART", "USD", "Broadcom"),
    ("ORCL",  "SMART", "USD", "Oracle"),
    ("CSCO",  "SMART", "USD", "Cisco"),
    # ── US Financials ─────────────────────────────────────────────────────────
    ("JPM",   "SMART", "USD", "JPMorgan"),
    ("BAC",   "SMART", "USD", "Bank of America"),
    ("WFC",   "SMART", "USD", "Wells Fargo"),
    ("GS",    "SMART", "USD", "Goldman Sachs"),
    ("MS",    "SMART", "USD", "Morgan Stanley"),
    ("V",     "SMART", "USD", "Visa"),
    ("MA",    "SMART", "USD", "Mastercard"),
    # ── US Healthcare ─────────────────────────────────────────────────────────
    ("JNJ",   "SMART", "USD", "J&J"),
    ("UNH",   "SMART", "USD", "UnitedHealth"),
    ("LLY",   "SMART", "USD", "Eli Lilly"),
    ("ABBV",  "SMART", "USD", "AbbVie"),
    ("PFE",   "SMART", "USD", "Pfizer"),
    ("MRK",   "SMART", "USD", "Merck"),
    ("AZN",   "SMART", "USD", "AstraZeneca"),
    # ── US Consumer / Retail ─────────────────────────────────────────────────
    ("AMZN",  "SMART", "USD", "Amazon"),
    ("WMT",   "SMART", "USD", "Walmart"),
    ("COST",  "SMART", "USD", "Costco"),
    ("MCD",   "SMART", "USD", "McDonald's"),
    ("KO",    "SMART", "USD", "Coca-Cola"),
    ("PEP",   "SMART", "USD", "PepsiCo"),
    ("PG",    "SMART", "USD", "P&G"),
    ("HD",    "SMART", "USD", "Home Depot"),
    # ── US Energy ────────────────────────────────────────────────────────────
    ("XOM",   "SMART", "USD", "ExxonMobil"),
    ("CVX",   "SMART", "USD", "Chevron"),
    ("SHEL",  "SMART", "USD", "Shell"),
    ("BP",    "SMART", "USD", "BP"),
    # ── US Industrials ───────────────────────────────────────────────────────
    ("CAT",   "SMART", "USD", "Caterpillar"),
    ("BA",    "SMART", "USD", "Boeing"),
    ("GE",    "SMART", "USD", "GE"),
    ("RTX",   "SMART", "USD", "RTX"),
    # ── ETFs ─────────────────────────────────────────────────────────────────
    ("SPY",   "SMART", "USD", "S&P 500 ETF"),
    ("QQQ",   "SMART", "USD", "Nasdaq ETF"),
    ("IWM",   "SMART", "USD", "Russell 2000 ETF"),
    ("DIA",   "SMART", "USD", "Dow ETF"),
    ("GLD",   "SMART", "USD", "Gold ETF"),
]

# Deduplicate by symbol (AMZN appears twice above as example)
seen = set()
STOCKS = [s for s in STOCKS if not (s[0] in seen or seen.add(s[0]))]

# ── FX Pairs ─────────────────────────────────────────────────────────────────
FX_PAIRS = [
    ("EUR", "USD"), ("GBP", "USD"), ("USD", "JPY"),
    ("USD", "CHF"), ("AUD", "USD"), ("USD", "CAD"),
    ("NZD", "USD"), ("EUR", "GBP"), ("EUR", "JPY"),
    ("GBP", "JPY"),
]

# ══════════════════════════════════════════════════════════════════════════════
# TERMINAL COLOURS
# ══════════════════════════════════════════════════════════════════════════════

RS="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
GRN="\033[92m"; RED="\033[91m"; YLW="\033[93m"; CYN="\033[96m"; WHT="\033[97m"

def c(text, *codes): return "".join(codes)+str(text)+RS

def signal_badge(sig):
    return {
        "BUY":     c(f" ▲ BUY     ", GRN, BOLD),
        "SELL":    c(f" ▼ SELL    ", RED, BOLD),
        "NEUTRAL": c(f" ● NEUTRAL ", YLW),
        "N/A":     c(f"   N/A     ", DIM),
    }.get(sig, sig)

def bar(pct, width=20):
    """Simple ASCII progress bar."""
    filled = int(width * pct)
    return c("█" * filled, CYN) + c("░" * (width - filled), DIM)

# ══════════════════════════════════════════════════════════════════════════════
# MA LOGIC
# ══════════════════════════════════════════════════════════════════════════════

def add_mas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ma200"] = df["close"].rolling(200, min_periods=200).mean()
    df["ma500"] = df["close"].rolling(500, min_periods=500).mean()
    return df

def compute_signal(df: pd.DataFrame) -> str:
    if df is None or len(df) < 200:
        return "N/A"
    df = add_mas(df)
    row = df.iloc[-1]
    p, m2, m5 = row["close"], row["ma200"], row["ma500"]
    if pd.isna(m2):
        return "N/A"
    if pd.isna(m5):                         # only MA200 available
        if p > m2: return "BUY"
        if p < m2: return "SELL"
        return "NEUTRAL"
    # Both MAs available — trend-aligned signal
    if p > m2 and m2 > m5: return "BUY"
    if p < m2 and m2 < m5: return "SELL"
    return "NEUTRAL"

def last5_str(df: pd.DataFrame) -> str:
    if df is None or df.empty: return "—"
    return "  ".join(
        f"{r['date'].strftime('%m/%d')}:{r['close']:.2f}"
        for _, r in df.tail(5).iterrows()
    )

def ma_values(df: pd.DataFrame):
    """Return (price, ma200, ma500) as formatted strings."""
    if df is None or df.empty:
        return "—", "—", "—"
    df = add_mas(df)
    row = df.iloc[-1]
    fmt = lambda v: f"{v:.4f}" if v < 100 else f"{v:.2f}"
    p   = fmt(row["close"])
    m2  = fmt(row["ma200"]) if not pd.isna(row["ma200"]) else "—"
    m5  = fmt(row["ma500"]) if not pd.isna(row["ma500"]) else "<500bars"
    return p, m2, m5

# ══════════════════════════════════════════════════════════════════════════════
# ASYNC FETCH  — asyncio.wait_for gives TRUE hard timeout (fixes Python 3.12)
# ══════════════════════════════════════════════════════════════════════════════

async def fetch_bars(ib: IB, contract, what: str, label: str) -> pd.DataFrame | None:
    """
    Fetch daily bars with a hard timeout and automatic retry on pacing.
    For FX:   whatToShow="BID"  (MIDPOINT → Error 162 without subscription)
    For STK:  whatToShow="ADJUSTED_LAST"
    """
    what_list = [what]
    if getattr(contract, "secType", "") == "CASH":
        what_list = ["BID", "ASK"]      # try BID first, fall back to ASK

    for w in what_list:
        for attempt in range(2):
            try:
                coro = ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime="",
                    durationStr=HISTORY_PERIOD,
                    barSizeSetting="1 day",
                    whatToShow=w,
                    useRTH=True,
                    formatDate=1,
                    keepUpToDate=False,
                )
                bars = await asyncio.wait_for(coro, timeout=REQUEST_TIMEOUT)
                if bars:
                    df = util.df(bars)
                    df["date"] = pd.to_datetime(df["date"])
                    return df[["date","open","high","low","close","volume"]].reset_index(drop=True)
                break   # empty result, try next whatToShow

            except asyncio.TimeoutError:
                print(c(f"⏱ ", YLW), end="", flush=True)
                break   # don't retry on timeout

            except Exception as exc:
                err = str(exc).lower()
                if "pacing" in err:
                    wait = 65 * (attempt + 1)
                    print(c(f"\n   ⏳ Pacing ({label}) — sleep {wait}s…", YLW))
                    await asyncio.sleep(wait)
                elif "162" in str(exc) or "cancelled" in err:
                    break   # Error 162: subscription missing, try next whatToShow
                else:
                    print(c(f" ERR ", RED), end="", flush=True)
                    return None

    return None

# ══════════════════════════════════════════════════════════════════════════════
# PRINT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def print_header():
    print()
    print(c("╔" + "═"*66 + "╗", CYN))
    print(c("║", CYN) + c("  MA200 + MA500 SCREENER  —  Major Stocks + FX Pairs".center(66), BOLD+WHT) + c("║", CYN))
    print(c("║", CYN) + c(f"  {datetime.now():%A %d %B %Y  %H:%M:%S}".center(66), DIM) + c("║", CYN))
    print(c("╚" + "═"*66 + "╝", CYN))

def print_section(title, subtitle=""):
    print()
    print(c(f"  ┌─ {title} " + ("─"*(60-len(title))), CYN))
    if subtitle:
        print(c(f"  │  {subtitle}", DIM))
    print(c("  └" + "─"*65, CYN))

def print_row(idx, total, name, label, price, ma200, ma500, signal, extra=""):
    pct  = idx / total
    prog = bar(pct, width=6)
    sig  = signal_badge(signal)
    name_col = c(f"{name:<20}", WHT)
    vals = c(f"P:{price:<10} MA200:{ma200:<10} MA500:{ma500}", DIM)
    print(f"  {prog} [{idx:02d}/{total}] {sig} {name_col}  {vals}{extra}")

def print_results_table(results: list[dict]):
    print()
    print(c("╔" + "═"*66 + "╗", CYN))
    print(c("║", CYN) + c("  FINAL RESULTS".center(66), BOLD+WHT) + c("║", CYN))
    print(c("╠" + "═"*66 + "╣", CYN))

    for sig_label, icon in [("BUY","▲"), ("SELL","▼"), ("NEUTRAL","●"), ("N/A","○")]:
        group = [r for r in results if r["signal"] == sig_label]
        if not group:
            continue

        badge = signal_badge(sig_label)
        print(c("║", CYN) + f"  {badge}  {len(group):>2} instruments".ljust(64) + c("║", CYN))
        print(c("╠" + "─"*66 + "╣", CYN))

        for r in group:
            tag  = c(f"[{r['type']:3s}]", DIM)
            name = f"{r['instrument']:<10}  {r['name']:<22}"
            p, m2, m5 = r["price"], r["ma200"], r["ma500"]
            vals = c(f"P:{p}  MA200:{m2}  MA500:{m5}", DIM)
            line = f"   {tag} {name} {vals}"
            # pad/truncate to fit column width
            visible_len = len(f"   [{r['type']:3s}] {r['instrument']:<10}  {r['name']:<22} P:{p}  MA200:{m2}  MA500:{m5}")
            pad = max(0, 65 - visible_len)
            print(c("║", CYN) + line + " "*pad + c("║", CYN))

        print(c("╠" + "═"*66 + "╣", CYN))

    # Counts summary
    buys  = sum(1 for r in results if r["signal"]=="BUY")
    sells = sum(1 for r in results if r["signal"]=="SELL")
    neut  = sum(1 for r in results if r["signal"]=="NEUTRAL")
    na    = sum(1 for r in results if r["signal"]=="N/A")
    summary = (c(f" ▲{buys} BUY", GRN) + "  " +
               c(f"▼{sells} SELL", RED) + "  " +
               c(f"●{neut} NEUTRAL", YLW) + "  " +
               c(f"○{na} N/A", DIM))
    print(c("║", CYN) + f"  {summary}".ljust(80) + c("║", CYN))
    print(c("╚" + "═"*66 + "╝", CYN))

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def run():
    ib = IB()
    print_header()

    # ── Connect (try TWS then IB Gateway) ─────────────────────────────────────
    print()
    connected = False
    for port, name in [(TWS_PORT, "TWS"), (4001, "IB Gateway")]:
        try:
            await ib.connectAsync(TWS_HOST, port, clientId=CLIENT_ID, timeout=10)
            print(c(f"  ✔  Connected to {name}  ({TWS_HOST}:{port})", GRN))
            connected = True
            break
        except Exception as e:
            print(c(f"  ✗  {name} port {port}: {e}", DIM))

    if not connected:
        print(c("\n  Cannot connect. Make sure TWS or IB Gateway is open", RED))
        print(c("  and API access is enabled (Edit → Global Config → API).\n", YLW))
        sys.exit(1)

    results = []

    # ── STOCKS ────────────────────────────────────────────────────────────────
    print_section(
        f"STOCKS  ({len(STOCKS)} instruments)",
        f"ADJUSTED_LAST bars  •  {HISTORY_PERIOD} history  •  {REQUEST_TIMEOUT}s timeout per request"
    )

    for idx, (sym, exch, cur, name) in enumerate(STOCKS, 1):
        contract = Stock(sym, exch, cur)
        df = await fetch_bars(ib, contract, "ADJUSTED_LAST", sym)

        sig        = compute_signal(df)
        rows       = len(df) if df is not None else 0
        p, m2, m5  = ma_values(df)

        print_row(idx, len(STOCKS), f"{sym} {name}", sym, p, m2, m5, sig,
                  extra=c(f"  [{rows}d]", DIM))

        results.append({
            "instrument": sym, "name": name, "type": "STK",
            "signal": sig, "rows": rows,
            "price": p, "ma200": m2, "ma500": m5,
            "last5": last5_str(df),
        })
        await asyncio.sleep(SLEEP_BETWEEN)

    # ── FX PAIRS ─────────────────────────────────────────────────────────────
    print_section(
        f"FX PAIRS  ({len(FX_PAIRS)} pairs)",
        "BID bars (no subscription needed)  •  MIDPOINT skipped to avoid Error 162"
    )

    for idx, (base, quote) in enumerate(FX_PAIRS, 1):
        label    = f"{base}/{quote}"
        contract = Forex(f"{base}{quote}")
        df       = await fetch_bars(ib, contract, "BID", label)

        sig        = compute_signal(df)
        rows       = len(df) if df is not None else 0
        p, m2, m5  = ma_values(df)

        print_row(idx, len(FX_PAIRS), label, label, p, m2, m5, sig,
                  extra=c(f"  [{rows}d]", DIM))

        results.append({
            "instrument": label, "name": label, "type": "FX",
            "signal": sig, "rows": rows,
            "price": p, "ma200": m2, "ma500": m5,
            "last5": last5_str(df),
        })
        await asyncio.sleep(SLEEP_BETWEEN)

    ib.disconnect()

    # ── RESULTS TABLE ────────────────────────────────────────────────────────
    print_results_table(results)

    # ── LAST 5 CLOSES (detail view) ───────────────────────────────────────────
    print()
    print(c("  LAST 5 CLOSES (date:price)", BOLD+CYN))
    print(c("  " + "─"*66, DIM))
    for r in results:
        tag = c(f"[{r['type']}]", DIM)
        print(f"  {tag}  {r['instrument']:<12}  {c(r['last5'], DIM)}")

    # ── CSV EXPORT ────────────────────────────────────────────────────────────
    csv_name = f"ma_screener_{datetime.now():%Y%m%d_%H%M%S}.csv"
    df_out = pd.DataFrame(results)
    # Expand last5 into columns
    df_out.to_csv(csv_name, index=False)
    print()
    print(c(f"  📄  Results saved → {csv_name}", CYN))
    print()


def main():
    util.run(run())   # ib_insync's event-loop runner (handles Python 3.12 correctly)

if __name__ == "__main__":
    main()