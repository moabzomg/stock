#!/usr/bin/env python3
"""
Stock Scraper — All Available US Stocks, Last Month
====================================================
Fixed:
  - 'primaryExch' accessed from contractDetails not contract
  - Scanner fallback: if IBKR scanner fails, uses S&P500 + NASDAQ100 list
  - Safe empty-dataframe handling

RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  python3 scrape_stocks.py
"""

from ib_insync import IB, Stock, ScannerSubscription
import pandas as pd
import time
from datetime import datetime

PORT      = 7496
HOST      = '127.0.0.1'
CLIENT_ID = 10

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── Broad fallback ticker list (S&P500 + NASDAQ100 + extras) ─────────────────
# Used if IBKR scanner permissions are not enabled
FALLBACK_TICKERS = [
    # Mega cap tech
    'AAPL','MSFT','NVDA','GOOGL','GOOG','AMZN','META','TSLA','AVGO','ORCL',
    'NFLX','ADBE','CRM','NOW','INTC','AMD','QCOM','TXN','MU','AMAT',
    'LRCX','KLAC','MRVL','MCHP','SNPS','CDNS','FTNT','PANW','CRWD','DDOG',
    'ZS','OKTA','SNOW','PLTR','COIN','HOOD','RBLX','UBER','LYFT','DASH',
    # Financials
    'JPM','BAC','GS','MS','WFC','C','USB','PNC','TFC','COF',
    'AXP','V','MA','PYPL','SQ','AFRM','BLK','SCHW','ICE','CME',
    # Healthcare
    'UNH','JNJ','LLY','ABBV','MRK','PFE','BMY','AMGN','GILD','REGN',
    'VRTX','ISRG','BSX','MDT','ELV','CI','HUM','CVS','MCK','ABC',
    # Consumer
    'AMZN','WMT','COST','TGT','HD','LOW','TJX','ROST','DLTR','DG',
    'MCD','SBUX','CMG','YUM','QSR','BKNG','ABNB','MAR','HLT','H',
    # Energy
    'XOM','CVX','COP','SLB','OXY','MPC','PSX','VLO','HAL','BKR',
    # Industrials
    'BA','LMT','RTX','NOC','GD','CAT','DE','HON','GE','MMM',
    'UPS','FDX','CSX','NSC','UNP',
    # Consumer tech / growth
    'SHOP','MELI','SE','GRAB','NTES','BIDU','JD','PDD','BABA',
    'SPOT','PINS','SNAP','TWTR','MTCH','IAC','ZM','DOCU','PTON',
    # Autos
    'TSLA','F','GM','RIVN','LCID','NIO','LI','XPEV',
    # ETFs (good for volume reference)
    'SPY','QQQ','IWM','DIA','GLD','SLV','TLT','HYG','XLF','XLK',
    # More large caps
    'BRK.B','PG','KO','PEP','PM','MO','MDLZ','CL','EL','NKE',
    'LULU','PVH','VFC','HBI','UA','DECK','ONON','CROX',
    # Semiconductors extra
    'ON','WOLF','SWKS','QRVO','MPWR','ENTG','ACLS','MKSI',
    # Biotech
    'MRNA','BNTX','BIIB','ILMN','IONS','ALNY','SRPT','BMRN',
    # REITs
    'PLD','AMT','EQIX','CCI','SPG','O','VICI','WELL','AVB','EQR',
    # Banks extra
    'RF','HBAN','KEY','CFG','MTB','FITB','ZION','CMA','WAL','SIVB',
]

# ── Connect ───────────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  Stock Scraper — US Stocks, Last Month")
print("="*60 + "\n")

ib = IB()
try:
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=15)
    log("✅ Connected\n")
except Exception as e:
    log(f"❌ {e}"); exit(1)

# ── Step 1: Try IBKR scanners, fall back to static list ──────────────────────
log("🔍 Attempting IBKR scanner discovery...")

SCANNERS = [
    ('TOP_VOLUME_RATE', 200),
    ('HOT_BY_VOLUME',   200),
    ('MOST_ACTIVE_USD', 200),
    ('TOP_PERC_GAIN',   200),
    ('TOP_PERC_LOSE',   200),
    ('HIGH_VS_52W_HL',  200),
]

seen    = set()
tickers = []

for scan_code, n_rows in SCANNERS:
    try:
        sub = ScannerSubscription(
            instrument='STK',
            locationCode='STK.US.MAJOR',
            scanCode=scan_code,
            numberOfRows=n_rows,
        )
        results = ib.reqScannerData(sub)
        added = 0
        for item in results:
            try:
                # FIX: access contract details correctly
                cd  = item.contractDetails
                con = cd.contract if hasattr(cd, 'contract') else item.contract
                sym = con.symbol

                # FIX: primaryExch is on contractDetails, not contract
                exch = getattr(cd, 'primaryExch', None) or \
                       getattr(con, 'primaryExch', None) or \
                       getattr(con, 'exchange', 'SMART') or 'SMART'

                if sym and sym not in seen and sym.isalpha() and len(sym) <= 5:
                    seen.add(sym)
                    tickers.append({'symbol': sym, 'exchange': exch, 'source': scan_code})
                    added += 1
            except Exception:
                continue

        log(f"  ✅ {scan_code}: +{added} stocks (total: {len(tickers)})")
        time.sleep(0.8)

    except Exception as e:
        log(f"  ⚠️  {scan_code} failed: {str(e)[:60]}")
        time.sleep(1)

# ── Fallback if scanner returned nothing ──────────────────────────────────────
if len(tickers) == 0:
    log("\n⚠️  IBKR scanner returned 0 results.")
    log("   This usually means market data subscriptions aren't enabled.")
    log("   Falling back to built-in list of 250+ major US stocks...\n")
    for sym in FALLBACK_TICKERS:
        clean = sym.replace('.', ' ')  # BRK.B → BRK B for IBKR
        tickers.append({'symbol': clean, 'exchange': 'SMART', 'source': 'fallback'})
else:
    # Supplement scanner results with fallback to ensure good coverage
    for sym in FALLBACK_TICKERS:
        clean = sym.replace('.', ' ')
        if sym not in seen and clean not in seen:
            seen.add(sym)
            tickers.append({'symbol': clean, 'exchange': 'SMART', 'source': 'fallback'})

# Deduplicate
seen2    = set()
unique   = []
for t in tickers:
    if t['symbol'] not in seen2:
        seen2.add(t['symbol'])
        unique.append(t)
tickers = unique

log(f"\n📋 Total tickers to fetch: {len(tickers)}\n")
log(f"📥 Fetching last 1 month of daily OHLCV data...")
log(f"   Estimated time: {len(tickers)*0.7/60:.0f}–{len(tickers)*1.2/60:.0f} minutes\n")

# ── Step 2: Fetch 1 month of data ─────────────────────────────────────────────
rows_all = []
summary  = []
failed   = []
success  = 0

for i, t in enumerate(tickers):
    sym = t['symbol']
    try:
        contract = Stock(sym, 'SMART', 'USD')
        ib.qualifyContracts(contract)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='1 M',
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1,
        )

        if not bars:
            failed.append(sym)
            time.sleep(0.5)
            continue

        closes  = [b.close  for b in bars]
        volumes = [b.volume for b in bars]
        highs   = [b.high   for b in bars]
        lows    = [b.low    for b in bars]
        latest  = bars[-1]
        first   = bars[0]

        month_chg = (latest.close - first.open) / first.open * 100 if first.open else 0

        for b in bars:
            rows_all.append({
                'symbol' : sym,
                'date'   : str(b.date),
                'open'   : b.open,
                'high'   : b.high,
                'low'    : b.low,
                'close'  : b.close,
                'volume' : b.volume,
            })

        summary.append({
            'symbol'          : sym,
            'source'          : t['source'],
            'bars'            : len(bars),
            'date_from'       : str(bars[0].date),
            'date_to'         : str(bars[-1].date),
            'open_month'      : round(first.open, 4),
            'close_latest'    : round(latest.close, 4),
            'high_month'      : round(max(highs), 4),
            'low_month'       : round(min(lows), 4),
            'month_change_pct': round(month_chg, 2),
            'avg_daily_volume': int(sum(volumes) / len(volumes)) if volumes else 0,
            'latest_volume'   : latest.volume,
        })

        success += 1

        if (i + 1) % 20 == 0 or i < 3:
            log(f"  [{i+1:>4}/{len(tickers)}] ✅ {sym:<7} "
                f"close=${latest.close:>8.2f}  "
                f"month {month_chg:>+6.1f}%  "
                f"vol {latest.volume:>12,}")

        time.sleep(0.6)

    except Exception as e:
        failed.append(sym)
        err = str(e)
        if 'No security' not in err and 'No market' not in err:
            log(f"  [{i+1:>4}/{len(tickers)}] ❌ {sym}: {err[:50]}")
        time.sleep(0.7)

ib.disconnect()
log(f"\n🔌 Disconnected")
log(f"✅ Fetched : {success} stocks")
log(f"❌ Failed  : {len(failed)} stocks\n")

# ── Step 3: Save & report ─────────────────────────────────────────────────────
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

df_ohlcv = pd.DataFrame(rows_all)
df_sum   = pd.DataFrame(summary)

if df_ohlcv.empty:
    log("❌ No data to save — check TWS connection and market data subscriptions")
    exit(1)

# Sort summary by month change
df_sum = df_sum.sort_values('month_change_pct', ascending=False).reset_index(drop=True)

ohlcv_file   = f'stocks_ohlcv_{ts}.csv'
summary_file = f'stocks_summary_{ts}.csv'
failed_file  = f'stocks_failed_{ts}.txt'

df_ohlcv.to_csv(ohlcv_file, index=False)
df_sum.to_csv(summary_file, index=False)
with open(failed_file, 'w') as f:
    f.write('\n'.join(failed))

# ── Print report ──────────────────────────────────────────────────────────────
print("\n" + "="*60)
print("  SCRAPE COMPLETE")
print("="*60)
print(f"  Tickers attempted : {len(tickers)}")
print(f"  Successfully saved: {success}")
print(f"  Failed            : {len(failed)}")
print(f"  Total OHLCV rows  : {len(rows_all)}")
print(f"  Date range        : {df_sum['date_from'].min()} → {df_sum['date_to'].max()}")
print(f"  Lowest price      : ${df_sum['close_latest'].min():.2f}  "
      f"({df_sum.loc[df_sum['close_latest'].idxmin(),'symbol']})")
print(f"  Highest price     : ${df_sum['close_latest'].max():.2f}  "
      f"({df_sum.loc[df_sum['close_latest'].idxmax(),'symbol']})")
print(f"  Median price      : ${df_sum['close_latest'].median():.2f}")

print(f"\n  🟢 Top 15 gainers this month:")
print(f"  {'Symbol':<8} {'Change':>8}  {'Close':>8}  {'Avg Volume':>14}")
print(f"  {'─'*46}")
for _, r in df_sum.head(15).iterrows():
    print(f"  {r['symbol']:<8} {r['month_change_pct']:>+7.2f}%  "
          f"${r['close_latest']:>7.2f}  {r['avg_daily_volume']:>14,}")

print(f"\n  🔴 Top 15 losers this month:")
print(f"  {'Symbol':<8} {'Change':>8}  {'Close':>8}  {'Avg Volume':>14}")
print(f"  {'─'*46}")
for _, r in df_sum.tail(15).iterrows():
    print(f"  {r['symbol']:<8} {r['month_change_pct']:>+7.2f}%  "
          f"${r['close_latest']:>7.2f}  {r['avg_daily_volume']:>14,}")

print(f"\n  💹 Top 10 most active by avg daily volume:")
print(f"  {'Symbol':<8} {'Avg Volume':>14}  {'Close':>8}")
print(f"  {'─'*36}")
for _, r in df_sum.nlargest(10, 'avg_daily_volume').iterrows():
    print(f"  {r['symbol']:<8} {r['avg_daily_volume']:>14,}  ${r['close_latest']:>7.2f}")

print(f"\n  💾 Files saved:")
print(f"     {ohlcv_file}")
print(f"     {summary_file}")
print(f"     {failed_file}")
print("="*60 + "\n")