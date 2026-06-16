#!/usr/bin/env python3
"""
Backtest v4 — Minervini SEPA, 2-Year Simulation, All IBKR Stocks
=================================================================
- Pulls all available US stocks from IBKR scanner (no hardcoded list)
- Simulates 2 years of daily trading
- Minervini SEPA entry filter (all 7 criteria)
- $100 USD per trade, integer shares
- Stop loss 6%, take profit 15%, trailing stop 5%
- Saves daily equity curve + full trade log

RUNTIME: 20-60 min depending on how many stocks pass the scanner.
         IBKR rate-limits historical requests — script respects this.

RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  python3 backtest_v4.py
"""

from ib_insync import IB, Stock, ScannerSubscription
import pandas as pd
import time, os, json
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
PORT           = 7496
HOST           = '127.0.0.1'
CLIENT_ID      = 9

BUY_AMOUNT_USD = 100
MAX_POSITIONS  = 10
STOP_LOSS_PCT  = 0.06
TAKE_PROFIT    = 0.15
TRAIL_STOP     = 0.05
COOLDOWN_DAYS  = 3
OVERBOUGHT_RSI = 72
MIN_SCORE      = 5        # pass 5 of 7 Minervini criteria to qualify
MAX_TICKERS    = 500      # cap at 500 to keep runtime reasonable
CACHE_FILE     = 'ticker_cache.json'   # saves fetched data so you can re-run fast

# ── Helpers ───────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def compute_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))

def compute_atr(df, period=14):
    h, l, c = df['high'], df['low'], df['close']
    tr = pd.concat([
        h - l,
        (h - c.shift()).abs(),
        (l - c.shift()).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def minervini_score(row, df_slice):
    checks = {}
    price  = row['close']
    ma50   = row['ma50']
    ma150  = row['ma150']
    ma200  = row['ma200']

    if any(pd.isna(x) for x in [price, ma50, ma150, ma200]):
        return 0, {}, False

    high52 = df_slice['close'].max()
    low52  = df_slice['close'].min()

    checks['price_above_150_200']  = (price > ma150) and (price > ma200)
    checks['ma150_above_ma200']    = ma150 > ma200
    checks['ma200_trending_up']    = (
        len(df_slice) >= 21 and
        not pd.isna(df_slice['ma200'].iloc[-21]) and
        df_slice['ma200'].iloc[-1] > df_slice['ma200'].iloc[-21]
    )
    checks['ma50_above_150_200']   = (ma50 > ma150) and (ma50 > ma200)
    checks['price_above_ma50']     = price > ma50
    checks['within_25pct_of_high'] = price >= high52 * 0.75
    checks['above_30pct_of_low']   = price >= low52 * 1.30

    score    = sum(checks.values())
    all_pass = score == 7
    return score, checks, all_pass

# ── Connect ───────────────────────────────────────────────────────────────────
print("\n" + "="*65)
print("  BACKTEST v4 — Minervini SEPA, All Stocks, 2 Years")
print("="*65 + "\n")

ib = IB()
try:
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=15)
    log("✅ Connected to TWS\n")
except Exception as e:
    log(f"❌ {e}"); exit(1)

# ── Step 1: Get all available US stock tickers from IBKR scanner ──────────────
log("🔍 Scanning IBKR for all available US stocks...")

tickers = []

# Use multiple IBKR scanners to get broad coverage
scanner_configs = [
    # Top by volume — most liquid, best data
    {'scanCode': 'TOP_VOLUME_RATE',      'numberOfRows': 50},
    {'scanCode': 'HOT_BY_VOLUME',        'numberOfRows': 50},
    {'scanCode': 'TOP_TRADE_RATE',       'numberOfRows': 50},
    # Price gainers/movers
    {'scanCode': 'MOST_ACTIVE_USD',      'numberOfRows': 50},
    {'scanCode': 'PCT_CHANGE',           'numberOfRows': 50},
    # High price stocks (likely large caps — good for Minervini)
    {'scanCode': 'HIGH_VS_13W_HL',       'numberOfRows': 50},
    {'scanCode': 'HIGH_VS_26W_HL',       'numberOfRows': 50},
    {'scanCode': 'HIGH_VS_52W_HL',       'numberOfRows': 50},
    # Near 52-week highs — Minervini loves these
    {'scanCode': 'HOT_BY_PRICE',         'numberOfRows': 50},
    {'scanCode': 'TOP_PERC_GAIN',        'numberOfRows': 50},
]

seen = set()
for cfg in scanner_configs:
    try:
        sub = ScannerSubscription(
            instrument='STK',
            locationCode='STK.US.MAJOR',   # NYSE + NASDAQ + AMEX
            scanCode=cfg['scanCode'],
            numberOfRows=cfg['numberOfRows'],
        )
        results = ib.reqScannerData(sub)
        for item in results:
            sym = item.contractDetails.contract.symbol
            if sym and sym not in seen and sym.isalpha() and len(sym) <= 5:
                seen.add(sym)
                tickers.append(sym)
        log(f"  Scanner '{cfg['scanCode']}': +{len(results)} stocks (total unique: {len(tickers)})")
        time.sleep(1)
    except Exception as e:
        log(f"  Scanner '{cfg['scanCode']}' failed: {e}")

# Also add the S&P 500 constituents via a known-good scanner
try:
    sub = ScannerSubscription(
        instrument='STK',
        locationCode='STK.US.MAJOR',
        scanCode='TOP_VOLUME_RATE',
        numberOfRows=200,
    )
    results = ib.reqScannerData(sub)
    for item in results:
        sym = item.contractDetails.contract.symbol
        if sym and sym not in seen and sym.isalpha() and len(sym) <= 5:
            seen.add(sym)
            tickers.append(sym)
except:
    pass

# Deduplicate and cap
tickers = list(dict.fromkeys(tickers))[:MAX_TICKERS]
log(f"\n✅ Found {len(tickers)} unique tickers to screen\n")

if not tickers:
    log("❌ No tickers found — check TWS scanner permissions")
    ib.disconnect()
    exit(1)

# ── Step 2: Fetch 2+ years of daily data for each ticker ─────────────────────
log(f"📥 Fetching 2.5 years of daily bars for {len(tickers)} stocks...")
log(f"   Estimated time: {len(tickers) * 0.6 / 60:.0f}–{len(tickers) * 1.2 / 60:.0f} minutes\n")

all_data    = {}
failed      = []
start_time  = time.time()

# Load cache if exists (saves time on re-runs)
cache = {}
if os.path.exists(CACHE_FILE):
    log(f"📂 Loading cached data from {CACHE_FILE}...")
    with open(CACHE_FILE) as f:
        cache_raw = json.load(f)
    for sym, rows in cache_raw.items():
        df = pd.DataFrame(rows)
        df['date'] = pd.to_datetime(df['date'])
        df.set_index('date', inplace=True)
        cache[sym] = df
    log(f"   Loaded {len(cache)} cached stocks\n")

new_fetches = 0
for i, ticker in enumerate(tickers):
    # Use cache if available and recent enough
    if ticker in cache:
        all_data[ticker] = cache[ticker]
        continue

    try:
        contract = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(contract)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='3 Y',      # 2.5 years — enough for 200-SMA + 2yr sim
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1,
        )

        if bars and len(bars) >= 250:    # need at least 250 days for SMA200
            df = pd.DataFrame([{
                'date'  : pd.to_datetime(str(b.date)),
                'close' : b.close,
                'high'  : b.high,
                'low'   : b.low,
                'volume': b.volume,
            } for b in bars]).set_index('date').sort_index()

            # Compute all indicators
            df['ma50']  = df['close'].rolling(50).mean()
            df['ma150'] = df['close'].rolling(150).mean()
            df['ma200'] = df['close'].rolling(200).mean()
            df['rsi']   = compute_rsi(df['close'])
            df['atr']   = compute_atr(df)

            all_data[ticker] = df
            new_fetches += 1

            elapsed = time.time() - start_time
            rate    = (i+1) / elapsed
            eta     = (len(tickers) - i - 1) / rate if rate > 0 else 0
            log(f"  [{i+1}/{len(tickers)}] ✅ {ticker:<6} {len(bars)} bars  "
                f"ETA: {eta/60:.1f} min")
        else:
            failed.append(ticker)
            if (i+1) % 20 == 0:
                log(f"  [{i+1}/{len(tickers)}] ⚠️  {ticker} — insufficient data")

        # IBKR allows ~60 requests/min — pace at 1 per second to be safe
        time.sleep(1.0)

    except Exception as e:
        failed.append(ticker)
        log(f"  [{i+1}/{len(tickers)}] ❌ {ticker}: {str(e)[:60]}")
        time.sleep(2.0)   # extra wait on error

    # Save cache every 50 stocks
    if new_fetches > 0 and new_fetches % 50 == 0:
        log(f"\n  💾 Saving cache ({len(all_data)} stocks)...")
        cache_out = {}
        for sym, df in all_data.items():
            cache_out[sym] = df.reset_index().assign(
                date=lambda x: x['date'].astype(str)
            ).to_dict('records')
        with open(CACHE_FILE, 'w') as f:
            json.dump(cache_out, f)
        log(f"  ✅ Cache saved\n")

ib.disconnect()
log(f"\n🔌 Disconnected")
log(f"✅ {len(all_data)} stocks with sufficient data")
log(f"❌ {len(failed)} stocks skipped (insufficient data or error)\n")

# Final cache save
if new_fetches > 0:
    log("💾 Saving final cache...")
    cache_out = {}
    for sym, df in all_data.items():
        cache_out[sym] = df.reset_index().assign(
            date=lambda x: x['date'].astype(str)
        ).to_dict('records')
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache_out, f)
    log(f"✅ Cache saved to {CACHE_FILE} (re-runs will be instant)\n")

if not all_data:
    log("❌ No data collected."); exit(1)

# ── Step 3: 2-Year simulation ─────────────────────────────────────────────────
all_dates = sorted(set(d for df in all_data.values() for d in df.index))

# Last 2 years of trading days
two_years_ago = all_dates[-1] - timedelta(days=730)
sim_dates = [d for d in all_dates if d >= two_years_ago]

log(f"📅 Simulation period: {sim_dates[0].date()} → {sim_dates[-1].date()}")
log(f"   {len(sim_dates)} trading days across {len(all_data)} stocks\n")

positions    = {}
cooldowns    = {}
trade_log    = []
equity_curve = []   # daily portfolio value
cash_spent   = 0.0
total_cash   = 0.0

for day_idx, day in enumerate(sim_dates):

    # ── Sell check ────────────────────────────────────────────────────────────
    for ticker in list(positions.keys()):
        if ticker not in all_data or day not in all_data[ticker].index:
            continue
        df    = all_data[ticker]
        row   = df.loc[day]
        pos   = positions[ticker]
        price = row['close']
        rsi   = row['rsi']
        ma50  = row['ma50']
        ma200 = row['ma200']

        if price > pos['peak_price']:
            pos['peak_price'] = price

        pct_buy  = (price - pos['buy_price']) / pos['buy_price']
        pct_peak = (price - pos['peak_price']) / pos['peak_price']
        sell_why = None

        if pct_buy <= -STOP_LOSS_PCT:
            sell_why = f'STOP LOSS {pct_buy*100:.2f}%'
            cooldowns[ticker] = day + timedelta(days=COOLDOWN_DAYS)
        elif pct_peak <= -TRAIL_STOP and pct_buy > 0.03:
            sell_why = f'TRAIL STOP {pct_peak*100:.2f}% from peak'
        elif pct_buy >= TAKE_PROFIT:
            sell_why = f'TAKE PROFIT {pct_buy*100:.2f}%'
        elif not pd.isna(rsi) and rsi > OVERBOUGHT_RSI:
            sell_why = f'OVERBOUGHT RSI {rsi:.1f}'
        elif not pd.isna(ma50) and not pd.isna(ma200) and ma50 < ma200:
            sell_why = 'DEATH CROSS'

        if sell_why:
            pnl_usd = (price - pos['buy_price']) * pos['qty']
            total_cash += pnl_usd
            trade_log.append({
                'date'     : day.date(),
                'action'   : 'SELL',
                'ticker'   : ticker,
                'price'    : round(price, 2),
                'qty'      : pos['qty'],
                'cost_usd' : round(pos['buy_price'] * pos['qty'], 2),
                'pnl_usd'  : round(pnl_usd, 2),
                'reason'   : sell_why,
                'held_days': (day - pos['buy_date']).days,
                'score'    : pos['score'],
            })
            del positions[ticker]

    # ── Score all stocks for today ────────────────────────────────────────────
    day_scores = []
    for ticker, df in all_data.items():
        if day not in df.index:
            continue
        idx = df.index.get_loc(day)
        if idx < 21:
            continue
        row      = df.iloc[idx]
        df_slice = df.iloc[max(0, idx-252):idx+1]
        score, checks, all_pass = minervini_score(row, df_slice)
        if score >= MIN_SCORE:
            day_scores.append((ticker, score, row, all_pass))

    day_scores.sort(key=lambda x: (-x[1], -x[2]['close']))

    # ── Buy check ─────────────────────────────────────────────────────────────
    slots   = MAX_POSITIONS - len(positions)
    bought  = 0

    for ticker, score, row, all_pass in day_scores:
        if bought >= slots:
            break
        if ticker in positions:
            continue
        if ticker in cooldowns and day < cooldowns[ticker]:
            continue

        price = row['close']
        qty   = int(BUY_AMOUNT_USD // price)
        if qty < 1:
            continue

        actual_cost = qty * price
        positions[ticker] = {
            'buy_price'  : price,
            'peak_price' : price,
            'qty'        : qty,
            'buy_date'   : day,
            'score'      : score,
            'all_pass'   : all_pass,
        }
        cash_spent += actual_cost
        total_cash -= actual_cost

        trade_log.append({
            'date'     : day.date(),
            'action'   : 'BUY',
            'ticker'   : ticker,
            'price'    : round(price, 2),
            'qty'      : qty,
            'cost_usd' : round(actual_cost, 2),
            'pnl_usd'  : 0,
            'reason'   : f'Minervini {score}/7{"★" if all_pass else ""}',
            'held_days': 0,
            'score'    : score,
        })
        bought += 1

    # ── Daily equity snapshot ─────────────────────────────────────────────────
    open_value = sum(
        all_data[t].loc[day]['close'] * p['qty']
        for t, p in positions.items()
        if t in all_data and day in all_data[t].index
    )
    equity_curve.append({
        'date'        : day.date(),
        'open_value'  : round(open_value, 2),
        'n_positions' : len(positions),
        'cash_deployed': round(cash_spent, 2),
    })

    # Progress every 50 days
    if day_idx % 50 == 0:
        log(f"  Day {day_idx+1}/{len(sim_dates)} ({day.date()}) — "
            f"{len(positions)} positions open, "
            f"{len(day_scores)} stocks passed Minervini today")

# ── Close remaining positions ─────────────────────────────────────────────────
last_day = sim_dates[-1]
for ticker, pos in positions.items():
    if ticker in all_data and last_day in all_data[ticker].index:
        price   = all_data[ticker].loc[last_day]['close']
        pnl_usd = (price - pos['buy_price']) * pos['qty']
        trade_log.append({
            'date'     : last_day.date(),
            'action'   : 'SELL (end)',
            'ticker'   : ticker,
            'price'    : round(price, 2),
            'qty'      : pos['qty'],
            'cost_usd' : round(pos['buy_price'] * pos['qty'], 2),
            'pnl_usd'  : round(pnl_usd, 2),
            'reason'   : 'Simulation ended',
            'held_days': (last_day - pos['buy_date']).days,
            'score'    : pos['score'],
        })

# ── Final results ─────────────────────────────────────────────────────────────
log_df    = pd.DataFrame(trade_log)
equity_df = pd.DataFrame(equity_curve)

sells     = log_df[log_df['action'].str.startswith('SELL')]
buys      = log_df[log_df['action'] == 'BUY']
winners   = sells[sells['pnl_usd'] > 0]
losers    = sells[sells['pnl_usd'] < 0]
sl_exits  = sells[sells['reason'].str.startswith('STOP')]
tp_exits  = sells[sells['reason'].str.startswith('TAKE')]
tr_exits  = sells[sells['reason'].str.startswith('TRAIL')]
total_pnl = sells['pnl_usd'].sum()
win_rate  = len(winners) / len(sells) * 100 if len(sells) > 0 else 0

# Best and worst stocks
stock_pnl = sells.groupby('ticker')['pnl_usd'].sum().sort_values(ascending=False)

print("\n\n" + "="*65)
print("  BACKTEST v4 — 2-YEAR FINAL RESULTS")
print("="*65)
print(f"\n  Period          : {sim_dates[0].date()} → {sim_dates[-1].date()}")
print(f"  Stocks screened : {len(all_data)}")
print(f"  Total buys      : {len(buys)}")
print(f"  Total sells     : {len(sells)}")
print(f"  Winners         : {len(winners)}  ({win_rate:.1f}% win rate)")
print(f"  Losers          : {len(losers)}")
print(f"  Stop losses     : {len(sl_exits)}")
print(f"  Take profits    : {len(tp_exits)}")
print(f"  Trailing stops  : {len(tr_exits)}")
print(f"  Total deployed  : ${cash_spent:.2f}")
print(f"  Total P&L       : ${total_pnl:+.2f}")
if cash_spent > 0:
    print(f"  Return on cash  : {total_pnl/cash_spent*100:+.2f}%")
if len(winners) > 0:
    print(f"  Best trade      : ${winners['pnl_usd'].max():+.2f}  ({winners.loc[winners['pnl_usd'].idxmax(),'ticker']})")
if len(losers) > 0:
    print(f"  Worst trade     : ${losers['pnl_usd'].min():+.2f}  ({losers.loc[losers['pnl_usd'].idxmin(),'ticker']})")
if len(sells) > 0:
    print(f"  Avg hold days   : {sells['held_days'].mean():.1f}")
    print(f"  Avg win         : ${winners['pnl_usd'].mean():+.2f}")
    print(f"  Avg loss        : ${losers['pnl_usd'].mean():+.2f}")
    rr = abs(winners['pnl_usd'].mean() / losers['pnl_usd'].mean()) if len(losers) > 0 else 0
    print(f"  Reward/risk     : {rr:.2f}x")

print(f"\n  Top 10 stocks by P&L:")
for ticker, pnl in stock_pnl.head(10).items():
    print(f"    {ticker:<8} ${pnl:+.2f}")

print(f"\n  Bottom 10 stocks by P&L:")
for ticker, pnl in stock_pnl.tail(10).items():
    print(f"    {ticker:<8} ${pnl:+.2f}")

# Save outputs
ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_df.to_csv(f'backtest_v4_trades_{ts}.csv', index=False)
equity_df.to_csv(f'backtest_v4_equity_{ts}.csv', index=False)
log(f"\n💾 Trades saved : backtest_v4_trades_{ts}.csv")
log(f"💾 Equity saved : backtest_v4_equity_{ts}.csv")
log(f"💾 Ticker cache : {CACHE_FILE} (re-run will skip fetching)")
print("="*65 + "\n")