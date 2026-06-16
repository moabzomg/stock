#!/usr/bin/env python3
"""
Backtester — Simulate 1 Month of the Trading Algorithm
=======================================================
Uses IBKR historical data to replay the last 30 days
day-by-day and shows exactly what would have been bought,
sold, and what the P&L would have been.

RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  python3 backtest.py
"""

from ib_insync import IB, Stock
import pandas as pd
from datetime import datetime, timedelta

PORT      = 7496
HOST      = '127.0.0.1'
CLIENT_ID = 6

BUY_AMOUNT_GBP = 1.0
GBP_USD        = 1.27
MAX_POSITIONS  = 10
STOP_LOSS_PCT  = 0.03
TAKE_PROFIT    = 0.08
OVERBOUGHT_RSI = 75

TICKERS = [
    'NVR','BKNG','MSTR','CMG','AVGO','GOOG','GOOGL','META','AMZN',
    'NVDA','MSFT','AAPL','TSLA','NFLX','ADBE','CRM','NOW','ISRG',
    'ANET','PANW','MELI','LULU','AXON','DDOG','CRWD','ZS','COIN',
    'SHOP','SNOW','PLTR','RBLX','ROKU','TTD','U','TWLO','DOCU',
    'ZM','PTON','LYFT','UBER','DASH','ABNB','HOOD','RIVN','LCID',
    'F','GM','BAC','JPM','GS','MS','BLK','SCHW','C','WFC',
    'USB','PNC','TFC','COF','AXP','V','MA','PYPL','SQ','AFRM',
    'XOM','CVX','COP','SLB','OXY','MPC','PSX','VLO','HAL','BKR',
    'JNJ','UNH','PFE','ABBV','MRK','LLY','BMY','AMGN','GILD','REGN',
    'WMT','COST','TGT','HD','LOW','TJX','ROST','DLTR','DG',
    'BA','LMT','RTX','NOC','GD','LHX','HII','TDG','HEI','KTOS'
]

def compute_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))

def score_row(row, prev_row):
    score = 0
    price, ma50, ma200, rsi = row['close'], row['ma50'], row['ma200'], row['rsi']
    if pd.isna(ma200) or pd.isna(ma50) or pd.isna(rsi):
        return 0
    if price > ma200: score += 2
    if price > ma50:  score += 1
    if 40 <= rsi <= 65: score += 2
    elif 30 <= rsi < 40: score += 1
    if not pd.isna(prev_row['ma50']) and not pd.isna(prev_row['ma200']):
        if prev_row['ma50'] <= prev_row['ma200'] and ma50 > ma200:
            score += 2
        elif ma50 > ma200:
            score += 1
    return score

# ── Connect & fetch data ──────────────────────────────────────────────────────
print("\n" + "="*60)
print("  BACKTESTER — 1 Month Simulation")
print("="*60 + "\n")

ib = IB()
try:
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    print("✅ Connected to TWS\n")
except Exception as e:
    print(f"❌ {e}"); exit(1)

print(f"📥 Fetching 14 months of data for {len(TICKERS)} stocks...")
all_data = {}
for i, ticker in enumerate(TICKERS):
    try:
        contract = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='2 Y',
            barSizeSetting='1 day', whatToShow='TRADES',
            useRTH=True, formatDate=1,
        )
        if bars and len(bars) > 210:
            df = pd.DataFrame([{
                'date' : pd.to_datetime(str(b.date)),
                'close': b.close, 'high': b.high, 'low': b.low,
            } for b in bars]).set_index('date').sort_index()
            df['ma50']  = df['close'].rolling(50).mean()
            df['ma200'] = df['close'].rolling(200).mean()
            df['rsi']   = compute_rsi(df['close'])
            all_data[ticker] = df
            print(f"  [{i+1}/{len(TICKERS)}] ✅ {ticker} — {len(df)} bars")
        else:
            print(f"  [{i+1}/{len(TICKERS)}] ⚠️  {ticker} — not enough data")
    except Exception as e:
        print(f"  [{i+1}/{len(TICKERS)}] ❌ {ticker}: {e}")
    import time; time.sleep(0.4)

ib.disconnect()
print(f"\n🔌 Disconnected — got data for {len(all_data)} stocks\n")

# ── Get the last 30 trading days as simulation window ────────────────────────
all_dates = sorted(set(
    d for df in all_data.values() for d in df.index
))
sim_dates = all_dates[-30:]  # last 30 trading days

print(f"📅 Simulating: {sim_dates[0].date()} → {sim_dates[-1].date()}")
print(f"   ({len(sim_dates)} trading days)\n")

# ── Run simulation ────────────────────────────────────────────────────────────
positions  = {}   # ticker -> {buy_price, qty, buy_date, buy_score}
trade_log  = []   # every buy/sell event
cash_spent = 0.0

for day in sim_dates:
    day_scores = []

    for ticker, df in all_data.items():
        if day not in df.index:
            continue
        idx = df.index.get_loc(day)
        if idx < 1:
            continue
        row      = df.iloc[idx]
        prev_row = df.iloc[idx - 1]
        s        = score_row(row, prev_row)
        day_scores.append((ticker, s, row))

    # Sort by score
    day_scores.sort(key=lambda x: x[1], reverse=True)

    # ── Check sells ───────────────────────────────────────────────────────────
    for ticker in list(positions.keys()):
        if ticker not in all_data or day not in all_data[ticker].index:
            continue
        row       = all_data[ticker].loc[day]
        pos       = positions[ticker]
        price     = row['close']
        rsi       = row['rsi']
        ma50      = row['ma50']
        ma200     = row['ma200']
        pct       = (price - pos['buy_price']) / pos['buy_price']
        sell_why  = None

        if pct <= -STOP_LOSS_PCT:          sell_why = f'STOP LOSS {pct*100:.2f}%'
        elif pct >= TAKE_PROFIT:           sell_why = f'TAKE PROFIT {pct*100:.2f}%'
        elif not pd.isna(rsi) and rsi > OVERBOUGHT_RSI: sell_why = f'OVERBOUGHT RSI {rsi:.1f}'
        elif not pd.isna(ma50) and not pd.isna(ma200) and ma50 < ma200:
            sell_why = 'DEATH CROSS'

        if sell_why:
            pnl_usd = (price - pos['buy_price']) * pos['qty']
            pnl_gbp = pnl_usd / GBP_USD
            trade_log.append({
                'date'     : day.date(),
                'action'   : 'SELL',
                'ticker'   : ticker,
                'price'    : round(price, 2),
                'qty'      : pos['qty'],
                'pnl_gbp'  : round(pnl_gbp, 4),
                'reason'   : sell_why,
                'held_days': (day - pos['buy_date']).days,
            })
            del positions[ticker]

    # ── Check buys ────────────────────────────────────────────────────────────
    slots = MAX_POSITIONS - len(positions)
    bought_today = 0
    for ticker, score, row in day_scores:
        if bought_today >= slots:
            break
        if ticker in positions:
            continue
        if score < 4:
            break
        price = row['close']
        qty   = round((BUY_AMOUNT_GBP * GBP_USD) / price, 6)
        positions[ticker] = {
            'buy_price': price,
            'qty'      : qty,
            'buy_date' : day,
            'score'    : score,
        }
        cash_spent += BUY_AMOUNT_GBP
        trade_log.append({
            'date'     : day.date(),
            'action'   : 'BUY',
            'ticker'   : ticker,
            'price'    : round(price, 2),
            'qty'      : qty,
            'pnl_gbp'  : 0,
            'reason'   : f'Score {score}/7',
            'held_days': 0,
        })
        bought_today += 1

# ── Close any still-open positions at last price ──────────────────────────────
last_day = sim_dates[-1]
for ticker, pos in positions.items():
    if ticker in all_data and last_day in all_data[ticker].index:
        price   = all_data[ticker].loc[last_day]['close']
        pnl_usd = (price - pos['buy_price']) * pos['qty']
        pnl_gbp = pnl_usd / GBP_USD
        trade_log.append({
            'date'     : last_day.date(),
            'action'   : 'SELL (end)',
            'ticker'   : ticker,
            'price'    : round(price, 2),
            'qty'      : pos['qty'],
            'pnl_gbp'  : round(pnl_gbp, 4),
            'reason'   : 'Simulation ended',
            'held_days': (last_day - pos['buy_date']).days,
        })

# ── Results ───────────────────────────────────────────────────────────────────
log_df   = pd.DataFrame(trade_log)
sells    = log_df[log_df['action'].str.startswith('SELL')]
buys     = log_df[log_df['action'] == 'BUY']
total_pnl = sells['pnl_gbp'].sum()
winners  = sells[sells['pnl_gbp'] > 0]
losers   = sells[sells['pnl_gbp'] < 0]
win_rate = len(winners) / len(sells) * 100 if len(sells) > 0 else 0

print("="*60)
print("  BACKTEST RESULTS")
print("="*60)
print(f"  Period        : {sim_dates[0].date()} → {sim_dates[-1].date()}")
print(f"  Total buys    : {len(buys)}")
print(f"  Total sells   : {len(sells)}")
print(f"  Winners       : {len(winners)}  ({win_rate:.1f}% win rate)")
print(f"  Losers        : {len(losers)}")
print(f"  Total spent   : £{cash_spent:.2f}")
print(f"  Total P&L     : £{total_pnl:+.4f}")
if len(winners) > 0:
    print(f"  Best trade    : £{winners['pnl_gbp'].max():+.4f} ({winners.loc[winners['pnl_gbp'].idxmax(),'ticker']})")
if len(losers) > 0:
    print(f"  Worst trade   : £{losers['pnl_gbp'].min():+.4f} ({losers.loc[losers['pnl_gbp'].idxmin(),'ticker']})")
if len(sells) > 0:
    print(f"  Avg hold days : {sells['held_days'].mean():.1f}")

print(f"\n{'─'*60}")
print("  Full trade log:")
print(f"{'─'*60}")
print(log_df.to_string(index=False))

ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_df.to_csv(f'backtest_{ts}.csv', index=False)
print(f"\n💾 Saved: backtest_{ts}.csv")
print("="*60 + "\n")
