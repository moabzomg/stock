#!/usr/bin/env python3
"""
Backtest v2 — Improved Algorithm
================================
Fixes from v1 analysis:
  1. Stop loss widened 3% → 6% (volatile stocks need room)
  2. Take profit raised 8% → 15% (let winners run)
  3. Trailing stop: lock in gains as price rises
  4. Rebuy cooldown: 3-day ban after stop loss on same stock
  5. Better scoring 0-10: adds momentum + volume + price strength
  6. Min score raised to 7/10 (stricter entry)
  7. Overbought RSI tightened: sell at 72 not 75

RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  python3 backtest_v2.py
"""

from ib_insync import IB, Stock
import pandas as pd
import time
from datetime import datetime, timedelta

PORT      = 7496
HOST      = '127.0.0.1'
CLIENT_ID = 7

BUY_AMOUNT_GBP = 1.0
GBP_USD        = 1.27
MAX_POSITIONS  = 10
STOP_LOSS_PCT  = 0.06    # FIX 1: was 3%, now 6%
TAKE_PROFIT    = 0.15    # FIX 2: was 8%, now 15%
TRAIL_STOP     = 0.05    # FIX 3: NEW — trail 5% below peak price
MIN_SCORE      = 7       # FIX 5: was 4, now 7
OVERBOUGHT_RSI = 72      # FIX 7: was 75, tighter
COOLDOWN_DAYS  = 3       # FIX 4: NEW — days before rebuying a stopped stock

TICKERS = [
    'NVDA','MSFT','AAPL','TSLA','META','GOOGL','AMZN','NFLX','ADBE','CRM',
    'NOW','ISRG','ANET','PANW','MELI','LULU','DDOG','CRWD','COIN','SHOP',
    'JPM','GS','V','MA','PYPL','BAC','UNH','LLY','ABBV','AMGN',
    'XOM','CVX','WMT','COST','HD','LOW','BA','LMT','RTX','PLTR',
]

def compute_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))

def compute_atr(df, period=14):
    """Average True Range — measures how much a stock normally moves per day"""
    h, l, c = df['high'], df['low'], df['close']
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def score_stock_v2(row, prev_row, df_slice):
    """
    New scoring 0-10:
      +2  price above MA200          (long-term uptrend)
      +1  price above MA50           (short-term uptrend)
      +2  RSI in 45-65 zone          (healthy momentum, not overbought)
      +1  MA50 above MA200           (trend confirmed)
      +1  golden cross               (MA50 just crossed above MA200)
      +1  momentum: price > 10d ago  (recent upward push)
      +1  volume above 20d average   (buying pressure confirmed)
      +1  low ATR%: calm stock       (less chance of random stop loss)
    Max = 10
    """
    score = 0
    price  = row['close']
    ma50   = row['ma50']
    ma200  = row['ma200']
    rsi    = row['rsi']

    if pd.isna(ma200) or pd.isna(ma50) or pd.isna(rsi):
        return 0

    # 1. Price vs MA200 (+2)
    if price > ma200:
        score += 2

    # 2. Price vs MA50 (+1)
    if price > ma50:
        score += 1

    # 3. RSI zone (+2 for ideal, +1 for ok)
    if 45 <= rsi <= 65:
        score += 2
    elif 35 <= rsi < 45:
        score += 1

    # 4. MA50 vs MA200 (+1)
    if ma50 > ma200:
        score += 1

    # 5. Golden cross today (+1 bonus)
    if not pd.isna(prev_row['ma50']) and not pd.isna(prev_row['ma200']):
        if prev_row['ma50'] <= prev_row['ma200'] and ma50 > ma200:
            score += 1

    # 6. Momentum: price higher than 10 days ago (+1)
    if len(df_slice) >= 10:
        price_10d_ago = df_slice['close'].iloc[-10]
        if price > price_10d_ago * 1.01:  # at least 1% higher
            score += 1

    # 7. Volume above 20-day average (+1) — proxy: high > recent avg high
    if len(df_slice) >= 20 and 'volume' in df_slice.columns:
        avg_vol = df_slice['volume'].rolling(20).mean().iloc[-1]
        if not pd.isna(avg_vol) and row.get('volume', 0) > avg_vol:
            score += 1
    else:
        # No volume data — give half credit based on price strength
        if price > ma50 * 1.005:
            score += 1

    # 8. Low volatility (ATR% < 3%) — less random stop loss noise (+1)
    if 'atr' in row and not pd.isna(row['atr']):
        atr_pct = row['atr'] / price
        if atr_pct < 0.03:
            score += 1

    return score

# ── Connect & fetch ───────────────────────────────────────────────────────────
print("\n" + "="*62)
print("  BACKTEST v2 — Improved Algorithm")
print("="*62 + "\n")

ib = IB()
try:
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    print("✅ Connected\n")
except Exception as e:
    print(f"❌ {e}"); exit(1)

print(f"📥 Fetching 14 months for {len(TICKERS)} stocks...")
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
                'date'  : pd.to_datetime(str(b.date)),
                'close' : b.close,
                'high'  : b.high,
                'low'   : b.low,
                'volume': b.volume,
            } for b in bars]).set_index('date').sort_index()

            df['ma50']  = df['close'].rolling(50).mean()
            df['ma200'] = df['close'].rolling(200).mean()
            df['rsi']   = compute_rsi(df['close'])
            df['atr']   = compute_atr(df)
            all_data[ticker] = df
            print(f"  [{i+1}/{len(TICKERS)}] ✅ {ticker}")
        else:
            print(f"  [{i+1}/{len(TICKERS)}] ⚠️  {ticker} — not enough data")
    except Exception as e:
        print(f"  [{i+1}/{len(TICKERS)}] ❌ {ticker}: {e}")
    time.sleep(0.4)

ib.disconnect()
print(f"\n🔌 Disconnected — {len(all_data)} stocks ready\n")

# ── Simulation ────────────────────────────────────────────────────────────────
all_dates   = sorted(set(d for df in all_data.values() for d in df.index))
sim_dates   = all_dates[-30:]
positions   = {}   # ticker -> {buy_price, qty, buy_date, peak_price}
cooldowns   = {}   # ticker -> date when cooldown expires
trade_log   = []
cash_spent  = 0.0

print(f"📅 Simulating: {sim_dates[0].date()} → {sim_dates[-1].date()}\n")

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
        df_slice = df.iloc[max(0, idx-20):idx+1]
        s        = score_stock_v2(row, prev_row, df_slice)
        day_scores.append((ticker, s, row))

    day_scores.sort(key=lambda x: x[1], reverse=True)

    # ── Sell check ────────────────────────────────────────────────────────────
    for ticker in list(positions.keys()):
        if ticker not in all_data or day not in all_data[ticker].index:
            continue

        row       = all_data[ticker].loc[day]
        pos       = positions[ticker]
        price     = row['close']
        rsi       = row['rsi']
        ma50      = row['ma50']
        ma200     = row['ma200']

        # Update trailing stop peak
        if price > pos['peak_price']:
            pos['peak_price'] = price

        pct_from_buy  = (price - pos['buy_price']) / pos['buy_price']
        pct_from_peak = (price - pos['peak_price']) / pos['peak_price']
        sell_why = None

        # FIX 1: wider stop loss at 6%
        if pct_from_buy <= -STOP_LOSS_PCT:
            sell_why = f'STOP LOSS {pct_from_buy*100:.2f}%'
            cooldowns[ticker] = day + timedelta(days=COOLDOWN_DAYS)  # FIX 4

        # FIX 3: trailing stop — if risen then fallen 5% from peak
        elif pct_from_peak <= -TRAIL_STOP and pct_from_buy > 0.03:
            sell_why = f'TRAIL STOP {pct_from_peak*100:.2f}% from peak'

        # FIX 2: higher take profit at 15%
        elif pct_from_buy >= TAKE_PROFIT:
            sell_why = f'TAKE PROFIT {pct_from_buy*100:.2f}%'

        # FIX 7: tighter overbought RSI
        elif not pd.isna(rsi) and rsi > OVERBOUGHT_RSI:
            sell_why = f'OVERBOUGHT RSI {rsi:.1f}'

        elif not pd.isna(ma50) and not pd.isna(ma200) and ma50 < ma200:
            sell_why = 'DEATH CROSS'

        if sell_why:
            pnl_gbp = ((price - pos['buy_price']) * pos['qty']) / GBP_USD
            trade_log.append({
                'date'     : day.date(),
                'action'   : 'SELL',
                'ticker'   : ticker,
                'price'    : round(price, 2),
                'qty'      : pos['qty'],
                'pnl_gbp'  : round(pnl_gbp, 4),
                'reason'   : sell_why,
                'held_days': (day - pos['buy_date']).days,
                'score'    : pos['score'],
            })
            del positions[ticker]

    # ── Buy check ─────────────────────────────────────────────────────────────
    slots = MAX_POSITIONS - len(positions)
    bought = 0
    for ticker, score, row in day_scores:
        if bought >= slots:
            break
        if score < MIN_SCORE:  # FIX 5: stricter min score
            break
        if ticker in positions:
            continue
        # FIX 4: cooldown check
        if ticker in cooldowns and day < cooldowns[ticker]:
            continue

        price = row['close']
        qty   = round((BUY_AMOUNT_GBP * GBP_USD) / price, 6)
        positions[ticker] = {
            'buy_price'  : price,
            'peak_price' : price,   # FIX 3: track for trailing stop
            'qty'        : qty,
            'buy_date'   : day,
            'score'      : score,
        }
        cash_spent += BUY_AMOUNT_GBP
        trade_log.append({
            'date'     : day.date(),
            'action'   : 'BUY',
            'ticker'   : ticker,
            'price'    : round(price, 2),
            'qty'      : qty,
            'pnl_gbp'  : 0,
            'reason'   : f'Score {score}/10',
            'held_days': 0,
            'score'    : score,
        })
        bought += 1

# ── Close open positions at end ───────────────────────────────────────────────
last_day = sim_dates[-1]
for ticker, pos in positions.items():
    if ticker in all_data and last_day in all_data[ticker].index:
        price   = all_data[ticker].loc[last_day]['close']
        pnl_gbp = ((price - pos['buy_price']) * pos['qty']) / GBP_USD
        trade_log.append({
            'date'     : last_day.date(),
            'action'   : 'SELL (end)',
            'ticker'   : ticker,
            'price'    : round(price, 2),
            'qty'      : pos['qty'],
            'pnl_gbp'  : round(pnl_gbp, 4),
            'reason'   : 'Simulation ended',
            'held_days': (last_day - pos['buy_date']).days,
            'score'    : pos['score'],
        })

# ── Results comparison ────────────────────────────────────────────────────────
log_df  = pd.DataFrame(trade_log)
sells   = log_df[log_df['action'].str.startswith('SELL')]
buys    = log_df[log_df['action'] == 'BUY']
winners = sells[sells['pnl_gbp'] > 0]
losers  = sells[sells['pnl_gbp'] < 0]
sl_exits = sells[sells['reason'].str.startswith('STOP')]
tp_exits = sells[sells['reason'].str.startswith('TAKE')]
tr_exits = sells[sells['reason'].str.startswith('TRAIL')]

win_rate  = len(winners) / len(sells) * 100 if len(sells) > 0 else 0
total_pnl = sells['pnl_gbp'].sum()

print("=" * 62)
print("  BACKTEST v2 RESULTS vs v1")
print("=" * 62)
print(f"  {'Metric':<28} {'v1 (old)':>10} {'v2 (new)':>10}")
print(f"  {'─'*52}")
print(f"  {'Total buys':<28} {'54':>10} {len(buys):>10}")
print(f"  {'Win rate':<28} {'27.8%':>10} {win_rate:>9.1f}%")
print(f"  {'Stop loss exits':<28} {'34':>10} {len(sl_exits):>10}")
print(f"  {'Take profit exits':<28} {'8':>10} {len(tp_exits):>10}")
print(f"  {'Trailing stop exits':<28} {'0':>10} {len(tr_exits):>10}")
print(f"  {'Total P&L':<28} {'£-0.5033':>10} £{total_pnl:>+.4f}")
if len(winners) > 0:
    print(f"  {'Avg win':<28} {'£+0.0661':>10} £{winners['pnl_gbp'].mean():>+.4f}")
if len(losers) > 0:
    print(f"  {'Avg loss':<28} {'£-0.0404':>10} £{losers['pnl_gbp'].mean():>+.4f}")
if len(sells) > 0:
    print(f"  {'Avg hold days':<28} {'8.6':>10} {sells['held_days'].mean():>9.1f}")

print(f"\n{'─'*62}")
print("  Full trade log:")
print(f"{'─'*62}")
print(log_df[['date','action','ticker','price','pnl_gbp','reason','held_days','score']].to_string(index=False))

ts = datetime.now().strftime('%Y%m%d_%H%M%S')
log_df.to_csv(f'backtest_v2_{ts}.csv', index=False)
print(f"\n💾 Saved: backtest_v2_{ts}.csv")
print("=" * 62 + "\n")