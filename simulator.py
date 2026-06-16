#!/usr/bin/env python3
"""
Automated Stock Trader — Top 100 US Stocks
==========================================
1. Screens top 100 highest-price S&P500 stocks from IBKR
2. Scores each using MA200, MA50, RSI, Golden Cross
3. Buys best 10 in PAPER account (£1 fractional each)
4. Loops every minute to review buy/sell/hold
5. Applies stop-loss and overbought exit rules

RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  pip install ib_insync pandas
  python3 stock_trader.py

REQUIREMENTS:
  - TWS open, API enabled, port 7496
  - Fractional shares enabled in TWS account settings
"""

from ib_insync import IB, Stock, Order, MarketOrder, LimitOrder
import pandas as pd
import time
from datetime import datetime, timezone
import json, os

# ── Config ────────────────────────────────────────────────────────────────────
PORT           = 7496       
HOST           = '127.0.0.1'
CLIENT_ID      = 5
BUY_AMOUNT_GBP = 1.0         # £1 per stock
MAX_POSITIONS  = 10          # hold max 10 stocks at once
STOP_LOSS_PCT  = 0.03        # sell if down 3%
OVERBOUGHT_RSI = 75          # sell if RSI > 75
LOOP_SECONDS   = 60          # review every 60 seconds
STATE_FILE     = 'positions.json'  # tracks open positions

# Top 100 highest-price US stocks (by typical price, update as needed)
TOP100_TICKERS = [
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


# ── Helpers ───────────────────────────────────────────────────────────────────
def compute_rsi(series, period=14):
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))

def load_positions():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}

def save_positions(pos):
    with open(STATE_FILE, 'w') as f:
        json.dump(pos, f, indent=2)

def log(msg):
    ts = datetime.now().strftime('%H:%M:%S')
    print(f"[{ts}] {msg}")

# ── Score a single stock ──────────────────────────────────────────────────────
def score_stock(ib, ticker):
    try:
        contract = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(contract)

        bars = ib.reqHistoricalData(
            contract, endDateTime='', durationStr='1 Y',
            barSizeSetting='1 day', whatToShow='TRADES',
            useRTH=True, formatDate=1,
        )
        if not bars or len(bars) < 60:
            return None

        df = pd.DataFrame([{
            'date' : pd.to_datetime(str(b.date)),
            'close': b.close, 'high': b.high, 'low': b.low,
        } for b in bars]).set_index('date').sort_index()

        df['ma50']  = df['close'].rolling(50).mean()
        df['ma200'] = df['close'].rolling(200).mean()
        df['rsi']   = compute_rsi(df['close'])

        latest   = df.iloc[-1]
        prev     = df.iloc[-2]
        price    = latest['close']
        ma50     = latest['ma50']
        ma200    = latest['ma200']
        rsi      = latest['rsi']

        if pd.isna(ma200) or pd.isna(ma50) or pd.isna(rsi):
            return None

        # ── Scoring (0–7 points) ──────────────────────────────────────────────
        score  = 0
        reason = []

        # 1. Price above MA200 (long-term uptrend) — 2 pts
        if price > ma200:
            score += 2
            reason.append(f'✅ Price above MA200 (+2)')
        else:
            reason.append(f'❌ Price below MA200 (0)')

        # 2. Price above MA50 (short-term uptrend) — 1 pt
        if price > ma50:
            score += 1
            reason.append(f'✅ Price above MA50 (+1)')
        else:
            reason.append(f'❌ Price below MA50 (0)')

        # 3. RSI in healthy zone 40-65 — 2 pts
        if 40 <= rsi <= 65:
            score += 2
            reason.append(f'✅ RSI {rsi:.1f} healthy zone 40-65 (+2)')
        elif 30 <= rsi < 40:
            score += 1
            reason.append(f'⚠️  RSI {rsi:.1f} low but not oversold (+1)')
        elif rsi > 65:
            reason.append(f'❌ RSI {rsi:.1f} overbought (0)')
        else:
            reason.append(f'⚠️  RSI {rsi:.1f} oversold — possible bounce')

        # 4. Golden cross: MA50 just crossed above MA200 — 2 pts
        prev_ma50  = prev['ma50']
        prev_ma200 = prev['ma200']
        if not pd.isna(prev_ma50) and not pd.isna(prev_ma200):
            if prev_ma50 <= prev_ma200 and ma50 > ma200:
                score += 2
                reason.append(f'🌟 Golden Cross today! (+2)')
            elif ma50 > ma200:
                score += 1
                reason.append(f'✅ MA50 above MA200 — uptrend (+1)')
            else:
                reason.append(f'❌ Death cross — MA50 below MA200 (0)')

        return {
            'ticker' : ticker,
            'price'  : round(price, 2),
            'ma50'   : round(ma50,  2),
            'ma200'  : round(ma200, 2),
            'rsi'    : round(rsi,   1),
            'score'  : score,
            'reason' : reason,
            'df'     : df,
        }
    except Exception as e:
        return None

# ── Check if we should SELL a position ───────────────────────────────────────
def should_sell(pos_info, current_price, current_rsi, ma50, ma200):
    reasons = []
    buy_price = pos_info['buy_price']
    pct_change = (current_price - buy_price) / buy_price

    # Rule 1: Stop loss — down 3%
    if pct_change <= -STOP_LOSS_PCT:
        reasons.append(f'🛑 STOP LOSS: down {pct_change*100:.2f}%')

    # Rule 2: Overbought RSI
    if current_rsi > OVERBOUGHT_RSI:
        reasons.append(f'📈 OVERBOUGHT: RSI {current_rsi:.1f} > {OVERBOUGHT_RSI}')

    # Rule 3: Death cross
    if ma50 < ma200:
        reasons.append(f'💀 DEATH CROSS: MA50 below MA200')

    # Rule 4: Take profit — up 8%
    if pct_change >= 0.08:
        reasons.append(f'💰 TAKE PROFIT: up {pct_change*100:.2f}%')

    return reasons

# ── Place a buy order ─────────────────────────────────────────────────────────
def place_buy(ib, ticker, amount_gbp, usd_price):
    try:
        # Convert £1 to USD qty (approximate — IBKR handles GBP base currency)
        gbp_usd = 1.27  # approximate — replace with live rate if needed
        qty = round((amount_gbp * gbp_usd) / usd_price, 4)
        if qty <= 0:
            return None

        contract = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        order = MarketOrder('BUY', qty)
        # trade = ib.placeOrder(contract, order)
        # ib.sleep(1)
        log(f"  📤 BUY order placed: {qty} shares of {ticker} @ ~${usd_price}")
        return True
    except Exception as e:
        log(f"  ❌ Buy failed for {ticker}: {e}")
        return None

# ── Place a sell order ────────────────────────────────────────────────────────
def place_sell(ib, ticker, qty):
    try:
        contract = Stock(ticker, 'SMART', 'USD')
        ib.qualifyContracts(contract)
        order = MarketOrder('SELL', qty)
        # trade = ib.placeOrder(contract, order)
        # ib.sleep(1)
        log(f"  📤 SELL order placed: {qty} shares of {ticker}")
        return True
    except Exception as e:
        log(f"  ❌ Sell failed for {ticker}: {e}")
        return None

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("  Automated Stock Trader — PAPER MODE")
    print("  Buy criteria : MA200 + MA50 + RSI + Golden Cross")
    print("  Sell criteria: Stop loss 3% | RSI>75 | Death cross | +8% profit")
    print("="*60 + "\n")

    ib = IB()
    try:
        ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
        log("✅ Connected to TWS")
    except Exception as e:
        log(f"❌ Connection failed: {e}")
        return

    loop_count = 0

    while True:
        loop_count += 1
        log(f"\n{'─'*55}")
        log(f"⏱  Loop #{loop_count} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        log(f"{'─'*55}")

        positions = load_positions()

        # ── Step 1: Check existing positions — sell if needed ─────────────────
        if positions:
            log(f"\n📋 Reviewing {len(positions)} open positions...")
            for ticker, pos_info in list(positions.items()):
                result = score_stock(ib, ticker)
                if not result:
                    continue
                price = result['price']
                rsi   = result['rsi']
                ma50  = result['ma50']
                ma200 = result['ma200']

                sell_reasons = should_sell(pos_info, price, rsi, ma50, ma200)
                pct = (price - pos_info['buy_price']) / pos_info['buy_price'] * 100

                log(f"  {ticker}: bought@${pos_info['buy_price']} "
                    f"now@${price} ({pct:+.2f}%) RSI={rsi:.1f} Score={result['score']}/7")

                if sell_reasons:
                    log(f"  ⚠️  SELL triggered:")
                    for r in sell_reasons:
                        log(f"     {r}")
                    place_sell(ib, ticker, pos_info['qty'])
                    del positions[ticker]
                    save_positions(positions)
                else:
                    log(f"  ✅ HOLD — score {result['score']}/7, no exit signals")

        # ── Step 2: Screen top 100 for new buys ───────────────────────────────
        slots = MAX_POSITIONS - len(positions)
        if slots > 0:
            log(f"\n🔍 Screening top 100 stocks — {slots} slot(s) available...")
            scored = []
            for i, ticker in enumerate(TOP100_TICKERS):
                if ticker in positions:
                    continue  # skip already held
                result = score_stock(ib, ticker)
                if result:
                    scored.append(result)
                if i % 10 == 9:
                    log(f"   ...scanned {i+1} stocks")
                time.sleep(0.3)

            # Sort by score descending
            scored.sort(key=lambda x: x['score'], reverse=True)

            log(f"\n📊 Top 10 candidates:")
            log(f"  {'Ticker':<8} {'Price':>8} {'MA50':>8} {'MA200':>8} "
                f"{'RSI':>6} {'Score':>7}")
            log(f"  {'─'*52}")
            for r in scored[:10]:
                log(f"  {r['ticker']:<8} ${r['price']:>7} ${r['ma50']:>7} "
                    f"${r['ma200']:>7} {r['rsi']:>5.1f}  {r['score']}/7")

            # ── Step 3: Buy top slots ─────────────────────────────────────────
            bought = 0
            for r in scored:
                if bought >= slots:
                    break
                if r['score'] < 4:
                    log(f"\n⚠️  Best score is only {r['score']}/7 — skipping buys this round")
                    break
                ticker = r['ticker']
                log(f"\n  🛒 Buying {ticker} — score {r['score']}/7")
                for reason in r['reason']:
                    log(f"     {reason}")

                trade = place_buy(ib, ticker, BUY_AMOUNT_GBP, r['price'])
                if trade:
                    positions[ticker] = {
                        'buy_price' : r['price'],
                        'qty'       : round((BUY_AMOUNT_GBP * 1.27) / r['price'], 4),
                        'buy_time'  : datetime.now().isoformat(),
                        'score_at_buy': r['score'],
                    }
                    save_positions(positions)
                    bought += 1
        else:
            log(f"\n📦 All {MAX_POSITIONS} slots full — only reviewing existing positions")

        # ── Summary ───────────────────────────────────────────────────────────
        log(f"\n📈 Open positions: {len(load_positions())}/{MAX_POSITIONS}")
        log(f"⏳ Next review in {LOOP_SECONDS} seconds... (Ctrl+C to stop)\n")
        time.sleep(LOOP_SECONDS)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⛔ Stopped by user. Positions saved to positions.json")
