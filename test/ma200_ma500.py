#!/usr/bin/env python3
"""
MA200 + MA500 Screener — All FX Pairs + Major Stocks
=====================================================
Connects to IBKR, fetches 3 years of daily bars for every
instrument, computes MA200 + MA500, then shows the last 5
days and a BUY/SELL/NEUTRAL signal for each.

RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  pip install ib_insync pandas plotly
  python3 ma_screener.py

NOTE: Takes 2-5 minutes — IBKR rate-limits historical requests.
"""

from ib_insync import IB, Forex, Stock
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
from datetime import datetime

PORT      = 7496
HOST      = '127.0.0.1'
CLIENT_ID = 4

# ── Instruments ───────────────────────────────────────────────────────────────
FX_PAIRS = [
    ('GBP', 'HKD'), ('GBP', 'USD'), ('GBP', 'EUR'), ('GBP', 'JPY'),
    ('GBP', 'AUD'), ('GBP', 'CAD'), ('GBP', 'CHF'), ('GBP', 'NZD'),
    ('EUR', 'USD'), ('EUR', 'JPY'), ('EUR', 'CHF'), ('EUR', 'AUD'),
    ('USD', 'JPY'), ('USD', 'CAD'), ('USD', 'CHF'), ('USD', 'HKD'),
    ('AUD', 'USD'), ('NZD', 'USD'),
]

STOCKS = [
    # US Tech
    ('AAPL',  'SMART', 'USD'),
    ('MSFT',  'SMART', 'USD'),
    ('NVDA',  'SMART', 'USD'),
    ('GOOGL', 'SMART', 'USD'),
    ('AMZN',  'SMART', 'USD'),
    ('META',  'SMART', 'USD'),
    ('TSLA',  'SMART', 'USD'),
    # UK stocks (LSE)
    ('SHEL',  'LSE',   'GBP'),
    ('HSBA',  'LSE',   'GBP'),
    ('BP',    'LSE',   'GBP'),
    ('VOD',   'LSE',   'GBP'),
    # HK stocks
    ('0005',  'SEHK',  'HKD'),  # HSBC HK
    ('0700',  'SEHK',  'HKD'),  # Tencent
    ('0941',  'SEHK',  'HKD'),  # China Mobile
]

# ── Connect ───────────────────────────────────────────────────────────────────
print("\n" + "="*65)
print("  MA200 + MA500 Screener — FX + Stocks")
print("="*65)
print(f"\n⏳ Connecting to TWS on {HOST}:{PORT}...")

ib = IB()
try:
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    print("✅ Connected!\n")
except Exception as e:
    print(f"❌ Connection failed: {e}")
    exit(1)

# ── Helper: compute MAs and return last 5 rows ────────────────────────────────
def compute_mas(bars, name):
    if len(bars) < 200:
        return None, f"⚠️  Only {len(bars)} bars — need 200+ for MA200"

    df = pd.DataFrame([{
        'date'  : pd.to_datetime(str(b.date)),
        'open'  : b.open,
        'high'  : b.high,
        'low'   : b.low,
        'close' : b.close,
    } for b in bars])
    df.set_index('date', inplace=True)
    df.sort_index(inplace=True)

    df['ma200'] = df['close'].rolling(200).mean()
    df['ma500'] = df['close'].rolling(500).mean() if len(df) >= 500 else float('nan')

    # Signal based on latest values
    latest = df.iloc[-1]
    price  = latest['close']
    ma200  = latest['ma200']
    ma500  = latest['ma500']

    if pd.isna(ma500):
        if price > ma200:
            signal = '🟢 ABOVE MA200'
        else:
            signal = '🔴 BELOW MA200'
        note = '(MA500 needs 500+ days)'
    else:
        if price > ma200 and price > ma500 and ma200 > ma500:
            signal = '🟢 STRONG BUY'
        elif price > ma200 and price > ma500:
            signal = '🟢 BUY'
        elif price < ma200 and price < ma500 and ma200 < ma500:
            signal = '🔴 STRONG SELL'
        elif price < ma200 and price < ma500:
            signal = '🔴 SELL'
        else:
            signal = '🟡 NEUTRAL'
        note = ''

    return df, signal, note

# ── Fetch all instruments ─────────────────────────────────────────────────────
results   = []  # summary rows
all_dfs   = {}  # name -> full df for charting

total = len(FX_PAIRS) + len(STOCKS)
count = 0

# FX
print("📥 Fetching FX pairs...")
for base, quote in FX_PAIRS:
    name = f"{base}/{quote}"
    count += 1
    print(f"  [{count}/{total}] {name}...", end=' ', flush=True)

    try:
        contract = Forex(base + quote)
        ib.qualifyContracts(contract)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='3 Y',
            barSizeSetting='1 day',
            whatToShow='MIDPOINT',
            useRTH=False,
            formatDate=1,
        )
        time.sleep(0.5)  # respect IBKR rate limit

        if not bars:
            print("no data")
            continue

        result = compute_mas(bars, name)
        df     = result[0]
        signal = result[1]
        note   = result[2] if len(result) > 2 else ''

        if df is None:
            print(signal)
            continue

        last5  = df.tail(5)
        latest = df.iloc[-1]

        all_dfs[name] = df
        results.append({
            'name'    : name,
            'type'    : 'FX',
            'price'   : round(latest['close'], 5),
            'ma200'   : round(latest['ma200'], 5),
            'ma500'   : round(latest['ma500'], 5) if not pd.isna(latest['ma500']) else 'N/A',
            'vs_ma200': round((latest['close'] / latest['ma200'] - 1) * 100, 2),
            'signal'  : signal,
            'bars'    : len(bars),
        })
        print(f"✅ {signal}")

    except Exception as e:
        print(f"❌ {e}")

# Stocks
print("\n📥 Fetching stocks...")
for ticker, exchange, currency in STOCKS:
    name = f"{ticker}"
    count += 1
    print(f"  [{count}/{total}] {name}...", end=' ', flush=True)

    try:
        contract = Stock(ticker, exchange, currency)
        ib.qualifyContracts(contract)

        bars = ib.reqHistoricalData(
            contract,
            endDateTime='',
            durationStr='3 Y',
            barSizeSetting='1 day',
            whatToShow='TRADES',
            useRTH=True,
            formatDate=1,
        )
        time.sleep(0.5)

        if not bars:
            print("no data")
            continue

        result = compute_mas(bars, name)
        df     = result[0]
        signal = result[1]
        note   = result[2] if len(result) > 2 else ''

        if df is None:
            print(signal)
            continue

        latest = df.iloc[-1]
        all_dfs[name] = df
        results.append({
            'name'    : name,
            'type'    : 'Stock',
            'price'   : round(latest['close'], 3),
            'ma200'   : round(latest['ma200'], 3),
            'ma500'   : round(latest['ma500'], 3) if not pd.isna(latest['ma500']) else 'N/A',
            'vs_ma200': round((latest['close'] / latest['ma200'] - 1) * 100, 2),
            'signal'  : signal,
            'bars'    : len(bars),
        })
        print(f"✅ {signal}")

    except Exception as e:
        print(f"❌ {e}")

ib.disconnect()
print("\n🔌 Disconnected from TWS\n")

if not results:
    print("❌ No data collected. Check TWS connection.")
    exit(1)

# ── Print last 5 days table per instrument ────────────────────────────────────
print("\n" + "="*65)
print("  LAST 5 DAYS — MA200 + MA500 for each instrument")
print("="*65)

for name, df in all_dfs.items():
    last5 = df.tail(5)[['close','ma200','ma500']].copy()
    last5.columns = ['Close', 'MA200', 'MA500']
    last5 = last5.round(5)
    sig = next((r['signal'] for r in results if r['name'] == name), '')
    print(f"\n  {name}  {sig}")
    print(last5.to_string())

# ── Summary ranking table ─────────────────────────────────────────────────────
summary = pd.DataFrame(results)
summary_sorted = summary.sort_values('vs_ma200', ascending=False)

print("\n\n" + "="*80)
print("  RANKING — Price vs MA200 (strongest to weakest)")
print("="*80)
print(f"  {'Name':<12} {'Type':<7} {'Price':>10} {'MA200':>10} {'MA500':>10} {'%vMA200':>8}  Signal")
print(f"  {'─'*75}")
for _, row in summary_sorted.iterrows():
    print(f"  {row['name']:<12} {row['type']:<7} {row['price']:>10} "
          f"{row['ma200']:>10} {str(row['ma500']):>10} "
          f"{row['vs_ma200']:>7.2f}%  {row['signal']}")

# ── Save summary CSV ──────────────────────────────────────────────────────────
ts       = datetime.now().strftime('%Y%m%d_%H%M%S')
csv_file = f"ma_screener_{ts}.csv"
summary_sorted.to_csv(csv_file, index=False)
print(f"\n💾 Summary saved: {csv_file}")

# ── Build interactive chart ───────────────────────────────────────────────────
print("📊 Building summary chart...")

# Chart 1: % above/below MA200 bar chart
names   = summary_sorted['name'].tolist()
pcts    = summary_sorted['vs_ma200'].tolist()
signals = summary_sorted['signal'].tolist()

colors = []
for s in signals:
    if 'STRONG BUY' in s or 'ABOVE' in s:
        colors.append('#26a69a')
    elif 'BUY' in s:
        colors.append('#66bb6a')
    elif 'STRONG SELL' in s:
        colors.append('#ef5350')
    elif 'SELL' in s:
        colors.append('#ff7043')
    else:
        colors.append('#f39c12')

fig = make_subplots(
    rows=2, cols=1,
    row_heights=[0.65, 0.35],
    vertical_spacing=0.08,
    subplot_titles=(
        '% Above / Below MA200  (green = above, red = below)',
        'Price vs MA200 vs MA500 — select instrument above to highlight'
    )
)

fig.add_trace(go.Bar(
    x=names, y=pcts,
    marker_color=colors,
    name='% vs MA200',
    text=[f"{p:+.2f}%" for p in pcts],
    textposition='outside',
), row=1, col=1)

fig.add_hline(y=0, line_color='white', line_width=1,
              opacity=0.4, row=1, col=1)

# Price chart for first instrument (GBP/HKD or first available)
first_name = 'GBP/HKD' if 'GBP/HKD' in all_dfs else list(all_dfs.keys())[0]
df_plot = all_dfs[first_name].tail(250)  # last 250 days

fig.add_trace(go.Scatter(
    x=df_plot.index, y=df_plot['close'],
    name='Price', line=dict(color='#d1d4dc', width=1.5)
), row=2, col=1)

fig.add_trace(go.Scatter(
    x=df_plot.index, y=df_plot['ma200'],
    name='MA200', line=dict(color='#f39c12', width=2)
), row=2, col=1)

if not df_plot['ma500'].isna().all():
    fig.add_trace(go.Scatter(
        x=df_plot.index, y=df_plot['ma500'],
        name='MA500', line=dict(color='#3498db', width=2)
    ), row=2, col=1)

fig.update_layout(
    title=f"MA200 + MA500 Screener — {len(results)} instruments — {datetime.now().strftime('%Y-%m-%d %H:%M')}",
    template='plotly_dark',
    paper_bgcolor='#131722',
    plot_bgcolor='#131722',
    font=dict(color='#d1d4dc', size=11),
    height=800,
    margin=dict(l=60, r=60, t=80, b=60),
)
fig.update_xaxes(gridcolor='#2a2e39')
fig.update_yaxes(gridcolor='#2a2e39')

chart_file = f"ma_screener_chart_{ts}.html"
fig.write_html(chart_file, auto_open=True)
print(f"✅ Chart opened: {chart_file}\n")
print("="*65 + "\n")