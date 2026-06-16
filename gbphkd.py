#!/usr/bin/env python3
"""
GBP/HKD — Last 1 Minute Data Collector (fixed)
RUN:
  cd ~/Desktop/stock
  source venv/bin/activate
  python3 gbphkd_lastminute.py
"""

from ib_insync import IB, Forex
import pandas as pd
from datetime import datetime, timezone, timedelta

PORT      = 7496
HOST      = '127.0.0.1'
CLIENT_ID = 2

print("\n" + "="*55)
print("  GBP/HKD Last-Minute Data Collector")
print("="*55)
print(f"\n⏳ Connecting to TWS on {HOST}:{PORT}...")

ib = IB()
try:
    ib.connect(HOST, PORT, clientId=CLIENT_ID, timeout=10)
    print("✅ Connected to TWS successfully!\n")
except Exception as e:
    print(f"\n❌ Connection failed: {e}")
    exit(1)

contract = Forex('GBPHKD')
ib.qualifyContracts(contract)
print(f"📌 Contract : {contract.symbol}/{contract.currency}")
print(f"   Exchange  : {contract.exchange}\n")

# ── Check if market is open ───────────────────────────────────────────────────
details = ib.reqContractDetails(contract)
now_utc = datetime.now(timezone.utc)

# Forex is closed Saturday and most of Sunday (opens Sun ~9pm ET = Mon 1am UTC)
weekday = now_utc.weekday()  # 0=Mon 6=Sun
hour    = now_utc.hour

market_open = True
if weekday == 5:  # Saturday — always closed
    market_open = False
elif weekday == 6 and hour < 21:  # Sunday before 9pm ET (1am UTC Mon)
    market_open = False

if not market_open:
    print("⚠️  Forex market is currently closed.")
    print("   Opens: Sunday 9pm ET / Monday 1am UTC / Monday 9am HKT\n")

# ── Fetch last 60 seconds of tick data (fixed: provide startDateTime) ─────────
print("📥 Fetching last 60 seconds of bid/ask ticks...")

# Fix: provide explicit start and end times (IBKR requires 2 of 3 params)
end_dt   = now_utc
start_dt = end_dt - timedelta(seconds=60)

# Format IBKR expects: "YYYYMMDD HH:MM:SS UTC"
start_str = start_dt.strftime('%Y%m%d %H:%M:%S UTC')
end_str   = end_dt.strftime('%Y%m%d %H:%M:%S UTC')

print(f"   From : {start_str}")
print(f"   To   : {end_str}\n")

ticks = ib.reqHistoricalTicks(
    contract,
    startDateTime=start_str,
    endDateTime=end_str,
    numberOfTicks=1000,
    whatToShow='BID_ASK',
    useRth=False,
)

# ── Fetch 1-minute OHLCV bars (last 5 bars for context) ──────────────────────
print("📊 Fetching last 5 one-minute OHLCV bars...")
bars = ib.reqHistoricalData(
    contract,
    endDateTime='',
    durationStr='600 S',       # last 10 minutes
    barSizeSetting='1 min',
    whatToShow='MIDPOINT',
    useRTH=False,
    formatDate=1,
)

ib.disconnect()
print("🔌 Disconnected from TWS\n")

# ── Show tick data ────────────────────────────────────────────────────────────
if ticks:
    rows = []
    for t in ticks:
        tick_time = t.time.replace(tzinfo=timezone.utc)
        rows.append({
            'time (UTC)' : tick_time.strftime('%H:%M:%S.%f')[:-3],
            'bid'        : t.priceBid,
            'ask'        : t.priceAsk,
            'mid'        : round((t.priceBid + t.priceAsk) / 2, 6),
            'spread'     : round(t.priceAsk - t.priceBid, 6),
            'bid_size'   : t.sizeBid,
            'ask_size'   : t.sizeAsk,
        })

    df = pd.DataFrame(rows)

    print("="*65)
    print(f"  GBP/HKD Ticks — Last 60 Seconds  ({len(df)} ticks found)")
    print("="*65)
    print(df.to_string(index=False))

    # Summary
    print(f"\n{'─'*65}")
    print("  Summary")
    print(f"{'─'*65}")
    first_mid = df['mid'].iloc[0]
    last_mid  = df['mid'].iloc[-1]
    change    = last_mid - first_mid
    direction = "↑ UP" if change > 0 else "↓ DOWN" if change < 0 else "→ FLAT"

    print(f"  Ticks     : {len(df)}")
    print(f"  High bid  : {df['bid'].max():.5f}")
    print(f"  Low  bid  : {df['bid'].min():.5f}")
    print(f"  Last mid  : {last_mid:.5f}")
    print(f"  Avg spread: {df['spread'].mean():.6f}  ({df['spread'].mean()*10000:.2f} pips)")
    print(f"  Direction : {direction}  ({change:+.6f})")

    filename = f"gbphkd_ticks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"\n💾 Saved to: {filename}")
else:
    if market_open:
        print("⚠️  No ticks in last 60 seconds — market may be very quiet.")
    else:
        print("⚠️  No ticks — market is closed. Run again when market opens.")
        print("   Next open: Monday 9am HKT\n")

# ── Show OHLCV bars ───────────────────────────────────────────────────────────
if bars:
    print(f"\n{'='*65}")
    print("  Last 5 One-Minute OHLCV Bars (Midpoint)")
    print(f"{'='*65}")
    print(f"  {'Time':<30} {'Open':>9} {'High':>9} {'Low':>9} {'Close':>9} {'Pips':>6}")
    print(f"  {'─'*60}")
    for bar in bars[-5:]:
        pips = round((bar.high - bar.low) * 10000, 1)
        print(f"  {str(bar.date):<30} {bar.open:>9.5f} {bar.high:>9.5f} {bar.low:>9.5f} {bar.close:>9.5f} {pips:>6.1f}")

print(f"\n{'='*65}\n")