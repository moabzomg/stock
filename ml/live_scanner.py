"""
live_scanner.py
Run this daily (e.g. after market close) to get today's ML-ranked buy signals.
Usage: python live_scanner.py

Requires a saved model (minervini_ml_model.joblib) from main.py
"""

import asyncio
import pandas as pd
import numpy as np
from data_fetcher import IBDataFetcher
from screener import MinerviniScreener
from feature_engineer import FeatureEngineer
from ml_model import MLTradingModel

# ── Config ────────────────────────────────────────────────────────────────────
UNIVERSE = [
    "AAPL","MSFT","NVDA","META","GOOGL","AMZN","TSLA","AVGO","AMD","CRM",
    "ADBE","ORCL","NOW","SNPS","KLAC","LRCX","AMAT","MRVL","PANW","CRWD",
    "UNH","LLY","ABBV","MRK","TMO","ISRG","DXCM","PODD","IDXX","VEEV",
    "CAT","DE","ITW","GWW","CTAS","FDX","UPS","XOM","CVX","SLB",
    "COST","HD","LOW","NKE","LULU","DECK","ONON","CELH","MNST","ELF",
    "V","MA","GS","MS","BLK","SPGI","MCO","ICE","CME",
    "ENPH","AXON","TMDX","WING","FOUR","ASTS","EXLS","TREX",
]
IB_HOST   = "127.0.0.1"
IB_PORT   = 7496
IB_CLIENT = 2
ML_THRESHOLD = 0.60


async def scan():
    print("=" * 60)
    print("  MINERVINI ML — LIVE SIGNAL SCANNER")
    print("=" * 60)

    # Load model
    try:
        model = MLTradingModel.load()
        print(f"\n✓ Loaded model with {len(model.feature_cols)} features\n")
    except FileNotFoundError:
        print("ERROR: No saved model found. Run main.py first.")
        return

    engineer = FeatureEngineer()
    screener = MinerviniScreener()

    # Fetch 1 year of data (enough for all indicators)
    fetcher  = IBDataFetcher(IB_HOST, IB_PORT, IB_CLIENT)
    raw_data = await fetcher.fetch_universe(UNIVERSE, years=1)
    screened = screener.screen_universe(raw_data)

    signals = []
    for sym, df in screened.items():
        if df.empty or "minervini_pass" not in df.columns:
            continue
        if not df["minervini_pass"].iloc[-1]:
            continue

        # Build feature row for the most recent bar
        try:
            feat_df = engineer._features_for_symbol(df, sym)
            if feat_df.empty:
                continue
            last_row = feat_df.iloc[-1]
            X = last_row[model.feature_cols].values.reshape(1, -1)
            if np.any(~np.isfinite(X)):
                continue
            prob = model.predict_proba(X)[0]
            score= df["minervini_score"].iloc[-1]
            price= df["Close"].iloc[-1]
            signals.append({
                "Symbol":       sym,
                "Price":        f"${price:.2f}",
                "ML Score":     f"{prob:.2%}",
                "Minervini /7": int(score),
                "RS 20d":       f"{last_row.get('mom_20d', 0):.1%}",
                "RS 60d":       f"{last_row.get('mom_60d', 0):.1%}",
                "RSI":          f"{last_row.get('rsi_14', 0):.0f}",
                "Signal":       "✅ BUY" if prob >= ML_THRESHOLD else "⚠️  WEAK",
            })
        except Exception as e:
            print(f"  ✗ {sym}: {e}")

    if not signals:
        print("No stocks passed all 7 Minervini rules today.")
        return

    sig_df = pd.DataFrame(signals).sort_values("ML Score", ascending=False)
    print(f"Stocks passing Minervini screen today: {len(signals)}")
    print(f"Strong signals (ML ≥ {ML_THRESHOLD:.0%}): "
          f"{len(sig_df[sig_df['Signal'].str.startswith('✅')])}\n")
    print(sig_df.to_string(index=False))

    # Save to CSV
    sig_df.to_csv("today_signals.csv", index=False)
    print("\n✅ Signals saved to today_signals.csv")


if __name__ == "__main__":
    asyncio.run(scan())
