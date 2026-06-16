# Minervini Trend Template + ML Trading System

Combines Mark Minervini's **7-rule trend template** with a **RandomForest + XGBoost ensemble** trained on 5 years of IB data.

---

## Architecture

```
main.py
  ├── data_fetcher.py    → IB TWS (port 7496) or yfinance fallback
  ├── screener.py        → All 7 Minervini rules as boolean columns
  ├── feature_engineer.py→ 23 technical features + forward return target
  ├── ml_model.py        → RF + XGB ensemble, walk-forward CV
  ├── backtester.py      → Event-driven sim with stops/targets
  └── report.py          → HTML report + equity curve PNG
live_scanner.py          → Daily signal scanner (uses saved model)
```

---

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start IB TWS or Gateway, enable API on port 7496
#    TWS: File → Global Configuration → API → Settings
#    Enable: "Enable ActiveX and Socket Clients"
#    Socket port: 7496

# 3. Train model + backtest (5 years)
python main.py

# 4. Daily live scan (after market close)
python live_scanner.py
```

---

## The 7 Minervini Rules

| # | Rule | Why |
|---|------|-----|
| 1 | Price > 150-SMA & 200-SMA | Above long-term trend |
| 2 | 150-SMA > 200-SMA | Short MA accelerating away |
| 3 | 200-SMA rising for ≥ 1 month | Uptrend in the trend itself |
| 4 | 50-SMA > 150-SMA & 200-SMA | Full bullish alignment |
| 5 | Price > 50-SMA | Buying pressure confirmed |
| 6 | Price within 25% of 52-week high | Not broken, not lagging |
| 7 | Price ≥ 30% above 52-week low | Has already proven recovery |

---

## ML Model Details

- **Target**: stock outperforms by ≥ 3% over the next 20 trading days
- **Algorithm**: soft-voting ensemble (RandomForest 300 trees + XGBoost 300 rounds)
- **Validation**: 5-fold walk-forward TimeSeriesSplit (no lookahead)
- **Threshold**: min 60% predicted probability to enter

### Features (23 total)
- Price momentum: 5, 10, 20, 60, 120-day returns
- Relative strength vs 50/150/200-SMA
- Distance from 52-week high/low
- Volatility: 20d/60d std, ATR%
- Volume: ratio vs 20d/50d avg, trend
- Technical: RSI-14, MACD histogram, BB position
- Trend quality: 50/200-SMA slope, Minervini score (0–7)

---

## Backtest Rules

| Parameter | Value |
|-----------|-------|
| Initial capital | $100,000 |
| Max positions | 10 |
| Risk per trade | 2% of portfolio |
| Stop loss | –7% |
| Take profit | +20% |
| Max hold | 20 trading days |
| Commission | 0.1% per side |

---

## Output Files

| File | Description |
|------|-------------|
| `minervini_ml_report.html` | Full HTML report with equity curve, trade log, feature importance |
| `equity_curve.png` | Dark-mode equity curve + drawdown chart |
| `minervini_ml_model.joblib` | Saved model for live_scanner.py |
| `today_signals.csv` | Today's ranked buy signals |

---

## Notes

- If IB TWS is not running, the system automatically falls back to **yfinance**
- All backtesting uses **adjusted prices** to handle splits/dividends
- Walk-forward CV ensures no lookahead bias in model evaluation
- The screener can be extended to any universe (S&P 500, Russell 1000, etc.)
