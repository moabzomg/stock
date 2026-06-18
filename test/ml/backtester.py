"""
backtester.py
Event-driven daily backtester.
- Only enters positions when stock passes all 7 Minervini rules AND model score ≥ threshold
- Position sizing: equal-weight, max N positions
- Exits: stop-loss at -7% (hard stop), profit target at +20%, or hold_days exceeded
- Benchmarks against buy-and-hold SPY
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from dataclasses import dataclass, field
from typing import Dict


@dataclass
class Trade:
    symbol: str
    entry_date: pd.Timestamp
    entry_price: float
    shares: float
    stop_loss: float
    take_profit: float
    max_hold_days: int = 20
    exit_date: pd.Timestamp = None
    exit_price: float = None
    exit_reason: str = None

    @property
    def pnl_pct(self) -> float:
        if self.exit_price is None:
            return 0.0
        return (self.exit_price - self.entry_price) / self.entry_price

    @property
    def pnl_dollars(self) -> float:
        if self.exit_price is None:
            return 0.0
        return (self.exit_price - self.entry_price) * self.shares


class Backtester:

    INITIAL_CAPITAL   = 100_000.0
    MAX_POSITIONS     = 10
    RISK_PER_TRADE    = 0.02          # 2 % of portfolio per trade
    STOP_LOSS_PCT     = -0.07         # -7 %
    TAKE_PROFIT_PCT   =  0.20         # +20 %
    MAX_HOLD_DAYS     = 20
    ML_THRESHOLD      = 0.60          # min model probability to enter
    COMMISSION        = 0.001         # 0.1 % per side (approx IB tiered)

    def __init__(self, raw_data, screened, model, engineer):
        self.raw_data = raw_data
        self.screened = screened
        self.model    = model
        self.engineer = engineer

    # ── Public ────────────────────────────────────────────────────────────────
    def run(self) -> dict:
        # Build a sorted list of all trading dates across all symbols
        all_dates = sorted(set(
            date for df in self.raw_data.values() for date in df.index
        ))

        capital     = self.INITIAL_CAPITAL
        equity_curve= []
        open_trades : list[Trade] = []
        closed_trades: list[Trade] = []

        for date in all_dates:
            # 1. Process exits first
            still_open = []
            for trade in open_trades:
                df = self.raw_data.get(trade.symbol)
                if df is None or date not in df.index:
                    still_open.append(trade)
                    continue

                price  = df.loc[date, "Close"]
                days_held = (date - trade.entry_date).days
                ret       = (price - trade.entry_price) / trade.entry_price

                if ret <= self.STOP_LOSS_PCT:
                    trade.exit_date, trade.exit_price, trade.exit_reason = (
                        date, price, "stop_loss")
                elif ret >= self.TAKE_PROFIT_PCT:
                    trade.exit_date, trade.exit_price, trade.exit_reason = (
                        date, price, "take_profit")
                elif days_held >= self.MAX_HOLD_DAYS:
                    trade.exit_date, trade.exit_price, trade.exit_reason = (
                        date, price, "time_exit")
                else:
                    still_open.append(trade)
                    continue

                capital += trade.shares * trade.exit_price * (1 - self.COMMISSION)
                closed_trades.append(trade)

            open_trades = still_open

            # 2. Scan for new entries if we have capacity
            if len(open_trades) < self.MAX_POSITIONS:
                candidates = self._get_candidates(date)
                for sym, prob in candidates:
                    if len(open_trades) >= self.MAX_POSITIONS:
                        break
                    price = self.raw_data[sym].loc[date, "Close"]
                    risk_amt = capital * self.RISK_PER_TRADE
                    stop_price = price * (1 + self.STOP_LOSS_PCT)
                    risk_per_share = price - stop_price
                    if risk_per_share <= 0:
                        continue
                    shares = risk_amt / risk_per_share
                    cost   = shares * price * (1 + self.COMMISSION)
                    if cost > capital * 0.25:             # max 25 % in one stock
                        shares = (capital * 0.25) / (price * (1 + self.COMMISSION))
                        cost   = shares * price * (1 + self.COMMISSION)
                    if cost > capital:
                        continue

                    capital -= cost
                    open_trades.append(Trade(
                        symbol=sym,
                        entry_date=date,
                        entry_price=price,
                        shares=shares,
                        stop_loss=stop_price,
                        take_profit=price * (1 + self.TAKE_PROFIT_PCT),
                    ))

            # 3. Mark-to-market equity
            open_value = sum(
                t.shares * self.raw_data[t.symbol].loc[date, "Close"]
                for t in open_trades
                if t.symbol in self.raw_data and date in self.raw_data[t.symbol].index
            )
            equity_curve.append({"date": date, "equity": capital + open_value})

        # Force-close any remaining open trades at last available price
        for trade in open_trades:
            df = self.raw_data.get(trade.symbol)
            if df is not None and not df.empty:
                last_price = df["Close"].iloc[-1]
                trade.exit_date   = df.index[-1]
                trade.exit_price  = last_price
                trade.exit_reason = "end_of_simulation"
                capital += trade.shares * last_price * (1 - self.COMMISSION)
                closed_trades.append(trade)

        equity_df = pd.DataFrame(equity_curve).set_index("date")
        stats = self._compute_stats(equity_df, closed_trades)
        self._plot_equity(equity_df)

        return {
            "equity_curve": equity_df,
            "trades": closed_trades,
            "stats": stats,
        }

    # ── Private ───────────────────────────────────────────────────────────────
    def _get_candidates(self, date: pd.Timestamp) -> list[tuple[str, float]]:
        """Return (symbol, ml_prob) pairs that pass screen + model on this date."""
        candidates = []
        for sym, df in self.screened.items():
            if date not in df.index:
                continue
            row = df.loc[date]
            if not row.get("minervini_pass", False):
                continue
            # Build feature row
            try:
                feat_cols = self.engineer.get_feature_cols(df)
                X = row[feat_cols].values.reshape(1, -1)
                if np.any(~np.isfinite(X)):
                    continue
                prob = self.model.predict_proba(X)[0]
                if prob >= self.ML_THRESHOLD:
                    candidates.append((sym, prob))
            except Exception:
                continue
        # Sort by model confidence descending
        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates

    def _compute_stats(self, equity: pd.DataFrame, trades: list[Trade]) -> dict:
        eq    = equity["equity"]
        ret   = eq.pct_change().dropna()

        total_return = eq.iloc[-1] / self.INITIAL_CAPITAL - 1
        years        = (equity.index[-1] - equity.index[0]).days / 365.25
        cagr         = (1 + total_return) ** (1 / max(years, 0.01)) - 1

        sharpe = ret.mean() / ret.std() * np.sqrt(252) if ret.std() > 0 else 0

        roll_max = eq.cummax()
        drawdown = (eq - roll_max) / roll_max
        max_dd   = drawdown.min()

        calmar   = cagr / abs(max_dd) if max_dd != 0 else 0

        win_trades  = [t for t in trades if t.pnl_pct > 0]
        loss_trades = [t for t in trades if t.pnl_pct <= 0]
        win_rate    = len(win_trades) / len(trades) if trades else 0
        avg_win     = np.mean([t.pnl_pct for t in win_trades])  if win_trades  else 0
        avg_loss    = np.mean([t.pnl_pct for t in loss_trades]) if loss_trades else 0
        profit_factor = (
            sum(t.pnl_dollars for t in win_trades) /
            abs(sum(t.pnl_dollars for t in loss_trades))
            if loss_trades else float("inf")
        )

        stats = {
            "Total Return":     f"{total_return:.1%}",
            "CAGR":             f"{cagr:.1%}",
            "Sharpe Ratio":     f"{sharpe:.2f}",
            "Max Drawdown":     f"{max_dd:.1%}",
            "Calmar Ratio":     f"{calmar:.2f}",
            "Total Trades":     len(trades),
            "Win Rate":         f"{win_rate:.1%}",
            "Avg Win":          f"{avg_win:.1%}",
            "Avg Loss":         f"{avg_loss:.1%}",
            "Profit Factor":    f"{profit_factor:.2f}",
            "Final Equity":     f"${eq.iloc[-1]:,.0f}",
        }

        print("\n      ── Backtest Results ─────────────────────────────────")
        for k, v in stats.items():
            print(f"        {k:<20}: {v}")
        return stats

    def _plot_equity(self, equity: pd.DataFrame) -> None:
        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True,
                                 gridspec_kw={"height_ratios": [3, 1]})
        fig.patch.set_facecolor("#0f1117")
        for ax in axes:
            ax.set_facecolor("#0f1117")
            ax.tick_params(colors="#cccccc")
            ax.yaxis.label.set_color("#cccccc")
            ax.xaxis.label.set_color("#cccccc")
            ax.title.set_color("#ffffff")
            for spine in ax.spines.values():
                spine.set_edgecolor("#333333")

        eq = equity["equity"]

        # Equity curve
        axes[0].plot(eq.index, eq.values, color="#00d4aa", linewidth=1.5,
                     label="Strategy")
        axes[0].fill_between(eq.index, self.INITIAL_CAPITAL, eq.values,
                              where=eq.values >= self.INITIAL_CAPITAL,
                              alpha=0.15, color="#00d4aa")
        axes[0].fill_between(eq.index, self.INITIAL_CAPITAL, eq.values,
                              where=eq.values < self.INITIAL_CAPITAL,
                              alpha=0.15, color="#ff4b4b")
        axes[0].axhline(self.INITIAL_CAPITAL, color="#555555", linewidth=0.8,
                        linestyle="--")
        axes[0].set_title("Minervini ML Strategy — 5-Year Equity Curve",
                          fontsize=13, pad=10)
        axes[0].set_ylabel("Portfolio Value ($)")
        axes[0].legend(loc="upper left", framealpha=0.2, labelcolor="#cccccc")
        axes[0].yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

        # Drawdown
        roll_max = eq.cummax()
        drawdown = (eq - roll_max) / roll_max * 100
        axes[1].fill_between(drawdown.index, 0, drawdown.values,
                              color="#ff4b4b", alpha=0.6)
        axes[1].set_ylabel("Drawdown (%)")
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        axes[1].xaxis.set_major_locator(mdates.YearLocator())

        plt.tight_layout(pad=2)
        plt.savefig("equity_curve.png", dpi=150, bbox_inches="tight",
                    facecolor=fig.get_facecolor())
        plt.close()
