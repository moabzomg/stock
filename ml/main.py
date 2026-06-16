"""
backtester.py  (v2 — fixed candidate detection)
Event-driven daily backtester.

Root cause of 0 trades (fixed):
  - _get_candidates was calling engineer.get_feature_cols(screened_df), but
    screened_df only has OHLCV + Minervini rule columns — not the 23 engineered
    features. The KeyError was silently swallowed by the bare `except Exception`.
  - Fix: pre-compute a feature DataFrame for every symbol once in __init__,
    then look up the row for the current date during the loop.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker
import matplotlib.dates as mdates
from dataclasses import dataclass
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

    INITIAL_CAPITAL = 100_000.0
    MAX_POSITIONS   = 10
    RISK_PER_TRADE  = 0.02      # 2 % of portfolio per trade
    STOP_LOSS_PCT   = -0.07     # –7 %
    TAKE_PROFIT_PCT =  0.20     # +20 %
    MAX_HOLD_DAYS   = 20        # calendar days
    ML_THRESHOLD    = 0.55      # min model probability to enter (lowered from 0.60)
    COMMISSION      = 0.001     # 0.1 % per side

    def __init__(self, raw_data, screened, model, engineer):
        self.raw_data = raw_data
        self.screened = screened
        self.model    = model
        self.engineer = engineer

        # ── Pre-compute feature DataFrames (the core fix) ─────────────────────
        # During training, engineer._features_for_symbol returns rows only where
        # minervini_pass=True, with the 23 feature columns + target + symbol.
        # We rebuild that same df per symbol and index it by date for O(1) lookups.
        print("      Pre-computing feature look-up tables …", flush=True)
        self._feat_cache: Dict[str, pd.DataFrame] = {}
        skipped = 0
        for sym, df in screened.items():
            try:
                feat_df = engineer._features_for_symbol(df, sym)
                if feat_df.empty:
                    skipped += 1
                    continue
                # Keep only the columns the model was trained on
                keep = [c for c in model.feature_cols if c in feat_df.columns]
                if len(keep) != len(model.feature_cols):
                    skipped += 1
                    continue
                self._feat_cache[sym] = feat_df[keep]
            except Exception as e:
                skipped += 1
        loaded = len(self._feat_cache)
        print(f"      Feature cache: {loaded} symbols loaded, {skipped} skipped.")
        if loaded == 0:
            raise RuntimeError(
                "Feature cache is empty — no symbols have passing Minervini rows "
                "with complete features. Check that your data covers ≥200 trading days."
            )

    # ── Public ────────────────────────────────────────────────────────────────
    def run(self) -> dict:
        all_dates = sorted(set(
            date for df in self.raw_data.values() for date in df.index
        ))

        capital      = self.INITIAL_CAPITAL
        equity_curve = []
        open_trades: list[Trade]  = []
        closed_trades: list[Trade]= []
        in_position: set[str]     = set()

        total_candidate_days = 0

        for date in all_dates:

            # 1. Process exits
            still_open = []
            for trade in open_trades:
                price_df = self.raw_data.get(trade.symbol)
                if price_df is None or date not in price_df.index:
                    still_open.append(trade)
                    continue

                price     = float(price_df.loc[date, "Close"])
                days_held = (date - trade.entry_date).days
                ret       = (price - trade.entry_price) / trade.entry_price

                if ret <= self.STOP_LOSS_PCT:
                    reason = "stop_loss"
                elif ret >= self.TAKE_PROFIT_PCT:
                    reason = "take_profit"
                elif days_held >= self.MAX_HOLD_DAYS:
                    reason = "time_exit"
                else:
                    still_open.append(trade)
                    continue

                trade.exit_date   = date
                trade.exit_price  = price
                trade.exit_reason = reason
                capital += trade.shares * price * (1 - self.COMMISSION)
                closed_trades.append(trade)
                in_position.discard(trade.symbol)

            open_trades = still_open

            # 2. Find new entries
            if len(open_trades) < self.MAX_POSITIONS:
                candidates = self._get_candidates(date, in_position)
                total_candidate_days += len(candidates)

                for sym, prob in candidates:
                    if len(open_trades) >= self.MAX_POSITIONS:
                        break
                    price_df = self.raw_data.get(sym)
                    if price_df is None or date not in price_df.index:
                        continue

                    price          = float(price_df.loc[date, "Close"])
                    stop_price     = price * (1 + self.STOP_LOSS_PCT)
                    risk_per_share = price - stop_price
                    if risk_per_share <= 0:
                        continue

                    risk_amt = capital * self.RISK_PER_TRADE
                    shares   = risk_amt / risk_per_share

                    # Cap at 25 % of portfolio
                    max_shares = (capital * 0.25) / (price * (1 + self.COMMISSION))
                    shares = min(shares, max_shares)

                    cost = shares * price * (1 + self.COMMISSION)
                    if cost > capital or shares < 0.01:
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
                    in_position.add(sym)

            # 3. Mark-to-market
            open_value = 0.0
            for t in open_trades:
                pdf = self.raw_data.get(t.symbol)
                if pdf is not None and date in pdf.index:
                    open_value += t.shares * float(pdf.loc[date, "Close"])
            equity_curve.append({"date": date, "equity": capital + open_value})

        # Force-close remaining
        for trade in open_trades:
            pdf = self.raw_data.get(trade.symbol)
            if pdf is not None and not pdf.empty:
                last_price = float(pdf["Close"].iloc[-1])
                trade.exit_date   = pdf.index[-1]
                trade.exit_price  = last_price
                trade.exit_reason = "end_of_simulation"
                capital += trade.shares * last_price * (1 - self.COMMISSION)
                closed_trades.append(trade)

        print(f"      Total candidate signals across all dates: {total_candidate_days}")

        equity_df = pd.DataFrame(equity_curve).set_index("date")
        stats     = self._compute_stats(equity_df, closed_trades)
        self._plot_equity(equity_df)

        return {"equity_curve": equity_df, "trades": closed_trades, "stats": stats}

    # ── Private ───────────────────────────────────────────────────────────────
    def _get_candidates(
        self, date: pd.Timestamp, in_position: set
    ) -> list[tuple[str, float]]:
        """
        For each symbol with a feature row on `date`:
          1. Confirm minervini_pass is still True on that date (from screened df)
          2. Look up the pre-built feature row from _feat_cache
          3. Score with the ML model
        """
        candidates = []

        for sym, feat_df in self._feat_cache.items():
            if sym in in_position:
                continue
            if date not in feat_df.index:
                continue

            # Double-check Minervini pass from screened df (source of truth)
            screened_df = self.screened.get(sym)
            if screened_df is None or date not in screened_df.index:
                continue
            if not bool(screened_df.loc[date, "minervini_pass"]):
                continue

            try:
                row = feat_df.loc[date]
                X   = row[self.model.feature_cols].values.astype(float).reshape(1, -1)
                if not np.all(np.isfinite(X)):
                    continue
                prob = float(self.model.predict_proba(X)[0])
                if prob >= self.ML_THRESHOLD:
                    candidates.append((sym, prob))
            except Exception as e:
                # Uncomment for debugging: print(f"        [bt] {sym} @ {date}: {e}")
                continue

        candidates.sort(key=lambda x: x[1], reverse=True)
        return candidates

    # ── Stats & plot (unchanged) ───────────────────────────────────────────────
    def _compute_stats(self, equity: pd.DataFrame, trades: list[Trade]) -> dict:
        eq  = equity["equity"]
        ret = eq.pct_change().dropna()

        total_return = eq.iloc[-1] / self.INITIAL_CAPITAL - 1
        years        = (equity.index[-1] - equity.index[0]).days / 365.25
        cagr         = (1 + total_return) ** (1 / max(years, 0.01)) - 1
        sharpe       = ret.mean() / ret.std() * np.sqrt(252) if ret.std() > 0 else 0
        roll_max     = eq.cummax()
        drawdown     = (eq - roll_max) / roll_max
        max_dd       = drawdown.min()
        calmar       = cagr / abs(max_dd) if max_dd != 0 else 0

        win_t  = [t for t in trades if t.pnl_pct > 0]
        loss_t = [t for t in trades if t.pnl_pct <= 0]
        win_rate      = len(win_t) / len(trades) if trades else 0
        avg_win       = float(np.mean([t.pnl_pct for t in win_t]))  if win_t  else 0.0
        avg_loss      = float(np.mean([t.pnl_pct for t in loss_t])) if loss_t else 0.0
        profit_factor = (
            sum(t.pnl_dollars for t in win_t) /
            abs(sum(t.pnl_dollars for t in loss_t))
            if loss_t else float("inf")
        )

        stats = {
            "Total Return":  f"{total_return:.1%}",
            "CAGR":          f"{cagr:.1%}",
            "Sharpe Ratio":  f"{sharpe:.2f}",
            "Max Drawdown":  f"{max_dd:.1%}",
            "Calmar Ratio":  f"{calmar:.2f}",
            "Total Trades":  len(trades),
            "Win Rate":      f"{win_rate:.1%}",
            "Avg Win":       f"{avg_win:.1%}",
            "Avg Loss":      f"{avg_loss:.1%}",
            "Profit Factor": f"{profit_factor:.2f}",
            "Final Equity":  f"${eq.iloc[-1]:,.0f}",
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

        axes[0].plot(eq.index, eq.values, color="#00d4aa", linewidth=1.5,
                     label="Strategy")
        axes[0].fill_between(eq.index, self.INITIAL_CAPITAL, eq.values,
                              where=eq.values >= self.INITIAL_CAPITAL,
                              alpha=0.15, color="#00d4aa")
        axes[0].fill_between(eq.index, self.INITIAL_CAPITAL, eq.values,
                              where=eq.values < self.INITIAL_CAPITAL,
                              alpha=0.15, color="#ff4b4b")
        axes[0].axhline(self.INITIAL_CAPITAL, color="#555555",
                        linewidth=0.8, linestyle="--")
        axes[0].set_title("Minervini ML Strategy — 5-Year Equity Curve",
                          fontsize=13, pad=10)
        axes[0].set_ylabel("Portfolio Value ($)")
        axes[0].legend(loc="upper left", framealpha=0.2, labelcolor="#cccccc")
        axes[0].yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(lambda x, _: f"${x:,.0f}"))

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