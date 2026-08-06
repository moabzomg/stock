#!/usr/bin/env python3
"""
backtest.py — Buy-low/sell-high mean-reversion backtester.

Reads the same year-partitioned files minute_extract.py writes:
    data/<year>/<SYMBOL>_daily_<year>.csv
    data/<year>/<SYMBOL>_minute_<year>.csv

Strategy (per symbol, per timeframe — long only, one position at a time):
    Entry ("buy low") — ALL of:
        1. close <= lower Bollinger Band   (price is statistically depressed)
        2. RSI  <  oversold threshold      (momentum confirms the dip)
        3. close > long-term SMA           (only buy dips *within* an uptrend —
                                             this is the regime filter that keeps
                                             the strategy from catching falling
                                             knives in a downtrend)
    Exit ("sell high") — FIRST of, checked in this order each bar:
        1. Stop loss:    close <= entry_price - stop_atr_mult * ATR(at entry)
        2. Take profit:  close >= upper Bollinger Band  OR  RSI > overbought
        3. Time stop:    held for max_hold bars with no exit signal

This is a template, not investment advice — tune the parameters, and always
validate against out-of-sample data before trusting the numbers.

Usage:
    python3 backtest.py                              # all symbols in watch.txt, both timeframes
    python3 backtest.py --timeframe daily
    python3 backtest.py AAPL MSFT --timeframe minute
    python3 backtest.py --capital 25000 --rsi-buy 30 --rsi-sell 70
    python3 backtest.py --start 20250101 --end 20250630   # only trade within this range
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

DATA_DIR   = "data"
OUT_DIR    = "results"

# Separate default lookback windows per timeframe: a 20-bar Bollinger Band
# means 20 *days* on daily data but only 20 *minutes* on minute data, which
# is far too twitchy for a mean-reversion signal — so minute defaults use
# longer bar counts to represent a comparable amount of wall-clock time.
DEFAULTS = {
    "daily":  dict(sma_trend=50, bb_period=20, rsi_period=14, atr_period=14, max_hold=20),
    "minute": dict(sma_trend=390, bb_period=60, rsi_period=14, atr_period=30, max_hold=180),
}


# ── Data loading (mirrors minute_extract.py's year-partition merge) ──────────

def _list_years():
    if not os.path.isdir(DATA_DIR):
        return []
    return sorted(d for d in os.listdir(DATA_DIR)
                  if d.isdigit() and os.path.isdir(os.path.join(DATA_DIR, d)))


def load_symbol_bars(symbol, timeframe):
    """Merge <SYMBOL>_<timeframe>_<year>.csv across every year folder found
    on disk into one sorted DataFrame indexed by datetime."""
    frames = []
    for year in _list_years():
        path = os.path.join(DATA_DIR, year, f"{symbol}_{timeframe}_{year}.csv")
        if not os.path.exists(path):
            continue
        df = pd.read_csv(path)
        if df.empty:
            continue
        frames.append(df)
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    df["datetime"] = pd.to_datetime(df["datetime"], format="%Y%m%d%H%M" if timeframe == "minute" else "%Y%m%d")
    df = df.drop_duplicates(subset="datetime").sort_values("datetime").reset_index(drop=True)
    for col in ("open", "close", "high", "low", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["open", "close", "high", "low"])
    df = df[(df["open"] > 0) & (df["close"] > 0) & (df["high"] >= df["low"])]
    return df.reset_index(drop=True)


def load_symbols(path):
    try:
        with open(path) as f:
            return [l.strip().upper() for l in f if l.strip() and not l.startswith("#")]
    except FileNotFoundError:
        sys.exit(f"ERROR: {path} not found")


def parse_range_bound(s, is_end):
    """Parse --start/--end. Accepts YYYYMMDD (whole-day granularity) or
    YYYYMMDDHHmm (exact bar, mainly useful on minute data). A bare date used
    as --end is treated as through the end of that day, so --end 20250630
    includes all of June 30th rather than cutting off at midnight."""
    if s is None:
        return None
    s = s.strip()
    for fmt in ("%Y%m%d%H%M", "%Y%m%d"):
        try:
            dt = datetime.strptime(s, fmt)
        except ValueError:
            continue
        if fmt == "%Y%m%d" and is_end:
            dt = dt + timedelta(days=1) - timedelta(minutes=1)
        return dt
    sys.exit(f"ERROR: bad --{'end' if is_end else 'start'} date '{s}', "
              f"expected YYYYMMDD or YYYYMMDDHHmm")


# ── Indicators ────────────────────────────────────────────────────────────────

def add_indicators(df, sma_trend, bb_period, rsi_period, atr_period):
    df = df.copy()
    df["sma_trend"] = df["close"].rolling(sma_trend, min_periods=sma_trend).mean()

    bb_mid = df["close"].rolling(bb_period, min_periods=bb_period).mean()
    bb_std = df["close"].rolling(bb_period, min_periods=bb_period).std()
    df["bb_mid"]   = bb_mid
    df["bb_upper"] = bb_mid + 2 * bb_std
    df["bb_lower"] = bb_mid - 2 * bb_std

    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / rsi_period, min_periods=rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / rsi_period, min_periods=rsi_period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi"] = 100 - (100 / (1 + rs))
    df["rsi"] = df["rsi"].fillna(50)  # no movement yet -> neutral, not a signal either way

    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr"] = tr.ewm(alpha=1 / atr_period, min_periods=atr_period, adjust=False).mean()

    return df


# ── Trade simulation ─────────────────────────────────────────────────────────

def simulate(df, capital, rsi_buy, rsi_sell, stop_atr_mult, max_hold,
             signal_window=0, progress_cb=None):
    """Long-only, one position at a time, fully-invested per trade.

    signal_window: how many bars back a BB+RSI oversold "dip" can have fired
    and still count as live, as long as price is above the trend SMA *now*.
    0 (default) reproduces the original strict same-bar requirement. This
    exists because requiring the dip and the trend confirmation on the exact
    same bar turned out to almost never happen for trending growth stocks —
    real pullbacks often bottom a bar or two before the trend average
    reflects it. Once a stale dip is used to enter, it's consumed so it
    can't immediately retrigger a second entry.

    Returns (trades: list[dict], equity: DataFrame[datetime, equity]).
    Vectorized into numpy arrays up front — this loop still runs once per
    bar (needed for the stateful position tracking), but on ~1M-row minute
    files that's the difference between minutes and seconds.
    """
    n = len(df)
    if n == 0:
        return [], pd.DataFrame(columns=["datetime", "equity"])

    close     = df["close"].to_numpy()
    bb_lower  = df["bb_lower"].to_numpy()
    bb_upper  = df["bb_upper"].to_numpy()
    rsi       = df["rsi"].to_numpy()
    sma_trend = df["sma_trend"].to_numpy()
    atr       = df["atr"].to_numpy()
    dt_list   = df["datetime"].tolist()

    ready_mask = ~(np.isnan(sma_trend) | np.isnan(bb_lower) | np.isnan(bb_upper) | np.isnan(atr))
    if not ready_mask.any():
        return [], pd.DataFrame(columns=["datetime", "equity"])
    ready = int(np.argmax(ready_mask))

    dip_cond = (close <= bb_lower) & (rsi < rsi_buy)

    trades = []
    equity_vals = np.empty(n)
    cash = capital
    shares = 0.0
    position = None
    bars_since_dip = signal_window + 1  # "no recent dip" until one actually fires
    progress_step = max((n - ready) // 20, 1)

    for i in range(ready, n):
        price = close[i]
        if dip_cond[i]:
            bars_since_dip = 0
        else:
            bars_since_dip += 1

        if position is None:
            buy_signal = (bars_since_dip <= signal_window) and (price > sma_trend[i])
            if buy_signal and cash > 0:
                shares = cash / price
                position = {"entry_i": i, "entry_price": price, "entry_atr": atr[i], "bars_held": 0}
                cash = 0.0
                bars_since_dip = signal_window + 1  # consume it, don't let it retrigger next bar
        else:
            position["bars_held"] += 1
            exit_price, reason = None, None

            stop_price = position["entry_price"] - stop_atr_mult * position["entry_atr"]
            if price <= stop_price:
                exit_price, reason = price, "stop_loss"
            elif price >= bb_upper[i] or rsi[i] > rsi_sell:
                exit_price, reason = price, "take_profit"
            elif position["bars_held"] >= max_hold:
                exit_price, reason = price, "time_stop"

            if exit_price is not None:
                cash = shares * exit_price
                pnl_pct = (exit_price / position["entry_price"] - 1) * 100
                trades.append({
                    "entry_date": dt_list[position["entry_i"]].isoformat(),
                    "entry_price": round(position["entry_price"], 4),
                    "exit_date": dt_list[i].isoformat(),
                    "exit_price": round(exit_price, 4),
                    "pnl_pct": round(pnl_pct, 3),
                    "bars_held": position["bars_held"],
                    "exit_reason": reason,
                })
                shares = 0.0
                position = None

        equity_vals[i] = cash if position is None else shares * price

        if progress_cb and (i - ready) % progress_step == 0:
            progress_cb(i - ready, n - ready, dt_list[i])

    if progress_cb:
        progress_cb(n - ready, n - ready, dt_list[-1])  # final 100% tick

    equity = pd.DataFrame({"datetime": dt_list[ready:n], "equity": equity_vals[ready:n]})
    return trades, equity



# ── Metrics ───────────────────────────────────────────────────────────────────

def compute_metrics(trades, equity, capital):
    if equity.empty:
        return None
    final_equity = equity["equity"].iloc[-1]
    total_return_pct = (final_equity / capital - 1) * 100

    span_days = max((equity["datetime"].iloc[-1] - equity["datetime"].iloc[0]).days, 1)
    years = span_days / 365.25
    cagr_pct = ((final_equity / capital) ** (1 / years) - 1) * 100 if years > 0 and final_equity > 0 else 0.0

    running_max = equity["equity"].cummax()
    drawdown = (equity["equity"] - running_max) / running_max
    max_drawdown_pct = drawdown.min() * 100

    rets = equity["equity"].pct_change().dropna()
    sharpe = 0.0
    if len(rets) > 1 and rets.std() > 0:
        periods_per_year = 252 if span_days / max(len(equity), 1) >= 0.5 else 252 * 390
        sharpe = (rets.mean() / rets.std()) * np.sqrt(periods_per_year)

    n = len(trades)
    wins = [t for t in trades if t["pnl_pct"] > 0]
    losses = [t for t in trades if t["pnl_pct"] <= 0]
    win_rate = (len(wins) / n * 100) if n else 0.0
    avg_win = np.mean([t["pnl_pct"] for t in wins]) if wins else 0.0
    avg_loss = np.mean([t["pnl_pct"] for t in losses]) if losses else 0.0
    gross_profit = sum(t["pnl_pct"] for t in wins)
    gross_loss = abs(sum(t["pnl_pct"] for t in losses))
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    return {
        "final_equity": round(final_equity, 2),
        "total_return_pct": round(total_return_pct, 2),
        "cagr_pct": round(cagr_pct, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "sharpe": round(sharpe, 2),
        "num_trades": n,
        "win_rate_pct": round(win_rate, 1),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "profit_factor": None if profit_factor == float("inf") else round(profit_factor, 2),
    }


# ── Per symbol/timeframe run ──────────────────────────────────────────────────

def downsample(df, max_points=2000):
    """Dashboard-friendly point cap so multi-year minute data doesn't ship a
    huge JSON payload — keeps the shape of the curve, not every bar."""
    if len(df) <= max_points:
        return df
    step = len(df) // max_points
    return df.iloc[::step]


def _make_progress_printer(symbol, timeframe):
    """Overwrites a single terminal line so a slow ~1M-row minute simulation
    shows visible progress and the current simulated date, instead of
    sitting silent for a couple of minutes with no feedback."""
    def cb(done, total, current_dt):
        pct = int(100 * done / total) if total else 100
        end = "\n" if done >= total else "\r"
        print(f"    ... {symbol}/{timeframe} {pct:3d}%  "
              f"at {current_dt:%Y-%m-%d %H:%M}          ", end=end, flush=True)
    return cb


def compute_diagnostics(df, rsi_buy, signal_window=0):
    """Bar counts for each entry sub-condition, post warm-up, so a 0-trade
    result is explainable rather than a black box: which of the three AND'd
    conditions (BB touch / RSI oversold / above trend) is the bottleneck,
    and how many bars satisfy all three at once (this is an upper bound on
    trade count — the simulator also needs no position already open).
    all_conditions_bars_windowed additionally reports the count once
    signal_window tolerance is applied (dip within the last N bars, trend
    checked now) — this is what actually drives trade count when
    signal_window > 0, since the strict same-bar count can be 0 while the
    windowed one isn't."""
    sub = df.dropna(subset=["sma_trend", "bb_lower", "rsi"])
    if sub.empty:
        return {"bars_total": len(df), "bars_warmed_up": 0, "bb_touch_bars": 0,
                "rsi_oversold_bars": 0, "above_trend_bars": 0, "all_conditions_bars": 0,
                "all_conditions_bars_windowed": 0, "signal_window": signal_window}
    cond_bb    = sub["close"] <= sub["bb_lower"]
    cond_rsi   = sub["rsi"] < rsi_buy
    cond_trend = sub["close"] > sub["sma_trend"]
    combined   = cond_bb & cond_rsi & cond_trend
    dip_recent = (cond_bb & cond_rsi).rolling(signal_window + 1, min_periods=1).max().astype(bool)
    combined_windowed = dip_recent & cond_trend
    return {
        "bars_total": len(df),
        "bars_warmed_up": len(sub),
        "bb_touch_bars": int(cond_bb.sum()),
        "rsi_oversold_bars": int(cond_rsi.sum()),
        "above_trend_bars": int(cond_trend.sum()),
        "all_conditions_bars": int(combined.sum()),
        "all_conditions_bars_windowed": int(combined_windowed.sum()),
        "signal_window": signal_window,
    }


def explain_zero_trades(symbol, timeframe, diag, rsi_buy):
    if diag["bars_warmed_up"] == 0:
        print(f"    -> not enough bars for indicators to warm up "
              f"({diag['bars_total']} bars on disk)")
        return
    counts = [
        ("touches lower Bollinger Band", diag["bb_touch_bars"]),
        (f"RSI < {rsi_buy:g}", diag["rsi_oversold_bars"]),
        ("close > trend SMA", diag["above_trend_bars"]),
    ]
    bottleneck = min(counts, key=lambda c: c[1])
    print(f"    -> of {diag['bars_warmed_up']} usable bars: "
          + ", ".join(f"{label}: {n}" for label, n in counts)
          + f" | all three same bar: {diag['all_conditions_bars']}"
          + (f" | within {diag['signal_window']}-bar window: {diag['all_conditions_bars_windowed']}"
             if diag["signal_window"] > 0 else ""))
    if diag["signal_window"] == 0 and diag["all_conditions_bars"] == 0:
        print(f"    -> tightest constraint: '{bottleneck[0]}' ({bottleneck[1]} bars). "
              f"Try --signal-window 3 (or higher) to let the dip and the trend check "
              f"land on different bars instead of requiring the exact same one.")
    else:
        print(f"    -> tightest constraint: '{bottleneck[0]}' ({bottleneck[1]} bars) "
              f"— loosen that threshold if you expected trades here")


def run_one(symbol, timeframe, args):
    df = load_symbol_bars(symbol, timeframe)
    if df is None or len(df) < 50:
        print(f"  [{symbol}/{timeframe}] not enough data, skipping")
        return None

    d = DEFAULTS[timeframe]
    # Indicators are computed on the FULL history so a --start date doesn't
    # cost you the warm-up window — e.g. --start 20250101 with sma_trend=50
    # still gets a real 50-day trend value on day 1, using late-2024 bars,
    # instead of the first ~50 days of the range being unusable.
    df = add_indicators(df, d["sma_trend"], d["bb_period"], args.rsi_period, d["atr_period"])

    sim_df = df
    if args.start_dt is not None:
        sim_df = sim_df[sim_df["datetime"] >= args.start_dt]
    if args.end_dt is not None:
        sim_df = sim_df[sim_df["datetime"] <= args.end_dt]
    sim_df = sim_df.reset_index(drop=True)
    if sim_df.empty:
        print(f"  [{symbol}/{timeframe}] no bars in the requested date range, skipping")
        return None

    range_note = "" if (args.start_dt or args.end_dt) else " (full history — no --start/--end given)"
    print(f"  [{symbol}/{timeframe}] simulating {sim_df['datetime'].iloc[0]:%Y-%m-%d %H:%M} "
          f"→ {sim_df['datetime'].iloc[-1]:%Y-%m-%d %H:%M}  ({len(sim_df):,} bars){range_note}")

    show_progress = len(sim_df) > 50000
    progress_cb = _make_progress_printer(symbol, timeframe) if show_progress else None

    trades, equity = simulate(sim_df, args.capital, args.rsi_buy, args.rsi_sell,
                               args.stop_atr_mult, d["max_hold"],
                               signal_window=args.signal_window, progress_cb=progress_cb)
    metrics = compute_metrics(trades, equity, args.capital)
    if metrics is None:
        print(f"  [{symbol}/{timeframe}] indicators never warmed up in this range, skipping")
        return None

    os.makedirs(OUT_DIR, exist_ok=True)
    trades_path = os.path.join(OUT_DIR, f"{symbol}_{timeframe}_trades.csv")
    pd.DataFrame(trades).to_csv(trades_path, index=False)

    eq_small = downsample(equity)
    equity_curve = [{"t": r["datetime"].isoformat(), "e": round(r["equity"], 2)}
                     for _, r in eq_small.iterrows()]

    # Price + bands series for the dashboard's signal chart — sliced to the
    # post-warm-up region since bands are undefined before that anyway.
    warmed = sim_df.dropna(subset=["bb_lower", "bb_upper", "sma_trend"])
    px_small = downsample(warmed)
    price_curve = [
        {"t": r["datetime"].isoformat(), "c": round(r["close"], 4),
         "bu": round(r["bb_upper"], 4), "bl": round(r["bb_lower"], 4),
         "sma": round(r["sma_trend"], 4)}
        for _, r in px_small.iterrows()
    ]

    diag = compute_diagnostics(sim_df, args.rsi_buy, args.signal_window)

    print(f"  [{symbol}/{timeframe}] {metrics['num_trades']} trades, "
          f"{metrics['total_return_pct']:+.1f}% return, "
          f"{metrics['win_rate_pct']:.0f}% win rate")
    if metrics["num_trades"] == 0:
        explain_zero_trades(symbol, timeframe, diag, args.rsi_buy)
    elif args.verbose:
        print(f"    -> of {diag['bars_warmed_up']} usable bars: "
              f"BB touch {diag['bb_touch_bars']}, RSI<{args.rsi_buy:g} {diag['rsi_oversold_bars']}, "
              f"above trend {diag['above_trend_bars']}, all three {diag['all_conditions_bars']}")

    shipped_trades = trades[-200:]  # cap shipped to dashboard; full history is in the CSV
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "metrics": metrics,
        "diagnostics": diag,
        "equity_curve": equity_curve,
        "price_curve": price_curve,
        "trades": shipped_trades,
        "trades_capped": len(trades) > len(shipped_trades),
        "bars_used": len(sim_df),
        "start": sim_df["datetime"].iloc[0].isoformat(),
        "end": sim_df["datetime"].iloc[-1].isoformat(),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("symbols", nargs="*", help="Symbols to backtest (default: everything in --watchlist)")
    p.add_argument("--watchlist", "-w", default="watch.txt")
    p.add_argument("--timeframe", choices=["daily", "minute", "both"], default="both")
    p.add_argument("--capital", type=float, default=10000.0)
    p.add_argument("--rsi-period", type=int, default=14)
    p.add_argument("--rsi-buy", type=float, default=35.0, help="Oversold threshold to arm a buy")
    p.add_argument("--rsi-sell", type=float, default=65.0, help="Overbought threshold to force a sell")
    p.add_argument("--stop-atr-mult", type=float, default=2.0, help="Stop-loss distance in multiples of ATR at entry")
    p.add_argument("--signal-window", type=int, default=0,
                    help="Bars a BB+RSI dip signal stays valid before it expires (0 = must land on the "
                         "same bar as the trend check, the strict original behavior). Try 3-5 if you're "
                         "getting 0 trades on daily data.")
    p.add_argument("--start", default=None, help="Backtest start: YYYYMMDD or YYYYMMDDHHmm. Omit to use all available history.")
    p.add_argument("--end", default=None, help="Backtest end (inclusive): YYYYMMDD or YYYYMMDDHHmm. Omit to use all available history.")
    p.add_argument("--verbose", action="store_true", help="Print entry-condition bar counts for every run, not just 0-trade ones")
    args = p.parse_args()
    args.start_dt = parse_range_bound(args.start, is_end=False)
    args.end_dt   = parse_range_bound(args.end, is_end=True)

    symbols = [s.upper() for s in args.symbols] if args.symbols else load_symbols(args.watchlist)
    timeframes = ["daily", "minute"] if args.timeframe == "both" else [args.timeframe]

    print(f"[backtest] {len(symbols)} symbol(s) x {len(timeframes)} timeframe(s), "
          f"${args.capital:,.0f} starting capital per run")

    results = []
    for symbol in symbols:
        for tf in timeframes:
            r = run_one(symbol, tf, args)
            if r:
                results.append(r)

    if not results:
        sys.exit("No results produced — check that data/<year>/<SYMBOL>_*.csv files exist.")

    os.makedirs(OUT_DIR, exist_ok=True)
    summary = {
        "generated_at": pd.Timestamp.now(tz="UTC").isoformat(),
        "capital": args.capital,
        "params": {
            "rsi_period": args.rsi_period, "rsi_buy": args.rsi_buy, "rsi_sell": args.rsi_sell,
            "stop_atr_mult": args.stop_atr_mult, "signal_window": args.signal_window,
            "start": args.start_dt.isoformat() if args.start_dt else None,
            "end": args.end_dt.isoformat() if args.end_dt else None,
            "timeframe_defaults": DEFAULTS,
        },
        "runs": results,
    }
    with open(os.path.join(OUT_DIR, "summary.json"), "w") as f:
        json.dump(summary, f)

    print(f"\n[done] wrote {len(results)} run(s) to {OUT_DIR}/summary.json "
          f"(open dashboard.html in a browser served from the project root)")


if __name__ == "__main__":
    main()