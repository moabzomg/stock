#!/usr/bin/env python3
"""
Backtest Visualisation
======================
Reads the backtest CSV and produces an interactive Plotly dashboard with:
  1. Cumulative PnL over time
  2. Per-trade PnL bar chart (coloured by outcome)
  3. Exit-reason breakdown (pie chart)
  4. PnL by ticker (bar chart)
  5. Held-days distribution (histogram)

Usage:
    python visualise_backtest.py                         # uses default path
    python visualise_backtest.py path/to/backtest.csv   # custom path
"""

import sys
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ── Load data ────────────────────────────────────────────────────────────────
CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "backtest.csv"

df = pd.read_csv(CSV_PATH, parse_dates=["date"])
df = df.sort_values("date").reset_index(drop=True)

# Sell rows carry the realised PnL; buys always have 0
sells = df[df["action"].isin(["SELL", "SELL (end)"])].copy()
sells["cumulative_pnl"] = sells["pnl_gbp"].cumsum()

# Categorise each exit
def exit_category(row):
    r = row["reason"]
    if "STOP LOSS" in r:
        return "Stop Loss"
    if "TAKE PROFIT" in r:
        return "Take Profit"
    if "OVERBOUGHT" in r:
        return "Overbought (RSI)"
    if "Simulation ended" in r:
        return "Sim End"
    return "Other"

sells["category"] = sells.apply(exit_category, axis=1)

COLOURS = {
    "Stop Loss":      "#ef4444",
    "Take Profit":    "#22c55e",
    "Overbought (RSI)": "#f59e0b",
    "Sim End":        "#94a3b8",
    "Other":          "#a78bfa",
}
bar_colours = sells["category"].map(COLOURS)

# ── Build dashboard ───────────────────────────────────────────────────────────
fig = make_subplots(
    rows=3, cols=2,
    subplot_titles=(
        "Cumulative PnL (£)",
        "Per-Trade PnL (£)",
        "Exit Reason Breakdown",
        "PnL by Ticker (£)",
        "Holding Period Distribution (days)",
        "",          # blank — pie takes up visual space already
    ),
    specs=[
        [{"type": "xy"},   {"type": "xy"}],
        [{"type": "pie"},  {"type": "xy"}],
        [{"type": "xy"},   {"type": "xy"}],
    ],
    vertical_spacing=0.12,
    horizontal_spacing=0.10,
)

# 1 ── Cumulative PnL line ────────────────────────────────────────────────────
fig.add_trace(
    go.Scatter(
        x=sells["date"],
        y=sells["cumulative_pnl"],
        mode="lines+markers",
        line=dict(color="#6366f1", width=2),
        marker=dict(size=6),
        fill="tozeroy",
        fillcolor="rgba(99,102,241,0.1)",
        name="Cumulative PnL",
        hovertemplate="Date: %{x|%Y-%m-%d}<br>Cumul PnL: £%{y:.4f}<extra></extra>",
    ),
    row=1, col=1,
)

# Zero line
fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.3)", row=1, col=1)

# 2 ── Per-trade PnL bars ─────────────────────────────────────────────────────
fig.add_trace(
    go.Bar(
        x=sells["date"],
        y=sells["pnl_gbp"],
        marker_color=bar_colours,
        name="Trade PnL",
        text=sells["ticker"],
        textposition="outside",
        hovertemplate=(
            "Ticker: %{text}<br>"
            "Date: %{x|%Y-%m-%d}<br>"
            "PnL: £%{y:.4f}<extra></extra>"
        ),
    ),
    row=1, col=2,
)

# 3 ── Exit reason pie ────────────────────────────────────────────────────────
reason_counts = sells["category"].value_counts()
fig.add_trace(
    go.Pie(
        labels=reason_counts.index,
        values=reason_counts.values,
        marker=dict(colors=[COLOURS.get(c, "#888") for c in reason_counts.index]),
        hole=0.4,
        textinfo="label+percent",
        name="Exit Reasons",
        hovertemplate="%{label}: %{value} trades (%{percent})<extra></extra>",
    ),
    row=2, col=1,
)

# 4 ── PnL by ticker ──────────────────────────────────────────────────────────
ticker_pnl = (
    sells.groupby("ticker")["pnl_gbp"]
    .sum()
    .sort_values()
)
ticker_bar_colours = ["#22c55e" if v >= 0 else "#ef4444" for v in ticker_pnl.values]

fig.add_trace(
    go.Bar(
        x=ticker_pnl.index,
        y=ticker_pnl.values,
        marker_color=ticker_bar_colours,
        name="Ticker PnL",
        hovertemplate="Ticker: %{x}<br>Total PnL: £%{y:.4f}<extra></extra>",
    ),
    row=2, col=2,
)

# 5 ── Held-days histogram (only closed trades) ───────────────────────────────
held = sells[sells["held_days"] > 0]["held_days"]

fig.add_trace(
    go.Histogram(
        x=held,
        nbinsx=20,
        marker_color="#6366f1",
        opacity=0.8,
        name="Held Days",
        hovertemplate="Held: %{x} days<br>Count: %{y}<extra></extra>",
    ),
    row=3, col=1,
)

# ── Summary annotation ────────────────────────────────────────────────────────
total_pnl   = sells["pnl_gbp"].sum()
win_rate    = (sells["pnl_gbp"] > 0).mean() * 100
n_trades    = len(sells)
avg_win     = sells.loc[sells["pnl_gbp"] > 0, "pnl_gbp"].mean()
avg_loss    = sells.loc[sells["pnl_gbp"] < 0, "pnl_gbp"].mean()

summary_text = (
    f"<b>Summary</b><br>"
    f"Trades: {n_trades}  |  Total PnL: £{total_pnl:.4f}<br>"
    f"Win rate: {win_rate:.1f}%  |  Avg win: £{avg_win:.4f}  |  Avg loss: £{avg_loss:.4f}"
)

fig.add_annotation(
    text=summary_text,
    xref="paper", yref="paper",
    x=0.75, y=0.01,
    showarrow=False,
    align="center",
    bgcolor="rgba(30,30,50,0.85)",
    bordercolor="#6366f1",
    borderwidth=1,
    borderpad=8,
    font=dict(size=12, color="white"),
)

# ── Layout ────────────────────────────────────────────────────────────────────
fig.update_layout(
    title=dict(
        text="Backtest Dashboard",
        font=dict(size=22, color="white"),
        x=0.5,
    ),
    paper_bgcolor="#0f0f1a",
    plot_bgcolor="#1a1a2e",
    font=dict(color="#e2e8f0"),
    showlegend=False,
    height=1050,
    margin=dict(t=80, b=60, l=60, r=60),
)

# Style all xy axes
for axis in ["xaxis", "yaxis", "xaxis2", "yaxis2",
             "xaxis3", "yaxis3", "xaxis4", "yaxis4"]:
    fig.update_layout(**{
        axis: dict(
            gridcolor="rgba(255,255,255,0.07)",
            zerolinecolor="rgba(255,255,255,0.15)",
            tickfont=dict(color="#94a3b8"),
        )
    })

fig.write_html("backtest_dashboard_" + sys.argv[1][:-5] + ".html")
print("✓ Saved: backtest_dashboard_" + sys.argv[1][:-5] + ".html")

fig.show()