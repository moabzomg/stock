#!/usr/bin/env python3
"""
Trading Dashboard — HTML output
Usage: python3 trading_dashboard.py <csv_file>
Output: trading_dashboard_YYYYMMDD.html  (same dir as the CSV)

CSV columns expected (tab or comma separated):
  date, action, ticker, price, qty, cost_usd, pnl_usd, reason, held_days, score
"""

import sys
import os
import base64
import io
from datetime import datetime

import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter

# ── Palette ────────────────────────────────────────────────────────────────
BG    = "#0d1117"
PANEL = "#161b22"
BORDER= "#21262d"
GREEN = "#3fb950"
RED   = "#f85149"
GOLD  = "#d29922"
BLUE  = "#58a6ff"
MUTED = "#8b949e"
TEXT  = "#e6edf3"
WHITE = "#ffffff"

plt.rcParams.update({
    "figure.facecolor": BG, "axes.facecolor": PANEL,
    "axes.edgecolor": BORDER, "axes.labelcolor": TEXT,
    "xtick.color": MUTED, "ytick.color": MUTED,
    "text.color": TEXT, "grid.color": BORDER,
    "grid.linestyle": "--", "grid.linewidth": 0.5,
    "font.family": "monospace",
})

usd  = FuncFormatter(lambda x, _: f"${x:+.2f}")
usd0 = FuncFormatter(lambda x, _: f"${x:.0f}")

# ── Helpers ────────────────────────────────────────────────────────────────
def fig_to_b64(fig):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()

def panel(ax, title):
    ax.set_title(title, color=GOLD, fontsize=10, pad=8, loc="left")
    ax.grid(zorder=0)
    ax.set_axisbelow(True)

# ── Chart builders ─────────────────────────────────────────────────────────
def chart_pnl_bar(sells):
    fig, ax = plt.subplots(figsize=(9, 3.5), facecolor=BG)
    colors = [GREEN if p >= 0 else RED for p in sells["pnl_usd"]]
    labels = [f"{r.ticker}\n{r.date.strftime('%b %d')}" for _, r in sells.iterrows()]
    bars = ax.bar(labels, sells["pnl_usd"], color=colors, width=0.55, zorder=3)
    for bar, val in zip(bars, sells["pnl_usd"]):
        yoff = 0.06 if val >= 0 else -0.06
        va   = "bottom" if val >= 0 else "top"
        ax.text(bar.get_x() + bar.get_width() / 2, val + yoff,
                f"${val:+.2f}", ha="center", va=va, fontsize=9,
                color=WHITE, fontweight="bold")
    ax.axhline(0, color=BORDER, linewidth=1)
    ax.yaxis.set_major_formatter(usd)
    panel(ax, "PnL per Closed Trade (USD)")
    fig.tight_layout()
    return fig_to_b64(fig)

def chart_cumulative(sells):
    fig, ax = plt.subplots(figsize=(5.5, 3.5), facecolor=BG)
    cum = sells["pnl_usd"].cumsum().reset_index(drop=True)
    idx = list(range(len(cum)))
    ax.plot(idx, cum, color=BLUE, lw=2, zorder=3, marker="o",
            markersize=6, markerfacecolor=WHITE, markeredgecolor=BLUE)
    ax.fill_between(idx, cum, alpha=0.15, color=BLUE, zorder=2)
    for i, v in enumerate(cum):
        ax.text(i, v + abs(cum.max() - cum.min()) * 0.04,
                f"${v:+.2f}", ha="center", fontsize=8, color=BLUE)
    ax.set_xticks(idx)
    ax.set_xticklabels([f"T{i+1}" for i in idx], fontsize=8)
    ax.yaxis.set_major_formatter(usd)
    panel(ax, "Cumulative PnL")
    fig.tight_layout()
    return fig_to_b64(fig)

def chart_capital(buys):
    fig, ax = plt.subplots(figsize=(9, max(3.5, len(buys) * 0.4 + 1)), facecolor=BG)
    buy_s = buys.sort_values("cost_usd")
    bars  = ax.barh(buy_s["ticker"], buy_s["cost_usd"], color=BLUE, height=0.6, zorder=3)
    for bar, val in zip(bars, buy_s["cost_usd"]):
        ax.text(val + buy_s["cost_usd"].max() * 0.01,
                bar.get_y() + bar.get_height() / 2,
                f"${val:.2f}", va="center", fontsize=8, color=WHITE)
    ax.xaxis.set_major_formatter(usd0)
    panel(ax, "Capital Deployed per Buy")
    ax.grid(axis="x")
    ax.grid(axis="y", alpha=0)
    fig.tight_layout()
    return fig_to_b64(fig)

def chart_hold_pnl(sells):
    fig, ax = plt.subplots(figsize=(5.5, 3.5), facecolor=BG)
    colors = [GREEN if p >= 0 else RED for p in sells["pnl_usd"]]
    ax.scatter(sells["held_days"], sells["pnl_usd"],
               c=colors, s=120, zorder=4, edgecolors=WHITE, linewidths=0.5)
    for _, r in sells.iterrows():
        ax.annotate(r["ticker"], (r["held_days"], r["pnl_usd"]),
                    textcoords="offset points", xytext=(6, 3), fontsize=8, color=TEXT)
    ax.axhline(0, color=BORDER, lw=1)
    ax.yaxis.set_major_formatter(usd)
    ax.set_xlabel("Days Held", fontsize=8)
    panel(ax, "Hold Days vs PnL")
    fig.tight_layout()
    return fig_to_b64(fig)

def chart_exit_pie(sells):
    def classify(r):
        if "RSI" in r:    return "Overbought RSI"
        if "STOP" in r:   return "Stop Loss"
        if "TARGET" in r: return "Target Hit"
        return "Other"
    sells = sells.copy()
    sells["exit_type"] = sells["reason"].apply(classify)
    counts = sells["exit_type"].value_counts()
    wedge_colors = [GREEN, RED, GOLD, BLUE][:len(counts)]
    fig, ax = plt.subplots(figsize=(4.5, 3.5), facecolor=BG)
    wedges, texts, autotexts = ax.pie(
        counts.values, labels=counts.index,
        colors=wedge_colors, autopct="%1.0f%%", startangle=90,
        textprops={"color": TEXT, "fontsize": 8},
        wedgeprops={"edgecolor": BG, "linewidth": 2},
        pctdistance=0.72,
    )
    for at in autotexts:
        at.set_fontsize(8); at.set_color(BG); at.set_fontweight("bold")
    ax.set_title("Exit Reason Mix", color=GOLD, fontsize=10, pad=8, loc="left")
    fig.tight_layout()
    return fig_to_b64(fig)

def chart_timeline(df):
    fig, ax = plt.subplots(figsize=(10, 2.8), facecolor=BG)
    min_date = df["date"].min()
    df = df.copy()
    df["dn"] = (df["date"] - min_date).dt.days
    buys  = df[df["action"] == "BUY"]
    sells = df[df["action"] == "SELL"]
    ax.scatter(buys["dn"],  [1]*len(buys),  color=GREEN, s=90, zorder=4,
               edgecolors=WHITE, linewidths=0.4, label="BUY")
    ax.scatter(sells["dn"], [0]*len(sells), color=RED,   s=90, zorder=4,
               edgecolors=WHITE, linewidths=0.4, label="SELL")
    for _, r in df.iterrows():
        y = 1 if r["action"] == "BUY" else 0
        ax.text(r["dn"], y + (0.12 if y else -0.18), r["ticker"],
                ha="center", fontsize=6.5, color=TEXT)
    all_days = sorted(df["dn"].unique())
    ax.set_xticks(all_days)
    ax.set_xticklabels(
        [(min_date + pd.Timedelta(days=d)).strftime("%b %d") for d in all_days],
        fontsize=7)
    ax.set_yticks([0, 1])
    ax.set_yticklabels(["SELL", "BUY"], fontsize=8)
    ax.set_ylim(-0.5, 1.6)
    ax.grid(axis="x", zorder=0)
    ax.legend(fontsize=7, loc="upper right", facecolor=PANEL,
              edgecolor=BORDER, labelcolor=TEXT)
    ax.set_title("Trade Timeline", color=GOLD, fontsize=10, pad=8, loc="left")
    fig.tight_layout()
    return fig_to_b64(fig)

# ── Summary stats ──────────────────────────────────────────────────────────
def build_stats(df, sells, buys):
    total_pnl    = sells["pnl_usd"].sum()
    wins         = (sells["pnl_usd"] > 0).sum()
    losses       = (sells["pnl_usd"] < 0).sum()
    win_rate     = wins / len(sells) * 100 if len(sells) else 0
    avg_win      = sells[sells["pnl_usd"] > 0]["pnl_usd"].mean() if wins else 0
    avg_loss     = sells[sells["pnl_usd"] < 0]["pnl_usd"].mean() if losses else 0
    deployed     = buys["cost_usd"].sum()
    roi          = total_pnl / deployed * 100 if deployed else 0
    avg_hold     = sells["held_days"].mean() if len(sells) else 0
    date_range   = f"{df['date'].min().strftime('%b %d')} – {df['date'].max().strftime('%b %d, %Y')}"
    best         = sells.loc[sells["pnl_usd"].idxmax(), "ticker"] if len(sells) else "—"
    worst        = sells.loc[sells["pnl_usd"].idxmin(), "ticker"] if len(sells) else "—"
    return dict(
        total_pnl=total_pnl, wins=int(wins), losses=int(losses),
        total_sells=len(sells), win_rate=win_rate,
        avg_win=avg_win, avg_loss=avg_loss,
        deployed=deployed, roi=roi, avg_hold=avg_hold,
        date_range=date_range, best=best, worst=worst,
    )

# ── HTML builder ───────────────────────────────────────────────────────────
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Trading Dashboard — {date_range}</title>
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}

  :root {{
    --bg:     #0d1117;
    --panel:  #161b22;
    --border: #21262d;
    --green:  #3fb950;
    --red:    #f85149;
    --gold:   #d29922;
    --blue:   #58a6ff;
    --muted:  #8b949e;
    --text:   #e6edf3;
    --white:  #ffffff;
  }}

  body {{
    background: var(--bg);
    color: var(--text);
    font-family: 'JetBrains Mono', 'Fira Code', 'Cascadia Code', ui-monospace, monospace;
    font-size: 13px;
    line-height: 1.5;
    padding: 24px 20px 48px;
  }}

  /* ── Header ── */
  header {{
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    border-bottom: 1px solid var(--border);
    padding-bottom: 14px;
    margin-bottom: 24px;
    flex-wrap: wrap;
    gap: 8px;
  }}
  .logo {{ font-size: 11px; color: var(--muted); letter-spacing: .12em; text-transform: uppercase; }}
  h1 {{
    font-size: 22px;
    font-weight: 700;
    color: var(--white);
    letter-spacing: -.01em;
  }}
  .datebadge {{
    font-size: 11px;
    color: var(--muted);
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 3px 10px;
  }}

  /* ── KPI strip ── */
  .kpi-strip {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
    gap: 10px;
    margin-bottom: 24px;
  }}
  .kpi {{
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 14px 16px;
  }}
  .kpi-label {{ font-size: 10px; color: var(--muted); text-transform: uppercase; letter-spacing: .08em; margin-bottom: 5px; }}
  .kpi-value {{ font-size: 22px; font-weight: 700; line-height: 1; }}
  .kpi-value.pos {{ color: var(--green); }}
  .kpi-value.neg {{ color: var(--red); }}
  .kpi-value.neu {{ color: var(--blue); }}
  .kpi-value.gld {{ color: var(--gold); }}

  /* ── Chart grid ── */
  .grid-2 {{ display: grid; grid-template-columns: 3fr 2fr; gap: 14px; margin-bottom: 14px; }}
  .grid-3 {{ display: grid; grid-template-columns: 3fr 2fr 1.6fr; gap: 14px; margin-bottom: 14px; }}
  .full   {{ margin-bottom: 14px; }}

  .card {{
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
  }}
  .card-title {{
    font-size: 10px;
    font-weight: 600;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: .1em;
    padding: 10px 14px 0;
  }}
  .card img {{ width: 100%; height: auto; display: block; }}

  /* ── Trade table ── */
  .table-wrap {{
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow-x: auto;
    margin-bottom: 14px;
  }}
  table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
  thead th {{
    background: var(--bg);
    color: var(--muted);
    text-transform: uppercase;
    font-size: 10px;
    letter-spacing: .08em;
    font-weight: 600;
    padding: 9px 12px;
    text-align: left;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }}
  tbody tr {{ border-bottom: 1px solid var(--border); transition: background .12s; }}
  tbody tr:last-child {{ border-bottom: none; }}
  tbody tr:hover {{ background: rgba(255,255,255,.03); }}
  tbody td {{ padding: 8px 12px; white-space: nowrap; }}
  .buy  {{ color: var(--green); font-weight: 600; }}
  .sell {{ color: var(--red);   font-weight: 600; }}
  .pnl-pos {{ color: var(--green); font-weight: 600; }}
  .pnl-neg {{ color: var(--red);   font-weight: 600; }}
  .pnl-zer {{ color: var(--muted); }}
  .tag {{
    display: inline-block;
    background: rgba(88,166,255,.12);
    border: 1px solid rgba(88,166,255,.25);
    border-radius: 3px;
    padding: 1px 6px;
    font-size: 10px;
    color: var(--blue);
  }}
  .tag.rsi  {{ background: rgba(211,153,34,.12); border-color: rgba(211,153,34,.3); color: var(--gold); }}
  .tag.stop {{ background: rgba(248,81,73,.12);  border-color: rgba(248,81,73,.3);  color: var(--red); }}

  /* ── Footer ── */
  footer {{ font-size: 10px; color: var(--muted); text-align: right; margin-top: 32px; border-top: 1px solid var(--border); padding-top: 12px; }}

  @media (max-width: 720px) {{
    .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
  }}
</style>
</head>
<body>

<header>
  <div>
    <div class="logo">Trading Journal</div>
    <h1>{date_range}</h1>
  </div>
  <span class="datebadge">Generated {generated}</span>
</header>

<!-- KPI strip -->
<div class="kpi-strip">
  <div class="kpi">
    <div class="kpi-label">Total PnL</div>
    <div class="kpi-value {pnl_cls}">{total_pnl_fmt}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Win Rate</div>
    <div class="kpi-value gld">{win_rate:.0f}%</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Trades</div>
    <div class="kpi-value neu">{wins}W / {losses}L</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Avg Win</div>
    <div class="kpi-value pos">{avg_win_fmt}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Avg Loss</div>
    <div class="kpi-value neg">{avg_loss_fmt}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Deployed</div>
    <div class="kpi-value neu">${deployed:.0f}</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">ROI</div>
    <div class="kpi-value {roi_cls}">{roi:+.2f}%</div>
  </div>
  <div class="kpi">
    <div class="kpi-label">Avg Hold</div>
    <div class="kpi-value neu">{avg_hold:.1f}d</div>
  </div>
</div>

<!-- Row 1: PnL bar + cumulative -->
<div class="grid-2">
  <div class="card">
    <div class="card-title">PnL per Closed Trade</div>
    <img src="data:image/png;base64,{img_pnl}" alt="PnL per trade">
  </div>
  <div class="card">
    <div class="card-title">Cumulative PnL</div>
    <img src="data:image/png;base64,{img_cum}" alt="Cumulative PnL">
  </div>
</div>

<!-- Row 2: Capital + Hold vs PnL + Pie -->
<div class="grid-3">
  <div class="card">
    <div class="card-title">Capital Deployed per Buy</div>
    <img src="data:image/png;base64,{img_cap}" alt="Capital deployed">
  </div>
  <div class="card">
    <div class="card-title">Hold Days vs PnL</div>
    <img src="data:image/png;base64,{img_hold}" alt="Hold days vs PnL">
  </div>
  <div class="card">
    <div class="card-title">Exit Reason Mix</div>
    <img src="data:image/png;base64,{img_pie}" alt="Exit reasons">
  </div>
</div>

<!-- Timeline -->
<div class="full card">
  <div class="card-title">Trade Timeline</div>
  <img src="data:image/png;base64,{img_tl}" alt="Trade timeline">
</div>

<!-- Trade log table -->
<div class="table-wrap">
  <table>
    <thead>
      <tr>
        <th>Date</th><th>Action</th><th>Ticker</th>
        <th>Price</th><th>Qty</th><th>Cost $</th>
        <th>PnL $</th><th>Held</th><th>Reason</th>
      </tr>
    </thead>
    <tbody>
{table_rows}
    </tbody>
  </table>
</div>

<footer>Source: {source_file} &nbsp;·&nbsp; {total_rows} rows</footer>

</body>
</html>"""

def make_tag(reason):
    if "RSI" in reason:  return f'<span class="tag rsi">{reason}</span>'
    if "STOP" in reason: return f'<span class="tag stop">{reason}</span>'
    return f'<span class="tag">{reason}</span>'

def build_table_rows(df):
    rows = []
    for _, r in df.iterrows():
        action_cls = "buy" if r["action"] == "BUY" else "sell"
        pnl = r["pnl_usd"]
        if pnl > 0:   pnl_cls, pnl_str = "pnl-pos", f"+${pnl:.2f}"
        elif pnl < 0: pnl_cls, pnl_str = "pnl-neg", f"-${abs(pnl):.2f}"
        else:         pnl_cls, pnl_str = "pnl-zer", "—"
        rows.append(
            f"      <tr>"
            f"<td>{r['date'].strftime('%Y-%m-%d')}</td>"
            f"<td class='{action_cls}'>{r['action']}</td>"
            f"<td><strong>{r['ticker']}</strong></td>"
            f"<td>${r['price']:.2f}</td>"
            f"<td>{int(r['qty'])}</td>"
            f"<td>${r['cost_usd']:.2f}</td>"
            f"<td class='{pnl_cls}'>{pnl_str}</td>"
            f"<td>{int(r['held_days'])}d</td>"
            f"<td>{make_tag(r['reason'])}</td>"
            f"</tr>"
        )
    return "\n".join(rows)

# ── Main ───────────────────────────────────────────────────────────────────
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 trading_dashboard.py <trades.csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    if not os.path.exists(csv_path):
        print(f"Error: file not found — {csv_path}")
        sys.exit(1)

    # Detect separator
    with open(csv_path) as f:
        sample = f.read(512)
    sep = "\t" if "\t" in sample else ","

    df = pd.read_csv(csv_path, sep=sep)
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])

    sells = df[df["action"] == "SELL"].copy()
    buys  = df[df["action"] == "BUY"].copy()

    if sells.empty:
        print("Warning: no SELL rows found — charts may be sparse.")

    stats = build_stats(df, sells, buys)

    print("Rendering charts…")
    img_pnl  = chart_pnl_bar(sells)  if not sells.empty else ""
    img_cum  = chart_cumulative(sells) if not sells.empty else ""
    img_cap  = chart_capital(buys)   if not buys.empty  else ""
    img_hold = chart_hold_pnl(sells) if not sells.empty else ""
    img_pie  = chart_exit_pie(sells) if not sells.empty else ""
    img_tl   = chart_timeline(df)

    table_rows = build_table_rows(df)

    total_pnl = stats["total_pnl"]
    roi       = stats["roi"]

    html = HTML_TEMPLATE.format(
        date_range    = stats["date_range"],
        generated     = datetime.now().strftime("%Y-%m-%d %H:%M"),
        total_pnl_fmt = f"${total_pnl:+.2f}",
        pnl_cls       = "pos" if total_pnl >= 0 else "neg",
        win_rate      = stats["win_rate"],
        wins          = stats["wins"],
        losses        = stats["losses"],
        avg_win_fmt   = f"${stats['avg_win']:+.2f}",
        avg_loss_fmt  = f"${stats['avg_loss']:+.2f}",
        deployed      = stats["deployed"],
        roi           = roi,
        roi_cls       = "pos" if roi >= 0 else "neg",
        avg_hold      = stats["avg_hold"],
        img_pnl       = img_pnl,
        img_cum       = img_cum,
        img_cap       = img_cap,
        img_hold      = img_hold,
        img_pie       = img_pie,
        img_tl        = img_tl,
        table_rows    = table_rows,
        source_file   = os.path.basename(csv_path),
        total_rows    = len(df),
    )

    today = datetime.now().strftime("%Y%m%d")
    out_dir  = os.path.dirname(os.path.abspath(csv_path))
    out_path = os.path.join(out_dir, f"trading_dashboard_{today}.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Saved → {out_path}")

if __name__ == "__main__":
    main()