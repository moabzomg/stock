"""
report.py
Generates a self-contained HTML report with equity curve, feature importance,
trade log, and model CV scores.
"""

import base64
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime


def generate_report(results: dict, model) -> None:
    stats        = results["stats"]
    trades       = results["trades"]
    equity_curve = results["equity_curve"]
    cv_scores    = model.cv_scores

    # Embed equity curve PNG
    img_b64 = ""
    img_path = Path("equity_curve.png")
    if img_path.exists():
        img_b64 = base64.b64encode(img_path.read_bytes()).decode()

    # Feature importance table
    fi       = model.feature_importance()
    fi_rows  = ""
    if not fi.empty:
        for feat, imp in fi.head(15).items():
            bar = int(imp / fi.max() * 200)
            fi_rows += (
                f"<tr><td>{feat}</td><td>{imp:.4f}</td>"
                f"<td><div class='bar' style='width:{bar}px'></div></td></tr>"
            )

    # Trade log (last 50)
    trade_rows = ""
    for t in sorted(trades, key=lambda x: x.entry_date, reverse=True)[:50]:
        color = "#22c55e" if t.pnl_pct > 0 else "#ef4444"
        trade_rows += (
            f"<tr>"
            f"<td>{t.symbol}</td>"
            f"<td>{t.entry_date.date()}</td>"
            f"<td>${t.entry_price:.2f}</td>"
            f"<td>{t.exit_date.date() if t.exit_date else '—'}</td>"
            f"<td>${t.exit_price:.2f if t.exit_price else 0:.2f}</td>"
            f"<td style='color:{color}'>{t.pnl_pct:.1%}</td>"
            f"<td>${t.pnl_dollars:,.0f}</td>"
            f"<td>{t.exit_reason or '—'}</td>"
            f"</tr>"
        )

    stats_rows = "".join(
        f"<tr><td>{k}</td><td><strong>{v}</strong></td></tr>"
        for k, v in stats.items()
    )

    cv_html = f"""
    <tr><td>AUC (mean ± std)</td><td><strong>{cv_scores.get('auc_mean',0):.3f}
    ± {cv_scores.get('auc_std',0):.3f}</strong></td></tr>
    <tr><td>Precision</td><td><strong>{cv_scores.get('prec_mean',0):.3f}</strong></td></tr>
    <tr><td>Recall</td><td><strong>{cv_scores.get('rec_mean',0):.3f}</strong></td></tr>
    <tr><td>F1 Score</td><td><strong>{cv_scores.get('f1_mean',0):.3f}</strong></td></tr>
    """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Minervini ML Report</title>
<style>
  :root {{
    --bg: #0f1117; --surface: #1a1d27; --border: #2d3148;
    --text: #e2e8f0; --muted: #64748b; --accent: #00d4aa;
    --danger: #ef4444; --success: #22c55e;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: 'Inter', system-ui, sans-serif;
         font-size: 14px; line-height: 1.6; padding: 40px; }}
  h1 {{ font-size: 26px; color: var(--accent); margin-bottom: 4px; }}
  h2 {{ font-size: 16px; color: var(--accent); margin: 32px 0 12px;
        border-bottom: 1px solid var(--border); padding-bottom: 6px; }}
  .meta {{ color: var(--muted); font-size: 12px; margin-bottom: 32px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
           gap: 12px; margin-bottom: 32px; }}
  .card {{ background: var(--surface); border: 1px solid var(--border); border-radius: 8px;
           padding: 16px; }}
  .card .label {{ color: var(--muted); font-size: 12px; margin-bottom: 4px; }}
  .card .value {{ font-size: 20px; font-weight: 700; color: var(--accent); }}
  table {{ width: 100%; border-collapse: collapse; margin-bottom: 24px; font-size: 13px; }}
  th {{ background: var(--surface); color: var(--muted); padding: 8px 12px;
        text-align: left; font-weight: 600; border-bottom: 1px solid var(--border); }}
  td {{ padding: 7px 12px; border-bottom: 1px solid var(--border); }}
  tr:hover td {{ background: rgba(255,255,255,0.02); }}
  .bar {{ height: 8px; background: var(--accent); border-radius: 4px; opacity: 0.75; }}
  img {{ max-width: 100%; border-radius: 8px; border: 1px solid var(--border); }}
  .rules {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 10px; }}
  .rule {{ background: var(--surface); border: 1px solid var(--border); border-radius: 6px;
           padding: 10px 14px; font-size: 12px; }}
  .rule .num {{ color: var(--accent); font-weight: 700; margin-right: 6px; }}
  .section {{ margin-bottom: 40px; }}
</style>
</head>
<body>
<h1>🏆 Minervini Trend Template + ML Trading System</h1>
<p class="meta">Generated {datetime.now().strftime("%Y-%m-%d %H:%M")} &nbsp;|&nbsp;
5-Year Backtest &nbsp;|&nbsp; IB TWS port 7496</p>

<h2>Performance Summary</h2>
<div class="grid">
  {"".join(f'<div class="card"><div class="label">{k}</div><div class="value">{v}</div></div>' for k,v in stats.items())}
</div>

<h2>Equity Curve</h2>
<div class="section">
  {'<img src="data:image/png;base64,' + img_b64 + '">' if img_b64 else '<p>equity_curve.png not found</p>'}
</div>

<h2>ML Model — Walk-Forward CV Scores</h2>
<table><thead><tr><th>Metric</th><th>Value</th></tr></thead>
<tbody>{cv_html}</tbody></table>

<h2>Top 15 Feature Importances</h2>
<table><thead><tr><th>Feature</th><th>Importance</th><th>Relative</th></tr></thead>
<tbody>{fi_rows}</tbody></table>

<h2>Minervini 7-Rule Screen</h2>
<div class="rules section">
  <div class="rule"><span class="num">1</span> Price &gt; 150-SMA &amp; 200-SMA — above long-term trend</div>
  <div class="rule"><span class="num">2</span> 150-SMA &gt; 200-SMA — shorter MA accelerating</div>
  <div class="rule"><span class="num">3</span> 200-SMA trending up for ≥ 1 month</div>
  <div class="rule"><span class="num">4</span> 50-SMA &gt; 150-SMA &amp; 200-SMA — full alignment</div>
  <div class="rule"><span class="num">5</span> Price &gt; 50-SMA — buying pressure confirmed</div>
  <div class="rule"><span class="num">6</span> Price within 25 % of 52-week high</div>
  <div class="rule"><span class="num">7</span> Price ≥ 30 % above 52-week low</div>
</div>

<h2>Trade Log (Most Recent 50)</h2>
<table>
<thead><tr>
  <th>Symbol</th><th>Entry</th><th>Entry $</th>
  <th>Exit</th><th>Exit $</th><th>Return</th><th>P&amp;L</th><th>Reason</th>
</tr></thead>
<tbody>{trade_rows}</tbody>
</table>

</body></html>"""

    Path("minervini_ml_report.html").write_text(html, encoding="utf-8")
    print("      Report saved → minervini_ml_report.html")
