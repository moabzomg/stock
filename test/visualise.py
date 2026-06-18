import sys
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

if len(sys.argv) < 2:
    print("Usage: python3 visualise.py <csvfile>")
    sys.exit(1)

fname = sys.argv[1]
df = pd.read_csv(fname)

is_min = len(df['date'].astype(str).iloc[0]) == 14

if is_min:
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d%H%M%S')
else:
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.02)

fig.add_trace(go.Candlestick(
    x=df['date'], open=df['open'], high=df['high'], low=df['low'], close=df['close'],
    name='price'
), row=1, col=1)

fig.add_trace(go.Bar(
    x=df['date'], y=df['volume'], name='volume', marker_color='rgba(100,100,200,0.5)'
), row=2, col=1)

fig.update_layout(title=fname, xaxis_rangeslider_visible=False, template='plotly_dark')

out = fname.replace('.csv', '.html')
fig.write_html(out)
print(f"saved {out}")