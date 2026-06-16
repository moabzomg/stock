"""
data_fetcher.py
Pulls daily OHLCV bars from Interactive Brokers via ib_insync.
Falls back to yfinance if IB is not reachable (useful for development).
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict

try:
    from ib_insync import IB, Stock, util
    IB_AVAILABLE = True
except ImportError:
    IB_AVAILABLE = False
    print("  [warn] ib_insync not installed — will use yfinance fallback.")


class IBDataFetcher:
    def __init__(self, host: str, port: int, client_id: int):
        self.host = host
        self.port = port
        self.client_id = client_id

    # ── Public ────────────────────────────────────────────────────────────────
    async def fetch_universe(
        self, symbols: list[str], years: int = 5
    ) -> Dict[str, pd.DataFrame]:
        if IB_AVAILABLE:
            return await self._fetch_from_ib(symbols, years)
        else:
            return self._fetch_from_yfinance(symbols, years)

    # ── IB path ───────────────────────────────────────────────────────────────
    async def _fetch_from_ib(
        self, symbols: list[str], years: int
    ) -> Dict[str, pd.DataFrame]:
        ib = IB()
        result: Dict[str, pd.DataFrame] = {}

        try:
            await ib.connectAsync(self.host, self.port, clientId=self.client_id)
            print(f"      Connected to IB TWS/Gateway at {self.host}:{self.port}")

            duration = f"{years} Y"

            for sym in symbols:
                try:
                    contract = Stock(sym, "SMART", "USD")
                    await ib.qualifyContractsAsync(contract)
                    bars = await ib.reqHistoricalDataAsync(
                        contract,
                        endDateTime="",
                        durationStr=duration,
                        barSizeSetting="1 day",
                        whatToShow="ADJUSTED_LAST",
                        useRTH=True,
                        formatDate=1,
                    )
                    if bars:
                        df = util.df(bars)[["date", "open", "high", "low", "close", "volume"]]
                        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
                        df["Date"] = pd.to_datetime(df["Date"])
                        df.set_index("Date", inplace=True)
                        df = df[df["Close"] > 0].dropna()
                        result[sym] = df
                        print(f"        ✓ {sym}: {len(df)} bars")
                    else:
                        print(f"        ✗ {sym}: no data")
                except Exception as e:
                    print(f"        ✗ {sym}: {e}")
                await asyncio.sleep(0.1)   # stay within IB pacing limits

        finally:
            ib.disconnect()

        return result

    # ── yfinance fallback ─────────────────────────────────────────────────────
    def _fetch_from_yfinance(
        self, symbols: list[str], years: int
    ) -> Dict[str, pd.DataFrame]:
        try:
            import yfinance as yf
        except ImportError:
            raise RuntimeError(
                "Neither ib_insync nor yfinance is available. "
                "Install one: pip install ib_insync  OR  pip install yfinance"
            )

        result: Dict[str, pd.DataFrame] = {}
        end   = datetime.today()
        start = end - timedelta(days=years * 365)

        print(f"      [yfinance fallback] Downloading {len(symbols)} symbols …")
        raw = yf.download(
            symbols,
            start=start.strftime("%Y-%m-%d"),
            end=end.strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False,
            threads=True,
        )

        if isinstance(raw.columns, pd.MultiIndex):
            for sym in symbols:
                try:
                    df = raw.xs(sym, axis=1, level=1)[
                        ["Open", "High", "Low", "Close", "Volume"]
                    ].dropna()
                    if len(df) > 50:
                        result[sym] = df
                except Exception:
                    pass
        else:
            result[symbols[0]] = raw[["Open", "High", "Low", "Close", "Volume"]].dropna()

        print(f"      Downloaded {len(result)} symbols via yfinance.")
        return result
