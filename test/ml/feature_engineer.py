"""
feature_engineer.py
Builds a rich feature matrix from screened OHLCV data.
Target: did the stock outperform SPY by ≥ 3 % over the next 20 trading days?
"""

import pandas as pd
import numpy as np
from typing import Dict


class FeatureEngineer:

    FORWARD_DAYS   = 20      # prediction horizon
    WIN_THRESHOLD  = 0.03    # outperform SPY by this much to be labelled 1

    # ── Public ────────────────────────────────────────────────────────────────
    def build_features(
        self,
        screened: Dict[str, pd.DataFrame],
        spy_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """
        Iterates over screened universe, computes features on every
        minervini_pass row, and stacks into a single DataFrame.
        """
        frames = []
        for sym, df in screened.items():
            try:
                feat = self._features_for_symbol(df, sym)
                frames.append(feat)
            except Exception as e:
                print(f"        [features] {sym} skipped: {e}")

        if not frames:
            raise RuntimeError("No feature rows generated — check screened data.")

        combined = pd.concat(frames).dropna()
        combined = combined[np.isfinite(combined.select_dtypes("number")).all(axis=1)]
        print(f"      Total rows (pass+features): {len(combined):,}  "
              f"| Positive labels: {combined['target'].mean():.1%}")
        return combined

    # Canonical ordered list — must match what _features_for_symbol produces
    FEATURE_COLS = [
        "mom_5d","mom_10d","mom_20d","mom_60d","mom_120d",
        "rs_50","rs_150","rs_200",
        "dist_52w_high","dist_52w_low",
        "vol_20d","vol_60d","atr_pct",
        "vol_ratio_20d","vol_ratio_50d","vol_trend",
        "minervini_score_norm",
        "bb_pct","rsi_14",
        "macd_hist","macd_cross",
        "sma200_slope","sma50_slope",
    ]

    def get_feature_cols(self, df: pd.DataFrame = None) -> list[str]:
        """Return the fixed ordered list of feature column names."""
        return self.FEATURE_COLS

    # ── Private ───────────────────────────────────────────────────────────────
    def _features_for_symbol(self, df: pd.DataFrame, sym: str) -> pd.DataFrame:
        df = df.copy()
        close  = df["Close"]
        volume = df["Volume"]
        high   = df["High"]
        low    = df["Low"]

        # ── Price momentum ────────────────────────────────────────────────────
        for d in [5, 10, 20, 60, 120]:
            df[f"mom_{d}d"] = close.pct_change(d)

        # ── Relative strength vs own moving averages ──────────────────────────
        df["rs_50"]  = (close / df["sma_50"]  - 1)
        df["rs_150"] = (close / df["sma_150"] - 1)
        df["rs_200"] = (close / df["sma_200"] - 1)

        # ── Distance from 52-week extremes ────────────────────────────────────
        df["dist_52w_high"] = (close / df["high_52w"] - 1)   # negative = below high
        df["dist_52w_low"]  = (close / df["low_52w"]  - 1)   # positive = above low

        # ── Volatility ────────────────────────────────────────────────────────
        df["vol_20d"]  = close.pct_change().rolling(20).std()
        df["vol_60d"]  = close.pct_change().rolling(60).std()
        df["atr_14"]   = self._atr(high, low, close, 14)
        df["atr_pct"]  = df["atr_14"] / close

        # ── Volume analysis ───────────────────────────────────────────────────
        df["vol_ratio_20d"]  = volume / volume.rolling(20).mean()
        df["vol_ratio_50d"]  = volume / volume.rolling(50).mean()
        df["vol_trend"]      = volume.rolling(10).mean() / volume.rolling(30).mean()

        # ── Trend alignment (Minervini score = partial quality signal) ────────
        df["minervini_score_norm"] = df["minervini_score"] / 7.0

        # ── Bollinger band position ───────────────────────────────────────────
        bb_mid   = close.rolling(20).mean()
        bb_std   = close.rolling(20).std()
        df["bb_pct"] = (close - (bb_mid - 2 * bb_std)) / (4 * bb_std)

        # ── RSI ───────────────────────────────────────────────────────────────
        df["rsi_14"] = self._rsi(close, 14)

        # ── MACD ──────────────────────────────────────────────────────────────
        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd  = ema12 - ema26
        signal= macd.ewm(span=9, adjust=False).mean()
        df["macd_hist"]  = (macd - signal) / close
        df["macd_cross"] = (macd > signal).astype(int)

        # ── SMA slope (rate of change of 200-SMA) ────────────────────────────
        df["sma200_slope"] = df["sma_200"].pct_change(5)
        df["sma50_slope"]  = df["sma_50"].pct_change(5)

        # ── Forward return (target) ───────────────────────────────────────────
        df["fwd_return"] = close.shift(-self.FORWARD_DAYS) / close - 1
        df["target"]     = (df["fwd_return"] > self.WIN_THRESHOLD).astype(int)

        # ── Keep only rows where all 7 Minervini rules pass ──────────────────
        df = df[df["minervini_pass"]].copy()
        df["symbol"] = sym

        all_cols = self.FEATURE_COLS + ["target", "fwd_return", "symbol"]
        # Drop rows where any feature is NaN/inf
        out = df[all_cols].replace([np.inf, -np.inf], np.nan).dropna(subset=self.FEATURE_COLS)
        return out

    # ── Helpers ───────────────────────────────────────────────────────────────
    @staticmethod
    def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
        delta = close.diff()
        gain  = delta.clip(lower=0).rolling(period).mean()
        loss  = (-delta.clip(upper=0)).rolling(period).mean()
        rs    = gain / loss.replace(0, np.nan)
        return 100 - 100 / (1 + rs)

    @staticmethod
    def _atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs(),
        ], axis=1).max(axis=1)
        return tr.rolling(period).mean()