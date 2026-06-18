"""
screener.py
Implements all 7 Minervini Trend Template rules as boolean columns.
Each rule can be inspected individually for debugging / research.
"""

import pandas as pd
import numpy as np
from typing import Dict


class MinerviniScreener:
    """
    Rule  Description
    ────  ───────────────────────────────────────────────────────────
    1     Price > 150-SMA AND Price > 200-SMA
    2     150-SMA > 200-SMA
    3     200-SMA trending up for at least 1 month (21 trading days)
    4     50-SMA > 150-SMA AND 50-SMA > 200-SMA
    5     Price > 50-SMA
    6     Price is within 25% of 52-week high
    7     Price is at least 30% above 52-week low
    """

    # ── Public ────────────────────────────────────────────────────────────────
    def screen_universe(
        self, data: Dict[str, pd.DataFrame]
    ) -> Dict[str, pd.DataFrame]:
        """
        Returns the same dict with Minervini indicator columns added.
        Rows where ALL 7 rules pass are flagged as minervini_pass=True.
        """
        result = {}
        for sym, df in data.items():
            try:
                result[sym] = self._add_rules(df.copy())
            except Exception as e:
                print(f"        [screener] {sym} skipped: {e}")
        return result

    def passing_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        """Return only rows where all 7 rules are satisfied."""
        if "minervini_pass" not in df.columns:
            df = self._add_rules(df)
        return df[df["minervini_pass"]]

    # ── Private ───────────────────────────────────────────────────────────────
    def _add_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        close = df["Close"]

        # Moving averages
        df["sma_50"]  = close.rolling(50).mean()
        df["sma_150"] = close.rolling(150).mean()
        df["sma_200"] = close.rolling(200).mean()

        # 52-week window = 252 trading days
        df["high_52w"] = close.rolling(252).max()
        df["low_52w"]  = close.rolling(252).min()

        # 200-SMA slope: compare today vs 21 trading days ago
        df["sma_200_21d_ago"] = df["sma_200"].shift(21)

        # ── 7 rules ──────────────────────────────────────────────────────────
        df["rule1"] = (close > df["sma_150"]) & (close > df["sma_200"])
        df["rule2"] = df["sma_150"] > df["sma_200"]
        df["rule3"] = df["sma_200"] > df["sma_200_21d_ago"]          # trending up
        df["rule4"] = (df["sma_50"] > df["sma_150"]) & (df["sma_50"] > df["sma_200"])
        df["rule5"] = close > df["sma_50"]
        df["rule6"] = close >= (df["high_52w"] * 0.75)               # within 25% of 52w high
        df["rule7"] = close >= (df["low_52w"]  * 1.30)               # 30%+ above 52w low

        df["minervini_score"] = (
            df[["rule1","rule2","rule3","rule4","rule5","rule6","rule7"]]
            .sum(axis=1)
        )
        df["minervini_pass"] = df["minervini_score"] == 7

        return df.dropna(subset=["sma_200", "high_52w", "low_52w"])
