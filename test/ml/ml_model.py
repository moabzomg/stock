"""
ml_model.py
RandomForest + XGBoost soft-voting ensemble trained on Minervini-filtered data.
Uses walk-forward (time-series) cross-validation to avoid look-ahead bias.
"""

import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier, VotingClassifier
from sklearn.preprocessing import RobustScaler
from sklearn.metrics import (
    classification_report, roc_auc_score, precision_score,
    recall_score, f1_score
)
from sklearn.model_selection import TimeSeriesSplit

try:
    from xgboost import XGBClassifier
    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    print("  [warn] xgboost not installed — using RandomForest only.")


class MLTradingModel:

    MODEL_PATH = "minervini_ml_model.joblib"

    def __init__(self):
        self.scaler   = RobustScaler()
        self.model    = None
        self.feature_cols: list[str] = []
        self.cv_scores: dict = {}

    # ── Public ────────────────────────────────────────────────────────────────
    def train(self, feature_data: pd.DataFrame) -> None:
        self.feature_cols = [
            c for c in feature_data.columns
            if c not in {"target", "symbol", "fwd_return"}
        ]
        X = feature_data[self.feature_cols].values
        y = feature_data["target"].values

        # Walk-forward CV to evaluate without look-ahead bias
        print("      Running walk-forward cross-validation (5 splits) …")
        self._walk_forward_cv(X, y)

        # Final model on ALL data
        print("      Training final ensemble on full dataset …")
        X_scaled = self.scaler.fit_transform(X)
        self.model = self._build_ensemble()
        self.model.fit(X_scaled, y)

        joblib.dump({"model": self.model, "scaler": self.scaler,
                     "feature_cols": self.feature_cols}, self.MODEL_PATH)
        print(f"      Model saved → {self.MODEL_PATH}")

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        X_scaled = self.scaler.transform(X)
        return self.model.predict_proba(X_scaled)[:, 1]

    def predict(self, X: np.ndarray, threshold: float = 0.55) -> np.ndarray:
        return (self.predict_proba(X) >= threshold).astype(int)

    def feature_importance(self) -> pd.Series:
        """Return averaged feature importances across ensemble members."""
        importances = []
        for name, est in self.model.named_estimators_.items():
            if hasattr(est, "feature_importances_"):
                importances.append(est.feature_importances_)
        if not importances:
            return pd.Series()
        avg = np.mean(importances, axis=0)
        return pd.Series(avg, index=self.feature_cols).sort_values(ascending=False)

    @classmethod
    def load(cls, path: str = None):
        obj = cls()
        path = path or cls.MODEL_PATH
        saved = joblib.load(path)
        obj.model        = saved["model"]
        obj.scaler       = saved["scaler"]
        obj.feature_cols = saved["feature_cols"]
        return obj

    # ── Private ───────────────────────────────────────────────────────────────
    def _build_ensemble(self) -> VotingClassifier:
        rf = RandomForestClassifier(
            n_estimators=300,
            max_depth=8,
            min_samples_leaf=20,
            max_features="sqrt",
            class_weight="balanced",
            n_jobs=-1,
            random_state=42,
        )
        estimators = [("rf", rf)]

        if XGB_AVAILABLE:
            xgb = XGBClassifier(
                n_estimators=300,
                max_depth=5,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                scale_pos_weight=1,
                use_label_encoder=False,
                eval_metric="logloss",
                random_state=42,
                n_jobs=-1,
            )
            estimators.append(("xgb", xgb))

        return VotingClassifier(estimators=estimators, voting="soft", n_jobs=-1)

    def _walk_forward_cv(self, X: np.ndarray, y: np.ndarray) -> None:
        tscv   = TimeSeriesSplit(n_splits=5)
        aucs, precs, recs, f1s = [], [], [], []

        for fold, (train_idx, test_idx) in enumerate(tscv.split(X), 1):
            X_tr, X_te = X[train_idx], X[test_idx]
            y_tr, y_te = y[train_idx], y[test_idx]

            scaler = RobustScaler()
            X_tr_s = scaler.fit_transform(X_tr)
            X_te_s = scaler.transform(X_te)

            m = self._build_ensemble()
            m.fit(X_tr_s, y_tr)

            proba = m.predict_proba(X_te_s)[:, 1]
            pred  = (proba >= 0.55).astype(int)

            auc   = roc_auc_score(y_te, proba) if len(np.unique(y_te)) > 1 else 0.5
            prec  = precision_score(y_te, pred, zero_division=0)
            rec   = recall_score(y_te, pred, zero_division=0)
            f1    = f1_score(y_te, pred, zero_division=0)

            aucs.append(auc); precs.append(prec); recs.append(rec); f1s.append(f1)
            print(f"        Fold {fold}: AUC={auc:.3f}  Prec={prec:.3f}  "
                  f"Rec={rec:.3f}  F1={f1:.3f}")

        self.cv_scores = {
            "auc_mean":  float(np.mean(aucs)),
            "auc_std":   float(np.std(aucs)),
            "prec_mean": float(np.mean(precs)),
            "rec_mean":  float(np.mean(recs)),
            "f1_mean":   float(np.mean(f1s)),
        }
        print(f"\n      ── CV Summary ──────────────────────────────────────")
        print(f"        AUC  : {self.cv_scores['auc_mean']:.3f} ± {self.cv_scores['auc_std']:.3f}")
        print(f"        Prec : {self.cv_scores['prec_mean']:.3f}")
        print(f"        Rec  : {self.cv_scores['rec_mean']:.3f}")
        print(f"        F1   : {self.cv_scores['f1_mean']:.3f}")
