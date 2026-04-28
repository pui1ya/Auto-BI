"""
RFM-based customer segmentation — robust to low-cardinality data.
"""
import pandas as pd
import numpy as np
from app.analytics.kpi import load_transactions


def _safe_quartile_score(series: pd.Series, ascending: bool = True) -> pd.Series:
    """Score a series 1-4. Falls back to rank percentile when qcut fails."""
    n = series.nunique()
    q = min(4, n)

    if q >= 2:
        try:
            labels = list(range(1, q + 1))
            scored = pd.qcut(series, q=q, labels=labels, duplicates="drop").astype(float)
            if q < 4:
                scored = ((scored - 1) / (q - 1) * 3 + 1).round().clip(1, 4)
            if not ascending:
                scored = 5 - scored
            return scored
        except Exception:
            pass

    # Fallback: percentile rank → 1-4
    pct = series.rank(pct=True)
    scored = (pct * 3.99).clip(1, 4).round().astype(float)
    if not ascending:
        scored = 5 - scored
    return scored


def compute_rfm(df: pd.DataFrame = None) -> pd.DataFrame:
    if df is None:
        df = load_transactions()
    if df.empty:
        return pd.DataFrame()

    reference_date = df["date"].max() + pd.Timedelta(days=1)

    rfm = df.groupby("customer_id").agg(
        recency=("date", lambda x: (reference_date - x.max()).days),
        frequency=("transaction_id", "count"),
        monetary=("revenue", "sum"),
    ).reset_index()

    for col, ascending in [("recency", False), ("frequency", True), ("monetary", True)]:
        rfm[f"{col[0]}_score"] = _safe_quartile_score(rfm[col], ascending=ascending)

    rfm["rfm_score"] = rfm["r_score"] + rfm["f_score"] + rfm["m_score"]

    score_min = rfm["rfm_score"].min()
    score_max = rfm["rfm_score"].max()

    if score_min == score_max:
        rfm["segment"] = "Loyal Customers"
    else:
        span = score_max - score_min
        bins = sorted(set([
            score_min - 0.01,
            score_min + span * 0.25,
            score_min + span * 0.55,
            score_min + span * 0.80,
            score_max + 0.01,
        ]))
        if len(bins) < 5:
            rfm["segment"] = "Loyal Customers"
        else:
            rfm["segment"] = pd.cut(
                rfm["rfm_score"],
                bins=bins,
                labels=["Lost", "At Risk", "Loyal Customers", "Champions"],
                right=True,
            ).astype(str)

    rfm["segment"] = rfm["segment"].replace({"nan": "Loyal Customers", "": "Loyal Customers"})
    rfm["monetary"] = rfm["monetary"].round(2)
    return rfm


def segment_summary(df: pd.DataFrame = None) -> pd.DataFrame:
    rfm = compute_rfm(df)
    if rfm.empty:
        return pd.DataFrame()

    summary = rfm.groupby("segment").agg(
        customer_count=("customer_id", "count"),
        avg_recency=("recency", "mean"),
        avg_frequency=("frequency", "mean"),
        avg_monetary=("monetary", "mean"),
        total_revenue=("monetary", "sum"),
    ).reset_index()
    return summary.round(2)
