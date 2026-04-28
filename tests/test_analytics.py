import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def make_df(n=200):
    np.random.seed(0)
    base = datetime(2024, 1, 1)
    return pd.DataFrame({
        "transaction_id": [f"T{i}" for i in range(n)],
        "customer_id": [f"C{np.random.randint(1, 21)}" for _ in range(n)],
        "product_id": [f"P{np.random.randint(1, 11)}" for _ in range(n)],
        "category": np.random.choice(["Electronics", "Books", "Food"], n),
        "revenue": np.random.uniform(10, 200, n).round(2),
        "quantity": np.random.randint(1, 5, n),
        "date": [base + timedelta(days=i % 90) for i in range(n)],
        "region": np.random.choice(["North", "South"], n),
        "channel": np.random.choice(["Online", "Retail"], n),
    })


def test_kpi_summary():
    from app.analytics.kpi import compute_kpis
    df = make_df()
    kpis = compute_kpis(df)
    assert kpis.total_revenue > 0
    assert kpis.total_orders > 0
    assert kpis.unique_customers > 0
    assert kpis.avg_order_value > 0


def test_rfm_segments():
    from app.analytics.segmentation import compute_rfm
    df = make_df()
    rfm = compute_rfm(df)
    assert not rfm.empty
    assert "segment" in rfm.columns
    assert set(rfm["segment"].unique()).issubset(
        {"Champions", "Loyal Customers", "At Risk", "Lost", "nan"}
    )


def test_segment_summary_shape():
    from app.analytics.segmentation import segment_summary
    df = make_df()
    summ = segment_summary(df)
    assert not summ.empty
    assert "customer_count" in summ.columns


def test_forecast_returns_dataframe():
    # Patch load to use in-memory data
    import app.analytics.kpi as kpi_mod
    df = make_df()
    orig = kpi_mod.revenue_by_day

    def patched():
        return df.groupby(df["date"].dt.date)["revenue"].sum().reset_index().rename(
            columns={"date": "ds", "revenue": "y"}
        )

    kpi_mod.revenue_by_day = patched
    from app.analytics.forecasting import forecast_revenue
    result = forecast_revenue(horizon=14)
    kpi_mod.revenue_by_day = orig

    assert len(result) == 14
    assert "predicted" in result.columns


def test_churn_summary_keys():
    from app.analytics.churn import churn_summary, build_churn_model
    import app.analytics.churn as churn_mod
    import app.analytics.segmentation as seg_mod

    df = make_df()
    orig_rfm = seg_mod.compute_rfm

    def patched_rfm(*a, **kw):
        return orig_rfm(df)

    seg_mod.compute_rfm = patched_rfm
    stats = churn_summary()
    seg_mod.compute_rfm = orig_rfm

    assert "churn_rate" in stats
    assert "churned_count" in stats
    assert "at_risk_count" in stats