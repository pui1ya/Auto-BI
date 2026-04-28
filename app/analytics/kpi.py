"""
KPI calculations from the transactions table.
"""
import pandas as pd
from sqlalchemy import text
from app.core.database import sync_engine
from app.models.schemas import KPISummary


def load_transactions() -> pd.DataFrame:
    with sync_engine.connect() as conn:
        return pd.read_sql("SELECT * FROM transactions", conn, parse_dates=["date"])


def compute_kpis(df: pd.DataFrame = None, period_days: int = 30) -> KPISummary:
    if df is None:
        df = load_transactions()

    if df.empty:
        return KPISummary(
            total_revenue=0, total_orders=0, unique_customers=0,
            avg_order_value=0, revenue_growth=0, top_category="N/A", top_region="N/A"
        )

    cutoff = df["date"].max() - pd.Timedelta(days=period_days)
    prev_cutoff = cutoff - pd.Timedelta(days=period_days)

    current = df[df["date"] >= cutoff]
    previous = df[(df["date"] >= prev_cutoff) & (df["date"] < cutoff)]

    total_revenue = current["revenue"].sum()
    prev_revenue = previous["revenue"].sum() if not previous.empty else total_revenue
    growth = ((total_revenue - prev_revenue) / prev_revenue * 100) if prev_revenue else 0.0

    top_category = (
        current.groupby("category")["revenue"].sum().idxmax()
        if not current.empty else "N/A"
    )
    top_region = (
        current.groupby("region")["revenue"].sum().idxmax()
        if not current.empty else "N/A"
    )

    return KPISummary(
        total_revenue=round(total_revenue, 2),
        total_orders=len(current),
        unique_customers=current["customer_id"].nunique(),
        avg_order_value=round(current["revenue"].mean(), 2) if not current.empty else 0,
        revenue_growth=round(growth, 2),
        top_category=top_category,
        top_region=top_region,
    )


def revenue_by_day(df: pd.DataFrame = None) -> pd.DataFrame:
    if df is None:
        df = load_transactions()
    return df.groupby(df["date"].dt.date)["revenue"].sum().reset_index().rename(
        columns={"date": "ds", "revenue": "y"}
    )


def revenue_by_category(df: pd.DataFrame = None) -> pd.DataFrame:
    if df is None:
        df = load_transactions()
    return df.groupby("category")["revenue"].sum().reset_index().sort_values("revenue", ascending=False)


def revenue_by_region(df: pd.DataFrame = None) -> pd.DataFrame:
    if df is None:
        df = load_transactions()
    return df.groupby("region")["revenue"].sum().reset_index().sort_values("revenue", ascending=False)


def revenue_by_channel(df: pd.DataFrame = None) -> pd.DataFrame:
    if df is None:
        df = load_transactions()
    return df.groupby("channel")["revenue"].sum().reset_index().sort_values("revenue", ascending=False)