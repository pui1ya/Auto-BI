"""
Feature engineering: derive time features, customer metrics, etc.
"""
import pandas as pd


def transform_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Time features
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["week"] = df["date"].dt.isocalendar().week.astype(int)
    df["day_of_week"] = df["date"].dt.dayofweek
    df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)
    df["quarter"] = df["date"].dt.quarter

    # Revenue per unit
    df["revenue_per_unit"] = (df["revenue"] / df["quantity"]).round(2)

    # Ensure string types
    df["transaction_id"] = df["transaction_id"].astype(str)
    df["customer_id"] = df["customer_id"].astype(str)

    return df