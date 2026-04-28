import pytest
import pandas as pd
from app.etl.clean import clean_transactions
from app.etl.transform import transform_transactions
from app.etl.validate import validate_transactions


def sample_df():
    return pd.DataFrame({
        "transaction_id": ["T001", "T002", "T003", "T001"],  # T001 duplicate
        "customer_id": ["C01", "C02", "C03", "C01"],
        "product_id": ["P1", "P2", "P3", "P1"],
        "category": ["Electronics", "Books", None, "Electronics"],
        "revenue": [120.0, 45.0, -5.0, 120.0],  # negative + duplicate
        "quantity": [2, 1, 1, 2],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-01"],
        "region": ["North", "South", "East", "North"],
        "channel": ["Online", "Retail", "Mobile", "Online"],
    })


def test_clean_removes_duplicates():
    df = clean_transactions(sample_df())
    assert df["transaction_id"].nunique() == len(df)


def test_clean_removes_negative_revenue():
    df = clean_transactions(sample_df())
    assert (df["revenue"] > 0).all()


def test_clean_fills_null_category():
    df = clean_transactions(sample_df())
    assert df["category"].isna().sum() == 0


def test_transform_adds_time_features():
    df = clean_transactions(sample_df())
    df = transform_transactions(df)
    for col in ["year", "month", "week", "day_of_week", "is_weekend", "quarter"]:
        assert col in df.columns


def test_validate_returns_errors_for_duplicates():
    df = clean_transactions(sample_df())
    # Manually re-inject duplicate
    dup = df.iloc[[0]].copy()
    df2 = pd.concat([df, dup], ignore_index=True)
    _, errors = validate_transactions(df2)
    assert any("duplicate" in e.lower() for e in errors)


def test_validate_passes_clean_data():
    df = clean_transactions(sample_df())
    df = transform_transactions(df)
    valid, errors = validate_transactions(df)
    assert len(valid) > 0