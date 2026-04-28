"""
Validate cleaned dataframe before writing to DB.
Returns (valid_df, list_of_error_strings).
"""
import pandas as pd
from typing import Tuple, List

REQUIRED_COLUMNS = ["transaction_id", "customer_id", "revenue", "date", "quantity"]
MIN_REVENUE = 0.01
MAX_REVENUE = 1_000_000


def validate_transactions(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    errors: List[str] = []
    df = df.copy()

    # Required columns present?
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return df, errors

    # Revenue range
    bad_rev = df[(df["revenue"] < MIN_REVENUE) | (df["revenue"] > MAX_REVENUE)]
    if len(bad_rev):
        errors.append(f"{len(bad_rev)} rows with revenue out of range [{MIN_REVENUE}, {MAX_REVENUE}]")
        df = df[(df["revenue"] >= MIN_REVENUE) & (df["revenue"] <= MAX_REVENUE)]

    # Duplicate transaction IDs
    dupes = df[df.duplicated("transaction_id", keep="first")]
    if len(dupes):
        errors.append(f"{len(dupes)} duplicate transaction_ids dropped")
        df.drop_duplicates("transaction_id", keep="first", inplace=True)

    # Null dates after parse
    null_dates = df["date"].isna().sum()
    if null_dates:
        errors.append(f"{null_dates} rows with unparseable dates dropped")
        df.dropna(subset=["date"], inplace=True)

    return df.reset_index(drop=True), errors

    

        
        


        