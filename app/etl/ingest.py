"""
Ingest raw CSV / JSON / Excel files from data/raw/ into the database.
"""
import os
import glob
import traceback
import pandas as pd
from datetime import datetime
from app.core.config import settings
from app.core.database import sync_engine
from app.models.schemas import Transaction
from sqlalchemy.orm import Session
from sqlalchemy import text


SUPPORTED_EXTENSIONS = [".csv", ".json", ".xlsx", ".xls"]


def load_file(filepath: str) -> pd.DataFrame:
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        for encoding in ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]:
            try:
                df = pd.read_csv(filepath, encoding=encoding, on_bad_lines="skip")
                print(f"[ETL] Loaded '{os.path.basename(filepath)}' encoding={encoding} shape={df.shape}")
                return df
            except UnicodeDecodeError:
                continue
            except Exception as e:
                raise ValueError(f"CSV parse error: {e}")
        raise ValueError("Could not decode CSV with any known encoding. Re-save as UTF-8.")
    elif ext == ".json":
        try:
            return pd.read_json(filepath)
        except Exception:
            return pd.read_json(filepath, lines=True)
    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file type: {ext}")


def _error_hint(error_msg: str) -> str:
    msg = error_msg.lower()
    if "revenue" in msg:
        return "No revenue/price column found. Rename your price column to 'revenue', 'amount', 'price', 'total', 'gmv', 'earnings', or 'sales'."
    if "codec" in msg or "decode" in msg or "utf" in msg:
        return "Encoding issue. Open in Excel and re-save as CSV UTF-8."
    if "date" in msg:
        return "Date column couldn't be parsed. Use YYYY-MM-DD or DD/MM/YYYY format."
    if "empty" in msg:
        return "File appears empty or has no usable rows after cleaning."
    return "Check terminal logs — columns are printed during ingestion to help debug."


def ingest_all(raw_dir: str = settings.RAW_DATA_PATH) -> dict:
    files = []
    for ext in SUPPORTED_EXTENSIONS:
        files.extend(glob.glob(os.path.join(raw_dir, f"*{ext}")))

    results = {"ingested": [], "errors": []}

    for fp in files:
        try:
            df = load_file(fp)
            print(f"[ETL] Columns in '{os.path.basename(fp)}': {df.columns.tolist()}")

            from app.etl.clean import clean_transactions
            from app.etl.transform import transform_transactions
            from app.etl.validate import validate_transactions

            df = clean_transactions(df)

            if df.empty:
                results["errors"].append({
                    "file": fp,
                    "error": "No valid rows after cleaning.",
                    "hint": "Check that the file has a numeric price/revenue column.",
                })
                continue

            df = transform_transactions(df)
            valid, issues = validate_transactions(df)

            if issues:
                results["errors"].append({"file": fp, "issues": issues})

            _save_transactions(valid)
            results["ingested"].append(fp)
            print(f"[ETL] Saved {len(valid)} rows from '{os.path.basename(fp)}'")

        except Exception as e:
            print(f"[ETL] Error on '{os.path.basename(fp)}':\n{traceback.format_exc()}")
            results["errors"].append({
                "file": fp,
                "error": str(e),
                "hint": _error_hint(str(e)),
            })

    return results


def _scalar(val):
    """Safely extract a plain Python scalar from a value that might be a pandas Series."""
    if hasattr(val, 'iloc'):
        val = val.iloc[0]
    if hasattr(val, 'item'):
        return val.item()
    return val


def _save_transactions(df: pd.DataFrame):
    # Drop duplicate columns — pandas returns a Series instead of scalar for dupes
    df = df.loc[:, ~df.columns.duplicated()].copy().reset_index(drop=True)

    with Session(sync_engine) as session:
        saved = 0
        for _, row in df.iterrows():
            tid = str(_scalar(row["transaction_id"]))

            exists = session.execute(
                text("SELECT 1 FROM transactions WHERE transaction_id=:tid"),
                {"tid": tid}
            ).fetchone()
            if exists:
                continue

            t = Transaction(
                transaction_id=tid,
                customer_id=str(_scalar(row["customer_id"])),
                product_id=str(_scalar(row.get("product_id", "UNKNOWN"))),
                category=str(_scalar(row.get("category", "Unknown"))),
                revenue=float(_scalar(row["revenue"])),
                quantity=int(_scalar(row.get("quantity", 1))),
                date=pd.to_datetime(_scalar(row["date"])),
                region=str(_scalar(row.get("region", "Unknown"))),
                channel=str(_scalar(row.get("channel", "Unknown"))),
            )
            session.add(t)
            saved += 1
        session.commit()
        print(f"[ETL] Committed {saved} new rows to DB")


def generate_sample_data(n: int = 500):
    """Generate synthetic data so the app works out-of-the-box."""
    import numpy as np
    np.random.seed(42)
    os.makedirs(settings.RAW_DATA_PATH, exist_ok=True)

    categories = ["Electronics", "Clothing", "Food", "Books", "Sports"]
    regions = ["North", "South", "East", "West"]
    channels = ["Online", "Retail", "Mobile"]

    dates = pd.date_range(end=datetime.today(), periods=n, freq="6h")
    df = pd.DataFrame({
        "transaction_id": [f"T{i:05d}" for i in range(n)],
        "customer_id":    [f"C{np.random.randint(1, 101):04d}" for _ in range(n)],
        "product_id":     [f"P{np.random.randint(1, 51):03d}" for _ in range(n)],
        "category":       np.random.choice(categories, n),
        "revenue":        np.round(np.random.exponential(scale=80, size=n) + 10, 2),
        "quantity":       np.random.randint(1, 6, n),
        "date":           dates,
        "region":         np.random.choice(regions, n),
        "channel":        np.random.choice(channels, n),
    })
    path = os.path.join(settings.RAW_DATA_PATH, "sample_transactions.csv")
    df.to_csv(path, index=False)
    return path