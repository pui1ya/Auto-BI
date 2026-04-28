"""
Data cleaning: handle nulls, duplicates, outliers, column naming.
Supports a wide variety of real-world CSV schemas via alias mapping
and revenue synthesis when no direct revenue column is present.
"""
import pandas as pd
import numpy as np
import hashlib


# ── Column alias map ──────────────────────────────────────────────────────────
# All keys are lowercased & space-normalized before matching.
COLUMN_ALIASES = {
    # transaction_id
    "order_id":           "transaction_id",
    "order_number":       "transaction_id",
    "orderid":            "transaction_id",
    "sale_id":            "transaction_id",
    "saleid":             "transaction_id",
    "invoice_id":         "transaction_id",
    "invoice_number":     "transaction_id",
    "invoiceid":          "transaction_id",
    "txn_id":             "transaction_id",
    "txnid":              "transaction_id",
    "transaction_number": "transaction_id",
    "id":                 "transaction_id",
    "record_id":          "transaction_id",
    "row_id":             "transaction_id",
    "listing_id":         "transaction_id",

    # customer_id
    "cust_id":            "customer_id",
    "custid":             "customer_id",
    "client_id":          "customer_id",
    "clientid":           "customer_id",
    "buyer_id":           "customer_id",
    "buyerid":            "customer_id",
    "user_id":            "customer_id",
    "userid":             "customer_id",
    "account_id":         "customer_id",
    "accountid":          "customer_id",
    "member_id":          "customer_id",
    "memberid":           "customer_id",
    "seller_id":          "customer_id",   # seller-focused datasets
    "sellerid":           "customer_id",
    "seller_name":        "customer_id",
    "shop_name":          "customer_id",
    "store_id":           "customer_id",
    "storeid":            "customer_id",
    "partner_id":         "customer_id",

    # product_id
    "prod_id":            "product_id",
    "prodid":             "product_id",
    "sku":                "product_id",
    "item_id":            "product_id",
    "itemid":             "product_id",
    "product_code":       "product_id",
    "product_name":       "product_id",
    "item_name":          "product_id",
    "listing_title":      "product_id",
    "asin":               "product_id",
    "upc":                "product_id",

    # revenue
    "amount":             "revenue",
    "amount_paid":        "revenue",
    "total_amount":       "revenue",
    "total_price":        "revenue",
    "total_revenue":      "revenue",
    "sale_amount":        "revenue",
    "sale_price":         "revenue",
    "selling_price":      "revenue",
    "net_revenue":        "revenue",
    "gross_revenue":      "revenue",
    "gmv":                "revenue",
    "gross_merchandise_value": "revenue",
    "order_value":        "revenue",
    "order_total":        "revenue",
    "payment_amount":     "revenue",
    "price":              "revenue",
    "unit_price":         "revenue",
    "earnings":           "revenue",
    "income":             "revenue",
    "sales":              "revenue",
    "sales_amount":       "revenue",
    "value":              "revenue",
    "subtotal":           "revenue",
    "invoice_amount":     "revenue",
    "transaction_amount": "revenue",
    "total":              "revenue",
    "net_sales":          "revenue",
    "revenue_usd":        "revenue",
    "revenue_inr":        "revenue",

    # quantity
    "qty":                "quantity",
    "qty_sold":           "quantity",
    "units":              "quantity",
    "units_sold":         "quantity",
    "num_items":          "quantity",
    "item_count":         "quantity",
    "order_quantity":     "quantity",
    "count":              "quantity",
    "volume":             "quantity",

    # date
    "order_date":         "date",
    "order_time":         "date",
    "sale_date":          "date",
    "purchased_at":       "date",
    "purchase_date":      "date",
    "transaction_date":   "date",
    "transaction_time":   "date",
    "created_at":         "date",
    "created_date":       "date",
    "invoice_date":       "date",
    "payment_date":       "date",
    "shipped_date":       "date",
    "event_date":         "date",
    "timestamp":          "date",
    "time":               "date",
    "datetime":           "date",
    "date_of_sale":       "date",
    "month":              "date",
    "week":               "date",
    "year":               "date",

    # category
    "product_category":   "category",
    "item_category":      "category",
    "product_type":       "category",
    "item_type":          "category",
    "department":         "category",
    "genre":              "category",
    "type":               "category",
    "subcategory":        "category",
    "segment":            "category",
    "vertical":           "category",

    # region
    "country":            "region",
    "state":              "region",
    "city":               "region",
    "location":           "region",
    "market":             "region",
    "territory":          "region",
    "zone":               "region",
    "geography":          "region",
    "geo":                "region",
    "store_location":     "region",
    "shipping_country":   "region",

    # channel
    "sales_channel":      "channel",
    "order_channel":      "channel",
    "source":             "channel",
    "platform":           "channel",
    "medium":             "channel",
    "store_type":         "channel",
    "fulfillment_channel":"channel",
    "acquisition_channel":"channel",
}

# Columns that can be multiplied together to synthesise revenue
REVENUE_SYNTHESIS_PAIRS = [
    ("unit_price", "quantity"),
    ("unit_price", "qty"),
    ("price",      "quantity"),
    ("price",      "qty"),
    ("price",      "units_sold"),
    ("cost",       "quantity"),
    ("rate",       "quantity"),
    ("rate",       "units"),
    ("fee",        "quantity"),
]


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase + underscore-normalise all column names."""
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[\s\-/\\]+", "_", regex=True)
        .str.replace(r"[^\w]", "", regex=True)
    )
    return df


def _synthesise_revenue(df: pd.DataFrame) -> pd.DataFrame:
    """
    If no revenue column exists, try to compute it from known column pairs.
    Falls back to the largest numeric column if nothing else works.
    """
    if "revenue" in df.columns:
        return df

    # Try price × quantity pairs (on pre-alias column names)
    for price_col, qty_col in REVENUE_SYNTHESIS_PAIRS:
        if price_col in df.columns and qty_col in df.columns:
            p = pd.to_numeric(df[price_col], errors="coerce")
            q = pd.to_numeric(df[qty_col],   errors="coerce").fillna(1)
            synth = (p * q).round(2)
            if synth.gt(0).any():
                df["revenue"] = synth
                print(f"[ETL] Synthesised revenue from '{price_col}' × '{qty_col}'")
                return df

    # Try any single column that looks like a price / value
    candidates = [c for c in df.columns
                  if any(k in c for k in ("price", "cost", "fee", "rate", "value", "gmv",
                                          "earnings", "income", "sales", "amount", "pay"))]
    for col in candidates:
        numeric = pd.to_numeric(df[col], errors="coerce")
        if numeric.gt(0).sum() > len(df) * 0.5:
            df["revenue"] = numeric.round(2)
            print(f"[ETL] Used '{col}' as revenue (best numeric candidate)")
            return df

    # Last resort: largest numeric column
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        best = max(numeric_cols, key=lambda c: df[c].median() if pd.to_numeric(df[c], errors="coerce").gt(0).any() else -1)
        df["revenue"] = pd.to_numeric(df[best], errors="coerce").round(2)
        print(f"[ETL] Fallback: used '{best}' as revenue")

    return df


def _synthesise_transaction_id(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a stable transaction_id when none exists."""
    if "transaction_id" in df.columns:
        return df
    # Hash a few columns together for a stable, unique-ish ID
    cols = [c for c in ["customer_id", "date", "revenue", "product_id"] if c in df.columns]
    if cols:
        df["transaction_id"] = (
            df[cols].astype(str).agg("|".join, axis=1)
            + df.index.astype(str)
        ).apply(lambda x: "SYN_" + hashlib.md5(x.encode()).hexdigest()[:10])
    else:
        df["transaction_id"] = ["SYN_" + str(i).zfill(6) for i in range(len(df))]
    return df


def _synthesise_customer_id(df: pd.DataFrame) -> pd.DataFrame:
    """Use any name/email-like column, or assign row-based IDs."""
    if "customer_id" in df.columns:
        return df
    name_cols = [c for c in df.columns if any(k in c for k in
                 ("name", "email", "user", "buyer", "client", "seller", "shop", "store", "partner"))]
    if name_cols:
        df["customer_id"] = df[name_cols[0]].astype(str).str.strip()
        print(f"[ETL] Used '{name_cols[0]}' as customer_id")
    else:
        df["customer_id"] = ["CUST_" + str(i % 500).zfill(4) for i in range(len(df))]
    return df


def _synthesise_date(df: pd.DataFrame) -> pd.DataFrame:
    """Try to find any date-like column, or assign sequential dates."""
    if "date" in df.columns:
        return df
    for col in df.columns:
        if df[col].dtype == object:
            parsed = pd.to_datetime(df[col], errors="coerce", infer_datetime_format=True)
            if parsed.notna().sum() > len(df) * 0.5:
                df["date"] = parsed
                print(f"[ETL] Used '{col}' as date (auto-detected)")
                return df
    # No date found — spread rows across the past 90 days
    from datetime import datetime, timedelta
    base = datetime.today()
    df["date"] = [base - timedelta(days=i % 90) for i in range(len(df))]
    print("[ETL] No date column found — assigned synthetic rolling dates")
    return df


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # ── 1. Normalise column names ─────────────────────────────────────────────
    df = _normalise_columns(df)

    # ── 2. Apply alias map ────────────────────────────────────────────────────
    # Only rename if destination column doesn't already exist (avoid overwriting)
    rename_map = {k: v for k, v in COLUMN_ALIASES.items()
                  if k in df.columns and v not in df.columns}
    df.rename(columns=rename_map, inplace=True)

    # ── 3. Synthesise missing critical columns ────────────────────────────────
    df = _synthesise_revenue(df)
    df = _synthesise_date(df)
    df = _synthesise_customer_id(df)
    df = _synthesise_transaction_id(df)

    # ── 4. Drop full duplicate rows ───────────────────────────────────────────
    df.drop_duplicates(inplace=True)

    # ── 5. Parse & validate date ──────────────────────────────────────────────
    df["date"] = pd.to_datetime(df["date"], errors="coerce", infer_datetime_format=True)
    df.dropna(subset=["date"], inplace=True)

    # Edge case: future dates more than 1 year ahead → likely bad data
    future_cutoff = pd.Timestamp.today() + pd.DateOffset(years=1)
    df = df[df["date"] <= future_cutoff]

    # Edge case: dates before 1990 → almost always a parsing artifact
    df = df[df["date"] >= pd.Timestamp("1990-01-01")]

    # ── 6. Revenue cleaning ───────────────────────────────────────────────────
    df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")

    # Strip currency symbols from string revenue columns before coercion
    if df["revenue"].isna().all():
        for col in df.select_dtypes(include="object").columns:
            cleaned = df[col].astype(str).str.replace(r"[₹$€£,\s]", "", regex=True)
            numeric = pd.to_numeric(cleaned, errors="coerce")
            if numeric.gt(0).sum() > len(df) * 0.4:
                df["revenue"] = numeric
                print(f"[ETL] Parsed currency string from '{col}' as revenue")
                break

    # Remove zero / negative revenue
    df = df[df["revenue"].notna() & (df["revenue"] > 0)]

    # Edge case: revenue stored in paise/cents (median > 10000 and looks like subunit)
    if df["revenue"].median() > 10_000:
        # Heuristic: if >80% of values are > 100x a "normal" order (~500),
        # assume subunit and divide by 100
        if (df["revenue"] > 50_000).mean() > 0.8:
            df["revenue"] = (df["revenue"] / 100).round(2)
            print("[ETL] Detected subunit currency (paise/cents) — divided by 100")

    # ── 7. Quantity ───────────────────────────────────────────────────────────
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).clip(lower=1).astype(int)
    else:
        df["quantity"] = 1

    # ── 8. Optional string fields ─────────────────────────────────────────────
    for col in ["category", "region", "channel", "product_id"]:
        if col not in df.columns:
            df[col] = "Unknown"
        else:
            df[col] = (
                df[col].fillna("Unknown")
                .astype(str).str.strip().str.title()
                .replace({"": "Unknown", "Nan": "Unknown", "None": "Unknown", "Na": "Unknown"})
            )

    # ── 9. Remove revenue outliers (> 3 std, only when enough data) ───────────
    if len(df) > 30:
        mu, sigma = df["revenue"].mean(), df["revenue"].std()
        if sigma > 0:
            df = df[df["revenue"] < mu + 3 * sigma]

    # ── 10. Ensure transaction_id uniqueness ──────────────────────────────────
    # If duplication crept back in after synthesis, deduplicate with suffix
    if df["transaction_id"].duplicated().any():
        mask = df["transaction_id"].duplicated(keep="first")
        df.loc[mask, "transaction_id"] = (
            df.loc[mask, "transaction_id"].astype(str)
            + "_" + df.loc[mask].groupby("transaction_id").cumcount().astype(str)
        )

    df.reset_index(drop=True, inplace=True)
    return df