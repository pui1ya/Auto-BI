"""
Churn prediction using a simple Random Forest on RFM features.
Also updates the customers table with churn scores.
"""
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from app.analytics.segmentation import compute_rfm
from app.core.database import sync_engine
from app.models.schemas import Customer
from sqlalchemy.orm import Session


CHURN_RECENCY_THRESHOLD = 60  # days inactive = churned for training labels


def build_churn_model(rfm: pd.DataFrame = None):
    if rfm is None:
        rfm = compute_rfm()
    if rfm.empty or len(rfm) < 20:
        return None, None

    features = ["recency", "frequency", "monetary"]
    rfm = rfm.dropna(subset=features)

    # Label: churned if recency > threshold and frequency == 1
    rfm["churn_label"] = (
        (rfm["recency"] > CHURN_RECENCY_THRESHOLD) & (rfm["frequency"] <= 1)
    ).astype(int)

    X = rfm[features].values
    y = rfm["churn_label"].values

    if y.sum() == 0 or y.sum() == len(y):
        # All same class — can't train, fall back to rule-based scores
        rfm["churn_score"] = (rfm["churn_label"].astype(float) * 0.8 +
                               (rfm["recency"] / rfm["recency"].max()) * 0.2).round(4)
        rfm["churned"] = rfm["churn_label"].astype(bool)
        return None, rfm

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    clf = RandomForestClassifier(n_estimators=50, random_state=42)
    clf.fit(X_scaled, y)

    rfm["churn_score"] = clf.predict_proba(X_scaled)[:, 1].round(4)
    rfm["churned"] = (rfm["churn_score"] > 0.5).astype(bool)
    return clf, rfm


def update_customer_churn_scores():
    _, rfm = build_churn_model()
    if rfm is None:
        return

    with Session(sync_engine) as session:
        for _, row in rfm.iterrows():
            cust = session.query(Customer).filter_by(customer_id=row["customer_id"]).first()
            if cust:
                cust.churn_score = float(row.get("churn_score", 0))
                cust.churned = bool(row.get("churned", False))
            else:
                c = Customer(
                    customer_id=row["customer_id"],
                    churn_score=float(row.get("churn_score", 0)),
                    churned=bool(row.get("churned", False)),
                    total_orders=int(row["frequency"]),
                    lifetime_value=float(row["monetary"]),
                    segment=str(row.get("segment", "Unknown")),
                )
                session.add(c)
        session.commit()


def churn_summary() -> dict:
    _, rfm = build_churn_model()
    if rfm is None or rfm.empty:
        return {"churn_rate": 0, "at_risk_count": 0, "churned_count": 0, "total_customers": 0}

    # Ensure columns exist with safe defaults
    if "churned" not in rfm.columns:
        rfm["churned"] = False
    if "churn_score" not in rfm.columns:
        rfm["churn_score"] = 0.0

    churned_count = int(rfm["churned"].sum())
    at_risk = int(((rfm["churn_score"] >= 0.3) & (rfm["churn_score"] < 0.5)).sum())
    churn_rate = round(churned_count / len(rfm) * 100, 2) if len(rfm) else 0
    return {
        "churn_rate": churn_rate,
        "at_risk_count": at_risk,
        "churned_count": churned_count,
        "total_customers": len(rfm),
    }