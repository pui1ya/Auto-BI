from fastapi import APIRouter
from app.analytics.segmentation import compute_rfm, segment_summary
from app.analytics.churn import churn_summary

router = APIRouter(prefix="/segments", tags=["Segments"])


@router.get("/rfm")
def get_rfm():
    df = compute_rfm()
    if df.empty:
        return []
    cols = [c for c in ["customer_id", "recency", "frequency", "monetary", "segment", "churn_score", "churned"] if c in df.columns]
    return df[cols].head(200).to_dict(orient="records")


@router.get("/summary")
def get_segment_summary():
    df = segment_summary()
    if df.empty:
        return []
    return df.to_dict(orient="records")


@router.get("/churn")
def get_churn_summary():
    return churn_summary()