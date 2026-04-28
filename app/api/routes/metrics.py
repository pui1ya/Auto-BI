from fastapi import APIRouter, Query
from app.analytics.kpi import compute_kpis, revenue_by_day, revenue_by_category, revenue_by_region, revenue_by_channel
from app.models.schemas import KPISummary

router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get("/kpis", response_model=KPISummary)
def get_kpis(period_days: int = Query(30, ge=1, le=365)):
    return compute_kpis(period_days=period_days)


@router.get("/revenue-by-day")
def get_revenue_by_day():
    df = revenue_by_day()
    return df.to_dict(orient="records")


@router.get("/revenue-by-category")
def get_revenue_by_category():
    df = revenue_by_category()
    return df.to_dict(orient="records")


@router.get("/revenue-by-region")
def get_revenue_by_region():
    df = revenue_by_region()
    return df.to_dict(orient="records")


@router.get("/revenue-by-channel")
def get_revenue_by_channel():
    df = revenue_by_channel()
    return df.to_dict(orient="records")