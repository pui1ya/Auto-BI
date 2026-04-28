from fastapi import APIRouter, Query
from typing import List
from app.analytics.forecasting import forecast_revenue
from app.models.schemas import ForecastPoint

router = APIRouter(prefix="/forecasts", tags=["Forecasts"])


@router.get("/revenue", response_model=List[ForecastPoint])
def get_revenue_forecast(horizon: int = Query(30, ge=1, le=90)):
    df = forecast_revenue(horizon=horizon)
    return df.to_dict(orient="records")