from fastapi import APIRouter, Query
from typing import List
from sqlalchemy.orm import Session
from app.core.database import sync_engine
from app.models.schemas import Insight, InsightOut
from app.insights.generator import generate_and_save_insights

router = APIRouter(prefix="/insights", tags=["Insights"])


@router.get("/", response_model=List[InsightOut])
def get_insights(limit: int = Query(20, ge=1, le=100)):
    with Session(sync_engine) as session:
        items = (
            session.query(Insight)
            .order_by(Insight.created_at.desc())
            .limit(limit)
            .all()
        )
    return items


@router.post("/generate")
def trigger_insight_generation():
    items = generate_and_save_insights()
    return {"generated": len(items), "insights": items}