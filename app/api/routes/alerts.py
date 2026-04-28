from fastapi import APIRouter, Query
from typing import List
from sqlalchemy.orm import Session
from app.core.database import sync_engine
from app.models.schemas import Alert, AlertOut
from app.insights.generator import check_and_fire_alerts

router = APIRouter(prefix="/alerts", tags=["Alerts"])


@router.get("/", response_model=List[AlertOut])
def get_alerts(unread_only: bool = Query(False)):
    with Session(sync_engine) as session:
        q = session.query(Alert).order_by(Alert.created_at.desc())
        if unread_only:
            q = q.filter(Alert.is_read == False)
        return q.limit(50).all()


@router.post("/check")
def check_alerts():
    fired = check_and_fire_alerts()
    return {"alerts_fired": len(fired)}


@router.patch("/{alert_id}/read")
def mark_read(alert_id: int):
    with Session(sync_engine) as session:
        alert = session.query(Alert).filter_by(id=alert_id).first()
        if alert:
            alert.is_read = True
            session.commit()
            return {"status": "ok"}
    return {"status": "not_found"}