"""
Persist generated insights to the DB and fire alerts when thresholds are crossed.
"""
from datetime import datetime
from sqlalchemy.orm import Session
from app.core.database import sync_engine
from app.models.schemas import Insight, Alert
from app.insights.rules import all_rules
from app.analytics.kpi import compute_kpis
from app.analytics.churn import churn_summary
from app.core.config import settings


def generate_and_save_insights():
    raw_insights = all_rules()
    with Session(sync_engine) as session:
        for item in raw_insights:
            ins = Insight(
                title=item["title"],
                body=item["body"],
                category=item["category"],
                severity=item["severity"],
                created_at=datetime.utcnow(),
            )
            session.add(ins)
        session.commit()
    return raw_insights


def check_and_fire_alerts():
    alerts = []
    kpis = compute_kpis()
    churn = churn_summary()

    threshold_rev = settings.REVENUE_DROP_THRESHOLD * 100
    if kpis.revenue_growth < -threshold_rev:
        alerts.append(Alert(
            alert_type="revenue_drop",
            message=f"Revenue dropped {abs(kpis.revenue_growth):.1f}% vs previous period.",
            value=kpis.revenue_growth,
            threshold=-threshold_rev,
        ))

    threshold_churn = settings.CHURN_SPIKE_THRESHOLD * 100
    if churn["churn_rate"] > threshold_churn:
        alerts.append(Alert(
            alert_type="churn_spike",
            message=f"Churn rate {churn['churn_rate']:.1f}% exceeds threshold {threshold_churn:.0f}%.",
            value=churn["churn_rate"],
            threshold=threshold_churn,
        ))

    if alerts:
        with Session(sync_engine) as session:
            for a in alerts:
                session.add(a)
            session.commit()

    return alerts