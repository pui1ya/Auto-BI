from datetime import datetime
from typing import Optional, List
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean
from pydantic import BaseModel
from app.core.database import Base


# ─── ORM Models ──────────────────────────────────────────────────────────────

class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    transaction_id = Column(String, unique=True, index=True)
    customer_id = Column(String, index=True)
    product_id = Column(String, index=True)
    category = Column(String, index=True)
    revenue = Column(Float)
    quantity = Column(Integer)
    date = Column(DateTime, index=True)
    region = Column(String)
    channel = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)


class Customer(Base):
    __tablename__ = "customers"

    id = Column(Integer, primary_key=True, index=True)
    customer_id = Column(String, unique=True, index=True)
    segment = Column(String)
    lifetime_value = Column(Float, default=0.0)
    first_purchase = Column(DateTime)
    last_purchase = Column(DateTime)
    total_orders = Column(Integer, default=0)
    churned = Column(Boolean, default=False)
    churn_score = Column(Float, default=0.0)


class Insight(Base):
    __tablename__ = "insights"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String)
    body = Column(Text)
    category = Column(String)   # revenue / churn / trend / anomaly
    severity = Column(String)   # info / warning / critical
    created_at = Column(DateTime, default=datetime.utcnow)


class Alert(Base):
    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, index=True)
    alert_type = Column(String)
    message = Column(Text)
    value = Column(Float)
    threshold = Column(Float)
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# ─── Pydantic Schemas ─────────────────────────────────────────────────────────

class TransactionIn(BaseModel):
    transaction_id: str
    customer_id: str
    product_id: str
    category: str
    revenue: float
    quantity: int
    date: datetime
    region: str
    channel: str


class TransactionOut(TransactionIn):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


class CustomerOut(BaseModel):
    customer_id: str
    segment: Optional[str]
    lifetime_value: float
    first_purchase: Optional[datetime]
    last_purchase: Optional[datetime]
    total_orders: int
    churned: bool
    churn_score: float

    class Config:
        from_attributes = True


class InsightOut(BaseModel):
    id: int
    title: str
    body: str
    category: str
    severity: str
    created_at: datetime

    class Config:
        from_attributes = True


class AlertOut(BaseModel):
    id: int
    alert_type: str
    message: str
    value: float
    threshold: float
    is_read: bool
    created_at: datetime

    class Config:
        from_attributes = True


class KPISummary(BaseModel):
    total_revenue: float
    total_orders: int
    unique_customers: int
    avg_order_value: float
    revenue_growth: float   # % vs previous period
    top_category: str
    top_region: str


class ForecastPoint(BaseModel):
    date: str
    predicted: float
    lower: float
    upper: float


class SegmentSummary(BaseModel):
    segment: str
    customer_count: int
    total_revenue: float
    avg_ltv: float
    churn_rate: float