from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    APP_NAME: str = "Auto BI"
    DEBUG: bool = True

    DATABASE_URL: str = "sqlite+aiosqlite:///./data/auto_bi.db"
    DATABASE_URL_SYNC: str = "sqlite:///./data/auto_bi.db"

    REDIS_URL: Optional[str] = None
    CACHE_TTL: int = 300  # seconds

    RAW_DATA_PATH: str = "data/raw"
    PROCESSED_DATA_PATH: str = "data/processed"

    # Forecasting
    FORECAST_HORIZON: int = 30  # days
    FORECAST_FREQ: str = "D"

    # Alerts
    REVENUE_DROP_THRESHOLD: float = 0.10   # 10% drop triggers alert
    CHURN_SPIKE_THRESHOLD: float = 0.15    # 15% churn spike triggers alert

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()