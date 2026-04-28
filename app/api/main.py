import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from app.core.database import init_db
from app.core.config import settings
from app.api.routes import metrics, forecasts, segments, insights, alerts, etl


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(settings.RAW_DATA_PATH, exist_ok=True)
    os.makedirs(settings.PROCESSED_DATA_PATH, exist_ok=True)
    await init_db()

    from app.core.database import sync_engine
    from sqlalchemy import text
    with sync_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar()

    if count == 0:
        from app.etl.ingest import generate_sample_data, ingest_all
        generate_sample_data(n=800)
        ingest_all()
        from app.analytics.churn import update_customer_churn_scores
        update_customer_churn_scores()
        from app.insights.generator import generate_and_save_insights, check_and_fire_alerts
        generate_and_save_insights()
        check_and_fire_alerts()
    yield


app = FastAPI(
    title="Auto BI API",
    description="Automated Business Intelligence — data in, decisions out.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(metrics.router)
app.include_router(forecasts.router)
app.include_router(segments.router)
app.include_router(insights.router)
app.include_router(alerts.router)
app.include_router(etl.router)

# Resolve dashboard path relative to the project root (2 levels up from app/api/)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_DASHBOARD_DIR = os.path.join(_PROJECT_ROOT, "dashboard")

@app.get("/ui")
def serve_dashboard():
    index = os.path.join(_DASHBOARD_DIR, "index.html")
    if os.path.exists(index):
        return FileResponse(index)
    return {"error": f"Dashboard not found at {index}"}

if os.path.isdir(_DASHBOARD_DIR):
    app.mount("/ui/static", StaticFiles(directory=_DASHBOARD_DIR), name="static")


@app.get("/")
def root():
    return {"message": "Auto BI is running", "docs": "/docs", "dashboard": "/ui"}


@app.get("/health")
def health():
    return {"status": "ok"}