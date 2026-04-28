import os
import shutil
from fastapi import APIRouter, UploadFile, File
from app.core.config import settings

router = APIRouter(prefix="/etl", tags=["ETL"])


@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    os.makedirs(settings.RAW_DATA_PATH, exist_ok=True)
    dest = os.path.join(settings.RAW_DATA_PATH, file.filename)
    with open(dest, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"saved": dest}


@router.post("/run")
def run_pipeline():
    from app.etl.ingest import ingest_all
    from app.analytics.churn import update_customer_churn_scores
    from app.insights.generator import generate_and_save_insights, check_and_fire_alerts

    result = ingest_all()
    update_customer_churn_scores()
    generate_and_save_insights()
    check_and_fire_alerts()
    return result


@router.post("/generate-sample")
def generate_sample():
    from app.etl.ingest import generate_sample_data, ingest_all
    path = generate_sample_data(n=800)
    ingest_all()
    return {"generated": path}