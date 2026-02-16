from fastapi import FastAPI
from app.api.routes import metrics

app = FastAPI(title="Auto BI API")

app.include_router(metrics.router, prefix="/metrics")

@app.get("/")
def health():
    return {"status": "Auto BI API running"}
