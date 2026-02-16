from fastapi import APIRouter
from app.core.database import load_dataframe

router = APIRouter()


@router.get("/daily")
def get_daily_metrics():
    return load_dataframe("daily_revenue_kpis").to_dict(orient="records")


@router.get("/customers")
def get_customer_kpi():
    return load_dataframe("customers").to_dict(orient="records")


@router.get("/top-customers")
def get_top_customers():

    customers = load_dataframe("customers")
    revenue = load_dataframe("daily_revenue_kpis")

    df = revenue.merge(customers, on="customer_id")

    top = (
        df.groupby("name")["total_sales"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .reset_index()
    )

    return top.to_dict(orient="records")