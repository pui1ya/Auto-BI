# Auto BI

> A machine that takes messy business data and turns it into decisions.

```
Data comes in → Clean it → Store it → Analyze it → Predict future → Show it → Decisions
```

---

## 🏗️ Project Structure

```
auto_bi/
├── app/
│   ├── api/routes/        # FastAPI endpoints
│   ├── core/              # DB, config, cache
│   ├── etl/               # Ingest, clean, transform, validate
│   ├── analytics/         # KPIs, segmentation, forecasting, churn, associations
│   ├── insights/          # Rule-based insight & alert generation
│   └── models/            # SQLAlchemy ORM + Pydantic schemas
├── dashboard/             # Streamlit dashboard
├── data/raw/              # Drop your CSV/JSON/Excel files here
├── data/processed/
└── tests/
```

---

## 🚀 Quickstart

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the Streamlit dashboard (all-in-one, easiest)
```bash
streamlit run dashboard/app.py
```
The app auto-generates 800 rows of synthetic data on first launch. Drop your own CSV files in `data/raw/` and click **Run ETL** to use real data.

### 3. Run the FastAPI backend (optional)
```bash
uvicorn app.api.main:app --reload
```
Visit [http://localhost:8000/docs](http://localhost:8000/docs) for interactive API docs.

---

## 📊 Features

| Feature | Details |
|---|---|
| **ETL Pipeline** | Ingest CSV/JSON/Excel, clean, validate, store in SQLite |
| **KPI Dashboard** | Revenue, orders, AOV, growth vs previous period |
| **Forecasting** | 30-day revenue forecast with confidence bands (Holt-Winters) |
| **RFM Segmentation** | Champions / Loyal / At Risk / Lost |
| **Churn Prediction** | Random Forest scoring on RFM features |
| **Basket Analysis** | Apriori association rules |
| **Insights** | Auto-generated business insights from rule engine |
| **Alerts** | Triggered when KPIs breach configured thresholds |
| **REST API** | Full FastAPI backend with Swagger docs |

---

## 📁 Bring Your Own Data

Drop any CSV with these columns into `data/raw/`:

| Column | Required | Notes |
|---|---|---|
| `transaction_id` | ✅ | Unique order ID |
| `customer_id` | ✅ | |
| `revenue` | ✅ | Numeric |
| `date` | ✅ | Any parseable date format |
| `product_id` | ❌ | Defaults to "UNKNOWN" |
| `category` | ❌ | Defaults to "Unknown" |
| `quantity` | ❌ | Defaults to 1 |
| `region` | ❌ | Defaults to "Unknown" |
| `channel` | ❌ | Defaults to "Unknown" |

Column aliases like `order_id`, `amount`, `order_date` are auto-mapped.

---

## ⚙️ Configuration

Edit `.env` or `app/core/config.py`:

```env
REVENUE_DROP_THRESHOLD=0.10   # 10% drop fires an alert
CHURN_SPIKE_THRESHOLD=0.15    # 15% churn rate fires an alert
FORECAST_HORIZON=30           # Days to forecast ahead
```

---

## 🧪 Tests

```bash
pytest tests/ -v
```

---

## 🔌 API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/metrics/kpis` | KPI summary |
| GET | `/metrics/revenue-by-day` | Daily revenue series |
| GET | `/metrics/revenue-by-category` | Revenue breakdown |
| GET | `/forecasts/revenue` | Revenue forecast |
| GET | `/segments/rfm` | RFM customer scores |
| GET | `/segments/summary` | Segment summary |
| GET | `/segments/churn` | Churn stats |
| GET | `/insights/` | Latest insights |
| POST | `/insights/generate` | Re-run insight engine |
| GET | `/alerts/` | All alerts |
| POST | `/alerts/check` | Check & fire alerts |