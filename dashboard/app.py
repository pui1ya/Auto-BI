"""
Auto BI – Streamlit Dashboard
Run: streamlit run dashboard/app.py
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

# ── Bootstrap DB & data on first run ─────────────────────────────────────────
import asyncio
from app.core.database import init_db, sync_engine
from sqlalchemy import text

async def _init():
    await init_db()

asyncio.run(_init())

os.makedirs("data/raw", exist_ok=True)
os.makedirs("data/processed", exist_ok=True)

with sync_engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar()

if count == 0:
    with st.spinner("Generating sample data & running analytics…"):
        from app.etl.ingest import generate_sample_data, ingest_all
        generate_sample_data(n=800)
        ingest_all()
        from app.analytics.churn import update_customer_churn_scores
        update_customer_churn_scores()
        from app.insights.generator import generate_and_save_insights, check_and_fire_alerts
        generate_and_save_insights()
        check_and_fire_alerts()

# ── Imports after bootstrap ───────────────────────────────────────────────────
from app.analytics.kpi import compute_kpis, revenue_by_day, revenue_by_category, revenue_by_region, revenue_by_channel
from app.analytics.forecasting import forecast_revenue
from app.analytics.segmentation import compute_rfm, segment_summary
from app.analytics.churn import churn_summary
from app.analytics.associations import basket_analysis
from app.insights.rules import all_rules
from app.models.schemas import Insight, Alert
from sqlalchemy.orm import Session

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Auto BI",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Space Grotesk', sans-serif; }
    .metric-card {
        background: linear-gradient(135deg, #1e1e2e 0%, #2a2a3e 100%);
        border: 1px solid #3a3a5c;
        border-radius: 12px;
        padding: 1.2rem 1.5rem;
        margin-bottom: 0.5rem;
    }
    .metric-value { font-size: 2rem; font-weight: 700; color: #a78bfa; }
    .metric-label { font-size: 0.85rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; }
    .metric-delta-pos { color: #34d399; font-size: 0.9rem; }
    .metric-delta-neg { color: #f87171; font-size: 0.9rem; }
    .insight-card {
        border-left: 4px solid #a78bfa;
        background: #1e1e2e;
        border-radius: 8px;
        padding: 0.9rem 1.2rem;
        margin-bottom: 0.6rem;
    }
    .insight-critical { border-left-color: #f87171; }
    .insight-warning  { border-left-color: #fbbf24; }
    .insight-info     { border-left-color: #34d399; }
    .section-title { font-size: 1.1rem; font-weight: 600; color: #c4b5fd; margin: 1rem 0 0.4rem; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ Auto BI")
    st.markdown("---")
    page = st.radio("Navigate", [
        "Overview",
        "Forecast",
        "Segments",
        "Churn",
        "Insights",
        "Basket Analysis",
        "ETL & Refresh",
    ])
    st.markdown("---")
    period = st.slider("KPI period (days)", 7, 180, 30)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE: OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────
if page == "Overview":
    st.title("Business Overview")

    kpis = compute_kpis(period_days=period)

    c1, c2, c3, c4 = st.columns(4)
    def kpi_card(col, label, value, delta=None):
        with col:
            delta_html = ""
            if delta is not None:
                cls = "metric-delta-pos" if delta >= 0 else "metric-delta-neg"
                arrow = "▲" if delta >= 0 else "▼"
                delta_html = f'<div class="{cls}">{arrow} {abs(delta):.1f}%</div>'
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-label">{label}</div>
                <div class="metric-value">{value}</div>
                {delta_html}
            </div>""", unsafe_allow_html=True)

    kpi_card(c1, "Total Revenue", f"${kpis.total_revenue:,.0f}", kpis.revenue_growth)
    kpi_card(c2, "Total Orders", f"{kpis.total_orders:,}")
    kpi_card(c3, "Unique Customers", f"{kpis.unique_customers:,}")
    kpi_card(c4, "Avg Order Value", f"${kpis.avg_order_value:,.2f}")

    st.markdown("---")

    # Revenue over time
    rev_day = revenue_by_day()
    if not rev_day.empty:
        rev_day["ds"] = pd.to_datetime(rev_day["ds"])
        fig = px.area(rev_day, x="ds", y="y",
                      title="Revenue Over Time",
                      labels={"ds": "Date", "y": "Revenue ($)"},
                      color_discrete_sequence=["#a78bfa"])
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="#e2e8f0", title_font_size=16)
        fig.update_xaxes(gridcolor="#2a2a3e")
        fig.update_yaxes(gridcolor="#2a2a3e")
        st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        cat = revenue_by_category()
        if not cat.empty:
            fig2 = px.bar(cat, x="revenue", y="category", orientation="h",
                          title="Revenue by Category",
                          color="revenue", color_continuous_scale="Purples")
            fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font_color="#e2e8f0", showlegend=False)
            fig2.update_xaxes(gridcolor="#2a2a3e")
            fig2.update_yaxes(gridcolor="#2a2a3e")
            st.plotly_chart(fig2, use_container_width=True)

    with col2:
        reg = revenue_by_region()
        if not reg.empty:
            fig3 = px.pie(reg, values="revenue", names="region",
                          title="Revenue by Region",
                          color_discrete_sequence=px.colors.sequential.Purples_r)
            fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="#e2e8f0")
            st.plotly_chart(fig3, use_container_width=True)

    ch = revenue_by_channel()
    if not ch.empty:
        fig4 = px.bar(ch, x="channel", y="revenue",
                      title="Revenue by Channel",
                      color="channel",
                      color_discrete_sequence=["#a78bfa", "#818cf8", "#34d399"])
        fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                           font_color="#e2e8f0", showlegend=False)
        fig4.update_xaxes(gridcolor="#2a2a3e")
        fig4.update_yaxes(gridcolor="#2a2a3e")
        st.plotly_chart(fig4, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: FORECAST
# ─────────────────────────────────────────────────────────────────────────────
elif page == "🔮 Forecast":
    st.title("🔮 Revenue Forecast")

    horizon = st.slider("Forecast horizon (days)", 7, 90, 30)

    with st.spinner("Running forecast model…"):
        forecast_df = forecast_revenue(horizon=horizon)
        hist = revenue_by_day()

    if forecast_df.empty:
        st.warning("Not enough data to generate a forecast. Upload more data first.")
    else:
        hist["ds"] = pd.to_datetime(hist["ds"])
        forecast_df["date"] = pd.to_datetime(forecast_df["date"])

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=hist["ds"], y=hist["y"],
            mode="lines", name="Historical",
            line=dict(color="#a78bfa", width=2)
        ))
        fig.add_trace(go.Scatter(
            x=forecast_df["date"], y=forecast_df["predicted"],
            mode="lines", name="Forecast",
            line=dict(color="#34d399", width=2, dash="dash")
        ))
        fig.add_trace(go.Scatter(
            x=pd.concat([forecast_df["date"], forecast_df["date"][::-1]]),
            y=pd.concat([forecast_df["upper"], forecast_df["lower"][::-1]]),
            fill="toself", fillcolor="rgba(52,211,153,0.1)",
            line=dict(color="rgba(255,255,255,0)"),
            name="Confidence Band"
        ))
        fig.update_layout(
            title=f"{horizon}-Day Revenue Forecast",
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#e2e8f0", legend=dict(bgcolor="rgba(0,0,0,0)")
        )
        fig.update_xaxes(gridcolor="#2a2a3e")
        fig.update_yaxes(gridcolor="#2a2a3e")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Forecast Table")
        st.dataframe(forecast_df.style.format({
            "predicted": "${:,.2f}", "lower": "${:,.2f}", "upper": "${:,.2f}"
        }), use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: SEGMENTS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "👥 Segments":
    st.title("👥 Customer Segments (RFM)")

    with st.spinner("Computing RFM…"):
        rfm = compute_rfm()
        seg_sum = segment_summary()

    if rfm.empty:
        st.warning("No customer data yet.")
    else:
        if not seg_sum.empty:
            c1, c2 = st.columns(2)
            with c1:
                fig = px.pie(seg_sum, values="customer_count", names="segment",
                             title="Customers by Segment",
                             color_discrete_sequence=["#a78bfa","#818cf8","#fbbf24","#f87171"])
                fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", font_color="#e2e8f0")
                st.plotly_chart(fig, use_container_width=True)
            with c2:
                fig2 = px.bar(seg_sum, x="segment", y="total_revenue",
                              title="Revenue by Segment", color="segment",
                              color_discrete_sequence=["#a78bfa","#818cf8","#fbbf24","#f87171"])
                fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                   font_color="#e2e8f0", showlegend=False)
                fig2.update_xaxes(gridcolor="#2a2a3e")
                fig2.update_yaxes(gridcolor="#2a2a3e")
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("### Segment Summary Table")
            st.dataframe(seg_sum, use_container_width=True)

        st.markdown("### RFM Scatter (Recency vs Monetary)")
        fig3 = px.scatter(
            rfm, x="recency", y="monetary", size="frequency",
            color="segment", hover_data=["customer_id"],
            color_discrete_sequence=["#a78bfa","#818cf8","#fbbf24","#f87171"],
            title="Customer RFM Map"
        )
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                           font_color="#e2e8f0")
        fig3.update_xaxes(gridcolor="#2a2a3e")
        fig3.update_yaxes(gridcolor="#2a2a3e")
        st.plotly_chart(fig3, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: CHURN
# ─────────────────────────────────────────────────────────────────────────────
elif page == "🚨 Churn":
    st.title("🚨 Churn Analysis")

    with st.spinner("Running churn model…"):
        stats = churn_summary()
        from app.analytics.churn import build_churn_model
        _, rfm = build_churn_model()

    c1, c2, c3 = st.columns(3)
    kpi_color = "#f87171" if stats["churn_rate"] > 15 else "#fbbf24" if stats["churn_rate"] > 5 else "#34d399"
    with c1:
        st.metric("Churn Rate", f"{stats['churn_rate']:.1f}%")
    with c2:
        st.metric("Churned Customers", stats["churned_count"])
    with c3:
        st.metric("At-Risk Customers", stats["at_risk_count"])

    if rfm is not None and not rfm.empty and "churn_score" in rfm.columns:
        fig = px.histogram(rfm, x="churn_score", nbins=20,
                           title="Churn Score Distribution",
                           color_discrete_sequence=["#f87171"])
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="#e2e8f0")
        fig.update_xaxes(gridcolor="#2a2a3e")
        fig.update_yaxes(gridcolor="#2a2a3e")
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### High Risk Customers")
        high_risk = rfm[rfm["churn_score"] >= 0.4][
            ["customer_id", "recency", "frequency", "monetary", "churn_score", "segment"]
        ].sort_values("churn_score", ascending=False).head(30)
        st.dataframe(high_risk, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: INSIGHTS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "💡 Insights":
    st.title("💡 AI-Generated Insights")

    col_refresh, _ = st.columns([1, 5])
    with col_refresh:
        if st.button("Refresh Insights"):
            from app.insights.generator import generate_and_save_insights
            generate_and_save_insights()
            st.success("Insights refreshed!")

    raw = all_rules()
    if not raw:
        st.info("No insights generated yet. Click Refresh.")
    else:
        for item in raw:
            sev = item["severity"]
            icon = {"critical": "🔴", "warning": "🟡", "info": "🟢"}.get(sev, "⚪")
            st.markdown(f"""
            <div class="insight-card insight-{sev}">
                <strong>{icon} {item['title']}</strong>
                <p style="margin:0.3rem 0 0; color:#94a3b8; font-size:0.9rem;">{item['body']}</p>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("###Active Alerts")
    with Session(sync_engine) as session:
        db_alerts = session.query(Alert).order_by(Alert.created_at.desc()).limit(10).all()

    if not db_alerts:
        st.success("No active alerts. All systems normal.")
    else:
        for a in db_alerts:
            st.warning(f"**{a.alert_type}** — {a.message}")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: BASKET ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
elif page == "Basket Analysis":
    st.title("Market Basket Analysis")

    with st.spinner("Running association rules…"):
        rules = basket_analysis()

    if rules.empty or "note" in rules.columns:
        st.info("Not enough transaction variety for basket analysis yet.")
    else:
        st.markdown("### Top Association Rules (by Lift)")
        fig = px.scatter(rules, x="support", y="confidence", size="lift",
                         hover_data=["antecedents", "consequents"],
                         color="lift", color_continuous_scale="Purples",
                         title="Support vs Confidence (bubble = lift)")
        fig.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                          font_color="#e2e8f0")
        fig.update_xaxes(gridcolor="#2a2a3e")
        fig.update_yaxes(gridcolor="#2a2a3e")
        st.plotly_chart(fig, use_container_width=True)

        st.dataframe(rules, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE: ETL & REFRESH
# ─────────────────────────────────────────────────────────────────────────────
elif page == "ETL & Refresh":
    st.title("ETL & Data Refresh")

    st.markdown("### Upload New Data")
    uploaded = st.file_uploader("Upload CSV / JSON / Excel", type=["csv", "json", "xlsx"])
    if uploaded:
        os.makedirs("data/raw", exist_ok=True)
        save_path = os.path.join("data/raw", uploaded.name)
        with open(save_path, "wb") as f:
            f.write(uploaded.getbuffer())
        st.success(f"Saved to {save_path}")

    st.markdown("---")
    st.markdown("### Run Full Pipeline")
    if st.button("▶ Run ETL + Analytics + Insights"):
        with st.spinner("Running pipeline…"):
            from app.etl.ingest import ingest_all
            result = ingest_all()
            from app.analytics.churn import update_customer_churn_scores
            update_customer_churn_scores()
            from app.insights.generator import generate_and_save_insights, check_and_fire_alerts
            generate_and_save_insights()
            check_and_fire_alerts()
        st.success("Pipeline complete!")
        st.json(result)

    st.markdown("---")
    st.markdown("### Generate Fresh Sample Data")
    n_rows = st.number_input("Number of rows", min_value=100, max_value=5000, value=800, step=100)
    if st.button("Generate Sample Data"):
        from app.etl.ingest import generate_sample_data, ingest_all
        path = generate_sample_data(n=int(n_rows))
        ingest_all()
        st.success(f"Generated {n_rows} rows → {path} and ingested.")