"""
Rule-based insight generation.
Each rule is a function that returns a list of (title, body, category, severity) tuples.
"""
import pandas as pd
from app.analytics.kpi import compute_kpis, load_transactions
from app.analytics.churn import churn_summary
from app.analytics.segmentation import segment_summary
from app.core.config import settings


def revenue_insights() -> list[dict]:
    insights = []
    kpis = compute_kpis()

    if kpis.revenue_growth < -settings.REVENUE_DROP_THRESHOLD * 100:
        insights.append({
            "title": "Revenue Decline Detected",
            "body": (
                f"Revenue dropped {abs(kpis.revenue_growth):.1f}% compared to the previous period. "
                f"Current period total: ${kpis.total_revenue:,.0f}. Investigate channel and category performance."
            ),
            "category": "revenue",
            "severity": "critical",
        })
    elif kpis.revenue_growth > 10:
        insights.append({
            "title": "Strong Revenue Growth",
            "body": (
                f"Revenue grew {kpis.revenue_growth:.1f}% vs previous period. "
                f"Total: ${kpis.total_revenue:,.0f}. Top driver: {kpis.top_category}."
            ),
            "category": "revenue",
            "severity": "info",
        })

    if kpis.avg_order_value < 30:
        insights.append({
            "title": "Low Average Order Value",
            "body": (
                f"AOV is ${kpis.avg_order_value:.2f}. Consider bundling products or upsell strategies."
            ),
            "category": "revenue",
            "severity": "warning",
        })

    return insights


def churn_insights() -> list[dict]:
    insights = []
    stats = churn_summary()

    if stats["churn_rate"] > settings.CHURN_SPIKE_THRESHOLD * 100:
        insights.append({
            "title": "High Churn Rate",
            "body": (
                f"{stats['churn_rate']:.1f}% of customers are predicted to churn. "
                f"{stats['at_risk_count']} more are at risk. Launch a re-engagement campaign."
            ),
            "category": "churn",
            "severity": "critical",
        })
    elif stats["churn_rate"] > 5:
        insights.append({
            "title": "Moderate Churn Risk",
            "body": (
                f"Churn rate at {stats['churn_rate']:.1f}%. "
                f"Monitor 'At Risk' segment closely ({stats['at_risk_count']} customers)."
            ),
            "category": "churn",
            "severity": "warning",
        })

    return insights


def segment_insights() -> list[dict]:
    insights = []
    seg = segment_summary()
    if seg.empty:
        return insights

    lost = seg[seg["segment"] == "Lost"]
    champions = seg[seg["segment"] == "Champions"]

    if not lost.empty:
        n = int(lost["customer_count"].values[0])
        rev = float(lost["total_revenue"].values[0])
        insights.append({
            "title": f"{n} Customers in 'Lost' Segment",
            "body": (
                f"These {n} customers generated ${rev:,.0f} historically but are now inactive. "
                "Consider win-back campaigns with targeted discounts."
            ),
            "category": "churn",
            "severity": "warning",
        })

    if not champions.empty:
        n = int(champions["customer_count"].values[0])
        rev = float(champions["total_revenue"].values[0])
        insights.append({
            "title": f"Champion Segment Driving Revenue",
            "body": (
                f"{n} champion customers contribute ${rev:,.0f}. "
                "Reward them with loyalty perks to maintain retention."
            ),
            "category": "revenue",
            "severity": "info",
        })

    return insights


def all_rules() -> list[dict]:
    results = []
    for fn in [revenue_insights, churn_insights, segment_insights]:
        try:
            results.extend(fn())
        except Exception:
            pass
    return results