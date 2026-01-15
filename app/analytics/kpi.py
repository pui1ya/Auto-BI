'''
LITERALLY FOR PERFORMING ALL KPIs. SO FAR I'M SOO PROUD OF MYSELF YAYYYYYYY. IK NOBODYS GONNA SEE THIS LOL
'''

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT))

import pandas as pd

#for returning total revenue, total order and average of those for the day
def compute_daily_revenue(transactions, orders):

    #merge two df for df_today
    df = pd.merge(transactions, orders, on = "order_id", how="outer")

    daily_revenue = (df.groupby('order_date').agg(
        total_sales = ("sales", "sum"),
        items_sold = ("quantity", "sum"),
        day_profit = ("profit", "sum"),
        discount_provided = ("discount", "sum")
    )
    .reset_index(drop = True)
    )

    daily_revenue['average_sold_value'] = (daily_revenue['total_sales']/daily_revenue['items_sold'])

    return daily_revenue

#i'm soooooo happyyyyy i encountered what i used to see in memes "production bugs" but i fixed those 🥹 there still might be errors but lets fix them all😤

if __name__ == "__main__":
    from app.etl.ingest import ingest_csv
    from app.etl.clean import clean_raw_data
    from app.etl.transform import table_builder
    from app.etl.validate import validate_raw_data

    raw = ingest_csv('/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv')
    clean = clean_raw_data(raw)

    builder = table_builder(clean)

    orders = builder.build_orders_table()
    transactions = builder.build_transactions_table()

    validate_raw_data(orders)
    validate_raw_data(transactions)

    print(compute_daily_revenue(transactions, orders))


