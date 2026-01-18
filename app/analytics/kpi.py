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
    .reset_index()
    )

    daily_revenue['average_sold_value'] = (daily_revenue['total_sales']/daily_revenue['items_sold'])

    return daily_revenue

#i'm soooooo happyyyyy i encountered what i used to see in memes "production bugs" but i fixed those 🥹 there still might be errors but lets fix them all😤


#for computing customer KPIs
#also for playing with OOPs ✋😏
class ComputeCustomerKpis():

    def __init__(self, customers, transactions, orders):
        self.cust = customers
        self.tran = transactions
        self.ord = orders

    def merge_df(self):
        df = pd.merge(self.cust, self.tran, on="customer_id", how="inner")
        df = pd.merge(self.ord, df, on="order_id", how="inner")
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        return df

    def sales_profit(self):
        df = self.merge_df()

        sales_and_profit = (df.groupby('customer_id').agg(
            total_sales = ("sales", "sum"),
            total_profit = ("profit", "sum"),
            total_discount = ("discount", "sum"),
            total_orders = ("order_id", "nunique")
        )
        .reset_index()
        )

        sales_and_profit['avg_order_value'] = sales_and_profit['total_sales']/sales_and_profit['total_orders']

        return sales_and_profit
    
    def purchase_behaviour(self):
        df = self.merge_df()

        purchase_behaviour = (df.groupby("customer_id").agg(
            total_orders = ("order_id", "nunique"),
            total_items_purchased = ("quantity", "sum")
        )
        .reset_index())

        purchase_behaviour['avg_items_per_order'] = purchase_behaviour['total_items_purchased'] / purchase_behaviour['total_orders']

        return purchase_behaviour
    
    def loyalty_and_engagement(self):
        df = self.merge_df()

        loyalty_and_engagement = (df.groupby("customer_id").agg(
            first_purchase_date = ("order_date", "min"),
            last_purchase_order = ("order_date", "max")
        )
        .reset_index()
        )

        loyalty_and_engagement['customer_lifetime_days'] = (loyalty_and_engagement['last_purchase_order'] - loyalty_and_engagement['first_purchase_date']).dt.days

        return loyalty_and_engagement
    
    def customer_quality(self):
        df = self.sales_profit()

        df["profit_margin"] = df["total_profit"] / df["total_sales"]
        df["discount_ratio"] = df["total_discount"] / df["total_sales"]

        return df[["customer_id", "profit_margin", "discount_ratio"]]
    

#for computing product kpis

def compute_product_kpis(products, orders, transactions):
    df = (
        transactions
        .merge(products, on="product_id", how="inner")
        .merge(orders, on="order_id", how="inner")
    )

    product_kpis = (
        df.groupby("product_id")
        .agg(
            total_sales=("sales", "sum"),
            total_quantity_sold=("quantity", "sum"),
            total_profit=("profit", "sum"),
            total_orders=("order_id", "nunique"),
            total_discount=("discount", "sum")
        )
        .reset_index()
    )

    product_kpis["avg_selling_price"] = (
        product_kpis["total_sales"] / product_kpis["total_quantity_sold"]
    )

    product_kpis["profit_margin"] = (
        product_kpis["total_profit"] / product_kpis["total_sales"]
    )

    product_kpis["discount_ratio"] = (
        product_kpis["total_discount"] / product_kpis["total_sales"]
    )

    product_kpis["is_loss_making"] = product_kpis["total_profit"] < 0
    product_kpis["is_discount_heavy"] = product_kpis["discount_ratio"] > 0.2

    return product_kpis

    

if __name__ == "__main__":
    from app.etl.ingest import ingest_csv
    from app.etl.clean import clean_raw_data
    from app.etl.transform import table_builder
    from app.etl.validate import validate_raw_data

    raw = ingest_csv('/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv')
    clean = clean_raw_data(raw)

    builder = table_builder(clean)
    customers = builder.build_customers_table()
    orders = builder.build_orders_table()
    transactions = builder.build_transactions_table()
    products = builder.build_products_table()

    kpi = ComputeCustomerKpis(customers, transactions, orders)
    print("Customer Quality")
    print(kpi.customer_quality())

    print("\nLoyalty and Engagement")
    print(kpi.loyalty_and_engagement())

    print("\nPurchase Behaviour")
    print(kpi.purchase_behaviour())

    print("\nSales and Profit")
    print(kpi.sales_profit())

    print("\nProduct KPIs")
    print(compute_product_kpis(products, orders, transactions))



