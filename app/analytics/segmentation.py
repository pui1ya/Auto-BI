#for segmenting customers into champions based on their rfm factor
#i did'nt know what rfm is but chatgpt told me to measure customers based on that
#recency, frequeny and monetary

import pandas as pd

class CustomerSegmentation:

    def __init__(self, customer_kpis, loyalty_df):
        self.df = customer_kpis.merge(
            loyalty_df, on="customer_id", how="inner"
        )

    def compute_rfm(self):
        today = self.df["last_purchase_order"].max()

        rfm = self.df[[
            "customer_id",
            "last_purchase_order",
            "total_orders",
            "total_sales"
        ]].copy()  

        rfm["recency"] = (today - rfm["last_purchase_order"]).dt.days
        rfm["frequency"] = rfm["total_orders"]
        rfm["monetary"] = rfm["total_sales"]

        return rfm[["customer_id", "recency", "frequency", "monetary"]]
    
    def rfm_segmentation(self):
        rfm = self.compute_rfm()

        rfm["segment"] = "Regular"

        rfm.loc[
            (rfm["recency"] <= 30) &
            (rfm["frequency"] >= 5) &
            (rfm["monetary"] >= rfm["monetary"].quantile(0.75)),
            "segment"
        ] = "Champions"

        rfm.loc[
            (rfm["recency"] > 90) &
            (rfm["frequency"] <= 2),
            "segment"
        ] = "At Risk"

        rfm.loc[
            rfm["frequency"] == 1,
            "segment"
        ] = "New Customers"

        return rfm

if __name__ == "__main__":
    from app.etl.ingest import ingest_csv
    from app.etl.clean import clean_raw_data
    from app.etl.transform import table_builder
    from app.analytics.kpi import ComputeCustomerKpis
    from app.analytics.segmentation import CustomerSegmentation

    df = "/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv"

    raw_df = ingest_csv(df)
    clean_df = clean_raw_data(raw_df)
    builder = table_builder(clean_df)

    customers = builder.build_customers_table()
    orders = builder.build_orders_table()
    transactions = builder.build_transactions_table()

    customer_kpis = ComputeCustomerKpis(
        customers=customers,
        transactions=transactions,
        orders=orders
    )

    sales_profit_df = customer_kpis.sales_profit()
    loyalty_df = customer_kpis.loyalty_and_engagement()

    segmentation = CustomerSegmentation(
        customer_kpis=sales_profit_df,
        loyalty_df=loyalty_df
    )

    rfm_df = segmentation.rfm_segmentation()

    print("\nRFM Segmentation Sample:\n")
    print(rfm_df.head())
    print(rfm_df.columns)
