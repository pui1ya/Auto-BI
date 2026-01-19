#to identify which customers are about to churn 
#no need for a different customer churn analysis project 🗿

import pandas as pd

class ChurnAnalyzer:

    def __init__(self, customer_kpis, loyalty_df, rfm_df):
        self.df = (
            customer_kpis
            .merge(loyalty_df, on="customer_id", how="inner")
            .merge(rfm_df, on="customer_id", how="inner")
        )

    def compute_churn_flags(self):
        self.df["churn_risk"] = "Low"

        self.df.loc[
            self.df["recency"] > 90,
            "churn_risk"
        ] = "High"

        self.df.loc[
            (self.df["recency"] > 60) &
            (self.df["total_profit"] < self.df["total_profit"].quantile(0.25)),
            "churn_risk"
        ] = "High"

        self.df.loc[
            self.df["total_discount"] > 0.3,
            "churn_risk"
        ] = "Medium"

        return self.df[[
            "customer_id",
            "segment",
            "recency",
            "total_profit",
            "total_discount",
            "churn_risk"
        ]]

    def churn_summary(self):
        return (
            self.df
            .groupby("churn_risk")
            .size()
            .reset_index(name="customers")
        )


if __name__ == "__main__":
    from app.etl.transform import table_builder
    from app.etl.clean import clean_raw_data
    from app.etl.ingest import ingest_csv
    from app.analytics.kpi import ComputeCustomerKpis
    from app.analytics.segmentation import CustomerSegmentation

    df = '/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv'

    ingest = ingest_csv(df)
    clean = clean_raw_data(ingest)
    builder = table_builder(clean)
    customers = builder.build_customers_table()
    transactions = builder.build_transactions_table()
    orders = builder.build_orders_table()

    kpis = ComputeCustomerKpis(
        customers=customers,
        transactions=transactions,
        orders=orders
    )

    sales_profit_df = kpis.sales_profit()
    loyalty_df = kpis.loyalty_and_engagement()

    print(sales_profit_df.columns)

    segmentation = CustomerSegmentation(
        customer_kpis=sales_profit_df,
        loyalty_df=loyalty_df
    )

    rfm_df = segmentation.rfm_segmentation()

    churn = ChurnAnalyzer(customer_kpis=sales_profit_df, loyalty_df=loyalty_df, rfm_df=rfm_df)
    print(churn.compute_churn_flags())
