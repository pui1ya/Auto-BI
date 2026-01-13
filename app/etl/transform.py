'''
transform cleaned data to tables:
* product table
* customer table
* order table
*transaction table
'''
import pandas as pd
import numpy as np

customer = pd.DataFrame()

customer_col = {"customer_id" : ["customer", "customer_id", "cust_id", "cust", "id", "client", "client_id", "customer id", "customerid"],
                "name" : ["name", "customer_name", "client_name", "customername", "clientname", "cust_name", "cli_name", "custname", "cliname"],
                "country": ["country", "shipped", "shippedto", "shipped_to_country", "cutomer_country", "cust_country", "client_country"],
                "state" : ["state", "customer_state", "customerstate", "client_state", "clientstate"],
                "city": ["city"],
                "postal_code" : ["postal_code", "postalcode", "zip", "zip_code"],
                "region" : ["region", "zone"]}

def get_columns(df, map):

    new_col = {}

    for key, value in map.items():
        for col in df.columns:
            if col.lower() in value:
                new_col[key] = col
                break
    
    return new_col


def build_customers_table(df):

    new_col = get_columns(df, customer_col)

    if "customer_id" not in new_col:
        raise ValueError("Customer Id not found in df")
    
    df_customer_col = list(new_col.values())

    customer = df[df_customer_col]
    customer.columns = list(new_col.keys())

    customer = customer.drop_duplicates()
    customer = customer.reset_index(drop = True)

    return customer


def build_products_table(df):
    pass

def build_orders_table(df):
    pass

def build_transactions_table(df):
    pass

if __name__ == "__main__":
    from clean import clean_raw_data
    from ingest import ingest_csv

    df = ingest_csv("/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv")
    df = clean_raw_data(df)

    customers = build_customers_table(df)
    print(customers.head())
    print(customers.shape)
