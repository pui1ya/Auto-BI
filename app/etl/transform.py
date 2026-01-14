'''
transform cleaned data to tables:
* product table
* customer table
* order table
*transaction table
'''
import pandas as pd
import numpy as np

customers = pd.DataFrame()
products = pd.DataFrame()
orders = pd.DataFrame()
transactions = pd.DataFrame()

customer_col = {"customer_id" : ["customer", "customerid", "custid", "cust", "id", "client", "clientid", "customerid", "customerid"],
                "name" : ["name", "customer_name", "client_name", "customername", "clientname", "cust_name", "cli_name", "custname", "cliname"],
                "country": ["country", "shipped", "shippedto", "shipped_to_country", "cutomer_country", "cust_country", "client_country"],
                "state" : ["state", "customer_state", "customerstate", "client_state", "clientstate"],
                "city": ["city"],
                "postal_code" : ["postal_code", "postalcode", "zip", "zip_code"],
                "region" : ["region", "zone"],
                "customer_type" : ["customertype", "segment", "typeofcustomer"]
                }

products_col = {"product_id" : ["proid", "item", "itemid", "itemno", "productno", "productid"],
                "product_name" : ["productname", "itemname", "orderitem"],
                "category" : ["category", "productcategory", "itemcategory", "ordercategory"],
                "sub_category" : ["subcategory", "productsubcategory", "itemsubcategory", "ordersubcategory"]
                }

orders_col = {"order_id": ["orderid", "ordernumber", "ordernum", "orderno"],
              "order_date": ["dateoforder", "orderdate", "orderon", "orderedon"],
              "ship_date": ["shipdate", "shippeddate", "dateofshipped", "shippedon", "shipon"],
              "ship_mode": ["shipmode", "shippingmode", 'typeofshipment', "shipmenttype", "modeofshipping", "shippingmode"]
              }

transactions_col = {"order_id": ["orderid", "ordernumber", "ordernum", "orderno"],
                    "product_id" : ["proid", "item", "itemid", "itemno", "productno", "productid"],
                    "customer_id" : ["customer", "customerid", "custid", "cust", "id", "client", "clientid", "customerid", "customerid"],
                    "sales" : ["sales", "sold", "sell", "itemssold", "solditems", "soldorders", "soldorder"],
                    "quantity" : ["quantity", "numberofitemssold", "numberofitems", "noofitem", "nonoofitem"],
                    "discount" : ["discount", "off", "offer"],
                    "profit" : ["profit", "gainedamount", "gain"],
                    "transaction_id" : ["transactionid", "transid"]
                    }

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

    customers = df[df_customer_col].copy()
    customers.columns = list(new_col.keys())

    customers = customers.drop_duplicates()
    customers = customers.reset_index(drop = True)

    return customers


def build_products_table(df):
    new_col = get_columns(df, products_col)

    if "product_id" not in new_col.keys():
        raise ValueError("product_id not found in df")

    df_products_col = list(new_col.values())
    products = df[df_products_col].copy()
    products.columns = list(new_col.keys())

    products = products.drop_duplicates()
    products = products.reset_index(drop = True)

    return products


def build_orders_table(df):
    new_columns = get_columns(df, orders_col)

    if "order_id" not in new_columns.keys():
        raise ValueError("order_id not found in df")

    df_order_col = list(new_columns.values())

    orders = df[df_order_col].copy()
    orders.columns = list(new_columns.keys())

    orders = orders.drop_duplicates()
    orders = orders.reset_index(drop = True)

    return orders


def build_transactions_table(df):
    new_cols = get_columns(df, transactions_col)

    required = ["order_id", "product_id", "customer_id"]
    missing = [r for r in required if r not in new_cols]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    
    df_transaction_cols = list(new_cols.values())

    transactions = df[df_transaction_cols].copy()
    transactions.columns = list(new_cols.keys())

    transactions = transactions.drop_duplicates()
    transactions = transactions.reset_index(drop = True)

    return transactions
    

if __name__ == "__main__":
    from clean import clean_raw_data
    from ingest import ingest_csv

    df = ingest_csv("/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv")
    df = clean_raw_data(df)

    customers = build_customers_table(df)
    print(customers.head())
    print(customers.shape)

    products = build_products_table(df)
    print(products.head())
    print(products.shape)

    orders = build_orders_table(df)
    print(orders.head())
    print(orders.shape)

    transactions = build_transactions_table(df)
    print(transactions.head())
    print(transactions.shape)
