'''
transform cleaned data to tables:
* product table
* customer table
* order table
*transaction table
'''
import pandas as pd
import numpy as np
from app.etl.validate import validate_raw_data
from app.etl.required import get_columns

#splitting the df into sub df

#relevant new df
customers = pd.DataFrame()
products = pd.DataFrame()
orders = pd.DataFrame()
transactions = pd.DataFrame()

#customer columns
customer_col = {"customer_id" : ["customer_id", "customer", "customerid", "custid", "cust", "id", "client", "clientid", "customerid", "customerid"],
                "name" : ["name", "customer_name", "client_name", "customername", "clientname", "cust_name", "cli_name", "custname", "cliname"],
                "country": ["country", "shipped", "shippedto", "shipped_to_country", "cutomer_country", "cust_country", "client_country"],
                "state" : ["state", "customer_state", "customerstate", "client_state", "clientstate"],
                "city": ["city"],
                "postal_code" : ["postal_code", "postalcode", "zip", "zip_code"],
                "region" : ["region", "zone"],
                "customer_type" : ["customertype", "segment", "typeofcustomer"]
                }

#product columns
products_col = {"product_id" : ["product_id", "proid", "item", "itemid", "itemno", "productno", "productid"],
                "product_name" : ["productname", "itemname", "orderitem"],
                "category" : ["category", "productcategory", "itemcategory", "ordercategory"],
                "sub_category" : ["subcategory", "productsubcategory", "itemsubcategory", "ordersubcategory"]
                }

#order columns
orders_col = {"order_id": ["orderid", "ordernumber", "ordernum", "orderno"],
              "order_date": ["dateoforder", "orderdate", "orderon", "orderedon"],
              "ship_date": ["shipdate", "shippeddate", "dateofshipped", "shippedon", "shipon"],
              "ship_mode": ["shipmode", "shippingmode", 'typeofshipment', "shipmenttype", "modeofshipping", "shippingmode"]
              }

#transaction columns
transactions_col = {"order_id": ["orderid", "ordernumber", "ordernum", "orderno"],
                    "product_id" : ["proid", "item", "itemid", "itemno", "productno", "productid"],
                    "customer_id" : ["customer", "customerid", "custid", "cust", "id", "client", "clientid", "customerid", "customerid"],
                    "sales" : ["sales", "sold", "sell", "itemssold", "solditems", "soldorders", "soldorder"],
                    "quantity" : ["quantity", "numberofitemssold", "numberofitems", "noofitem", "nonoofitem"],
                    "discount" : ["discount", "off", "offer"],
                    "profit" : ["profit", "gainedamount", "gain"],
                    "transaction_id" : ["transactionid", "transid"]
                    }

#to fetch the relevant columns from df

class table_builder:

    def __init__(self, df):
        self.df = df

    #building customers table
    def build_customers_table(self):

        new_col = get_columns(self.df, customer_col)

        if "customer_id" not in new_col:
            raise ValueError("Customer Id not found in df")
    
        df_customer_col = list(new_col.values())

        customers = self.df[df_customer_col].copy()
        customers.columns = list(new_col.keys())

        customers = customers.drop_duplicates()
        customers = customers.reset_index(drop = True)

        print(customers.head())
        print(customers.shape)

        return customers


#building products table
    def build_products_table(self):
        new_col = get_columns(self.df, products_col)

        if "product_id" not in new_col.keys():
            raise ValueError("product_id not found in df")

        df_products_col = list(new_col.values())
        products = self.df[df_products_col].copy()
        products.columns = list(new_col.keys())

        products = products.drop_duplicates()
        products = products.reset_index(drop = True)

        print(products.head())
        print(products.shape)

        return products


#building orders table
    def build_orders_table(self):
        new_columns = get_columns(self.df, orders_col)

        print(self.df.columns)

        if "order_id" not in new_columns.keys():
            raise ValueError("order_id not found in df")

        df_order_col = list(new_columns.values())

        orders = self.df[df_order_col].copy()
        orders.columns = list(new_columns.keys())

        orders['order_date'] = pd.to_datetime(orders['order_date'])
        orders['ship_date'] = pd.to_datetime(orders['ship_date'])

        orders = orders.drop_duplicates()
        orders = orders.reset_index(drop = True)

        print(orders.head())
        print(orders.shape)

        return orders


#building transactions table
    def build_transactions_table(self):
        new_cols = get_columns(self.df, transactions_col)

        required = ["order_id", "product_id", "customer_id"]
        missing = [r for r in required if r not in new_cols]

        if missing:
            raise ValueError(f"Missing required columns: {missing}")

    
        df_transaction_cols = list(new_cols.values())

        transactions = self.df[df_transaction_cols].copy()
        transactions.columns = list(new_cols.keys())

        transactions = transactions.drop_duplicates()
        transactions = transactions.reset_index(drop = True)

        print(transactions.head())
        print(transactions.shape)

        return transactions
    

# #main function for testing
# def building_tables(df):
#     from app.etl.clean import clean_raw_data
#     from app.etl.ingest import ingest_csv

#     df = ingest_csv("/Users/punyashrees/Documents/projects/auto-bi/Sample - Superstore.csv")
#     df = clean_raw_data(df)

#     builder = table_builder(df)

#     customers = builder.build_customers_table(df)
#     print(customers.head())
#     print(customers.shape)

#     products = builder.build_products_table(df)
#     print(products.head())
#     print(products.shape)

#     orders = builder.build_orders_table(df)
#     print(orders.head())
#     print(orders.shape)

#     transactions = builder.build_transactions_table(df)
#     print(transactions.head())
#     print(transactions.shape)

    
