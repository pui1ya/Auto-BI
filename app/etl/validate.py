'''
The aim of this file is for value checking and performs the following functions:
* raises error if dataset is empty
* if a column has too many missing values then again raise error
* if too many duplicates, raise an error
* process certain values that has negative values which are not valid
* to see atleast either of customer, order, product and transaction table are absent
'''

import pandas as pd
from transform import get_columns

def validate_raw_data(df):

    #to see if the df is empty
    if df.empty:
        raise ValueError("dataset is empty")
    

    #checking for too many missing values in a column

    for col in df.columns:
        missing = df[col].isna().sum()
        rows = len(df)
                
        missing_percent = (missing/rows)*100
        if missing_percent > 40:
            raise ValueError(f"{col} has {missing} missing rows")
        
    
    #for checking duplicate rows

    duplicates = df.duplicated().sum()
    if duplicates > 0:
        raise ValueError(f"dataset has {duplicates} rows")


    #for checking columns with negative values

    for col in df.columns:
        invalid_row = 0
        if pd.api.types.is_numeric_dtype(df[col]):
            if col == "profit":
                continue
            for val in df[col]:
                if val < 0:
                    invalid_row += 1

        if invalid_row > 0:
            raise ValueError(f"{col} has {invalid_row} rows with negative values")
        

    #for checking if df has id of any sort (order id, customer id, transaction id, product id)

    df_cols = (df.columns
               .str.lower()
               .str.strip()
               .str.replace(" ", "")
               .str.replace("_", "")
               .str.replace("-", ""))
    
    id_cols = {
        "customer_id" : ["customer", "customerid", "custid", "cust", "id", "client", "clientid", "customerid", "customerid"],
        "product_id" : ["proid", "item", "itemid", "itemno", "productno", "productid"],
        "order_id": ["orderid", "ordernumber", "ordernum", "orderno"],
        "transaction_id" : ["transactionid", "transid"]
    }

    mapped_cols = get_columns(df, id_cols)

    if len(mapped_cols) == 0:
        raise ValueError(f"{id_cols.keys()} are not present in df")
    
    return True
    


if __name__ == "__main__":
    df = pd.read_csv('/Users/punyashrees/Documents/projects/auto-bi/Computed insight - Success of active sellers.csv')
    validate_raw_data(df)

    

        
        


        