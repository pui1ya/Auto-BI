'''
to clean the dataset, i.e:
* remove duplicates
* fill nan
* remove unnecessary stuff
* fix data types
'''

import pandas as pd
import numpy as np

def clean_raw_data(df):

    df = df.copy()

    df.columns = (
        df.columns.str.strip().str.lower().str.replace(' ', '', regex=False)
    )

    columns = list(df.columns)

    #to fill nan
    for col in columns:
        if col not in df.columns:
            continue

        if df[col].isna().sum() > 0:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna("Unknown")
            else:
                df[col] = df[col].fillna(df[col].median())

    #to remove duplicates
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    print(f"Removed {before - after} duplicate rows")


    #to change datatype
    for col in columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except:
            pass


    return df

if __name__ == "__main__":
    from ingest import ingest_csv
    df = ingest_csv("/Users/punyashrees/Documents/projects/auto-bi/Computed insight - Success of active sellers.csv")
    clean_df = clean_raw_data(df)
    print(clean_df.info())
