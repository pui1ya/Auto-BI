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

    for col in columns:
        if col not in df.columns:
            continue

        if df[col].isna().sum() > 0:
            if df[col].dtype == 'object':
                df[col] = df[col].fillna(df[col].mode()[0])
            else:
                df[col] = df[col].fillna(df[col].median())

    return df

if __name__ == "__main__":
    from ingest import ingest_csv
    df = ingest_csv("/Users/punyashrees/Documents/projects/auto-bi/Computed insight - Success of active sellers.csv")
    clean_df = clean_raw_data(df)
    print(clean_df.info())
