'''
* read dataset from disk
* verify it exists
* load it to pd
* return the df
'''
import pandas as pd
from pathlib import Path

def ingest_csv(file_path):

    path = Path(file_path)
    if not path.exists():
        return FileNotFoundError(f"{file_path} not found")
    
    df = pd.read_csv(file_path)
    if df.empty:
        return ValueError("dataset is empty")
    else:
        return df
    
if __name__ == "__main__":
    df = ingest_csv('/Users/punyashrees/Documents/projects/auto-bi/train.csv')
    print(df.head())