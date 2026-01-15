'''
to keep all required functions get_columns was in tranform.py before then i encountered circular imports error
'''

import pandas as pd

def get_columns(df, map):

    new_col = {}

    for key, value in map.items():
        for col in df.columns:
            if col.lower() in value:
                new_col[key] = col
                break
    
    return new_col