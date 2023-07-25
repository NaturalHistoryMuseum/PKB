import numpy as np
import pandas as pd

def country_string_to_iso(country: str):
    return country
    
# def dict_to_parquet(data: dict, path):
#     df = pd.DataFrame(data)
#     df = df.replace(np.nan, None)   
#     df.astype(str).to_parquet(path, index=False)  
#     return df

# def to_parquet(df: pd.DataFrame, path, index=True):
#     df = df.replace(np.nan, None) 
#     df.astype(str).to_parquet(path, index=index)  
#     return df
    