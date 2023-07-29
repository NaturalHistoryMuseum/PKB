import numpy as np
import pandas as pd
import re
import country_converter as coco

def country_string_to_iso(country: str):
    return coco.convert(country, to='ISO2')


r = re.compile("^[a-zA-Z\s]*$")

# Text is alpha or space
def is_alpha(s):
    return bool(r.match(s))

    
# def dict_to_parquet(data: dict, path):
#     df = pd.DataFrame(data)
#     df = df.replace(np.nan, None)   
#     df.astype(str).to_parquet(path, index=False)  
#     return df

# def to_parquet(df: pd.DataFrame, path, index=True):
#     df = df.replace(np.nan, None) 
#     df.astype(str).to_parquet(path, index=index)  
#     return df
    