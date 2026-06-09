import numpy as np
import pandas as pd
import re
import country_converter as coco

def country_string_to_iso(country: str):

    print("CONVERT")
    return coco.convert(country, to='ISO2')


r = re.compile("^[a-zA-Z\s]*$")

# Text is alpha or space
def is_alpha(s):
    return bool(r.match(s))