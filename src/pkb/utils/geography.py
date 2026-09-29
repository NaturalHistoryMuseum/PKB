import re
from functools import lru_cache

import country_converter as coco


_CONVERTER = coco.CountryConverter()
_COUNTRIES = _CONVERTER.data


@lru_cache
def country_string_to_iso(country: str, to: str = "ISO2") -> str | None:
    value = country.strip().casefold()

    for column in ("ISO2", "ISO3", "name_short", "name_official"):
        if column == "ISO2":
            match = _COUNTRIES[column].astype(str).apply(
                lambda pattern: bool(re.fullmatch(pattern, country, re.IGNORECASE))
            )
        else:
            match = _COUNTRIES[column].astype(str).str.casefold().eq(value)
        if match.any():
            iso3 = _COUNTRIES.loc[match, "ISO3"].iloc[0]
            return _CONVERTER.convert(iso3, to=to)

    return None
