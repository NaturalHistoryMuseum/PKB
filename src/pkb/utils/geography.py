import country_converter as coco


def country_string_to_iso(country: str, to: str = "ISO2") -> str:
    return coco.convert(country, to=to)