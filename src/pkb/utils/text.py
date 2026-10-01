import ast
import re
import unicodedata

_ALPHA_SPACE_RE = re.compile(r"^[a-zA-Z\s]*$")


def normalize_unicode(text: str, form: str = "NFKD") -> str:
    return unicodedata.normalize(form, text)


def is_alpha_space(text: str) -> bool:
    return bool(_ALPHA_SPACE_RE.match(text))


def clean_scalar(value) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    if not value or value.lower() in {"nan", "nat", "none", "[none]", "<na>"}:
        return None
    return value


def clean_integer(value) -> int | None:
    value = clean_scalar(value)
    return int(value) if value else None


def parse_literal_collection(value) -> list[str]:
    value = clean_scalar(value)
    if not value:
        return []
    try:
        value = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return [value]
    if isinstance(value, (list, set, tuple)):
        return sorted(str(item) for item in value if clean_scalar(item))
    return [str(value)]
