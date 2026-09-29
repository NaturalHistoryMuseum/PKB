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
