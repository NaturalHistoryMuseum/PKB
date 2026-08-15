import re
import unicodedata

_ALPHA_SPACE_RE = re.compile(r"^[a-zA-Z\s]*$")


def normalize_unicode(text: str, form: str = "NFKD") -> str:
    return unicodedata.normalize(form, text)


def is_alpha_space(text: str) -> bool:
    return bool(_ALPHA_SPACE_RE.match(text))