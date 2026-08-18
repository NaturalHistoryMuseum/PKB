from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import pandas as pd


TITLE_WORDS = {
    "dr",
    "doctor",
    "prof",
    "professor",
    "sir",
    "dame",
    "mr",
    "mrs",
    "ms",
    "miss",
}

SUFFIX_WORDS = {
    "jr",
    "sr",
    "ii",
    "iii",
    "iv",
    "phd",
    "md",
    "fba",
    "frs",
}


@dataclass(frozen=True)
class ParsedPersonName:
    original: str
    canonical: str
    surname: str | None
    given_names: tuple[str, ...]
    initials: str
    initial_tokens: tuple[str, ...]
    alternate_names: tuple[str, ...] = ()

    @property
    def full_name(self) -> str | None:
        parts = [*self.given_names, self.surname]
        return " ".join(part for part in parts if part) or None

    @property
    def tokens(self) -> tuple[str, ...]:
        return (*self.given_names, *([self.surname] if self.surname else []))


def parse_person_name(value) -> ParsedPersonName | None:
    value = _clean_scalar(value)
    if not value:
        return None

    original = value
    value = strip_accents(value).replace("*", " ")
    alternate_names = tuple(
        _normalise_token(name)
        for name in re.findall(r"\(([^)]+)\)", value)
        if _normalise_token(name)
    )
    value = re.sub(r"\(([^)]+)\)", " ", value)

    if "," in value:
        surname_part, given_part = [part.strip() for part in value.split(",", 1)]
        surname = _clean_name_part(surname_part)
        given_names = _name_tokens(given_part)
    else:
        tokens = _name_tokens(value)
        surname = tokens[-1] if tokens else None
        given_names = tokens[:-1]

    surname = _normalise_token(surname) if surname else None
    given_tokens = tuple(
        token
        for token in (_normalise_token(token) for token in given_names)
        if token and token not in TITLE_WORDS | SUFFIX_WORDS
    )
    initial_tokens = tuple(token for token in given_tokens if len(token) == 1)
    given_names = tuple(token for token in given_tokens if len(token) > 1)

    tokens = (*given_tokens, *([surname] if surname else []))
    canonical = " ".join(tokens)
    initials = "".join(initial_tokens) or "".join(_initial(token) for token in given_names)

    return ParsedPersonName(
        original=original,
        canonical=canonical,
        surname=surname,
        given_names=given_names,
        initials=initials,
        initial_tokens=initial_tokens,
        alternate_names=alternate_names,
    )


def person_names_match_by_given_names(
    a: ParsedPersonName | None,
    b: ParsedPersonName | None,
) -> bool:
    if not a or not b or not a.surname or not b.surname:
        return False
    return bool(a.given_names and b.given_names) and (
        a.surname == b.surname
        and a.given_names == b.given_names
    )


def person_names_match_by_initials(
    a: ParsedPersonName | None,
    b: ParsedPersonName | None,
) -> bool:
    if not a or not b or not a.surname or not b.surname:
        return False
    return bool(a.initials and b.initials) and (
        a.surname == b.surname
        and a.initials == b.initials
    )


def person_names_compatible(
    a: ParsedPersonName | None,
    b: ParsedPersonName | None,
) -> bool:
    if not a or not b or not a.surname or not b.surname:
        return False
    if a.surname != b.surname:
        return False

    tokens_a = _comparison_given_tokens(a)
    tokens_b = _comparison_given_tokens(b)
    shorter, longer = sorted([tokens_a, tokens_b], key=len)
    if not shorter or not longer:
        return False

    for index, token in enumerate(shorter):
        other = longer[index]
        if len(token) == 1:
            if token != _initial(other):
                return False
        elif len(other) == 1:
            if _initial(token) != other:
                return False
        elif token != other:
            return False

    return True


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(char for char in text if not unicodedata.combining(char))


def _clean_scalar(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    value = str(value).strip()
    if not value or value.lower() in {"nan", "[none]"}:
        return None
    return value


def _clean_name_part(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9\s.]", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _name_tokens(value: str) -> tuple[str, ...]:
    value = _clean_name_part(value)
    return tuple(
        token
        for token in value.split()
        if token.lower().strip(".") not in TITLE_WORDS | SUFFIX_WORDS
    )


def _normalise_token(value: str | None) -> str | None:
    if not value:
        return None
    value = value.lower().strip().strip(".")
    value = re.sub(r"[^a-z0-9]", "", value)
    return value or None


def _initial(value: str) -> str:
    return value[:1]


def _comparison_given_tokens(name: ParsedPersonName) -> tuple[str, ...]:
    return name.given_names or name.initial_tokens
