from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

from nameparser import HumanName


@dataclass(frozen=True)
class ParsedPersonName:
    verbatim: str
    canonical: str
    surname: str | None
    given_names: tuple[str, ...]
    initials: tuple[str, ...]
    normalised: str

    @property
    def full_name(self) -> str | None:
        parts = [*self.given_names, self.surname]
        return " ".join(part for part in parts if part) or None

    @property
    def given_names_text(self) -> str:
        return " ".join(self.given_names)

    @property
    def initials_text(self) -> str:
        return "".join(self.initials)

    @property
    def tokens(self) -> tuple[str, ...]:
        return (*self.given_names, *([self.surname] if self.surname else []))


def parse_person_name(value) -> ParsedPersonName | None:
    value = _clean_scalar(value)
    if not value:
        return None
    return _parse_person_name(value)


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
    if not a.initials or not b.initials:
        return False

    a_initials = a.initials_text
    b_initials = b.initials_text
    return (
        a_initials == b_initials
        or a_initials.startswith(b_initials)
        or b_initials.startswith(a_initials)
    )


def strip_accents(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    return "".join(char for char in text if not unicodedata.combining(char))


def normalise_person_name_text(text: str) -> str:
    text = strip_accents(text).lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _parse_person_name(value: str) -> ParsedPersonName | None:
    verbatim = value
    value = _clean_name_for_parsing(value)
    if not value:
        return None

    name = HumanName(value)
    if " " not in value:
        given_names: tuple[str, ...] = ()
        surname = _normalise_name_part(value)
    else:
        given_names = tuple(
            part
            for part in (
                _normalise_name_part(name.first),
                *(_normalise_name_part(part) for part in name.middle.split()),
            )
            if part
        )
        surname = _normalise_name_part(name.last)

    initials = tuple(given_name[0] for given_name in given_names if given_name)
    canonical = " ".join(part for part in (*given_names, surname) if part)
    normalised = normalise_person_name_text(canonical)

    return ParsedPersonName(
        verbatim=verbatim,
        canonical=canonical,
        surname=surname,
        given_names=given_names,
        initials=initials,
        normalised=normalised,
    )


def _clean_scalar(value) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    if not value or value.lower() in {"nan", "[none]", "<na>", "none"}:
        return None
    return value


def _clean_name_for_parsing(value: str) -> str:
    value = strip_accents(value).replace("*", " ")
    value = re.sub(r"\(([^)]+)\)", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _normalise_name_part(value: str | None) -> str | None:
    if not value:
        return None
    value = normalise_person_name_text(value)
    return value or None
