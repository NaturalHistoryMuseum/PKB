from __future__ import annotations

from pkb.models.people.names import (
    parse_person_name,
    person_names_compatible,
    person_names_match_by_given_names,
    person_names_match_by_initials,
)


def test_parse_person_name_handles_surname_first_huh_full_name():
    name = parse_person_name("Sartorius, Carl (Carlos) *Christian Wilhelm")

    assert name is not None
    assert name.verbatim == "Sartorius, Carl (Carlos) *Christian Wilhelm"
    assert name.surname == "sartorius"
    assert name.given_names == ("carl", "christian", "wilhelm")
    assert name.given_names_text == "carl christian wilhelm"
    assert name.initials == ("c", "c", "w")
    assert name.initials_text == "ccw"
    assert name.normalised == "carl christian wilhelm sartorius"
    assert name.full_name == "carl christian wilhelm sartorius"


def test_parse_person_name_handles_initialled_standard_name():
    name = parse_person_name("C. C. W. Sartorius")

    assert name is not None
    assert name.surname == "sartorius"
    assert name.given_names == ("c", "c", "w")
    assert name.given_names_text == "c c w"
    assert name.initials == ("c", "c", "w")
    assert name.initials_text == "ccw"
    assert name.full_name == "c c w sartorius"


def test_person_names_match_by_given_names_when_full_names_align():
    a = parse_person_name("Sartorius, Carl Christian Wilhelm")
    b = parse_person_name("Carl Christian Wilhelm Sartorius")

    assert person_names_match_by_given_names(a, b)
    assert person_names_match_by_initials(a, b)
    assert person_names_compatible(a, b)


def test_person_names_compatible_when_initials_match_full_given_names():
    a = parse_person_name("C. C. W. Sartorius")
    b = parse_person_name("Sartorius, Carl Christian Wilhelm")

    assert not person_names_match_by_given_names(a, b)
    assert person_names_match_by_initials(a, b)
    assert person_names_compatible(a, b)


def test_person_names_compatible_with_parenthetical_extra_given_name():
    a = parse_person_name("Sartorius, Carl (Carlos) *Christian Wilhelm")
    b = parse_person_name("Sartorius, Carl Christian Wilhelm")

    assert person_names_match_by_given_names(a, b)
    assert person_names_match_by_initials(a, b)
    assert person_names_compatible(a, b)
