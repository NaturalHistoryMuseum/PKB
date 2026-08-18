from __future__ import annotations

from pkb.models.people.names import (
    parse_person_name,
    parse_person_names,
    person_names_compatible,
    person_names_match_by_given_names,
    person_names_match_by_initials,
)


def test_parse_person_name_handles_surname_first_huh_full_name():
    name = parse_person_name("Sartorius, Carl (Carlos) *Christian Wilhelm")

    assert name is not None
    assert name.surname == "sartorius"
    assert name.given_names == ("carl", "christian", "wilhelm")
    assert name.initials == "ccw"
    assert name.initial_tokens == ()
    assert name.full_name == "carl christian wilhelm sartorius"
    assert name.alternate_names == ("carlos",)


def test_parse_person_name_handles_initialled_standard_name():
    name = parse_person_name("C. C. W. Sartorius")

    assert name is not None
    assert name.surname == "sartorius"
    assert name.given_names == ()
    assert name.initials == "ccw"
    assert name.initial_tokens == ("c", "c", "w")
    assert name.full_name == "sartorius"


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
    assert a is not None
    assert "carlos" in a.alternate_names


def test_parse_person_names_orders_multiple_names_by_expansion():
    names = parse_person_names(
        "[S. R. Clemente y Rubio, S. de Rojas Clemente y Rubio, "
        "S. de Roxas Clemente y Rubio]"
    )

    assert len(names) == 3
    assert names[0].surname == "clemente y rubio"
    assert names[0].given_names == ("rojas",)
    assert names[0].initials == "s"
    assert names[0].initial_tokens == ("s",)
    assert names[0].full_name == "rojas clemente y rubio"
    assert "s r clemente y rubio" in names[0].alternate_canonical_names


def test_parse_person_name_chooses_most_expansive_name_variant():
    name = parse_person_name(
        "[S. R. Clemente y Rubio, S. de Rojas Clemente y Rubio, "
        "S. de Roxas Clemente y Rubio]"
    )

    assert name is not None
    assert name.given_names == ("rojas",)
    assert name.surname == "clemente y rubio"
