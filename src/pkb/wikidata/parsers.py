from __future__ import annotations

from typing import Any


def label(entity: dict[str, Any]) -> str | None:
    return entity.get("labels", {}).get("en", {}).get("value")


def aliases(entity: dict[str, Any]) -> list[str]:
    return [
        alias["value"]
        for alias in entity.get("aliases", {}).get("en", [])
    ]


def property_values(entity: dict[str, Any], pid: str) -> list[Any]:
    values = []

    for claim in entity.get("claims", {}).get(pid, []):
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue")

        if not datavalue:
            continue

        value = datavalue.get("value")

        if isinstance(value, dict) and value.get("entity-type") == "item":
            values.append(f"Q{value['numeric-id']}")
        elif isinstance(value, dict) and "time" in value:
            values.append(value["time"])
        else:
            values.append(value)

    return values


def property_value(entity: dict[str, Any], pid: str) -> Any | None:
    values = property_values(entity, pid)
    return values[0] if values else None


def parse_collector_entity(
    qid: str,
    entity: dict[str, Any],
    labels_by_qid: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels_by_qid = labels_by_qid or {}

    employer_ids = property_values(entity, "P108")
    citizenship_ids = property_values(entity, "P27")
    gender_id = property_value(entity, "P21")

    return {
        "wikidataID": qid,
        "name": label(entity),
        "aliases": aliases(entity),
        "birthName": property_value(entity, "P1477"),
        "dateOfBirth": property_value(entity, "P569"),
        "dateOfDeath": property_value(entity, "P570"),
        "viafID": property_value(entity, "P214"),
        "authorAbbreviation": property_value(entity, "P428"),
        "harvardIndexID": property_value(entity, "P6264"),
        "bionomiaID": property_value(entity, "P6944"),
        "ipniID": property_value(entity, "P586"),
        "isniID": property_value(entity, "P213"),
        "employerWikidataIDs": employer_ids,
        "employers": [
            labels_by_qid.get(qid)
            for qid in employer_ids
            if labels_by_qid.get(qid)
        ],
        "countryWikidataIDs": citizenship_ids,
        "countriesOfCitizenship": [
            labels_by_qid.get(qid)
            for qid in citizenship_ids
            if labels_by_qid.get(qid)
        ],
        "sexOrGenderWikidataID": gender_id,
        "sexOrGender": labels_by_qid.get(gender_id) if gender_id else None,
    }


def parse_institution_entity(qid: str, entity: dict[str, Any]) -> dict[str, Any]:
    return {
        "wikidataID": qid,
        "name": label(entity),
        "aliases": aliases(entity),
        "indexHerbariumCode": property_value(entity, "P5858"),
        "harvardIndexID": property_value(entity, "P6264"),
        "viafID": property_value(entity, "P214"),
        "isniID": property_value(entity, "P213"),
        "grSciCollID": property_values(entity, "P4090"),
        "rorID": property_values(entity, "P6782"),
        "countryWikidataID": property_value(entity, "P17"),
        "locationWikidataID": property_value(entity, "P276"),
    }