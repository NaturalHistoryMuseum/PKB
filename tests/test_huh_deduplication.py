from __future__ import annotations

import pandas as pd

from pkb.models.people.huh_deduplication import HUHPersonDeduplicator


def test_huh_deduplicator_clusters_matching_surname_initials_and_support():
    people = pd.DataFrame(
        [
            {
                "pkbPersonKey": "harvard_index:5",
                "source": "harvard_index",
                "harvardIndexID": "5",
                "name": "A. R. Brach",
                "fullName": "Brach, Anthony Robert",
                "standardName": "A. R. Brach",
                "birthYear": "1963",
                "deathYear": None,
                "dateOfBirth": "1963-01-01",
                "dateOfDeath": None,
                "geographyISO": ["US"],
                "herbariaCode": ["SYRF"],
                "institutionUUID": ["uuid-1"],
            },
            {
                "pkbPersonKey": "harvard_index:6",
                "source": "harvard_index",
                "harvardIndexID": "6",
                "name": "A. R. Brach",
                "fullName": "Brach, Anthony Robert",
                "standardName": "A. R. Brach",
                "birthYear": "1963",
                "deathYear": None,
                "dateOfBirth": "1963-01-01",
                "dateOfDeath": None,
                "geographyISO": ["US"],
                "herbariaCode": ["SYRF"],
                "institutionUUID": ["uuid-2"],
            },
        ]
    )

    result = HUHPersonDeduplicator().deduplicate(people)

    assert result.clusters.iloc[0]["canonicalHarvardIndexID"] == "5"
    assert list(result.clusters.iloc[0]["harvardIndexIDs"]) == ["5", "6"]
    assert set(result.deduplication_map["harvardIndexID"].tolist()) == {"5", "6"}
    assert set(result.deduplication_map["canonicalHarvardIndexID"].tolist()) == {"5"}
    assert result.pair_features.iloc[0]["likelyDuplicate"]


def test_huh_deduplicator_does_not_cluster_conflicting_dates():
    people = pd.DataFrame(
        [
            {
                "pkbPersonKey": "harvard_index:17085",
                "source": "harvard_index",
                "harvardIndexID": "17085",
                "name": "John Smith",
                "birthYear": "1846",
                "deathYear": "1930",
                "dateOfBirth": None,
                "dateOfDeath": None,
                "geographyISO": ["US"],
                "herbariaCode": ["GL"],
                "institutionUUID": [],
            },
            {
                "pkbPersonKey": "harvard_index:29566",
                "source": "harvard_index",
                "harvardIndexID": "29566",
                "name": "J. Smith",
                "birthYear": "1798",
                "deathYear": "1888",
                "dateOfBirth": None,
                "dateOfDeath": None,
                "geographyISO": ["US"],
                "herbariaCode": ["GL"],
                "institutionUUID": [],
            },
        ]
    )

    result = HUHPersonDeduplicator().deduplicate(people)

    assert result.clusters.empty
    assert len(result.deduplication_map) == 2
    assert result.deduplication_map["harvardIndexID"].tolist() == ["17085", "29566"]
    assert not result.pair_features.iloc[0]["likelyDuplicate"]


def test_huh_deduplicator_blocks_on_full_initials():
    people = pd.DataFrame(
        [
            {
                "pkbPersonKey": "harvard_index:5",
                "source": "harvard_index",
                "harvardIndexID": "5",
                "name": "A. R. Brach",
                "birthYear": "1963",
                "deathYear": None,
                "dateOfBirth": None,
                "dateOfDeath": None,
                "geographyISO": ["US"],
                "herbariaCode": ["SYRF"],
                "institutionUUID": [],
            },
            {
                "pkbPersonKey": "harvard_index:6",
                "source": "harvard_index",
                "harvardIndexID": "6",
                "name": "A. Brach",
                "birthYear": "1963",
                "deathYear": None,
                "dateOfBirth": None,
                "dateOfDeath": None,
                "geographyISO": ["US"],
                "herbariaCode": ["SYRF"],
                "institutionUUID": [],
            },
        ]
    )

    result = HUHPersonDeduplicator().deduplicate(people)

    assert result.clusters.empty
    assert len(result.deduplication_map) == 2
    assert result.pair_features.empty
