from __future__ import annotations

import pandas as pd

from pkb.models.people import PersonAggregator


def test_person_aggregator_links_records_with_shared_identifiers():
    source_people = pd.DataFrame(
        [
            {
                "pkbPersonKey": "wikidata:Q1",
                "source": "wikidata",
                "wikidataID": "Q1",
                "harvardIndexID": "5",
                "name": "Jane Collector",
            },
            {
                "pkbPersonKey": "harvard_index:5",
                "source": "harvard_index",
                "harvardIndexID": "5",
                "name": "Jane A. Collector",
            },
            {
                "pkbPersonKey": "bionomia:0000-0002",
                "source": "bionomia",
                "wikidataID": "Q1",
                "bionomiaID": "0000-0002",
                "name": "J. Collector",
            },
            {
                "pkbPersonKey": "wikidata:Q2",
                "source": "wikidata",
                "wikidataID": "Q2",
                "name": "Other Person",
            },
        ]
    )

    result = PersonAggregator().aggregate(source_people)

    jane = result.people[result.people["sourceCount"] == 3].iloc[0].to_dict()
    assert jane["name"] == "Jane A. Collector"
    assert jane["sources"] == ["bionomia", "harvard_index", "wikidata"]
    assert jane["wikidataIDs"] == ["Q1"]
    assert jane["harvardIndexIDs"] == ["5"]
    assert jane["bionomiaIDs"] == ["0000-0002"]

    jane_links = result.source_links[
        result.source_links["pkbPersonKey"] == jane["pkbPersonKey"]
    ]
    assert sorted(jane_links["sourcePersonKey"].tolist()) == [
        "bionomia:0000-0002",
        "harvard_index:5",
        "wikidata:Q1",
    ]
