from __future__ import annotations

import json
from datetime import UTC, datetime

import luigi
import pandas as pd

from pkb.storage import DatasetVersion, SourceObject, dataset_version_to_dict
from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorPropertyEntitiesSourceTask,
    WikiDataCollectorPropertyIDsTask,
    collect_property_qids_for_collectors,
)


def test_collect_property_qids_for_collectors_finds_property_entities():
    entities = {
        "Q1": {
            "claims": {
                "P27": [
                    {
                        "mainsnak": {
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 145,
                                }
                            }
                        }
                    }
                ],
                "P108": [
                    {
                        "mainsnak": {
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 309388,
                                }
                            }
                        }
                    }
                ],
                "P21": [
                    {
                        "mainsnak": {
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 6581097,
                                }
                            }
                        }
                    }
                ],
            }
        }
    }

    assert collect_property_qids_for_collectors(entities) == {
        "Q145",
        "Q309388",
        "Q6581097",
    }


def test_collector_property_ids_task_derives_qids_from_collector_manifest(
    monkeypatch,
    tmp_path,
):
    import pkb.tasks.sources.people.wikidata as source_module

    source_object = SourceObject(
        s3_bucket="pkb-source-data",
        s3_key="wikidata/people/collector-entities/batch-00000.json",
        s3_version_id="collector-version",
        sha256="sha",
        size=0,
    )
    manifest = DatasetVersion(
        dataset="wikidata_collector_entities",
        retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        source_url="https://www.wikidata.org/w/api.php",
        source_objects=[source_object],
    )
    manifest_path = tmp_path / "collector_entities.dataset-version.json"
    manifest_path.write_text(json.dumps(dataset_version_to_dict(manifest)))

    monkeypatch.setattr(
        WikiDataCollectorPropertyIDsTask,
        "input",
        lambda self: luigi.LocalTarget(manifest_path),
    )
    monkeypatch.setattr(
        WikiDataCollectorPropertyIDsTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "collector_property_ids.json"),
    )
    monkeypatch.setattr(source_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        source_module,
        "fetch_source_object",
        lambda **kwargs: json.dumps(
            {
                "entities": {
                    "Q1": {
                        "claims": {
                            "P27": [
                                {
                                    "mainsnak": {
                                        "datavalue": {
                                            "value": {
                                                "entity-type": "item",
                                                "numeric-id": 145,
                                            }
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        ).encode(),
    )

    task = WikiDataCollectorPropertyIDsTask()
    task.run()

    with open(task.output().path) as f:
        payload = json.load(f)

    assert payload["qids"] == ["Q145"]


def test_collector_property_entities_source_reads_cached_property_qids(
    monkeypatch,
    tmp_path,
):
    qids_path = tmp_path / "collector_property_ids.json"
    qids_path.write_text(json.dumps({"qids": ["Q145"]}))

    monkeypatch.setattr(
        WikiDataCollectorPropertyEntitiesSourceTask,
        "input",
        lambda self: luigi.LocalTarget(qids_path),
    )

    assert WikiDataCollectorPropertyEntitiesSourceTask().qids() == ["Q145"]


def test_wikidata_people_build_parses_stored_collector_and_related_entities(
    monkeypatch,
    tmp_path,
):
    import pkb.tasks.build.people.wikidata as build_module
    from pkb.tasks.build.people.wikidata import WikiDataPeopleTask

    collector_manifest = DatasetVersion(
        dataset="wikidata_collector_entities",
        retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        source_url="https://www.wikidata.org/w/api.php",
        source_objects=[
            SourceObject(
                s3_bucket="pkb-source-data",
                s3_key="wikidata/people/collector-entities/batch-00000.json",
                s3_version_id="collector-version",
                sha256="sha",
                size=0,
            )
        ],
    )
    property_entities_manifest = DatasetVersion(
        dataset="wikidata_collector_property_entities",
        retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        source_url="https://www.wikidata.org/w/api.php",
        source_objects=[
            SourceObject(
                s3_bucket="pkb-source-data",
                s3_key="wikidata/people/collector-property-entities/batch-00000.json",
                s3_version_id="related-version",
                sha256="sha",
                size=0,
            )
        ],
    )
    collector_manifest_path = tmp_path / "collector_entities.dataset-version.json"
    property_entities_manifest_path = (
        tmp_path / "collector_property_entities.dataset-version.json"
    )
    collector_manifest_path.write_text(json.dumps(dataset_version_to_dict(collector_manifest)))
    property_entities_manifest_path.write_text(
        json.dumps(dataset_version_to_dict(property_entities_manifest))
    )

    monkeypatch.setattr(
        WikiDataPeopleTask,
        "input",
        lambda self: {
            "collectors": luigi.LocalTarget(collector_manifest_path),
            "property_entities": luigi.LocalTarget(property_entities_manifest_path),
        },
    )
    monkeypatch.setattr(
        WikiDataPeopleTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "wikidata_people.parquet"),
    )
    monkeypatch.setattr(
        build_module,
        "read_entities_from_manifest",
        lambda path: (
            {
                "Q1": {
                    "labels": {"en": {"value": "Jane Collector"}},
                    "aliases": {"en": [{"value": "J. Collector"}]},
                    "claims": {
                        "P27": [
                            {
                                "mainsnak": {
                                    "datavalue": {
                                        "value": {
                                            "entity-type": "item",
                                            "numeric-id": 145,
                                        }
                                    }
                                }
                            }
                        ]
                    },
                }
            }
            if "collector_property_entities" not in str(path)
            else {
                "Q145": {
                    "labels": {"en": {"value": "United Kingdom"}},
                    "aliases": {},
                    "claims": {},
                }
            }
        ),
    )

    task = WikiDataPeopleTask()
    task.run()

    df = pd.read_parquet(task.output().path)
    row = df.iloc[0].to_dict()

    assert row["wikidataID"] == "Q1"
    assert row["name"] == "Jane Collector"
    assert list(row["aliases"]) == ["J. Collector"]
    assert list(row["countryWikidataIDs"]) == ["Q145"]
    assert list(row["countriesOfCitizenship"]) == ["United Kingdom"]
    assert row["pkbPersonKey"] == "wikidata:Q1"
    assert row["source"] == "wikidata"
