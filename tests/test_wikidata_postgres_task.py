from datetime import UTC, datetime

import luigi

from pkb.storage import DatasetVersion
from pkb.tasks.load.postgres.wikidata import WikiDataPeoplePostgresTask


def test_wikidata_people_postgres_rows(monkeypatch, tmp_path):
    import pkb.tasks.load.postgres.wikidata as task_module

    collector_path = tmp_path / "collector_entities.dataset-version.json"
    property_path = tmp_path / "collector_property_entities.dataset-version.json"
    version = DatasetVersion(
        dataset="wikidata_collector_entities",
        retrieved_at=datetime(2026, 8, 18, tzinfo=UTC),
        source_url="https://www.wikidata.org/w/api.php",
        source_objects=[],
    )
    messages = []
    monkeypatch.setattr(
        task_module.logger,
        "info",
        lambda message, *args: messages.append(message % args),
    )
    monkeypatch.setattr(
        WikiDataPeoplePostgresTask,
        "input",
        lambda self: {
            "collectors": luigi.LocalTarget(collector_path),
            "property_entities": luigi.LocalTarget(property_path),
        },
    )
    monkeypatch.setattr(task_module, "read_dataset_version", lambda path: version)
    monkeypatch.setattr(
        task_module,
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
                        ],
                        "P6264": [
                            {"mainsnak": {"datavalue": {"value": "123"}}}
                        ],
                    },
                }
            }
            if str(path) == str(collector_path)
            else {
                "Q145": {
                    "labels": {"en": {"value": "United Kingdom"}},
                    "claims": {},
                }
            }
        ),
    )

    task = WikiDataPeoplePostgresTask()
    [values] = list(task.rows())
    row = dict(zip((column[0] for column in task.columns), values))

    assert row["wikidata_id"] == "Q1"
    assert row["aliases"] == '["J. Collector"]'
    assert row["harvard_index_id"] == "123"
    assert row["countries_of_citizenship"] == '["United Kingdom"]'
    assert row["name_normalised"] == "jane collector"
    assert row["surname"] == "collector"
    assert row["given_names"] == "jane"
    assert row["initials"] == "j"
    assert row["dataset_version"] == "2026-08-18T00:00:00+00:00"
    assert messages[-1] == "Prepared 1 Wikidata people for people_wikidata"
