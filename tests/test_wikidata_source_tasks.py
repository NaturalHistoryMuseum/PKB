from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import luigi
import pandas as pd

from pkb.storage import DatasetVersion, SourceObject, dataset_version_to_dict
from pkb.tasks.sources.wikidata import (
    WikiDataEntityBatchSourceTask,
    WikiDataEntitiesSourceTask,
    WikiDataSPARQLSourceTask,
    qids_from_sparql_response,
)


SPARQL_BYTES = json.dumps(
    {
        "results": {
            "bindings": [
                {"item": {"value": "http://www.wikidata.org/entity/Q2"}},
                {"item": {"value": "http://www.wikidata.org/entity/Q1"}},
                {"item": {"value": "http://www.wikidata.org/entity/Q1"}},
            ]
        }
    }
).encode()


class FakeResponse:
    def __init__(self, *, content: bytes, url: str):
        self.content = content
        self.url = url


class ExampleSPARQLSourceTask(WikiDataSPARQLSourceTask):
    query = "SELECT ?item WHERE { ?item wdt:P31 wd:Q5 }"
    dataset = "wikidata_example_ids"
    s3_key = "wikidata/example/ids/source.json"
    output_path = "ids.dataset-version.json"


class ExampleEntityBatchSourceTask(WikiDataEntityBatchSourceTask):
    dataset = "wikidata_example_entities"
    s3_key_prefix = "wikidata/example/entities"
    entity_label = "example entities"
    batch_size = 2
    output_dir = Path("entity-batches")
    ids_source_task_class = ExampleSPARQLSourceTask


class ExampleEntitiesSourceTask(WikiDataEntitiesSourceTask):
    dataset = "wikidata_example_entities"
    s3_key_prefix = "wikidata/example/entities"
    entity_label = "example entities"
    batch_size = 2
    output_path = "entities.dataset-version.json"
    batch_task_class = ExampleEntityBatchSourceTask


def test_qids_from_sparql_response_returns_sorted_unique_ids():
    assert qids_from_sparql_response(SPARQL_BYTES) == ["Q1", "Q2"]


def test_wikidata_sparql_source_task_stores_raw_response(monkeypatch, tmp_path):
    import pkb.tasks.sources.wikidata as source_module

    calls = []

    monkeypatch.setattr(
        source_module,
        "run_sparql_query_response",
        lambda query: FakeResponse(
            content=SPARQL_BYTES,
            url="https://query.wikidata.org/sparql?query=example",
        ),
    )
    monkeypatch.setattr(source_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        source_module,
        "ensure_source_bucket",
        lambda **kwargs: calls.append(("ensure_source_bucket", kwargs)),
    )
    monkeypatch.setattr(
        source_module,
        "store_source_dataset",
        lambda **kwargs: calls.append(("store_source_dataset", kwargs))
        or DatasetVersion(
            dataset=kwargs["dataset"],
            retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
            source_url=kwargs["source_url"],
            sha256="sha",
            size=len(kwargs["data"]),
            s3_bucket="pkb-source-data",
            s3_key=kwargs["key"],
            s3_version_id="version-123",
        ),
    )

    monkeypatch.setattr(
        ExampleSPARQLSourceTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "ids.dataset-version.json"),
    )

    task = ExampleSPARQLSourceTask()
    task.run()

    with open(task.output().path) as f:
        manifest = json.load(f)

    assert manifest["dataset"] == "wikidata_example_ids"
    assert manifest["s3_key"] == "wikidata/example/ids/source.json"
    assert calls[0] == ("ensure_source_bucket", {"s3": "s3-client"})
    assert calls[1][1]["data"] == SPARQL_BYTES


def test_wikidata_entity_batch_source_task_stores_one_entity_batch(monkeypatch, tmp_path):
    import pkb.tasks.sources.wikidata as source_module

    calls = []

    monkeypatch.setattr(
        ExampleEntityBatchSourceTask,
        "input",
        lambda self: luigi.LocalTarget(tmp_path / "ids.dataset-version.json"),
    )
    monkeypatch.setattr(ExampleEntityBatchSourceTask, "output_dir", tmp_path)
    monkeypatch.setattr(
        source_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="wikidata_example_ids",
            retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
            source_url="https://query.wikidata.org/sparql",
            sha256="sha",
            size=len(SPARQL_BYTES),
            s3_bucket="pkb-source-data",
            s3_key="wikidata/example/ids/source.json",
            s3_version_id="ids-version",
        ),
    )
    monkeypatch.setattr(source_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(source_module, "fetch_source_dataset", lambda **kwargs: SPARQL_BYTES)
    monkeypatch.setattr(
        source_module,
        "ensure_source_bucket",
        lambda **kwargs: calls.append(("ensure_source_bucket", kwargs)),
    )
    monkeypatch.setattr(
        source_module,
        "fetch_entities_response",
        lambda qids, **kwargs: FakeResponse(
            content=json.dumps({"entities": {qid: {} for qid in qids}}).encode(),
            url="https://www.wikidata.org/w/api.php?action=wbgetentities",
        ),
    )

    def store_source_dataset(**kwargs):
        calls.append(("store_source_dataset", kwargs))
        return DatasetVersion(
            dataset=kwargs["dataset"],
            retrieved_at=kwargs["retrieved_at"],
            source_url=kwargs["source_url"],
            sha256="sha",
            size=len(kwargs["data"]),
            s3_bucket="pkb-source-data",
            s3_key=kwargs["key"],
            s3_version_id=f"{kwargs['key']}-version",
        )

    monkeypatch.setattr(source_module, "store_source_dataset", store_source_dataset)

    task = ExampleEntityBatchSourceTask(batch_index=0)
    task.run()

    with open(task.output().path) as f:
        manifest = json.load(f)

    assert manifest["dataset"] == "wikidata_example_entities"
    assert [obj["s3_key"] for obj in manifest["source_objects"]] == [
        "wikidata/example/entities/batch-00000.json"
    ]
    assert calls[1][1]["key"] == "wikidata/example/entities/batch-00000.json"


def test_wikidata_entities_source_task_yields_batches_and_combines_manifests(
    monkeypatch,
    tmp_path,
):
    monkeypatch.setattr(ExampleEntitiesSourceTask, "qids", lambda self: ["Q1", "Q2", "Q3"])
    monkeypatch.setattr(ExampleEntityBatchSourceTask, "output_dir", tmp_path / "batches")
    monkeypatch.setattr(
        ExampleEntitiesSourceTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "entities.dataset-version.json"),
    )

    task = ExampleEntitiesSourceTask()
    run = task.run()
    batch_tasks = next(run)

    assert [int(batch_task.batch_index) for batch_task in batch_tasks] == [0, 1]

    for batch_task in batch_tasks:
        source_object = SourceObject(
            s3_bucket="pkb-source-data",
            s3_key=f"wikidata/example/entities/batch-{batch_task.batch_index:05d}.json",
            s3_version_id=f"version-{batch_task.batch_index}",
            sha256="sha",
            size=10,
        )
        batch_manifest = DatasetVersion(
            dataset="wikidata_example_entities",
            retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
            source_url="https://www.wikidata.org/w/api.php",
            source_objects=[source_object],
        )
        Path(batch_task.output().path).parent.mkdir(parents=True, exist_ok=True)
        Path(batch_task.output().path).write_text(
            json.dumps(dataset_version_to_dict(batch_manifest))
        )

    try:
        next(run)
    except StopIteration:
        pass

    with open(task.output().path) as f:
        manifest = json.load(f)

    assert [obj["s3_key"] for obj in manifest["source_objects"]] == [
        "wikidata/example/entities/batch-00000.json",
        "wikidata/example/entities/batch-00001.json",
    ]


def test_wikidata_institutions_build_parses_stored_entity_batches(monkeypatch, tmp_path):
    import pkb.tasks.build.institutions.wikidata as build_module
    from pkb.tasks.build.institutions.wikidata import WikiDataInstitutionsTask

    source_object = SourceObject(
        s3_bucket="pkb-source-data",
        s3_key="wikidata/institutions/entities/batch-00001.json",
        s3_version_id="version-123",
        sha256="sha",
        size=0,
    )
    manifest = DatasetVersion(
        dataset="wikidata_institution_entities",
        retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        source_url="https://www.wikidata.org/w/api.php",
        source_objects=[source_object],
    )
    manifest_path = tmp_path / "entities.dataset-version.json"
    manifest_path.write_text(json.dumps(dataset_version_to_dict(manifest)))

    monkeypatch.setattr(
        WikiDataInstitutionsTask,
        "input",
        lambda self: luigi.LocalTarget(manifest_path),
    )
    monkeypatch.setattr(
        WikiDataInstitutionsTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "wikidata_institutions.parquet"),
    )
    monkeypatch.setattr(build_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        build_module,
        "fetch_source_object",
        lambda **kwargs: json.dumps(
            {
                "entities": {
                    "Q1": {
                        "labels": {"en": {"value": "Example Museum"}},
                        "aliases": {"en": [{"value": "Example"}]},
                        "claims": {},
                    }
                }
            }
        ).encode(),
    )

    task = WikiDataInstitutionsTask()
    task.run()

    df = pd.read_parquet(task.output().path)
    row = df.iloc[0].to_dict()

    assert row["wikidataID"] == "Q1"
    assert row["name"] == "Example Museum"
    assert list(row["aliases"]) == ["Example"]
    assert row["indexHerbariumCode"] is None
    assert row["harvardIndexID"] is None
    assert row["viafID"] is None
    assert row["isniID"] is None
    assert list(row["grSciCollID"]) == []
    assert list(row["rorID"]) == []
    assert row["countryWikidataID"] is None
    assert row["locationWikidataID"] is None
