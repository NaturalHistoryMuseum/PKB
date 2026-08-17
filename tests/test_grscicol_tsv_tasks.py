from __future__ import annotations

import json
from datetime import UTC, datetime

import luigi
import pandas as pd

from pkb.storage import DatasetVersion
from pkb.tasks.build.institutions.grscicol import (
    GRSciCollCollectionsTSVTask,
    GRSciCollInstitutionsTSVTask,
)
from pkb.tasks.sources.institutions.grscicol import (
    GRSciCollInstitutionsTSVSourceTask,
)


class FakeResponse:
    url = "https://api.gbif.org/v1/grscicoll/institution/export?active=true&format=TSV"
    content = b"key\tcode\tname\tcountry\n1\tNHM\tNatural History Museum\tGB\n"

    def raise_for_status(self):
        pass


def test_grscicol_tsv_source_stores_raw_export_and_writes_manifest(
    monkeypatch,
    tmp_path,
):
    import pkb.tasks.sources.institutions.grscicol as source_module

    calls = []

    monkeypatch.setattr(source_module, "GRSCICOLL_SOURCE_DIR", tmp_path)
    monkeypatch.setattr(
        source_module.requests,
        "get",
        lambda *args, **kwargs: calls.append(("get", args, kwargs)) or FakeResponse(),
    )
    monkeypatch.setattr(
        source_module,
        "get_s3_client",
        lambda: "s3-client",
    )
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

    task = GRSciCollInstitutionsTSVSourceTask()
    task.run()

    with open(task.output().path) as f:
        manifest = json.load(f)

    assert manifest["dataset"] == "grscicol_institutions"
    assert manifest["source_url"] == FakeResponse.url
    assert manifest["s3_bucket"] == "pkb-source-data"
    assert manifest["s3_key"] == "grscicol/institutions/source.tsv"
    assert manifest["s3_version_id"] == "version-123"

    assert calls[0] == (
        "get",
        ("https://api.gbif.org/v1/grscicoll/institution/export",),
        {
            "params": {
                "active": "true",
                "format": "TSV",
            },
            "timeout": 180,
        },
    )
    assert calls[1] == ("ensure_source_bucket", {"s3": "s3-client"})
    assert calls[2][0] == "store_source_dataset"
    assert calls[2][1]["dataset"] == "grscicol_institutions"
    assert calls[2][1]["key"] == "grscicol/institutions/source.tsv"


def test_grscicol_collection_tsv_build_maps_institution_key(monkeypatch, tmp_path):
    import pkb.tasks.build.institutions.grscicol as build_module

    monkeypatch.setattr(
        GRSciCollCollectionsTSVTask,
        "input",
        lambda self: luigi.LocalTarget(tmp_path / "collections.dataset-version.json"),
    )
    monkeypatch.setattr(
        build_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="grscicol_collections",
            retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
            source_url="https://example.org/collections.tsv",
            sha256="sha",
            size=0,
            s3_bucket="pkb-source-data",
            s3_key="grscicol/collections/source.tsv",
            s3_version_id="version-123",
        ),
    )
    monkeypatch.setattr(build_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        build_module,
        "fetch_source_dataset",
        lambda **kwargs: (
            b"key\tcode\tinstitution_key\tname\n"
            b"collection-1\tBOT\tinstitution-1\tBotany\n"
        ),
    )
    monkeypatch.setattr(
        "pkb.tasks.build.institutions.grscicol.settings.root_dir",
        tmp_path,
    )

    task = GRSciCollCollectionsTSVTask()
    task.run()

    df = pd.read_parquet(task.output().path)
    assert df.to_dict("records") == [
        {
            "key": "collection-1",
            "code": "BOT",
            "institutionKey": "institution-1",
            "name": "Botany",
        }
    ]


def test_grscicol_institution_tsv_build_preserves_expected_columns(
    monkeypatch,
    tmp_path,
):
    import pkb.tasks.build.institutions.grscicol as build_module

    monkeypatch.setattr(
        GRSciCollInstitutionsTSVTask,
        "input",
        lambda self: luigi.LocalTarget(tmp_path / "institutions.dataset-version.json"),
    )
    monkeypatch.setattr(
        build_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="grscicol_institutions",
            retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
            source_url="https://example.org/institutions.tsv",
            sha256="sha",
            size=0,
            s3_bucket="pkb-source-data",
            s3_key="grscicol/institutions/source.tsv",
            s3_version_id="version-123",
        ),
    )
    monkeypatch.setattr(build_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        build_module,
        "fetch_source_dataset",
        lambda **kwargs: (
            b"key\tcode\tname\tcountry\n"
            b"institution-1\tNHM\tNatural History Museum\tGB\n"
        ),
    )
    monkeypatch.setattr(
        "pkb.tasks.build.institutions.grscicol.settings.root_dir",
        tmp_path,
    )

    task = GRSciCollInstitutionsTSVTask()
    task.run()

    df = pd.read_parquet(task.output().path)
    assert df.to_dict("records") == [
        {
            "key": "institution-1",
            "code": "NHM",
            "name": "Natural History Museum",
            "countryISO3": "GBR",
            "ROR": None,
            "LSID": None,
            "CITES": None,
            "VIAF": None,
            "ISNI": None,
        }
    ]
