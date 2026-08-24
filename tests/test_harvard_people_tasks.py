from __future__ import annotations

import json
from datetime import UTC, datetime

import luigi
import pandas as pd

from pkb.storage import DatasetVersion
from pkb.tasks.build.people.harvard import HarvardIndexPeopleBuildTask
from pkb.tasks.sources.people.harvard import HarvardIndexCollectorsSourceTask


HARVARD_COLLECTORS_CSV = (
    b"id,Name,Standard/Label Name,Full Name,B & P Author Abbrev.,Date of birth,"
    b"Date of death,birthYear,deathYear,birthYearIsApprox,deathYearIsApprox,"
    b"geographyISO,herbariaCode,institutionUUID,Agent type,URL\n"
    b"5,Brach,A. R. Brach,\"Brach, Anthony Robert\",Brach,1963-01-01,,1963,,"
    b"False,,\"{'US'}\",\"{'SYRF'}\",\"['uuid-1']\",Person,"
    b"https://example.org/5\n"
    b"6,Brach,A. R. Brach,\"Brach, Anthony Robert\",Brach,1963-01-01,,1963,,"
    b"False,,\"{'US'}\",\"{'SYRF'}\",\"['uuid-2']\",Person,"
    b"https://example.org/6\n"
)


def test_harvard_index_collectors_source_stores_archived_csv(
    monkeypatch,
    tmp_path,
):
    import pkb.tasks.sources.people.harvard as source_module

    archived_csv = tmp_path / "huh-collectors.csv"
    archived_csv.write_bytes(HARVARD_COLLECTORS_CSV)
    calls = []

    monkeypatch.setattr(
        HarvardIndexCollectorsSourceTask,
        "input",
        lambda self: luigi.LocalTarget(archived_csv),
    )
    monkeypatch.setattr(
        HarvardIndexCollectorsSourceTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "collectors.dataset-version.json"),
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
            retrieved_at=datetime(2026, 8, 18, 12, 0, tzinfo=UTC),
            source_url=kwargs["source_url"],
            sha256="sha",
            size=len(kwargs["data"]),
            s3_bucket="pkb-source-data",
            s3_key=kwargs["key"],
            s3_version_id="version-123",
            upstream_version=kwargs["upstream_version"],
        ),
    )

    task = HarvardIndexCollectorsSourceTask()
    task.run()

    with open(task.output().path) as f:
        manifest = json.load(f)

    assert manifest["dataset"] == "harvard_index_collectors"
    assert manifest["s3_key"] == "harvard-index/people/collectors/source.csv"
    assert manifest["upstream_version"] == "historical scrape from src/pkb/tasks/harvard.py"
    assert calls[1][1]["data"] == HARVARD_COLLECTORS_CSV


def test_harvard_index_people_build_maps_archived_collectors(monkeypatch, tmp_path):
    import pkb.tasks.build.people.harvard as build_module

    monkeypatch.setattr(
        HarvardIndexPeopleBuildTask,
        "input",
        lambda self: luigi.LocalTarget(tmp_path / "collectors.dataset-version.json"),
    )
    monkeypatch.setattr(
        HarvardIndexPeopleBuildTask,
        "output",
        lambda self: {
            "people": luigi.LocalTarget(tmp_path / "harvard_index_people.parquet"),
            "deduplication_map": luigi.LocalTarget(
                tmp_path / "harvard_index_person_deduplication_map.parquet"
            ),
            "clusters": luigi.LocalTarget(
                tmp_path / "harvard_index_person_deduplication_clusters.parquet"
            ),
        },
    )
    monkeypatch.setattr(
        build_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="harvard_index_collectors",
            retrieved_at=datetime(2026, 8, 18, 12, 0, tzinfo=UTC),
            source_url="archived-local-file:data/assets/harvard-index/huh-collectors.csv",
            sha256="sha",
            size=len(HARVARD_COLLECTORS_CSV),
            s3_bucket="pkb-source-data",
            s3_key="harvard-index/people/collectors/source.csv",
            s3_version_id="version-123",
        ),
    )
    monkeypatch.setattr(build_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        build_module,
        "fetch_source_dataset",
        lambda **kwargs: HARVARD_COLLECTORS_CSV,
    )

    task = HarvardIndexPeopleBuildTask()
    task.run()

    output = task.output()
    df = pd.read_parquet(output["people"].path)
    deduplication_map = pd.read_parquet(output["deduplication_map"].path)
    clusters = pd.read_parquet(output["clusters"].path)
    row = df.iloc[0].to_dict()

    assert row["pkbPersonKey"] == "harvard_index:5"
    assert row["source"] == "harvard_index"
    assert list(row["harvardIndexID"]) == ["5", "6"]
    assert list(row["deduplicatedHarvardIndexIDs"]) == ["5", "6"]
    assert row["sourceRecordCount"] == 2
    assert row["name"] == "A. R. Brach"
    assert row["fullName"] == "Brach, Anthony Robert"
    assert row["authorAbbreviation"] == "Brach"
    assert list(row["geographyISO"]) == ["US"]
    assert list(row["herbariaCode"]) == ["SYRF"]
    assert list(row["institutionUUID"]) == ["uuid-1", "uuid-2"]
    assert set(deduplication_map["harvardIndexID"].tolist()) == {"5", "6"}
    assert clusters.iloc[0]["canonicalHarvardIndexID"] == "5"
