from __future__ import annotations

import json
from datetime import UTC, datetime

import luigi
import pandas as pd

from pkb.storage import DatasetVersion
from pkb.tasks.build.people.bionomia import BionomiaPeopleTask
from pkb.tasks.sources.people.bionomia import BionomiaPublicProfilesSourceTask


BIONOMIA_CSV = (
    b"Family,Given,Particle,OtherNames,LabelName,Country,Keywords,wikidata,ORCID,URL\n"
    b"Page,Roderic,,R. D. M. Page|Rod Page,,United Kingdom,,,"
    b"0000-0002-7101-9767,https://bionomia.net/0000-0002-7101-9767\n"
    b"Chatzimanolis,Stylianos,,S. Chatzimanolis,,United States,Taxonomist,"
    b"Q123,0000-0001-9008-0611,https://bionomia.net/0000-0001-9008-0611\n"
)


class FakeResponse:
    url = "https://bionomia.net/data/bionomia-public-profiles.csv"
    content = BIONOMIA_CSV

    def raise_for_status(self):
        pass


def test_bionomia_public_profiles_source_stores_raw_csv_manifest(
    monkeypatch,
    tmp_path,
):
    import pkb.tasks.sources.people.bionomia as source_module

    calls = []

    monkeypatch.setattr(
        BionomiaPublicProfilesSourceTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "public_profiles.dataset-version.json"),
    )
    monkeypatch.setattr(
        source_module.requests,
        "get",
        lambda *args, **kwargs: calls.append(("get", args, kwargs)) or FakeResponse(),
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
        ),
    )

    task = BionomiaPublicProfilesSourceTask()
    task.run()

    with open(task.output().path) as f:
        manifest = json.load(f)

    assert manifest["dataset"] == "bionomia_public_profiles"
    assert manifest["s3_key"] == "bionomia/people/public-profiles/source.csv"
    assert calls[0] == (
        "get",
        ("https://bionomia.net/data/bionomia-public-profiles.csv",),
        {"timeout": 180},
    )
    assert calls[2][1]["data"] == BIONOMIA_CSV


def test_bionomia_people_build_maps_public_profiles(monkeypatch, tmp_path):
    import pkb.tasks.build.people.bionomia as build_module

    monkeypatch.setattr(
        BionomiaPeopleTask,
        "input",
        lambda self: luigi.LocalTarget(tmp_path / "public_profiles.dataset-version.json"),
    )
    monkeypatch.setattr(
        BionomiaPeopleTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "bionomia_people.parquet"),
    )
    monkeypatch.setattr(
        build_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="bionomia_public_profiles",
            retrieved_at=datetime(2026, 8, 18, 12, 0, tzinfo=UTC),
            source_url="https://bionomia.net/data/bionomia-public-profiles.csv",
            sha256="sha",
            size=len(BIONOMIA_CSV),
            s3_bucket="pkb-source-data",
            s3_key="bionomia/people/public-profiles/source.csv",
            s3_version_id="version-123",
        ),
    )
    monkeypatch.setattr(build_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(build_module, "fetch_source_dataset", lambda **kwargs: BIONOMIA_CSV)

    task = BionomiaPeopleTask()
    task.run()

    df = pd.read_parquet(task.output().path)
    rows = df.to_dict("records")

    assert rows[0]["pkbPersonKey"] == "bionomia:0000-0002-7101-9767"
    assert rows[0]["source"] == "bionomia"
    assert rows[0]["bionomiaID"] == "0000-0002-7101-9767"
    assert rows[0]["orcidID"] == "0000-0002-7101-9767"
    assert rows[0]["name"] == "Roderic Page"
    assert list(rows[0]["aliases"]) == ["R. D. M. Page", "Rod Page"]
    assert rows[0]["country"] == "United Kingdom"
    assert rows[1]["wikidataID"] == "Q123"
    assert list(rows[1]["keywords"]) == ["Taxonomist"]


def test_people_task_aggregates_people_and_writes_source_links(monkeypatch, tmp_path):
    from pkb.tasks.build.people.people import PeopleTask
    import pkb.tasks.build.people.people as people_module

    wikidata_path = tmp_path / "wikidata_people.parquet"
    bionomia_path = tmp_path / "bionomia_people.parquet"
    harvard_path = tmp_path / "harvard_index_people.parquet"
    pd.DataFrame(
        [
            {
                "pkbPersonKey": "wikidata:Q1",
                "source": "wikidata",
                "wikidataID": "Q1",
                "harvardIndexID": "5",
                "name": "Jane Collector",
            }
        ]
    ).to_parquet(wikidata_path, index=False)
    pd.DataFrame(
        [
            {
                "pkbPersonKey": "bionomia:0000-0002",
                "source": "bionomia",
                "wikidataID": "Q1",
                "bionomiaID": "0000-0002",
                "name": "J. Collector",
            }
        ]
    ).to_parquet(bionomia_path, index=False)
    pd.DataFrame(
        [
            {
                "pkbPersonKey": "harvard_index:5",
                "source": "harvard_index",
                "harvardIndexID": ["5", "6"],
                "name": "Jane A. Collector",
            }
        ]
    ).to_parquet(harvard_path, index=False)

    monkeypatch.setattr(
        people_module.WikiDataPeopleTask,
        "output",
        lambda self: luigi.LocalTarget(wikidata_path),
    )
    monkeypatch.setattr(
        people_module.BionomiaPeopleTask,
        "output",
        lambda self: luigi.LocalTarget(bionomia_path),
    )
    monkeypatch.setattr(
        people_module.HarvardIndexPeopleBuildTask,
        "output",
        lambda self: luigi.LocalTarget(harvard_path),
    )
    monkeypatch.setattr(
        PeopleTask,
        "output",
        lambda self: {
            "people": luigi.LocalTarget(tmp_path / "people.parquet"),
            "source_links": luigi.LocalTarget(tmp_path / "person_source_links.parquet"),
        },
    )

    task = PeopleTask()
    task.run()

    people = pd.read_parquet(task.output()["people"].path)
    source_links = pd.read_parquet(task.output()["source_links"].path)

    assert len(people) == 1
    assert people.iloc[0]["name"] == "Jane A. Collector"
    assert people.iloc[0]["sourceCount"] == 3
    assert list(people.iloc[0]["harvardIndexIDs"]) == ["5", "6"]
    assert sorted(source_links["sourcePersonKey"].tolist()) == [
        "bionomia:0000-0002",
        "harvard_index:5",
        "wikidata:Q1",
    ]
