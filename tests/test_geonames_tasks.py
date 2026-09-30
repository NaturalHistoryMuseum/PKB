from datetime import UTC, datetime
from io import BytesIO
import json
from zipfile import ZipFile

import luigi
import pytest

from pkb.storage import DatasetVersion
from pkb.tasks.load.postgres.geonames import (
    GeoNamesAlternateNamesPostgresTask,
    GeoNamesPlacesPostgresTask,
)
from pkb.tasks.sources.places.geonames import (
    GeoNamesAlternateNamesSourceTask,
    GeoNamesPlacesSourceTask,
)


def _zip(filename, contents):
    data = BytesIO()
    with ZipFile(data, "w") as archive:
        archive.writestr(filename, contents)
    return data.getvalue()


@pytest.mark.parametrize(
    ("task_class", "filename", "dataset"),
    [
        (GeoNamesPlacesSourceTask, "allCountries.zip", "places"),
        (
            GeoNamesAlternateNamesSourceTask,
            "alternateNamesV2.zip",
            "alternate_names",
        ),
    ],
)
def test_geonames_source_stores_zip(monkeypatch, tmp_path, task_class, filename, dataset):
    import pkb.tasks.sources.places.geonames as task_module

    progress = {}

    class Progress:
        def __init__(self, **kwargs):
            progress.update(kwargs)
            progress["downloaded"] = 0

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def update(self, size):
            progress["downloaded"] += size

    class Response:
        url = f"https://download.geonames.org/export/dump/{filename}"
        headers = {"content-length": "3"}

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield b"zip"

        def close(self):
            pass

    calls = []
    monkeypatch.setattr(task_module, "GEONAMES_SOURCE_DIR", tmp_path)
    monkeypatch.setattr(task_module, "tqdm", Progress)
    monkeypatch.setattr(
        task_module.requests,
        "get",
        lambda *args, **kwargs: calls.append((args, kwargs)) or Response(),
    )
    monkeypatch.setattr(task_module, "get_s3_client", lambda: "s3")
    monkeypatch.setattr(task_module, "ensure_source_bucket", lambda **kwargs: None)
    monkeypatch.setattr(
        task_module,
        "store_source_dataset",
        lambda **kwargs: DatasetVersion(
            dataset=kwargs["dataset"],
            retrieved_at=datetime(2026, 9, 30, tzinfo=UTC),
            source_url=kwargs["source_url"],
            sha256="sha",
            size=3,
            s3_bucket="pkb-source-data",
            s3_key=kwargs["key"],
            s3_version_id="version-1",
        ),
    )

    task = task_class()
    task.run()

    with open(task.output().path) as manifest:
        version = json.load(manifest)
    assert version["dataset"] == f"geonames_{dataset}"
    assert version["s3_key"] == f"geonames/places/{filename}"
    assert calls == [
        (
            (f"https://download.geonames.org/export/dump/{filename}",),
            {"timeout": 1800, "stream": True},
        )
    ]
    assert progress["total"] == 3
    assert progress["downloaded"] == 3
    assert progress["desc"] == f"Downloading GeoNames {dataset}"


@pytest.mark.parametrize(
    ("task_class", "member", "contents", "expected"),
    [
        (
            GeoNamesPlacesPostgresTask,
            "allCountries.txt",
            "2643743\tLondon\tLondon\tLondres\t51.50853\t-0.12574\tP\tPPLC\tGB\t\tENG\t\t\t\t8961989\t\t25\tEurope/London\t2026-09-01\n",
            {
                "geoname_id": 2643743,
                "name": "London",
                "latitude": 51.50853,
                "feature_code": "PPLC",
                "population": 8961989,
                "elevation": None,
                "dataset_version": "version-1",
            },
        ),
        (
            GeoNamesAlternateNamesPostgresTask,
            "alternateNamesV2.txt",
            "123\t2643743\tfr\tLondres\t1\t\t\t\t1900\t\n",
            {
                "alternate_name_id": 123,
                "geoname_id": 2643743,
                "language": "fr",
                "name": "Londres",
                "is_preferred": True,
                "is_short": False,
                "valid_from": "1900",
                "valid_to": None,
                "dataset_version": "version-1",
            },
        ),
    ],
)
def test_geonames_postgres_rows(
    monkeypatch,
    tmp_path,
    task_class,
    member,
    contents,
    expected,
):
    import pkb.tasks.load.postgres.geonames as task_module

    path = tmp_path / "dataset-version.json"
    monkeypatch.setattr(
        task_class,
        "input",
        lambda self: luigi.LocalTarget(path),
    )
    monkeypatch.setattr(
        task_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="geonames",
            retrieved_at=datetime(2026, 9, 30, tzinfo=UTC),
            source_url="https://example.org/source.zip",
            sha256="sha",
            size=0,
            s3_bucket="pkb-source-data",
            s3_key="geonames/source.zip",
            s3_version_id="version-1",
        ),
    )
    monkeypatch.setattr(task_module, "get_s3_client", lambda: "s3")
    monkeypatch.setattr(
        task_module,
        "fetch_source_dataset",
        lambda **kwargs: _zip(member, contents),
    )

    task = task_class()
    values = next(task.rows())
    row = dict(zip((column[0] for column in task.columns), values))

    assert {column: row[column] for column in expected} == expected
