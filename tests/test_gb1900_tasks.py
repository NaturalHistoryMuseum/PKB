from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
import json
from zipfile import ZipFile

import luigi

from pkb.storage import DatasetVersion
from pkb.tasks.load.postgres.gb1900 import (
    GB1900_ARCHIVE_MEMBER,
    GB1900PlacesPostgresTask,
)
from pkb.tasks.sources.places.gb1900 import GB1900PlacesSourceTask


def test_gb1900_source_stores_archive(monkeypatch, tmp_path):
    import pkb.tasks.sources.places.gb1900 as task_module

    assert task_module.GB1900_URL.startswith("https://www.visionofbritain.org.uk/")
    data = b"archive"
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
        url = task_module.GB1900_URL
        headers = {"content-length": str(len(data))}

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield data

        def close(self):
            pass

    calls = []
    stored = {}
    monkeypatch.setattr(task_module, "GB1900_SOURCE_DIR", tmp_path)
    monkeypatch.setattr(task_module, "GB1900_SHA256", sha256(data).hexdigest())
    monkeypatch.setattr(task_module, "tqdm", Progress)
    monkeypatch.setattr(
        task_module.requests,
        "get",
        lambda *args, **kwargs: calls.append((args, kwargs)) or Response(),
    )
    monkeypatch.setattr(task_module, "get_s3_client", lambda: "s3")
    monkeypatch.setattr(task_module, "ensure_source_bucket", lambda **kwargs: None)

    def store_source_dataset(**kwargs):
        stored.update(kwargs)
        return DatasetVersion(
            dataset=kwargs["dataset"],
            retrieved_at=datetime(2026, 10, 4, tzinfo=UTC),
            source_url=kwargs["source_url"],
            sha256=sha256(kwargs["data"]).hexdigest(),
            size=len(kwargs["data"]),
            s3_bucket="pkb-source-data",
            s3_key=kwargs["key"],
            s3_version_id="version-1",
            upstream_version=kwargs["upstream_version"],
        )

    monkeypatch.setattr(task_module, "store_source_dataset", store_source_dataset)

    task = GB1900PlacesSourceTask()
    task.run()

    with open(task.output().path) as manifest:
        version = json.load(manifest)
    assert version["dataset"] == "gb1900_places"
    assert version["upstream_version"] == "July 2018"
    assert stored["key"] == f"gb1900/places/{task_module.GB1900_FILENAME}"
    assert calls == [
        (
            (task_module.GB1900_URL,),
            {"timeout": 1800, "stream": True, "verify": False},
        )
    ]
    assert progress["downloaded"] == len(data)


def test_gb1900_postgres_rows(monkeypatch, tmp_path):
    import pkb.storage.readers as reader_module

    csv_data = (
        "pin_id,final_text,nation,local_authority,parish,osgb_east,"
        "osgb_north,latitude,longitude,notes\n"
        "abc123,America Woods,England,Isle of Wight,Shanklin,"
        "458000.5,81200.25,50.6301,-1.1752,Checked\n"
    ).encode("utf-16")
    archive = BytesIO()
    with ZipFile(archive, "w") as zipped:
        zipped.writestr(GB1900_ARCHIVE_MEMBER, csv_data)

    path = tmp_path / "dataset-version.json"
    monkeypatch.setattr(
        GB1900PlacesPostgresTask,
        "input",
        lambda self: luigi.LocalTarget(path),
    )
    monkeypatch.setattr(
        reader_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="gb1900_places",
            retrieved_at=datetime(2026, 10, 4, tzinfo=UTC),
            source_url="https://example.org/gb1900.zip",
            sha256="sha",
            size=len(archive.getvalue()),
            s3_bucket="pkb-source-data",
            s3_key="gb1900/places/source.zip",
            s3_version_id="version-1",
        ),
    )
    monkeypatch.setattr(reader_module, "get_s3_client", lambda: "s3")
    monkeypatch.setattr(
        reader_module,
        "fetch_source_dataset",
        lambda **kwargs: archive.getvalue(),
    )

    task = GB1900PlacesPostgresTask()
    values = next(task.rows())
    row = dict(zip((column[0] for column in task.columns), values))

    assert row == {
        "pin_id": "abc123",
        "name": "America Woods",
        "nation": "England",
        "local_authority": "Isle of Wight",
        "parish": "Shanklin",
        "osgb_east": 458000.5,
        "osgb_north": 81200.25,
        "latitude": 50.6301,
        "longitude": -1.1752,
        "notes": "Checked",
        "dataset_version": "version-1",
    }


def test_gb1900_postgres_creates_name_index():
    queries = []

    class Cursor:
        def execute(self, query):
            queries.append(query)

    class Connection:
        def cursor(self):
            return Cursor()

    GB1900PlacesPostgresTask().post_copy(Connection())

    assert "places_gb1900_name_trgm" in queries[0]
