from __future__ import annotations

from io import BytesIO
import sys
from types import SimpleNamespace
from datetime import UTC, datetime

from pkb.storage.s3 import (
    DatasetVersion,
    dataset_version_from_dict,
    dataset_version_to_dict,
    ensure_source_bucket,
    fetch_source_dataset,
    store_source_dataset,
)


class FakeS3:
    def __init__(self, *, bucket_exists: bool = True):
        self.bucket_exists = bucket_exists
        self.calls = []

    def head_bucket(self, **kwargs):
        self.calls.append(("head_bucket", kwargs))
        if not self.bucket_exists:
            raise RuntimeError("missing bucket")

    def create_bucket(self, **kwargs):
        self.calls.append(("create_bucket", kwargs))

    def put_bucket_versioning(self, **kwargs):
        self.calls.append(("put_bucket_versioning", kwargs))

    def put_object(self, **kwargs):
        self.calls.append(("put_object", kwargs))
        return {
            "VersionId": "version-123",
        }

    def get_object(self, **kwargs):
        self.calls.append(("get_object", kwargs))
        return {
            "Body": BytesIO(b"source data"),
        }


def test_store_source_dataset_returns_dataset_version():
    s3 = FakeS3()
    retrieved_at = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)

    version = store_source_dataset(
        s3=s3,
        dataset="catalogue_of_life",
        source_url="https://example.org/source.zip",
        data=b"source data",
        bucket="pkb-source-data",
        key="catalogue-of-life/source.zip",
        upstream_version="2026-08",
        retrieved_at=retrieved_at,
    )

    assert version.dataset == "catalogue_of_life"
    assert version.retrieved_at == retrieved_at
    assert version.source_url == "https://example.org/source.zip"
    assert version.sha256 == "7ceed356ee3b63957db5803d56072b0d8e51df6b0c2c004b789e37e379999890"
    assert version.size == 11
    assert version.s3_bucket == "pkb-source-data"
    assert version.s3_key == "catalogue-of-life/source.zip"
    assert version.s3_version_id == "version-123"
    assert version.upstream_version == "2026-08"

    assert s3.calls == [
        (
            "put_object",
            {
                "Bucket": "pkb-source-data",
                "Key": "catalogue-of-life/source.zip",
                "Body": b"source data",
                "Metadata": {
                    "dataset": "catalogue_of_life",
                    "source-url": "https://example.org/source.zip",
                    "sha256": "7ceed356ee3b63957db5803d56072b0d8e51df6b0c2c004b789e37e379999890",
                },
            },
        ),
    ]


def test_ensure_source_bucket_creates_missing_bucket_and_enables_versioning():
    s3 = FakeS3(bucket_exists=False)

    ensure_source_bucket(
        s3=s3,
        bucket="pkb-source-data",
        region="eu-west-2",
    )

    assert s3.calls == [
        ("head_bucket", {"Bucket": "pkb-source-data"}),
        (
            "create_bucket",
            {
                "Bucket": "pkb-source-data",
                "CreateBucketConfiguration": {
                    "LocationConstraint": "eu-west-2",
                },
            },
        ),
        (
            "put_bucket_versioning",
            {
                "Bucket": "pkb-source-data",
                "VersioningConfiguration": {
                    "Status": "Enabled",
                },
            },
        ),
    ]


def test_ensure_source_bucket_uses_us_east_1_create_bucket_shape():
    s3 = FakeS3(bucket_exists=False)

    ensure_source_bucket(
        s3=s3,
        bucket="pkb-source-data",
        region="us-east-1",
    )

    assert s3.calls[1] == (
        "create_bucket",
        {
            "Bucket": "pkb-source-data",
        },
    )


def test_get_s3_client_passes_localstack_settings(monkeypatch):
    import pkb.storage.s3 as storage_s3

    calls = []

    fake_boto3 = SimpleNamespace(
        client=lambda *args, **kwargs: calls.append((args, kwargs)) or "client"
    )

    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    monkeypatch.setattr(storage_s3.settings, "aws_region", "eu-west-2")
    monkeypatch.setattr(storage_s3.settings, "s3_endpoint_url", "http://localhost:4566")
    monkeypatch.setattr(storage_s3.settings, "aws_access_key_id", "test")
    monkeypatch.setattr(storage_s3.settings, "aws_secret_access_key", "secret")

    assert storage_s3.get_s3_client() == "client"
    assert calls == [
        (
            ("s3",),
            {
                "region_name": "eu-west-2",
                "endpoint_url": "http://localhost:4566",
                "aws_access_key_id": "test",
                "aws_secret_access_key": "secret",
            },
        )
    ]


def test_dataset_version_round_trips_through_dict():
    dataset_version = DatasetVersion(
        dataset="grscicol_institutions",
        retrieved_at=datetime(2026, 8, 17, 12, 0, tzinfo=UTC),
        source_url="https://example.org/source.tsv",
        sha256="sha",
        size=11,
        s3_bucket="pkb-source-data",
        s3_key="grscicol/institutions/source.tsv",
        s3_version_id="version-123",
    )

    data = dataset_version_to_dict(dataset_version)

    assert data["retrieved_at"] == "2026-08-17T12:00:00+00:00"
    assert dataset_version_from_dict(data) == dataset_version


def test_fetch_source_dataset_uses_version_id_when_available():
    s3 = FakeS3()
    dataset_version = DatasetVersion(
        dataset="catalogue_of_life",
        retrieved_at=datetime(2026, 8, 16, 12, 0, tzinfo=UTC),
        source_url="https://example.org/source.zip",
        sha256="sha",
        size=11,
        s3_bucket="pkb-source-data",
        s3_key="catalogue-of-life/source.zip",
        s3_version_id="version-123",
    )

    assert fetch_source_dataset(s3=s3, dataset_version=dataset_version) == b"source data"
    assert s3.calls == [
        (
            "get_object",
            {
                "Bucket": "pkb-source-data",
                "Key": "catalogue-of-life/source.zip",
                "VersionId": "version-123",
            },
        ),
    ]
