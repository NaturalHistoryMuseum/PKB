from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pkb.config import settings


@dataclass(frozen=True)
class SourceObject:
    s3_bucket: str
    s3_key: str
    s3_version_id: str | None
    sha256: str
    size: int
    content_type: str | None = None


@dataclass(frozen=True)
class DatasetVersion:
    dataset: str
    retrieved_at: datetime
    source_url: str
    sha256: str | None = None
    size: int | None = None
    s3_bucket: str | None = None
    s3_key: str | None = None
    s3_version_id: str | None = None
    upstream_version: str | None = None
    source_objects: list[SourceObject] | None = None

    def __post_init__(self):
        if self.source_objects is None and self.s3_bucket and self.s3_key:
            object.__setattr__(
                self,
                "source_objects",
                [
                    SourceObject(
                        s3_bucket=self.s3_bucket,
                        s3_key=self.s3_key,
                        s3_version_id=self.s3_version_id,
                        sha256=self.sha256 or "",
                        size=self.size or 0,
                    )
                ],
            )

    @property
    def source_object(self) -> SourceObject:
        if not self.source_objects:
            raise ValueError(f"DatasetVersion {self.dataset} has no source objects")
        if len(self.source_objects) != 1:
            raise ValueError(
                f"DatasetVersion {self.dataset} has {len(self.source_objects)} source objects"
            )
        return self.source_objects[0]


def get_s3_client():
    """Return an S3 client configured for AWS or LocalStack."""

    import boto3

    kwargs: dict[str, Any] = {
        "region_name": settings.aws_region,
    }

    if settings.s3_endpoint_url:
        kwargs["endpoint_url"] = settings.s3_endpoint_url

    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id

    if settings.aws_secret_access_key:
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key

    return boto3.client("s3", **kwargs)


def ensure_source_bucket(
    *,
    s3,
    bucket: str | None = None,
    region: str | None = None,
) -> None:
    """Create the source-data bucket and enable S3 object versioning."""

    s3_bucket = bucket or settings.source_data_bucket
    s3_region = region or settings.aws_region

    try:
        s3.head_bucket(Bucket=s3_bucket)
    except Exception:
        create_kwargs: dict[str, Any] = {
            "Bucket": s3_bucket,
        }
        if s3_region != "us-east-1":
            create_kwargs["CreateBucketConfiguration"] = {
                "LocationConstraint": s3_region,
            }
        s3.create_bucket(**create_kwargs)

    s3.put_bucket_versioning(
        Bucket=s3_bucket,
        VersioningConfiguration={
            "Status": "Enabled",
        },
    )


def store_source_dataset(
    *,
    s3,
    dataset: str,
    source_url: str,
    data: bytes | bytearray,
    bucket: str | None = None,
    key: str | None = None,
    upstream_version: str | None = None,
    retrieved_at: datetime | None = None,
) -> DatasetVersion:
    """Store raw source bytes and return the provenance needed to cite them."""

    s3_bucket = bucket or settings.source_data_bucket
    s3_key = key or f"{dataset}/source"
    retrieved_at = retrieved_at or datetime.now(UTC)

    response = s3.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=data,
        Metadata={
            "dataset": dataset,
            "source-url": source_url,
            "sha256": sha256_bytes(data),
        },
    )

    return DatasetVersion(
        dataset=dataset,
        retrieved_at=retrieved_at,
        source_url=source_url,
        sha256=sha256_bytes(data),
        size=len(data),
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        s3_version_id=response.get("VersionId"),
        upstream_version=upstream_version,
        source_objects=[
            SourceObject(
                s3_bucket=s3_bucket,
                s3_key=s3_key,
                s3_version_id=response.get("VersionId"),
                sha256=sha256_bytes(data),
                size=len(data),
            )
        ],
    )


def dataset_version_to_dict(dataset_version: DatasetVersion) -> dict[str, Any]:
    source_objects = [
        {
            "s3_bucket": source_object.s3_bucket,
            "s3_key": source_object.s3_key,
            "s3_version_id": source_object.s3_version_id,
            "sha256": source_object.sha256,
            "size": source_object.size,
            "content_type": source_object.content_type,
        }
        for source_object in dataset_version.source_objects or []
    ]
    return {
        "dataset": dataset_version.dataset,
        "retrieved_at": dataset_version.retrieved_at.isoformat(),
        "source_url": dataset_version.source_url,
        "sha256": dataset_version.sha256,
        "size": dataset_version.size,
        "s3_bucket": dataset_version.s3_bucket,
        "s3_key": dataset_version.s3_key,
        "s3_version_id": dataset_version.s3_version_id,
        "upstream_version": dataset_version.upstream_version,
        "source_objects": source_objects,
    }


def dataset_version_from_dict(data: dict[str, Any]) -> DatasetVersion:
    source_objects = data.get("source_objects")
    if source_objects is not None:
        source_objects = [SourceObject(**source_object) for source_object in source_objects]

    return DatasetVersion(
        **{
            **data,
            "retrieved_at": datetime.fromisoformat(data["retrieved_at"]),
            "source_objects": source_objects,
        }
    )


def read_dataset_version(path: str) -> DatasetVersion:
    with open(path) as f:
        return dataset_version_from_dict(json.load(f))


def fetch_source_dataset(*, s3, dataset_version: DatasetVersion) -> bytes:
    return fetch_source_object(s3=s3, source_object=dataset_version.source_object)


def fetch_source_object(*, s3, source_object: SourceObject) -> bytes:
    get_kwargs: dict[str, Any] = {
        "Bucket": source_object.s3_bucket,
        "Key": source_object.s3_key,
    }
    if source_object.s3_version_id:
        get_kwargs["VersionId"] = source_object.s3_version_id

    response = s3.get_object(**get_kwargs)
    return response["Body"].read()


def sha256_bytes(data: bytes | bytearray) -> str:
    return hashlib.sha256(data).hexdigest()
