from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from pkb.config import settings


@dataclass(frozen=True)
class DatasetVersion:
    dataset: str
    retrieved_at: datetime
    source_url: str
    sha256: str
    size: int
    s3_bucket: str
    s3_key: str
    s3_version_id: str | None = None
    upstream_version: str | None = None


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
    data: bytes,
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
    )


def dataset_version_to_dict(dataset_version: DatasetVersion) -> dict[str, Any]:
    return {
        **asdict(dataset_version),
        "retrieved_at": dataset_version.retrieved_at.isoformat(),
    }


def dataset_version_from_dict(data: dict[str, Any]) -> DatasetVersion:
    return DatasetVersion(
        **{
            **data,
            "retrieved_at": datetime.fromisoformat(data["retrieved_at"]),
        }
    )


def read_dataset_version(path: str) -> DatasetVersion:
    with open(path) as f:
        return dataset_version_from_dict(json.load(f))


def fetch_source_dataset(*, s3, dataset_version: DatasetVersion) -> bytes:
    get_kwargs: dict[str, Any] = {
        "Bucket": dataset_version.s3_bucket,
        "Key": dataset_version.s3_key,
    }
    if dataset_version.s3_version_id:
        get_kwargs["VersionId"] = dataset_version.s3_version_id

    response = s3.get_object(**get_kwargs)
    return response["Body"].read()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
