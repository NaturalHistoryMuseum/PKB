from __future__ import annotations

import csv
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile

from pkb.config import logger
from pkb.storage.s3 import (
    fetch_source_dataset,
    get_s3_client,
    read_dataset_version,
)


def iter_source_dataset_rows(
    manifest_path,
    archive_member=None,
    delimiter="\t",
    encoding="utf-8",
):
    dataset_version = read_dataset_version(manifest_path)
    data = fetch_source_dataset(
        s3=get_s3_client(),
        dataset_version=dataset_version,
    )
    version = dataset_version.s3_version_id or dataset_version.sha256
    logger.info(
        "Reading %s from source version %s",
        archive_member or dataset_version.dataset,
        version,
    )

    if archive_member:
        with ZipFile(BytesIO(data)) as archive:
            with archive.open(archive_member) as source:
                for row in iter_delimited_rows(
                    source,
                    delimiter=delimiter,
                    encoding=encoding,
                ):
                    yield row, version
    else:
        for row in iter_delimited_rows(
            BytesIO(data),
            delimiter=delimiter,
            encoding=encoding,
        ):
            yield row, version


def iter_delimited_rows(source, delimiter="\t", encoding="utf-8"):
    with TextIOWrapper(source, encoding=encoding) as text:
        yield from csv.reader(text, delimiter=delimiter)
