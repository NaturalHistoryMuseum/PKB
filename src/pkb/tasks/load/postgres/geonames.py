from __future__ import annotations

import csv
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile

from pkb.config import logger, settings
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.base import PostgresCopyToTableTask
from pkb.tasks.sources.places.geonames import (
    GeoNamesAlternateNamesSourceTask,
    GeoNamesPlacesSourceTask,
)
from pkb.utils import clean_scalar


class GeoNamesPlacesPostgresTask(PostgresCopyToTableTask):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "places_geonames"
    columns = [
        ("geoname_id", "BIGINT PRIMARY KEY"),
        ("name", "TEXT NOT NULL"),
        ("ascii_name", "TEXT"),
        ("alternate_names", "TEXT"),
        ("latitude", "DOUBLE PRECISION"),
        ("longitude", "DOUBLE PRECISION"),
        ("feature_class", "CHAR(1)"),
        ("feature_code", "TEXT"),
        ("country_code", "CHAR(2)"),
        ("alternate_country_codes", "TEXT"),
        ("admin1_code", "TEXT"),
        ("admin2_code", "TEXT"),
        ("admin3_code", "TEXT"),
        ("admin4_code", "TEXT"),
        ("population", "BIGINT"),
        ("elevation", "INTEGER"),
        ("dem", "INTEGER"),
        ("timezone", "TEXT"),
        ("modification_date", "DATE"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return GeoNamesPlacesSourceTask()

    @property
    def update_id(self):
        return _update_id(self)

    def rows(self):
        count = 0
        for row, version in _source_rows(self.input().path, "allCountries.txt"):
            count += 1
            yield (
                int(row[0]),
                row[1],
                clean_scalar(row[2]),
                clean_scalar(row[3]),
                float(row[4]),
                float(row[5]),
                clean_scalar(row[6]),
                clean_scalar(row[7]),
                clean_scalar(row[8]),
                clean_scalar(row[9]),
                clean_scalar(row[10]),
                clean_scalar(row[11]),
                clean_scalar(row[12]),
                clean_scalar(row[13]),
                _integer(row[14]),
                _integer(row[15]),
                _integer(row[16]),
                clean_scalar(row[17]),
                clean_scalar(row[18]),
                version,
            )
        logger.info("Prepared %s GeoNames places for %s", count, self.table)

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")

class GeoNamesAlternateNamesPostgresTask(PostgresCopyToTableTask):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "places_geonames_alternate_names"
    columns = [
        ("alternate_name_id", "BIGINT PRIMARY KEY"),
        ("geoname_id", "BIGINT NOT NULL"),
        ("language", "TEXT"),
        ("name", "TEXT NOT NULL"),
        ("is_preferred", "BOOLEAN NOT NULL"),
        ("is_short", "BOOLEAN NOT NULL"),
        ("is_colloquial", "BOOLEAN NOT NULL"),
        ("is_historic", "BOOLEAN NOT NULL"),
        ("valid_from", "TEXT"),
        ("valid_to", "TEXT"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return GeoNamesAlternateNamesSourceTask()

    @property
    def update_id(self):
        return _update_id(self)

    def rows(self):
        count = 0
        for row, version in _source_rows(self.input().path, "alternateNamesV2.txt"):
            count += 1
            yield (
                int(row[0]),
                int(row[1]),
                clean_scalar(row[2]),
                row[3],
                row[4] == "1",
                row[5] == "1",
                row[6] == "1",
                row[7] == "1",
                clean_scalar(row[8]),
                clean_scalar(row[9]),
                version,
            )
        logger.info("Prepared %s GeoNames alternate names for %s", count, self.table)

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")

    def post_copy(self, connection):
        connection.cursor().execute(
            "CREATE INDEX IF NOT EXISTS places_geonames_alternate_names_geoname_id_idx "
            f"ON {self.table} (geoname_id)"
        )


def _source_rows(manifest_path, member):
    dataset_version = read_dataset_version(manifest_path)
    data = fetch_source_dataset(
        s3=get_s3_client(),
        dataset_version=dataset_version,
    )
    version = dataset_version.s3_version_id or dataset_version.sha256
    logger.info("Reading GeoNames %s from source version %s", member, version)
    with ZipFile(BytesIO(data)) as archive:
        with archive.open(member) as source:
            rows = csv.reader(TextIOWrapper(source, encoding="utf-8"), delimiter="\t")
            for row in rows:
                yield row, version


def _update_id(task):
    source = task.requires().output()
    if not source.exists():
        return task.task_id
    return f"{task.task_family}:{read_dataset_version(source.path).sha256}"


def _integer(value):
    value = clean_scalar(value)
    return int(value) if value else None
