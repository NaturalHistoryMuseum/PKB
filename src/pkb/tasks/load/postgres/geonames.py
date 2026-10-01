from __future__ import annotations

from pkb.config import logger, settings
from pkb.storage import iter_source_dataset_rows, read_dataset_version
from pkb.tasks.base import PostgresCopyToTableTask
from pkb.tasks.sources.places.geonames import (
    GeoNamesAdmin1CodesSourceTask,
    GeoNamesAdmin2CodesSourceTask,
    GeoNamesAlternateNamesSourceTask,
    GeoNamesHierarchySourceTask,
    GeoNamesPlacesSourceTask,
)
from pkb.utils import clean_integer, clean_scalar


class _GeoNamesPostgresTask(PostgresCopyToTableTask):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password

    @property
    def update_id(self):
        source = self.requires().output()
        if not source.exists():
            return self.task_id
        return f"{self.task_family}:{read_dataset_version(source.path).sha256}"

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")


class GeoNamesPlacesPostgresTask(_GeoNamesPostgresTask):
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

    def rows(self):
        count = 0
        for row, version in iter_source_dataset_rows(
            self.input().path, "allCountries.txt"
        ):
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
                clean_integer(row[14]),
                clean_integer(row[15]),
                clean_integer(row[16]),
                clean_scalar(row[17]),
                clean_scalar(row[18]),
                version,
            )
        logger.info("Prepared %s GeoNames places for %s", count, self.table)


class GeoNamesAlternateNamesPostgresTask(_GeoNamesPostgresTask):
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

    def rows(self):
        count = 0
        for row, version in iter_source_dataset_rows(
            self.input().path, "alternateNamesV2.txt"
        ):
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

    def post_copy(self, connection):
        connection.cursor().execute(
            "CREATE INDEX IF NOT EXISTS places_geonames_alternate_names_geoname_id_idx "
            f"ON {self.table} (geoname_id)"
        )


class GeoNamesHierarchyPostgresTask(_GeoNamesPostgresTask):
    table = "places_geonames_hierarchy"
    columns = [
        ("parent_geoname_id", "BIGINT NOT NULL"),
        ("child_geoname_id", "BIGINT NOT NULL"),
        ("relationship_type", "TEXT"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return GeoNamesHierarchySourceTask()

    def rows(self):
        count = 0
        for row, version in iter_source_dataset_rows(
            self.input().path, "hierarchy.txt"
        ):
            count += 1
            yield int(row[0]), int(row[1]), clean_scalar(row[2]), version
        logger.info("Prepared %s GeoNames relationships for %s", count, self.table)

    def post_copy(self, connection):
        cursor = connection.cursor()
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {self.table}_parent_idx "
            f"ON {self.table} (parent_geoname_id)"
        )
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS {self.table}_child_idx "
            f"ON {self.table} (child_geoname_id)"
        )


class _GeoNamesAdminCodesPostgresTask(_GeoNamesPostgresTask):
    source_task = None
    columns = [
        ("code", "TEXT PRIMARY KEY"),
        ("name", "TEXT NOT NULL"),
        ("ascii_name", "TEXT"),
        ("geoname_id", "BIGINT NOT NULL"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return self.source_task()

    def rows(self):
        count = 0
        for row, version in iter_source_dataset_rows(self.input().path):
            count += 1
            yield row[0], row[1], clean_scalar(row[2]), int(row[3]), version
        logger.info("Prepared %s GeoNames admin codes for %s", count, self.table)

    def post_copy(self, connection):
        connection.cursor().execute(
            f"CREATE INDEX IF NOT EXISTS {self.table}_geoname_id_idx "
            f"ON {self.table} (geoname_id)"
        )


class GeoNamesAdmin1CodesPostgresTask(_GeoNamesAdminCodesPostgresTask):
    source_task = GeoNamesAdmin1CodesSourceTask
    table = "places_geonames_admin1_codes"


class GeoNamesAdmin2CodesPostgresTask(_GeoNamesAdminCodesPostgresTask):
    source_task = GeoNamesAdmin2CodesSourceTask
    table = "places_geonames_admin2_codes"
