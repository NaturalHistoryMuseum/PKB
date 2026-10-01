from __future__ import annotations

from pkb.config import logger, settings
from pkb.storage import iter_source_dataset_rows, read_dataset_version
from pkb.tasks.base import PostgresCopyToTableTask, PostgresQueryTask
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


class GeoNamesRelationshipsPostgresTask(PostgresQueryTask):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "places_geonames_relationships"

    def requires(self):
        return [
            GeoNamesPlacesPostgresTask(),
            GeoNamesHierarchyPostgresTask(),
            GeoNamesAdmin1CodesPostgresTask(),
            GeoNamesAdmin2CodesPostgresTask(),
        ]

    @property
    def update_id(self):
        versions = ":".join(task.update_id for task in self.requires())
        return f"{self.task_family}:{versions}"

    query = """
        CREATE TABLE IF NOT EXISTS places_geonames_relationships (
            child_geoname_id BIGINT NOT NULL,
            parent_geoname_id BIGINT NOT NULL,
            relationship_type TEXT NOT NULL,
            PRIMARY KEY (child_geoname_id, parent_geoname_id, relationship_type)
        );

        TRUNCATE TABLE places_geonames_relationships;

        WITH countries AS (
            SELECT country_code, MIN(geoname_id) AS geoname_id
            FROM places_geonames
            WHERE feature_code IN ('PCLI', 'PCLD', 'PCLF', 'PCLS')
              AND country_code IS NOT NULL
            GROUP BY country_code
            HAVING COUNT(*) = 1
        ),
        admin3 AS (
            SELECT country_code, admin1_code, admin2_code, admin3_code,
                   MIN(geoname_id) AS geoname_id
            FROM places_geonames
            WHERE feature_code = 'ADM3'
              AND admin3_code IS NOT NULL
            GROUP BY country_code, admin1_code, admin2_code, admin3_code
            HAVING COUNT(*) = 1
        ),
        admin4 AS (
            SELECT country_code, admin1_code, admin2_code, admin3_code,
                   admin4_code, MIN(geoname_id) AS geoname_id
            FROM places_geonames
            WHERE feature_code = 'ADM4'
              AND admin4_code IS NOT NULL
            GROUP BY country_code, admin1_code, admin2_code, admin3_code,
                     admin4_code
            HAVING COUNT(*) = 1
        ),
        administrative_relationships AS (
            SELECT
                place.geoname_id AS child_geoname_id,
                COALESCE(
                    NULLIF(admin4.geoname_id, place.geoname_id),
                    NULLIF(admin3.geoname_id, place.geoname_id),
                    NULLIF(admin2.geoname_id, place.geoname_id),
                    NULLIF(admin1.geoname_id, place.geoname_id),
                    NULLIF(country.geoname_id, place.geoname_id)
                ) AS parent_geoname_id,
                'ADM' AS relationship_type
            FROM places_geonames AS place
            LEFT JOIN admin4
              ON admin4.country_code = place.country_code
             AND admin4.admin1_code IS NOT DISTINCT FROM place.admin1_code
             AND admin4.admin2_code IS NOT DISTINCT FROM place.admin2_code
             AND admin4.admin3_code IS NOT DISTINCT FROM place.admin3_code
             AND admin4.admin4_code = place.admin4_code
            LEFT JOIN admin3
              ON admin3.country_code = place.country_code
             AND admin3.admin1_code IS NOT DISTINCT FROM place.admin1_code
             AND admin3.admin2_code IS NOT DISTINCT FROM place.admin2_code
             AND admin3.admin3_code = place.admin3_code
            LEFT JOIN places_geonames_admin2_codes AS admin2
              ON admin2.code = place.country_code || '.' || place.admin1_code
                             || '.' || place.admin2_code
            LEFT JOIN places_geonames_admin1_codes AS admin1
              ON admin1.code = place.country_code || '.' || place.admin1_code
            LEFT JOIN countries AS country
              ON country.country_code = place.country_code
        )
        INSERT INTO places_geonames_relationships
        SELECT hierarchy.child_geoname_id,
               hierarchy.parent_geoname_id,
               COALESCE(hierarchy.relationship_type, 'hierarchy')
        FROM places_geonames_hierarchy AS hierarchy
        JOIN places_geonames AS child
          ON child.geoname_id = hierarchy.child_geoname_id
        JOIN places_geonames AS parent
          ON parent.geoname_id = hierarchy.parent_geoname_id
        WHERE hierarchy.child_geoname_id <> hierarchy.parent_geoname_id

        UNION

        SELECT derived.child_geoname_id,
               derived.parent_geoname_id,
               derived.relationship_type
        FROM administrative_relationships AS derived
        WHERE derived.parent_geoname_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM places_geonames_hierarchy AS hierarchy
              WHERE hierarchy.child_geoname_id = derived.child_geoname_id
                AND hierarchy.relationship_type = 'ADM'
          )

        ON CONFLICT DO NOTHING;

        CREATE INDEX IF NOT EXISTS places_geonames_relationships_parent_idx
        ON places_geonames_relationships (parent_geoname_id);
    """

    def run(self):
        super().run()
        connection = self.output().connect()
        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.table}")
            logger.info("Built %s GeoNames relationships", cursor.fetchone()[0])
        finally:
            connection.close()
