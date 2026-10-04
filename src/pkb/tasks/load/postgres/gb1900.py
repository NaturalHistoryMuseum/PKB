from __future__ import annotations

from pkb.config import logger, settings
from pkb.storage import iter_source_dataset_rows, read_dataset_version
from pkb.tasks.base import PostgresCopyToTableTask
from pkb.tasks.sources.places.gb1900 import GB1900PlacesSourceTask
from pkb.utils import clean_scalar


GB1900_ARCHIVE_MEMBER = (
    "GB1900_gazetteer_complete_july_2018/"
    "gb1900_gazetteer_complete_july_2018.csv"
)
GB1900_COLUMNS = [
    "pin_id",
    "final_text",
    "nation",
    "local_authority",
    "parish",
    "osgb_east",
    "osgb_north",
    "latitude",
    "longitude",
    "notes",
]


class GB1900PlacesPostgresTask(PostgresCopyToTableTask):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "places_gb1900"
    columns = [
        ("pin_id", "TEXT PRIMARY KEY"),
        ("name", "TEXT NOT NULL"),
        ("nation", "TEXT NOT NULL"),
        ("local_authority", "TEXT"),
        ("parish", "TEXT"),
        ("osgb_east", "DOUBLE PRECISION"),
        ("osgb_north", "DOUBLE PRECISION"),
        ("latitude", "DOUBLE PRECISION"),
        ("longitude", "DOUBLE PRECISION"),
        ("notes", "TEXT"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return GB1900PlacesSourceTask()

    @property
    def update_id(self):
        source = self.requires().output()
        if not source.exists():
            return self.task_id
        return f"{self.task_family}:{read_dataset_version(source.path).sha256}"

    def rows(self):
        rows = iter_source_dataset_rows(
            self.input().path,
            GB1900_ARCHIVE_MEMBER,
            delimiter=",",
            encoding="utf-16",
        )
        header, _ = next(rows)
        if header != GB1900_COLUMNS:
            raise ValueError(f"Unexpected GB1900 columns: {header}")

        count = 0
        for row, version in rows:
            count += 1
            yield (
                row[0],
                row[1],
                row[2],
                clean_scalar(row[3]),
                clean_scalar(row[4]),
                float(row[5]),
                float(row[6]),
                float(row[7]),
                float(row[8]),
                clean_scalar(row[9]),
                version,
            )
        logger.info("Prepared %s GB1900 places for %s", count, self.table)

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")

    def post_copy(self, connection):
        connection.cursor().execute(
            "CREATE INDEX IF NOT EXISTS places_gb1900_name_trgm "
            f"ON {self.table} USING GIN (lower(name) gin_trgm_ops)"
        )
