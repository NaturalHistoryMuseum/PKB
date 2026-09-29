from __future__ import annotations

import csv
import json
from io import StringIO

import pandas as pd
from luigi.contrib.postgres import CopyToTable

from pkb.config import logger, settings
from pkb.models.people import HUHPersonDeduplicator
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.sources.people.harvard import HarvardIndexCollectorsSourceTask
from pkb.utils import clean_scalar, parse_literal_collection


class HarvardIndexPeoplePostgresTask(CopyToTable):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "people_harvard_index"

    columns = [
        ("harvard_index_id", "TEXT PRIMARY KEY"),
        ("harvard_index_ids", "JSONB NOT NULL"),
        ("source_record_count", "INTEGER NOT NULL"),
        ("name", "TEXT"),
        ("full_name", "TEXT"),
        ("standard_name", "TEXT"),
        ("name_normalised", "TEXT"),
        ("surname", "TEXT"),
        ("given_names", "TEXT"),
        ("initials", "TEXT"),
        ("author_abbreviation", "TEXT"),
        ("birth_year", "TEXT"),
        ("death_year", "TEXT"),
        ("geography_iso", "JSONB NOT NULL"),
        ("herbaria_code", "JSONB NOT NULL"),
        ("institution_uuid", "JSONB NOT NULL"),
        ("url", "TEXT"),
        ("source_records", "JSONB NOT NULL"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return HarvardIndexCollectorsSourceTask()

    @property
    def update_id(self):
        source = self.requires().output()
        if not source.exists():
            return self.task_id
        return f"{self.task_family}:{read_dataset_version(source.path).sha256}"

    def rows(self):
        dataset_version = read_dataset_version(self.input().path)
        data = fetch_source_dataset(
            s3=get_s3_client(),
            dataset_version=dataset_version,
        )
        version = dataset_version.s3_version_id or dataset_version.sha256
        logger.info("Reading Harvard Index people from source version %s", version)

        people = pd.DataFrame(
            record
            for row in csv.DictReader(StringIO(data.decode("utf-8-sig")))
            if (record := _person_record(row))
        )
        result = HUHPersonDeduplicator().deduplicate(people)
        mapped = result.people.merge(
            result.deduplication_map[
                ["harvardIndexID", "canonicalHarvardIndexID"]
            ],
            on="harvardIndexID",
        )

        for canonical_id, group in mapped.groupby(
            "canonicalHarvardIndexID",
            sort=True,
        ):
            canonical = group[group["harvardIndexID"].eq(canonical_id)].iloc[0]
            values = {
                column: sorted(
                    {
                        item
                        for items in group[column]
                        for item in items
                    }
                )
                for column in ["geographyISO", "herbariaCode", "institutionUUID"]
            }

            yield (
                canonical_id,
                json.dumps(sorted(group["harvardIndexID"].tolist())),
                len(group),
                canonical["name"],
                canonical["fullName"],
                canonical["standardName"],
                canonical["nameNormalised"],
                canonical["surname"],
                canonical["givenNamesText"],
                canonical["initialsText"],
                canonical["authorAbbreviation"],
                canonical["birthYear"],
                canonical["deathYear"],
                json.dumps(values["geographyISO"]),
                json.dumps(values["herbariaCode"]),
                json.dumps(values["institutionUUID"]),
                canonical["url"],
                json.dumps(group["sourceRecord"].tolist()),
                version,
            )

        logger.info(
            "Harvard Index deduplication: %s source records -> %s people "
            "(%s duplicates removed across %s clusters)",
            len(people),
            mapped["canonicalHarvardIndexID"].nunique(),
            len(people) - mapped["canonicalHarvardIndexID"].nunique(),
            len(result.clusters),
        )

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")


def _person_record(row) -> dict | None:
    source_record = {key: clean_scalar(value) for key, value in row.items()}
    harvard_index_id = source_record.get("id")
    if not harvard_index_id:
        return None

    full_name = source_record.get("Full Name")
    standard_name = source_record.get("Standard/Label Name")
    return {
        "harvardIndexID": harvard_index_id,
        "name": standard_name or full_name or source_record.get("Name"),
        "fullName": full_name,
        "standardName": standard_name,
        "authorAbbreviation": source_record.get("B & P Author Abbrev."),
        "dateOfBirth": source_record.get("Date of birth"),
        "dateOfDeath": source_record.get("Date of death"),
        "birthYear": source_record.get("birthYear"),
        "deathYear": source_record.get("deathYear"),
        "geographyISO": parse_literal_collection(source_record.get("geographyISO")),
        "herbariaCode": parse_literal_collection(source_record.get("herbariaCode")),
        "institutionUUID": parse_literal_collection(source_record.get("institutionUUID")),
        "url": source_record.get("URL"),
        "sourceRecord": source_record,
    }
