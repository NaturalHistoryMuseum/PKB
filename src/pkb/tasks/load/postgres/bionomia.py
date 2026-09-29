from __future__ import annotations

import csv
from io import StringIO

from luigi.contrib.postgres import CopyToTable

from pkb.config import settings
from pkb.models.people.names import parse_person_name
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.sources.people.bionomia import BionomiaPublicProfilesSourceTask


class BionomiaPeoplePostgresTask(CopyToTable):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "people_source.bionomia_people"

    columns = [
        ("bionomia_id", "TEXT PRIMARY KEY"),
        ("family", "TEXT"),
        ("given", "TEXT"),
        ("particle", "TEXT"),
        ("other_names", "TEXT"),
        ("label_name", "TEXT"),
        ("country", "TEXT"),
        ("keywords", "TEXT"),
        ("wikidata_id", "TEXT"),
        ("orcid_id", "TEXT"),
        ("url", "TEXT"),
        ("name", "TEXT"),
        ("name_normalised", "TEXT"),
        ("surname", "TEXT"),
        ("given_names", "TEXT"),
        ("initials", "TEXT"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return BionomiaPublicProfilesSourceTask()

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

        for row in csv.DictReader(StringIO(data.decode("utf-8-sig"))):
            orcid = _clean(row.get("ORCID"))
            url = _clean(row.get("URL"))
            bionomia_id = orcid or (url.rstrip("/").rsplit("/", 1)[-1] if url else None)
            if not bionomia_id:
                continue

            name = _clean(row.get("LabelName")) or " ".join(
                filter(
                    None,
                    [
                        _clean(row.get("Given")),
                        _clean(row.get("Particle")),
                        _clean(row.get("Family")),
                    ],
                )
            )
            parsed = parse_person_name(name)

            yield (
                bionomia_id,
                _clean(row.get("Family")),
                _clean(row.get("Given")),
                _clean(row.get("Particle")),
                _clean(row.get("OtherNames")),
                _clean(row.get("LabelName")),
                _clean(row.get("Country")),
                _clean(row.get("Keywords")),
                _clean(row.get("wikidata")),
                orcid,
                url,
                name or None,
                parsed.normalised if parsed else None,
                parsed.surname if parsed else None,
                parsed.given_names_text if parsed else None,
                parsed.initials_text if parsed else None,
                version,
            )

    def create_table(self, connection):
        connection.cursor().execute("CREATE SCHEMA IF NOT EXISTS people_source")
        super().create_table(connection)

    def init_copy(self, connection):
        cursor = connection.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS people_source")
        cursor.execute(f"TRUNCATE TABLE {self.table}")


def _clean(value) -> str | None:
    value = str(value).strip() if value is not None else ""
    return value or None
