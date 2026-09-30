from __future__ import annotations

import csv
from io import StringIO

from pkb.config import logger, settings
from pkb.models.people.names import parse_person_name
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.base import PostgresCopyToTableTask
from pkb.tasks.sources.people.bionomia import BionomiaPublicProfilesSourceTask
from pkb.utils import clean_scalar


class BionomiaPeoplePostgresTask(PostgresCopyToTableTask):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "people_bionomia"

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
        logger.info("Reading Bionomia people from source version %s", version)
        count = 0

        for row in csv.DictReader(StringIO(data.decode("utf-8-sig"))):
            orcid = clean_scalar(row.get("ORCID"))
            url = clean_scalar(row.get("URL"))
            bionomia_id = orcid or (url.rstrip("/").rsplit("/", 1)[-1] if url else None)
            if not bionomia_id:
                continue

            name = clean_scalar(row.get("LabelName")) or " ".join(
                filter(
                    None,
                    [
                        clean_scalar(row.get("Given")),
                        clean_scalar(row.get("Particle")),
                        clean_scalar(row.get("Family")),
                    ],
                )
            )
            parsed = parse_person_name(name)

            count += 1
            yield (
                bionomia_id,
                clean_scalar(row.get("Family")),
                clean_scalar(row.get("Given")),
                clean_scalar(row.get("Particle")),
                clean_scalar(row.get("OtherNames")),
                clean_scalar(row.get("LabelName")),
                clean_scalar(row.get("Country")),
                clean_scalar(row.get("Keywords")),
                clean_scalar(row.get("wikidata")),
                orcid,
                url,
                name or None,
                parsed.normalised if parsed else None,
                parsed.surname if parsed else None,
                parsed.given_names_text if parsed else None,
                parsed.initials_text if parsed else None,
                version,
            )

        logger.info("Prepared %s Bionomia people for %s", count, self.table)

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")
