from __future__ import annotations

import json

from pkb.config import logger, settings
from pkb.models.people.names import parse_person_name
from pkb.storage import read_dataset_version
from pkb.tasks.base import PostgresCopyToTableTask
from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorEntitiesSourceTask,
    WikiDataCollectorPropertyEntitiesSourceTask,
    read_entities_from_manifest,
)
from pkb.wikidata.parsers import label, parse_collector_entity


class WikiDataPeoplePostgresTask(PostgresCopyToTableTask):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "people_wikidata"

    columns = [
        ("wikidata_id", "TEXT PRIMARY KEY"),
        ("name", "TEXT"),
        ("aliases", "JSONB NOT NULL"),
        ("birth_name", "TEXT"),
        ("date_of_birth", "TEXT"),
        ("date_of_death", "TEXT"),
        ("viaf_id", "TEXT"),
        ("author_abbreviation", "TEXT"),
        ("harvard_index_id", "TEXT"),
        ("bionomia_id", "TEXT"),
        ("ipni_id", "TEXT"),
        ("isni_id", "TEXT"),
        ("employer_wikidata_ids", "JSONB NOT NULL"),
        ("employers", "JSONB NOT NULL"),
        ("country_wikidata_ids", "JSONB NOT NULL"),
        ("countries_of_citizenship", "JSONB NOT NULL"),
        ("sex_or_gender_wikidata_id", "TEXT"),
        ("sex_or_gender", "TEXT"),
        ("name_normalised", "TEXT"),
        ("surname", "TEXT"),
        ("given_names", "TEXT"),
        ("initials", "TEXT"),
        ("dataset_version", "TEXT NOT NULL"),
    ]

    def requires(self):
        return {
            "collectors": WikiDataCollectorEntitiesSourceTask(),
            "property_entities": WikiDataCollectorPropertyEntitiesSourceTask(),
        }

    @property
    def update_id(self):
        sources = self.requires().values()
        if not all(source.output().exists() for source in sources):
            return self.task_id
        versions = [
            read_dataset_version(source.output().path).retrieved_at.isoformat()
            for source in self.requires().values()
        ]
        return f"{self.task_family}:{':'.join(versions)}"

    def rows(self):
        inputs = self.input()
        collector_version = read_dataset_version(inputs["collectors"].path)
        collector_entities = read_entities_from_manifest(inputs["collectors"].path)
        property_entities = read_entities_from_manifest(
            inputs["property_entities"].path
        )
        labels_by_qid = {
            qid: entity_label
            for qid, entity in property_entities.items()
            if (entity_label := label(entity))
        }
        version = collector_version.retrieved_at.isoformat()
        count = 0

        for qid in sorted(collector_entities):
            entity = collector_entities[qid]
            if not entity or entity.get("missing"):
                continue

            person = parse_collector_entity(qid, entity, labels_by_qid)
            parsed_name = parse_person_name(person["name"])
            count += 1
            yield (
                qid,
                person["name"],
                json.dumps(person["aliases"]),
                person["birthName"],
                person["dateOfBirth"],
                person["dateOfDeath"],
                person["viafID"],
                person["authorAbbreviation"],
                person["harvardIndexID"],
                person["bionomiaID"],
                person["ipniID"],
                person["isniID"],
                json.dumps(person["employerWikidataIDs"]),
                json.dumps(person["employers"]),
                json.dumps(person["countryWikidataIDs"]),
                json.dumps(person["countriesOfCitizenship"]),
                person["sexOrGenderWikidataID"],
                person["sexOrGender"],
                parsed_name.normalised if parsed_name else None,
                parsed_name.surname if parsed_name else None,
                parsed_name.given_names_text if parsed_name else None,
                parsed_name.initials_text if parsed_name else None,
                version,
            )

        logger.info("Prepared %s Wikidata people for %s", count, self.table)

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")
