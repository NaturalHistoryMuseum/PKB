from itertools import combinations

import pandas as pd
from luigi.contrib.postgres import CopyToTable

from pkb.config import logger, settings
from pkb.models.people import (
    build_people_identifier_map,
    match_people_by_identifier,
    match_people_by_logistic_regression,
)
from pkb.tasks.build.institutions.grscicol import GRSciCollInstitutionsTSVTask
from pkb.tasks.build.people.identifier_matches import read_people
from pkb.tasks.load.postgres.bionomia import BionomiaPeoplePostgresTask
from pkb.tasks.load.postgres.harvard import HarvardIndexPeoplePostgresTask
from pkb.tasks.load.postgres.wikidata import WikiDataPeoplePostgresTask


class PeopleIdentifiersPostgresTask(CopyToTable):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "people_identifiers"
    columns = [
        ("person_id", "TEXT NOT NULL"),
        ("source", "TEXT NOT NULL"),
        ("source_id", "TEXT NOT NULL"),
    ]

    def requires(self):
        return {
            "bionomia": BionomiaPeoplePostgresTask(),
            "harvard_index": HarvardIndexPeoplePostgresTask(),
            "wikidata": WikiDataPeoplePostgresTask(),
            "institutions": GRSciCollInstitutionsTSVTask(),
        }

    @property
    def update_id(self):
        sources = self.requires()
        versions = [
            sources[name].update_id
            for name in ("bionomia", "harvard_index", "wikidata")
        ]
        return f"{self.task_family}:{':'.join(versions)}"

    def rows(self):
        people = read_people()
        institutions = pd.read_parquet(self.input()["institutions"].path)

        for source, count in people.groupby("source").size().items():
            logger.info("Loaded %s people from %s", count, source)

        identifier_matches = match_people_by_identifier(people)
        identifier_pairs = identifier_matches.drop_duplicates(
            ["left_source", "left_source_id", "right_source", "right_source_id"]
        )
        logistic_matches = match_people_by_logistic_regression(
            people,
            identifier_matches,
            institutions,
        )
        identifiers = build_people_identifier_map(
            people,
            identifier_pairs,
            logistic_matches,
        )

        logger.info("Matched %s pairs by identifier", len(identifier_pairs))
        logger.info("Matched %s additional pairs by logistic regression", len(logistic_matches))
        clusters = identifiers.groupby("person_id")["source"].agg(set)
        logger.info(
            "Resolved %s source records into %s people; %s overlap datasets",
            len(identifiers),
            len(clusters),
            clusters.map(len).gt(1).sum(),
        )
        for left, right in combinations(sorted(people["source"].unique()), 2):
            overlap = clusters.map(lambda sources: {left, right}.issubset(sources)).sum()
            logger.info("%s/%s overlap: %s people", left, right, overlap)

        yield from identifiers.itertuples(index=False, name=None)

    def init_copy(self, connection):
        connection.cursor().execute(f"TRUNCATE TABLE {self.table}")
