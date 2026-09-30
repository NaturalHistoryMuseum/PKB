from __future__ import annotations

import luigi
import pandas as pd
import psycopg2

from pkb.config import logger, settings
from pkb.models.people import match_people_by_identifier
from pkb.tasks.base import BaseTask
from pkb.tasks.load.postgres.bionomia import BionomiaPeoplePostgresTask
from pkb.tasks.load.postgres.harvard import HarvardIndexPeoplePostgresTask
from pkb.tasks.load.postgres.wikidata import WikiDataPeoplePostgresTask


PEOPLE_DIR = settings.intermediate_dir / "people"


class PeopleIdentifierMatchesTask(BaseTask):
    def requires(self):
        return [
            BionomiaPeoplePostgresTask(),
            HarvardIndexPeoplePostgresTask(),
            WikiDataPeoplePostgresTask(),
        ]

    def output(self):
        return luigi.LocalTarget(PEOPLE_DIR / "people_identifier_matches.csv")

    def run(self):
        connection = psycopg2.connect(
            host=settings.postgres_host,
            port=settings.postgres_port,
            dbname=settings.postgres_database,
            user=settings.postgres_user,
            password=settings.postgres_password,
        )
        try:
            people = _read_dataframe(connection, PEOPLE_QUERY)
        finally:
            connection.close()

        matches = match_people_by_identifier(people)
        matches.to_csv(self.output().path, index=False)
        logger.info("Wrote %s identifier matches to %s", len(matches), self.output().path)


def _read_dataframe(connection, query: str) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query)
        return pd.DataFrame(
            cursor.fetchall(),
            columns=[column.name for column in cursor.description],
        )


PEOPLE_QUERY = """
    SELECT
        'bionomia' AS source,
        bionomia_id AS source_id,
        name,
        jsonb_build_array(name, label_name)
            || COALESCE(to_jsonb(string_to_array(other_names, '|')), '[]'::JSONB)
            AS name_variants,
        name_normalised,
        surname,
        given_names,
        initials,
        NULL::TEXT AS birth_date,
        NULL::TEXT AS death_date,
        CASE WHEN country IS NULL THEN '[]'::JSONB ELSE jsonb_build_array(country) END AS geography,
        '[]'::JSONB AS institutions,
        CASE WHEN wikidata_id IS NULL THEN '[]'::JSONB ELSE jsonb_build_array(wikidata_id) END AS wikidata_ids,
        '[]'::JSONB AS harvard_index_ids,
        jsonb_build_array(bionomia_id) AS bionomia_ids
    FROM people_bionomia

    UNION ALL

    SELECT
        'harvard_index',
        harvard_index_id,
        name,
        jsonb_build_array(name, full_name, standard_name),
        name_normalised,
        surname,
        given_names,
        initials,
        birth_year,
        death_year,
        geography_iso,
        institution_uuid,
        '[]'::JSONB,
        harvard_index_ids,
        '[]'::JSONB
    FROM people_harvard_index

    UNION ALL

    SELECT
        'wikidata',
        wikidata_id,
        name,
        jsonb_build_array(name, birth_name) || aliases,
        name_normalised,
        surname,
        given_names,
        initials,
        date_of_birth,
        date_of_death,
        countries_of_citizenship,
        employers,
        jsonb_build_array(wikidata_id),
        CASE WHEN harvard_index_id IS NULL THEN '[]'::JSONB ELSE jsonb_build_array(harvard_index_id) END,
        CASE WHEN bionomia_id IS NULL THEN '[]'::JSONB ELSE jsonb_build_array(bionomia_id) END
    FROM people_wikidata
"""
