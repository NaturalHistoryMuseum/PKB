from luigi.contrib.postgres import PostgresQuery

from pkb.config import settings
from pkb.tasks.load.postgres.bionomia import BionomiaPeoplePostgresTask
from pkb.tasks.load.postgres.harvard import HarvardIndexPeoplePostgresTask
from pkb.tasks.load.postgres.wikidata import WikiDataPeoplePostgresTask


class PeopleIdentifierMatchesPostgresTask(PostgresQuery):
    host = settings.postgres_host
    port = settings.postgres_port
    database = settings.postgres_database
    user = settings.postgres_user
    password = settings.postgres_password
    table = "people_identifier_matches"

    query = """
        CREATE TABLE IF NOT EXISTS people_identifier_matches (
            left_source TEXT NOT NULL,
            left_id TEXT NOT NULL,
            right_source TEXT NOT NULL,
            right_id TEXT NOT NULL,
            identifier_type TEXT NOT NULL,
            identifier_value TEXT NOT NULL,
            PRIMARY KEY (
                left_source,
                left_id,
                right_source,
                right_id,
                identifier_type,
                identifier_value
            )
        );

        TRUNCATE people_identifier_matches;

        INSERT INTO people_identifier_matches
        SELECT
            'bionomia',
            b.bionomia_id,
            'wikidata',
            w.wikidata_id,
            'wikidata_id',
            w.wikidata_id
        FROM people_bionomia b
        JOIN people_wikidata w ON w.wikidata_id = b.wikidata_id

        UNION

        SELECT
            'bionomia',
            b.bionomia_id,
            'wikidata',
            w.wikidata_id,
            'bionomia_id',
            w.bionomia_id
        FROM people_bionomia b
        JOIN people_wikidata w ON w.bionomia_id = b.bionomia_id

        UNION

        SELECT
            'harvard_index',
            h.harvard_index_id,
            'wikidata',
            w.wikidata_id,
            'harvard_index_id',
            w.harvard_index_id
        FROM people_harvard_index h
        JOIN people_wikidata w ON h.harvard_index_ids ? w.harvard_index_id;
    """

    def requires(self):
        return [
            BionomiaPeoplePostgresTask(),
            HarvardIndexPeoplePostgresTask(),
            WikiDataPeoplePostgresTask(),
        ]

    @property
    def update_id(self):
        versions = ":".join(task.update_id for task in self.requires())
        return f"{self.task_family}:{versions}"
