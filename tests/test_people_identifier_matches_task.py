import luigi
import pandas as pd

from pkb.models.people import match_people_by_identifier
from pkb.tasks.build.people.identifier_matches import PeopleIdentifierMatchesTask


PEOPLE = pd.DataFrame(
    [
        {
            "source": "bionomia",
            "source_id": "B1",
            "name": "Jane Collector",
            "name_normalised": "jane collector",
            "surname": "collector",
            "given_names": "jane",
            "initials": "j",
            "birth_date": None,
            "death_date": None,
            "geography": ["United Kingdom"],
            "wikidata_ids": ["Q1"],
            "harvard_index_ids": [],
            "bionomia_ids": ["B1"],
        },
        {
            "source": "harvard_index",
            "source_id": "H1",
            "name": "Collector, Jane",
            "name_normalised": "jane collector",
            "surname": "collector",
            "given_names": "jane",
            "initials": "j",
            "birth_date": "1900",
            "death_date": "1980",
            "geography": ["GB"],
            "wikidata_ids": [],
            "harvard_index_ids": ["H1"],
            "bionomia_ids": [],
        },
        {
            "source": "wikidata",
            "source_id": "Q1",
            "name": "Jane Collector",
            "name_normalised": "jane collector",
            "surname": "collector",
            "given_names": "jane",
            "initials": "j",
            "birth_date": "+1900-01-01T00:00:00Z",
            "death_date": "+1980-01-01T00:00:00Z",
            "geography": ["United Kingdom"],
            "wikidata_ids": ["Q1"],
            "harvard_index_ids": ["H1"],
            "bionomia_ids": ["B1"],
        },
    ]
)


def test_match_people_by_identifier_returns_pairs_with_source_fields():
    matches = match_people_by_identifier(PEOPLE)

    assert set(matches["identifier_type"]) == {
        "wikidata_id",
        "harvard_index_id",
        "bionomia_id",
    }
    assert set(matches["left_source_id"]) == {"B1", "H1"}
    assert set(matches["right_source_id"]) == {"Q1"}
    assert matches["left_name_normalised"].eq("jane collector").all()
    assert matches["right_name_normalised"].eq("jane collector").all()


def test_people_identifier_matches_task_writes_csv(monkeypatch, tmp_path):
    import pkb.tasks.build.people.identifier_matches as task_module

    class Connection:
        def close(self):
            pass

    monkeypatch.setattr(task_module.psycopg2, "connect", lambda **kwargs: Connection())
    monkeypatch.setattr(task_module, "_read_dataframe", lambda connection, query: PEOPLE)
    monkeypatch.setattr(
        PeopleIdentifierMatchesTask,
        "output",
        lambda self: luigi.LocalTarget(tmp_path / "identifier_matches.csv"),
    )

    task = PeopleIdentifierMatchesTask()
    task.run()

    matches = pd.read_csv(task.output().path)
    assert len(matches) == 3
