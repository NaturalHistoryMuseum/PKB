import luigi
import pandas as pd

from pkb.tasks.load.postgres.people import PeopleIdentifiersPostgresTask


def test_people_identifiers_postgres_rows(monkeypatch, tmp_path):
    import pkb.tasks.load.postgres.people as task_module

    people = pd.DataFrame(
        [
            {"source": "bionomia", "source_id": "B1"},
            {"source": "harvard_index", "source_id": "H1"},
            {"source": "wikidata", "source_id": "Q1"},
        ]
    )
    identifier_matches = pd.DataFrame(
        [
            {
                "left_source": "bionomia",
                "left_source_id": "B1",
                "right_source": "wikidata",
                "right_source_id": "Q1",
            }
        ]
    )
    logistic_matches = pd.DataFrame(
        [
            {
                "left_source": "harvard_index",
                "left_source_id": "H1",
                "right_source": "wikidata",
                "right_source_id": "Q1",
            }
        ]
    )
    messages = []
    monkeypatch.setattr(task_module, "read_people", lambda: people)
    monkeypatch.setattr(pd, "read_parquet", lambda path: pd.DataFrame())
    monkeypatch.setattr(
        task_module,
        "match_people_by_identifier",
        lambda people: identifier_matches,
    )
    monkeypatch.setattr(
        task_module,
        "match_people_by_logistic_regression",
        lambda *args: logistic_matches,
    )
    monkeypatch.setattr(
        task_module.logger,
        "info",
        lambda message, *args: messages.append(message % args),
    )
    monkeypatch.setattr(
        PeopleIdentifiersPostgresTask,
        "input",
        lambda self: {"institutions": luigi.LocalTarget(tmp_path / "institutions.parquet")},
    )

    rows = list(PeopleIdentifiersPostgresTask().rows())

    assert len(rows) == 3
    assert len({row[0] for row in rows}) == 1
    assert "Matched 1 pairs by identifier" in messages
    assert "Matched 1 additional pairs by logistic regression" in messages
    assert "Resolved 3 source records into 1 people; 1 overlap datasets" in messages
