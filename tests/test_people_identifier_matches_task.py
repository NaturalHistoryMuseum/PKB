import luigi
import pandas as pd

from pkb.models.people import (
    build_people_identifier_map,
    match_people_by_identifier,
    match_people_by_logistic_regression,
)
from pkb.tasks.build.people.identifier_matches import PeopleIdentifierMatchesTask


PEOPLE = pd.DataFrame(
    [
        {
            "source": "bionomia",
            "source_id": "B1",
            "name": "Jane Collector",
            "name_variants": ["Jane Collector", "Jane Gatherer"],
            "name_normalised": "jane collector",
            "surname": "collector",
            "given_names": "jane",
            "initials": "j",
            "birth_date": None,
            "death_date": None,
            "geography": ["United Kingdom"],
            "institutions": [],
            "wikidata_ids": ["Q1"],
            "harvard_index_ids": [],
            "bionomia_ids": ["B1"],
        },
        {
            "source": "harvard_index",
            "source_id": "H1",
            "name": "Collector, Jane",
            "name_variants": ["Collector, Jane"],
            "name_normalised": "jane collector",
            "surname": "collector",
            "given_names": "jane",
            "initials": "j",
            "birth_date": "1900",
            "death_date": "1980",
            "geography": ["GB"],
            "institutions": ["uuid-1"],
            "wikidata_ids": [],
            "harvard_index_ids": ["H1"],
            "bionomia_ids": [],
        },
        {
            "source": "wikidata",
            "source_id": "Q1",
            "name": "Jane Collector",
            "name_variants": ["Jane Collector", "Jane Gatherer"],
            "name_normalised": "jane collector",
            "surname": "collector",
            "given_names": "jane",
            "initials": "j",
            "birth_date": "+1900-01-01T00:00:00Z",
            "death_date": "+1980-01-01T00:00:00Z",
            "geography": ["United Kingdom"],
            "institutions": ["Example Herbarium"],
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
    assert "left_name_variants" in matches
    assert "right_name_variants" in matches
    assert "left_institutions" in matches
    assert "right_institutions" in matches


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


def test_logistic_matcher_adds_name_match_without_shared_identifier():
    people = []

    def add(source, source_id, first_name, surname, wikidata_ids=()):
        name = f"{first_name} {surname}"
        people.append(
            {
                "source": source,
                "source_id": source_id,
                "name": name,
                "name_variants": [name],
                "name_normalised": name.lower(),
                "surname": surname.lower(),
                "given_names": first_name.lower(),
                "initials": first_name[0].lower(),
                "birth_date": None,
                "death_date": None,
                "geography": [],
                "institutions": [],
                "wikidata_ids": list(wikidata_ids),
                "harvard_index_ids": [],
                "bionomia_ids": [],
            }
        )

    for index in range(4):
        add("bionomia", f"BP{index}", "Alice", f"Positive{index}", [f"QP{index}"])
        add("wikidata", f"QP{index}", "Alice", f"Positive{index}", [f"QP{index}"])
        add("bionomia", f"BN{index}", "Alice", f"Negative{index}", [f"QL{index}"])
        add("wikidata", f"QR{index}", "Bob", f"Negative{index}", [f"QR{index}"])

    add("harvard_index", "HT", "Clara", "Target")
    add("wikidata", "QT", "Clara", "Target")
    people = pd.DataFrame(people)
    identifier_matches = match_people_by_identifier(people)

    matches = match_people_by_logistic_regression(
        people,
        identifier_matches,
        pd.DataFrame(columns=["key", "name"]),
        threshold=0.5,
    )

    assert (matches["left_source_id"] == "HT").any()
    assert (matches["right_source_id"] == "QT").any()


def test_build_people_identifier_map_keeps_only_identifiers():
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

    identifiers = build_people_identifier_map(
        PEOPLE,
        identifier_matches,
        logistic_matches,
    )

    assert identifiers.columns.tolist() == ["person_id", "source", "source_id"]
    assert identifiers["person_id"].nunique() == 1
