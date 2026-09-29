from pkb.tasks.load.postgres.bionomia import BionomiaPeoplePostgresTask
from pkb.tasks.load.postgres.harvard import HarvardIndexPeoplePostgresTask
from pkb.tasks.load.postgres.people import PeopleIdentifierMatchesPostgresTask
from pkb.tasks.load.postgres.wikidata import WikiDataPeoplePostgresTask


def test_people_identifier_matches_uses_the_three_people_tables():
    task = PeopleIdentifierMatchesPostgresTask()

    assert [type(dependency) for dependency in task.requires()] == [
        BionomiaPeoplePostgresTask,
        HarvardIndexPeoplePostgresTask,
        WikiDataPeoplePostgresTask,
    ]
    assert "w.wikidata_id = b.wikidata_id" in task.query
    assert "w.bionomia_id = b.bionomia_id" in task.query
    assert "h.harvard_index_ids ? w.harvard_index_id" in task.query
