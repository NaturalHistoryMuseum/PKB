from datetime import UTC, datetime

import luigi

from pkb.storage import DatasetVersion
from pkb.tasks.load.postgres.bionomia import BionomiaPeoplePostgresTask


BIONOMIA_CSV = (
    b"Family,Given,Particle,OtherNames,LabelName,Country,Keywords,wikidata,ORCID,URL\n"
    b"Page,Roderic,,Rod Page,,United Kingdom,,Q1,0000-0002-7101-9767,"
    b"https://bionomia.net/0000-0002-7101-9767\n"
)


def test_bionomia_people_postgres_rows(monkeypatch, tmp_path):
    import pkb.tasks.load.postgres.bionomia as task_module

    messages = []
    monkeypatch.setattr(
        task_module.logger,
        "info",
        lambda message, *args: messages.append(message % args),
    )
    monkeypatch.setattr(
        BionomiaPeoplePostgresTask,
        "input",
        lambda self: luigi.LocalTarget(tmp_path / "dataset-version.json"),
    )
    monkeypatch.setattr(
        task_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="bionomia_public_profiles",
            retrieved_at=datetime(2026, 8, 18, tzinfo=UTC),
            source_url="https://bionomia.net/data/bionomia-public-profiles.csv",
            sha256="sha",
            size=len(BIONOMIA_CSV),
            s3_bucket="pkb-source-data",
            s3_key="bionomia/people/public-profiles/source.csv",
            s3_version_id="version-123",
        ),
    )
    monkeypatch.setattr(task_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        task_module,
        "fetch_source_dataset",
        lambda **kwargs: BIONOMIA_CSV,
    )

    task = BionomiaPeoplePostgresTask()
    [values] = list(task.rows())
    row = dict(zip((column[0] for column in task.columns), values))

    assert row["bionomia_id"] == "0000-0002-7101-9767"
    assert row["name"] == "Roderic Page"
    assert row["name_normalised"] == "roderic page"
    assert row["surname"] == "page"
    assert row["given_names"] == "roderic"
    assert row["initials"] == "r"
    assert row["dataset_version"] == "version-123"
    assert messages[-1] == "Prepared 1 Bionomia people for people_bionomia"
