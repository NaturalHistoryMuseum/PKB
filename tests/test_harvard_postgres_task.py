from datetime import UTC, datetime
import json

import luigi

from pkb.storage import DatasetVersion
from pkb.tasks.load.postgres.harvard import HarvardIndexPeoplePostgresTask


HARVARD_CSV = (
    b"id,Name,Standard/Label Name,Full Name,B & P Author Abbrev.,Date of birth,"
    b"Date of death,birthYear,deathYear,geographyISO,herbariaCode,institutionUUID,URL\n"
    b"5,Brach,A. R. Brach,\"Brach, Anthony Robert\",Brach,1963-01-01,,1963,,"
    b"\"{'US'}\",\"{'SYRF'}\",\"['uuid-1']\",https://example.org/5\n"
    b"6,Brach,A. R. Brach,\"Brach, Anthony Robert\",Brach,1963-01-01,,1963,,"
    b"\"{'US'}\",\"{'SYRF'}\",\"['uuid-2']\",https://example.org/6\n"
)


def test_harvard_index_people_postgres_rows(monkeypatch, tmp_path):
    import pkb.tasks.load.postgres.harvard as task_module

    messages = []
    monkeypatch.setattr(
        task_module.logger,
        "info",
        lambda message, *args: messages.append(message % args),
    )
    monkeypatch.setattr(
        HarvardIndexPeoplePostgresTask,
        "input",
        lambda self: luigi.LocalTarget(tmp_path / "dataset-version.json"),
    )
    monkeypatch.setattr(
        task_module,
        "read_dataset_version",
        lambda path: DatasetVersion(
            dataset="harvard_index_collectors",
            retrieved_at=datetime(2026, 8, 18, tzinfo=UTC),
            source_url="archived-local-file:data/assets/harvard-index/huh-collectors.csv",
            sha256="sha",
            size=len(HARVARD_CSV),
            s3_bucket="pkb-source-data",
            s3_key="harvard-index/people/collectors/source.csv",
            s3_version_id="version-123",
        ),
    )
    monkeypatch.setattr(task_module, "get_s3_client", lambda: "s3-client")
    monkeypatch.setattr(
        task_module,
        "fetch_source_dataset",
        lambda **kwargs: HARVARD_CSV,
    )

    task = HarvardIndexPeoplePostgresTask()
    [values] = list(task.rows())
    row = dict(zip((column[0] for column in task.columns), values))

    assert row["harvard_index_id"] == "5"
    assert json.loads(row["harvard_index_ids"]) == ["5", "6"]
    assert row["source_record_count"] == 2
    assert row["name_normalised"] == "anthony robert brach"
    assert row["surname"] == "brach"
    assert row["given_names"] == "anthony robert"
    assert row["initials"] == "ar"
    assert json.loads(row["institution_uuid"]) == ["uuid-1", "uuid-2"]
    assert len(json.loads(row["source_records"])) == 2
    assert row["dataset_version"] == "version-123"
    assert messages[-1].startswith(
        "Harvard Index deduplication: 2 source records -> 1 people "
    )
