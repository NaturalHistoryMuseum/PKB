from luigi.contrib.postgres import CopyToTable

from pkb.tasks.base import PostgresCopyToTableTask


class _Cursor:
    def __init__(self):
        self.queries = []

    def execute(self, query, parameters):
        self.queries.append((query, parameters))


class _Connection:
    def __init__(self):
        self.autocommit = False
        self.closed = False
        self.cursor_instance = _Cursor()

    def cursor(self):
        return self.cursor_instance

    def close(self):
        self.closed = True


class _Target:
    marker_table = "table_updates"
    update_id = "test-update"

    def __init__(self, exists=True):
        self.connection = _Connection()
        self._exists = exists

    def connect(self):
        return self.connection

    def exists(self, connection=None):
        return self._exists


class _PostgresTask(PostgresCopyToTableTask):
    host = "localhost"
    database = "pkb"
    user = "pkb"
    password = "pkb"
    table = "test_table"
    columns = [("value", "TEXT")]
    target = None

    def output(self):
        return self.target


def test_postgres_task_force_reports_incomplete():
    normal = _PostgresTask(force=False)
    normal.target = _Target()
    forced = _PostgresTask(force=True)
    forced.target = _Target()

    assert normal.complete()
    assert not forced.complete()


def test_postgres_task_force_removes_marker_before_run(monkeypatch):
    target = _Target()
    copied = []
    monkeypatch.setattr(CopyToTable, "run", lambda self: copied.append(True))
    task = _PostgresTask(force=True)
    task.target = target

    task.run()

    assert target.connection.autocommit
    assert target.connection.closed
    assert target.connection.cursor_instance.queries == [
        (
            "DELETE FROM table_updates WHERE update_id = %s",
            ("test-update",),
        )
    ]
    assert copied == [True]
