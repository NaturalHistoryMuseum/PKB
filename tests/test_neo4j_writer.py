from unittest.mock import MagicMock

import pytest

from pkb.graph.neo4j_writer import Neo4jWriter


def test_not_connected():
    writer = Neo4jWriter(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="password",
    )

    with pytest.raises(RuntimeError):
        writer.write_nodes(
            label="Institution",
            rows=[],
            key_field="institutionKey",
        )


def test_close_without_connect():
    writer = Neo4jWriter(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="password",
    )

    writer.close()


def test_create_constraint_generates_query():
    writer = Neo4jWriter(
        uri="bolt://localhost:7687",
        user="neo4j",
        password="password",
    )

    session = MagicMock()

    writer.driver = MagicMock()
    writer.driver.session.return_value.__enter__.return_value = session

    writer.create_node_constraint(
        label="Institution",
        key_field="institutionKey",
    )

    query = session.run.call_args.args[0]

    assert "Institution" in query
    assert "institutionKey" in query
    assert "UNIQUE" in query