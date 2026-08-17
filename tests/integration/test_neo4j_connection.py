import pytest

from pkb.graph.neo4j_client import neo4j_connection


@pytest.mark.integration
def test_can_connect():
    with neo4j_connection() as driver:
        result = driver.execute_query(
            "RETURN 1 AS n"
        )

        assert result.records[0]["n"] == 1