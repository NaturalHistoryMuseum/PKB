import pytest

from pkb.graph.janus_client import janus_connection


@pytest.mark.integration
def test_can_connect():
    with janus_connection() as g:
        assert g.V().count().next() >= 0