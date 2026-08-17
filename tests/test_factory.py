import pytest

from pkb.graph.factory import get_graph_writer
from pkb.graph.neo4j_writer import Neo4jWriter


def test_unknown_backend(monkeypatch):
    monkeypatch.setattr("pkb.graph.factory.settings.graph_backend", "banana")

    with pytest.raises(ValueError, match="Unsupported graph backend"):
        get_graph_writer()


def test_factory_returns_neo4j_writer(monkeypatch):
    monkeypatch.setattr("pkb.graph.factory.settings.graph_backend", "neo4j")
    monkeypatch.setattr("pkb.graph.factory.settings.neo4j_uri", "bolt://localhost:7687")
    monkeypatch.setattr("pkb.graph.factory.settings.neo4j_user", "neo4j")
    monkeypatch.setattr("pkb.graph.factory.settings.neo4j_password", "password")
    monkeypatch.setattr("pkb.graph.factory.settings.neo4j_database", "neo4j")

    writer = get_graph_writer()

    assert isinstance(writer, Neo4jWriter)


def test_factory_rejects_inactive_janus_backend(monkeypatch):
    monkeypatch.setattr("pkb.graph.factory.settings.graph_backend", "janus")
    monkeypatch.setattr("pkb.graph.factory.settings.graph_url", "ws://localhost:8182/gremlin")

    with pytest.raises(ValueError, match="JanusGraph backend is not active"):
        get_graph_writer()
