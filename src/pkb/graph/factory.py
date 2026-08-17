from pkb.config import settings
from pkb.graph.writer import GraphWriter


def get_graph_writer() -> GraphWriter:
    backend = settings.graph_backend.lower()

    if backend == "neo4j":
        from pkb.graph.neo4j_writer import Neo4jWriter

        return Neo4jWriter(
            uri=settings.neo4j_uri,
            user=settings.neo4j_user,
            password=settings.neo4j_password,
            database=settings.neo4j_database,
        )

    if backend == "janus":
        raise ValueError(
            "JanusGraph backend is not active in this refactor. "
            "Set PKB_GRAPH_BACKEND=neo4j."
        )

    raise ValueError(f"Unsupported graph backend: {settings.graph_backend}")
