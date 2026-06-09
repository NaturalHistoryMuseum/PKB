from __future__ import annotations

from collections.abc import Iterable

from pkb.graph.writer import GraphRow, GraphWriter


class JanusGraphWriter(GraphWriter):
    def __init__(self, gremlin_url: str) -> None:
        self.gremlin_url = gremlin_url
        self.connection = None
        self.g = None

    def connect(self) -> None:
        raise NotImplementedError("JanusGraphWriter.connect is not implemented yet.")

    def close(self) -> None:
        raise NotImplementedError("JanusGraphWriter.close is not implemented yet.")

    def create_node_constraint(self, label: str, key_field: str) -> None:
        raise NotImplementedError(
            "JanusGraphWriter.create_node_constraint is not implemented yet."
        )

    def write_nodes(
        self,
        label: str,
        rows: Iterable[GraphRow],
        key_field: str,
        batch_size: int = 1000,
    ) -> None:
        raise NotImplementedError("JanusGraphWriter.write_nodes is not implemented yet.")