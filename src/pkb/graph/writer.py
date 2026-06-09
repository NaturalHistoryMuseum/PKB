from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import Any

GraphRow = dict[str, Any]


class GraphWriter(ABC):
    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def create_node_constraint(self, label: str, key_field: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def write_nodes(
        self,
        label: str,
        rows: Iterable[GraphRow],
        key_field: str,
        batch_size: int = 1000,
    ) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def write_edges(
        self,
        source_label: str,
        target_label: str,
        rel_type: str,
        rows: Iterable[GraphRow],
        source_node_key: str,
        target_node_key: str,
        batch_size: int = 1000,
    ) -> None:
        raise NotImplementedError


def batched(rows: Iterable[GraphRow], batch_size: int) -> Iterable[list[GraphRow]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than 0")

    batch: list[GraphRow] = []

    for row in rows:
        batch.append(row)

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch