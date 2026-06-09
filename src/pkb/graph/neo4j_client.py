from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator

from neo4j import Driver, GraphDatabase

from pkb.config import settings


@contextmanager
def neo4j_connection() -> Iterator[Driver]:
    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(
            settings.neo4j_user,
            settings.neo4j_password,
        ),
    )

    try:
        yield driver
    finally:
        driver.close()