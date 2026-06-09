from __future__ import annotations

from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from pkb.config import settings


@contextmanager
def janus_connection() -> Iterator[Any]:
    conn = DriverRemoteConnection(
        settings.graph_url,
        settings.graph_traversal_source,
    )

    try:
        yield traversal().with_(conn)
    finally:
        conn.close()