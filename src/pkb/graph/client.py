from contextlib import contextmanager
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal

from pkb.config import settings


@contextmanager
def graph_connection():
    conn = DriverRemoteConnection(
        settings.graph_url,
        settings.graph_traversal_source,
    )

    try:
        yield traversal().with_(conn)
    finally:
        conn.close()