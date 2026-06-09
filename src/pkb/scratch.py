
from pkb.graph.client import graph_connection

with graph_connection() as g:
    print(g.V().count().next())