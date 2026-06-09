from __future__ import annotations

from collections.abc import Iterable

from pkb.graph.writer import GraphRow, GraphWriter, batched


class Neo4jWriter(GraphWriter):
    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: str = "neo4j",
    ) -> None:
        self.uri = uri
        self.auth = (user, password)
        self.database = database
        self.driver = None

    def connect(self) -> None:
        from neo4j import GraphDatabase

        self.driver = GraphDatabase.driver(self.uri, auth=self.auth)

    def close(self) -> None:
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    def create_node_constraint(self, label: str, key_field: str) -> None:
        self._ensure_connected()

        query = f"""
        CREATE CONSTRAINT {label.lower()}_{key_field}_unique IF NOT EXISTS
        FOR (n:{label})
        REQUIRE n.{key_field} IS UNIQUE
        """

        with self.driver.session(database=self.database) as session:
            session.run(query)

    def write_nodes(
        self,
        label: str,
        rows: Iterable[GraphRow],
        key_field: str,
        batch_size: int = 1000,
    ) -> None:
        self._ensure_connected()

        query = f"""
        UNWIND $rows AS row
        MERGE (n:{label} {{{key_field}: row.{key_field}}})
        SET n += row
        """

        for batch in batched(rows, batch_size):
            with self.driver.session(database=self.database) as session:
                session.execute_write(
                    lambda tx: tx.run(query, rows=batch).consume()
                )

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
        self._ensure_connected()

        query = f"""
            UNWIND $rows AS row

            OPTIONAL MATCH (source:{source_label}
                {{{source_node_key}: row.source_id}})

            OPTIONAL MATCH (target:{target_label}
                {{{target_node_key}: row.target_id}})

            WITH
                row,
                source,
                target,
                CASE WHEN source IS NULL THEN row.source_id ELSE NULL END AS missing_source,
                CASE WHEN target IS NULL THEN row.target_id ELSE NULL END AS missing_target

            WITH
                collect(missing_source) AS missing_sources,
                collect(missing_target) AS missing_targets,
                collect({{source: source, target: target}}) AS pairs

            WITH
                [x IN missing_sources WHERE x IS NOT NULL] AS missing_sources,
                [x IN missing_targets WHERE x IS NOT NULL] AS missing_targets,
                [p IN pairs WHERE p.source IS NOT NULL AND p.target IS NOT NULL] AS valid_pairs

            CALL {{
                WITH valid_pairs, missing_sources, missing_targets

                WITH valid_pairs
                WHERE size(missing_sources) = 0 AND size(missing_targets) = 0

                UNWIND valid_pairs AS pair
                WITH pair.source AS source, pair.target AS target
                MERGE (source)-[:{rel_type}]->(target)

                RETURN count(*) AS relationships_written
            }}

            RETURN
                coalesce(relationships_written, 0) AS relationships_written,
                missing_sources[..20] AS missing_sources,
                missing_targets[..20] AS missing_targets,
                size(missing_sources) AS missing_source_count,
                size(missing_targets) AS missing_target_count
        """

        total_written = 0

        for batch in batched(rows, batch_size):
            with self.driver.session(database=self.database) as session:
                result = session.execute_write(
                    lambda tx: tx.run(query, rows=batch).single()
                )
                print('*****')
                print(result)
                print('*****')

            missing_source_count = result["missing_source_count"]
            missing_target_count = result["missing_target_count"]

            if missing_source_count or missing_target_count:
                raise ValueError(
                    "Relationship load failed because one or more endpoint nodes "
                    "were missing. "
                    f"Relationship type: {rel_type}. "
                    f"Source: {source_label}.{source_node_key}. "
                    f"Target: {target_label}.{target_node_key}. "
                    f"Missing sources: {missing_source_count}; "
                    f"examples: {result['missing_sources']}. "
                    f"Missing targets: {missing_target_count}; "
                    f"examples: {result['missing_targets']}."
                )

            total_written += result["relationships_written"]         

    def _ensure_connected(self) -> None:
        if self.driver is None:
            raise RuntimeError("Graph writer is not connected. Call connect() first.")