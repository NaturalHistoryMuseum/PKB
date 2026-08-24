from __future__ import annotations

from dataclasses import dataclass

import networkx as nx
import pandas as pd


IDENTIFIER_COLUMNS = (
    "wikidataID",
    "harvardIndexID",
    "bionomiaID",
    "orcidID",
    "viafID",
    "isniID",
)

SOURCE_PRIORITY = {
    "harvard_index": 1,
    "wikidata": 2,
    "bionomia": 3,
}


@dataclass(frozen=True)
class PersonAggregationResult:
    people: pd.DataFrame
    source_links: pd.DataFrame


class PersonAggregator:
    """Aggregate source person records into deterministic PKB Person identities."""

    identifier_columns = IDENTIFIER_COLUMNS
    source_priority = SOURCE_PRIORITY

    def aggregate(self, source_people: pd.DataFrame) -> PersonAggregationResult:
        source_people = source_people.copy().reset_index(drop=True)
        source_people["sourcePersonKey"] = source_people["pkbPersonKey"].astype(str)
        source_people = source_people.drop(columns=["pkbPersonKey"])

        graph = self._source_record_graph(source_people)
        source_links = self._source_links(source_people, graph)
        clustered_people = source_links.drop(columns=["source"]).merge(
            source_people,
            on="sourcePersonKey",
            how="left",
        )
        people = self._people(clustered_people)

        return PersonAggregationResult(
            people=people,
            source_links=source_links,
        )

    def _source_record_graph(self, source_people: pd.DataFrame) -> nx.Graph:
        graph = nx.Graph()

        for source_person_key in source_people["sourcePersonKey"]:
            graph.add_node(source_person_key)

        for column in self.identifier_columns:
            if column not in source_people.columns:
                continue

            linked = source_people[
                ["sourcePersonKey", column]
            ].explode(column)
            linked = linked[linked[column].apply(_has_value)]
            for _, group in linked.groupby(column, dropna=True):
                keys = group["sourcePersonKey"].drop_duplicates().tolist()
                if len(keys) < 2:
                    continue

                first = keys[0]
                for key in keys[1:]:
                    graph.add_edge(first, key, reason=column)

        return graph

    def _source_links(
        self,
        source_people: pd.DataFrame,
        graph: nx.Graph,
    ) -> pd.DataFrame:
        source_lookup = source_people.set_index("sourcePersonKey")
        rows = []

        clusters = sorted(
            nx.connected_components(graph),
            key=lambda cluster: min(cluster),
        )
        for index, cluster in enumerate(clusters, start=1):
            pkb_person_key = f"PKBP{index:09d}"
            for source_person_key in sorted(cluster):
                source_row = source_lookup.loc[source_person_key]
                rows.append(
                    {
                        "pkbPersonKey": pkb_person_key,
                        "source": source_row["source"],
                        "sourcePersonKey": source_person_key,
                    }
                )

        return pd.DataFrame(rows)

    def _people(self, clustered_people: pd.DataFrame) -> pd.DataFrame:
        rows = []

        for pkb_person_key, group in clustered_people.groupby("pkbPersonKey", sort=True):
            rows.append(
                {
                    "pkbPersonKey": pkb_person_key,
                    "name": self._first_by_source_priority(group, "name"),
                    "birthYear": self._first_by_source_priority(group, "birthYear"),
                    "deathYear": self._first_by_source_priority(group, "deathYear"),
                    "wikidataIDs": _unique_values(group, "wikidataID"),
                    "harvardIndexIDs": _unique_values(group, "harvardIndexID"),
                    "bionomiaIDs": _unique_values(group, "bionomiaID"),
                    "orcidIDs": _unique_values(group, "orcidID"),
                    "viafIDs": _unique_values(group, "viafID"),
                    "isniIDs": _unique_values(group, "isniID"),
                    "sourceCount": len(group),
                    "sources": sorted(group["source"].dropna().unique().tolist()),
                }
            )

        return pd.DataFrame(rows)

    def _first_by_source_priority(self, group: pd.DataFrame, column: str):
        if column not in group.columns:
            return None

        values = (
            group.sort_values(
                "source",
                key=lambda sources: sources.map(self.source_priority).fillna(100),
            )[column]
            .dropna()
            .tolist()
        )

        for value in values:
            if _has_value(value):
                return value

        return None


def _unique_values(group: pd.DataFrame, column: str) -> list[str]:
    if column not in group.columns:
        return []

    values = []
    for value in group[column].dropna():
        if _is_collection_value(value):
            values.extend(value)
        else:
            values.append(value)

    return sorted({str(value) for value in values if _has_value(value)})


def _has_value(value) -> bool:
    if value is None:
        return False
    if _is_collection_value(value):
        return any(_has_value(item) for item in value)
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value != "nan"
    return True


def _is_collection_value(value) -> bool:
    return isinstance(value, (list, tuple, set)) or (
        hasattr(value, "tolist")
        and not isinstance(value, (str, bytes))
    )
