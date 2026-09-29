from __future__ import annotations

import re
from dataclasses import dataclass

import networkx as nx
import pandas as pd
import recordlinkage

from pkb.models.people.names import parse_person_name


@dataclass(frozen=True)
class HUHDeduplicationResult:
    people: pd.DataFrame
    deduplication_map: pd.DataFrame
    clusters: pd.DataFrame
    pair_features: pd.DataFrame


class HUHPersonDeduplicator:
    def __init__(self, max_block_size: int = 500):
        self.max_block_size = max_block_size

    def deduplicate(self, people: pd.DataFrame) -> HUHDeduplicationResult:
        work = _prepare(people, self.max_block_size)
        pair_features = _pair_features(work)
        duplicate_mask = pair_features["likelyDuplicate"].astype(bool)
        clusters = _clusters(
            work,
            pair_features[duplicate_mask],
        )

        return HUHDeduplicationResult(
            people=work,
            deduplication_map=_deduplication_map(work, clusters),
            clusters=clusters,
            pair_features=pair_features,
        )


def _prepare(people: pd.DataFrame, max_block_size: int) -> pd.DataFrame:
    work = people.copy().reset_index(drop=True)
    work["harvardIndexID"] = work["harvardIndexID"].astype(str)
    names = (
        work["fullName"]
        .combine_first(work["standardName"])
        .combine_first(work["name"])
    )
    parsed_names = names.apply(parse_person_name)
    work["nameNormalised"] = parsed_names.apply(
        lambda name: name.normalised if name else None
    )
    work["surname"] = parsed_names.apply(lambda name: name.surname if name else None)
    work["givenNamesText"] = parsed_names.apply(
        lambda name: name.given_names_text if name else None
    )
    work["initialsText"] = parsed_names.apply(
        lambda name: name.initials_text if name else None
    )
    work["birthYearClean"] = work["birthYear"].combine_first(
        work["dateOfBirth"]
    ).apply(_clean_year)
    work["deathYearClean"] = work["deathYear"].combine_first(
        work["dateOfDeath"]
    ).apply(_clean_year)
    work["candidateBlock"] = (
        work["surname"].fillna("")
        + "|"
        + work["initialsText"].fillna("")
    )
    work.loc[work["candidateBlock"].eq("|"), "candidateBlock"] = None

    counts = work["candidateBlock"].value_counts(dropna=True)
    work["candidateBlock"] = work["candidateBlock"].where(
        work["candidateBlock"].map(counts).le(max_block_size)
    )
    return work


def _pair_features(work: pd.DataFrame) -> pd.DataFrame:
    indexed = work.set_index("harvardIndexID", drop=False)
    indexer = recordlinkage.Index()
    indexer.block("candidateBlock")
    candidate_links = indexer.index(indexed)

    if len(candidate_links) == 0:
        return _empty_pair_features()

    compare = recordlinkage.Compare()
    compare.exact("surname", "surname", label="sameSurname")
    compare.exact("initialsText", "initialsText", label="sameInitials")
    for label, column, comparator in _HUH_COMPARE_FEATURES:
        compare.compare_vectorized(
            _compare_pairs,
            column,
            column,
            comparator,
            label=label,
        )

    pair_features = compare.compute(candidate_links, indexed).reset_index()
    pair_features = pair_features.rename(
        columns={
            pair_features.columns[0]: "leftHarvardIndexID",
            pair_features.columns[1]: "rightHarvardIndexID",
        }
    )
    pair_features["hasSupportingMatch"] = (
        pair_features["birthYearMatches"]
        | pair_features["deathYearMatches"]
        | pair_features["herbariaMatches"]
        | pair_features["geographyMatches"]
    )
    pair_features["noSupportingConflict"] = (
        pair_features["birthYearsCompatible"]
        & pair_features["deathYearsCompatible"]
        & pair_features["herbariaCompatible"]
        & pair_features["geographyCompatible"]
    )
    pair_features["likelyDuplicate"] = (
        pair_features["sameSurname"].eq(1)
        & pair_features["sameInitials"].eq(1)
        & pair_features["hasSupportingMatch"]
        & pair_features["noSupportingConflict"]
    )

    return pair_features[_PAIR_FEATURE_COLUMNS]


def _clusters(work: pd.DataFrame, duplicate_pairs: pd.DataFrame) -> pd.DataFrame:
    graph = nx.Graph()
    graph.add_edges_from(
        duplicate_pairs[
            ["leftHarvardIndexID", "rightHarvardIndexID"]
        ].itertuples(index=False, name=None)
    )

    rows = []
    for index, members in enumerate(nx.connected_components(graph), start=1):
        ids = _sort_ids(members)
        rows.append(
            {
                "huhClusterID": f"huh-cluster-{index:06d}",
                "canonicalHarvardIndexID": ids[0],
                "harvardIndexIDs": ids,
                "clusterSize": len(ids),
            }
        )

    return pd.DataFrame(
        rows,
        columns=[
            "huhClusterID",
            "canonicalHarvardIndexID",
            "harvardIndexIDs",
            "clusterSize",
        ],
    )


def _deduplication_map(work: pd.DataFrame, clusters: pd.DataFrame) -> pd.DataFrame:
    rows = []
    clustered_ids = set()

    for cluster in clusters.to_dict("records"):
        for harvard_index_id in cluster["harvardIndexIDs"]:
            rows.append(
                {
                    "huhClusterID": cluster["huhClusterID"],
                    "harvardIndexID": harvard_index_id,
                    "canonicalHarvardIndexID": cluster["canonicalHarvardIndexID"],
                    "pkbPersonKey": f"harvard_index:{cluster['canonicalHarvardIndexID']}",
                }
            )
            clustered_ids.add(harvard_index_id)

    for harvard_index_id in work["harvardIndexID"]:
        if harvard_index_id in clustered_ids:
            continue
        rows.append(
            {
                "huhClusterID": None,
                "harvardIndexID": harvard_index_id,
                "canonicalHarvardIndexID": harvard_index_id,
                "pkbPersonKey": f"harvard_index:{harvard_index_id}",
            }
        )

    return pd.DataFrame(rows).sort_values(
        ["canonicalHarvardIndexID", "harvardIndexID"],
        ignore_index=True,
    )


def _clean_year(value) -> int | None:
    if not _has_value(value):
        return None
    if match := re.search(r"\d{4}", str(value)):
        return int(match.group(0))
    return None


def _values_compatible(left, right) -> bool:
    return not (_has_value(left) and _has_value(right)) or left == right


def _values_match(left, right) -> bool:
    return _has_value(left) and _has_value(right) and left == right


def _lists_match(left, right) -> bool:
    left_values = set(_as_list(left))
    right_values = set(_as_list(right))
    return bool(left_values and right_values and left_values.intersection(right_values))


def _lists_compatible(left, right) -> bool:
    left_values = set(_as_list(left))
    right_values = set(_as_list(right))
    return not (left_values and right_values) or bool(
        left_values.intersection(right_values)
    )


def _compare_pairs(left: pd.Series, right: pd.Series, comparator) -> pd.Series:
    return pd.Series(
        [
            comparator(left_value, right_value)
            for left_value, right_value in zip(left, right)
        ],
        index=left.index,
    )


def _as_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if _has_value(item)]
    if isinstance(value, (tuple, set)):
        return [str(item) for item in value if _has_value(item)]
    if _has_value(value):
        return [str(value)]
    return []


def _has_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, (list, tuple, set)):
        return any(_has_value(item) for item in value)
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value.lower() not in {"nan", "none", "<na>"}
    return True


def _sort_ids(values) -> list[str]:
    return sorted(str(value) for value in values)


_HUH_COMPARE_FEATURES = [
    ("birthYearMatches", "birthYearClean", _values_match),
    ("deathYearMatches", "deathYearClean", _values_match),
    ("herbariaMatches", "herbariaCode", _lists_match),
    ("geographyMatches", "geographyISO", _lists_match),
    ("birthYearsCompatible", "birthYearClean", _values_compatible),
    ("deathYearsCompatible", "deathYearClean", _values_compatible),
    ("herbariaCompatible", "herbariaCode", _lists_compatible),
    ("geographyCompatible", "geographyISO", _lists_compatible),
]


_PAIR_FEATURE_COLUMNS = [
    "leftHarvardIndexID",
    "rightHarvardIndexID",
    "sameSurname",
    "sameInitials",
    "birthYearMatches",
    "deathYearMatches",
    "herbariaMatches",
    "geographyMatches",
    "birthYearsCompatible",
    "deathYearsCompatible",
    "herbariaCompatible",
    "geographyCompatible",
    "hasSupportingMatch",
    "noSupportingConflict",
    "likelyDuplicate",
]


def _empty_pair_features() -> pd.DataFrame:
    return pd.DataFrame([], columns=_PAIR_FEATURE_COLUMNS)
