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
    """Deduplicate Harvard Index botanist records before cross-source resolution."""

    def __init__(self, max_block_size: int = 500):
        self.max_block_size = max_block_size

    def deduplicate(self, people: pd.DataFrame) -> HUHDeduplicationResult:
        work = self._prepare(people)
        pair_features = self._pair_features(work)
        duplicate_pairs = pair_features[
            pair_features["likelyDuplicate"]
        ].copy()
        clusters = self._clusters(work, duplicate_pairs)
        deduplication_map = self._deduplication_map(work, clusters)
        deduped_people = self._deduped_people(work, deduplication_map)

        return HUHDeduplicationResult(
            people=deduped_people,
            deduplication_map=deduplication_map,
            clusters=clusters,
            pair_features=pair_features,
        )

    def _prepare(self, people: pd.DataFrame) -> pd.DataFrame:
        work = people.copy().reset_index(drop=True)
        work["harvardIndexID"] = work["harvardIndexID"].astype(str)

        parsed_names = work["name"].apply(parse_person_name)
        work["surname"] = parsed_names.apply(lambda name: name.surname if name else None)
        work["givenNames"] = parsed_names.apply(
            lambda name: name.given_names if name else ()
        )
        work["givenNamesText"] = parsed_names.apply(
            lambda name: name.given_names_text if name else None
        )
        work["initials"] = parsed_names.apply(lambda name: name.initials if name else ())
        work["initialsText"] = parsed_names.apply(
            lambda name: name.initials_text if name else None
        )
        work["normalisedName"] = parsed_names.apply(
            lambda name: name.normalised if name else None
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
        block_counts = work["candidateBlock"].value_counts(dropna=True)
        work["candidateBlock"] = work["candidateBlock"].where(
            work["candidateBlock"].map(block_counts).le(self.max_block_size)
        )
        work["informationScore"] = work.apply(_information_score, axis=1)
        return work

    def _pair_features(self, work: pd.DataFrame) -> pd.DataFrame:
        indexed = work.set_index("harvardIndexID", drop=False)
        indexer = recordlinkage.Index()
        indexer.block("candidateBlock")
        candidate_links = indexer.index(indexed)

        if len(candidate_links) == 0:
            return self._empty_pair_features()

        compare = recordlinkage.Compare()
        compare.exact("surname", "surname", label="sameSurname")
        compare.exact("initialsText", "initialsText", label="sameInitials")
        compare.exact("birthYearClean", "birthYearClean", label="sameBirthYear")
        compare.exact("deathYearClean", "deathYearClean", label="sameDeathYear")
        features = compare.compute(candidate_links, indexed)

        pairs = candidate_links.to_frame(index=False)
        pairs.columns = ["leftHarvardIndexID", "rightHarvardIndexID"]
        left = (
            indexed.loc[pairs["leftHarvardIndexID"]]
            .reset_index(drop=True)
            .drop(columns=["harvardIndexID"], errors="ignore")
            .add_suffix("Left")
        )
        right = (
            indexed.loc[pairs["rightHarvardIndexID"]]
            .reset_index(drop=True)
            .drop(columns=["harvardIndexID"], errors="ignore")
            .add_suffix("Right")
        )
        pair_features = pd.concat(
            [
                pairs.reset_index(drop=True),
                left,
                right,
                features.reset_index(drop=True),
            ],
            axis=1,
        )
        pair_features["blockValue"] = pair_features["candidateBlockLeft"]
        pair_features["leftName"] = pair_features["nameLeft"]
        pair_features["rightName"] = pair_features["nameRight"]
        pair_features["birthYearMatches"] = (
            pair_features["sameBirthYear"].eq(1)
            & pair_features["birthYearCleanLeft"].notna()
            & pair_features["birthYearCleanRight"].notna()
        )
        pair_features["deathYearMatches"] = (
            pair_features["sameDeathYear"].eq(1)
            & pair_features["deathYearCleanLeft"].notna()
            & pair_features["deathYearCleanRight"].notna()
        )
        pair_features["herbariaMatches"] = [
            _lists_match(left_value, right_value)
            for left_value, right_value in zip(
                pair_features["herbariaCodeLeft"],
                pair_features["herbariaCodeRight"],
            )
        ]
        pair_features["geographyMatches"] = [
            _lists_match(left_value, right_value)
            for left_value, right_value in zip(
                pair_features["geographyISOLeft"],
                pair_features["geographyISORight"],
            )
        ]
        pair_features["birthYearsCompatible"] = [
            _values_compatible(left_value, right_value)
            for left_value, right_value in zip(
                pair_features["birthYearCleanLeft"],
                pair_features["birthYearCleanRight"],
            )
        ]
        pair_features["deathYearsCompatible"] = [
            _values_compatible(left_value, right_value)
            for left_value, right_value in zip(
                pair_features["deathYearCleanLeft"],
                pair_features["deathYearCleanRight"],
            )
        ]
        pair_features["herbariaCompatible"] = [
            _lists_compatible(left_value, right_value)
            for left_value, right_value in zip(
                pair_features["herbariaCodeLeft"],
                pair_features["herbariaCodeRight"],
            )
        ]
        pair_features["geographyCompatible"] = [
            _lists_compatible(left_value, right_value)
            for left_value, right_value in zip(
                pair_features["geographyISOLeft"],
                pair_features["geographyISORight"],
            )
        ]
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

        return pair_features[
            [
                "leftHarvardIndexID",
                "rightHarvardIndexID",
                "blockValue",
                "leftName",
                "rightName",
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
        ]

    def _empty_pair_features(self) -> pd.DataFrame:
        return pd.DataFrame(
            [],
            columns=[
                "leftHarvardIndexID",
                "rightHarvardIndexID",
                "blockValue",
                "leftName",
                "rightName",
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
            ],
        )

    def _clusters(self, work: pd.DataFrame, duplicate_pairs: pd.DataFrame) -> pd.DataFrame:
        graph = nx.Graph()
        graph.add_nodes_from(work["harvardIndexID"])

        if not duplicate_pairs.empty:
            graph.add_edges_from(
                duplicate_pairs[
                    ["leftHarvardIndexID", "rightHarvardIndexID"]
                ].itertuples(index=False, name=None)
            )

        rows = []
        lookup = work.set_index("harvardIndexID", drop=False)
        cluster_number = 1
        for component in nx.connected_components(graph):
            members = sorted(component)
            if len(members) < 2:
                continue

            cluster_rows = lookup.loc[members].reset_index(drop=True)
            canonical_id = _canonical_harvard_index_id(cluster_rows)
            rows.append(
                {
                    "huhClusterID": f"huh-cluster-{cluster_number:06d}",
                    "canonicalHarvardIndexID": canonical_id,
                    "harvardIndexIDs": members,
                    "clusterSize": len(members),
                }
            )
            cluster_number += 1

        return pd.DataFrame(
            rows,
            columns=[
                "huhClusterID",
                "canonicalHarvardIndexID",
                "harvardIndexIDs",
                "clusterSize",
            ],
        )

    def _deduplication_map(
        self,
        work: pd.DataFrame,
        clusters: pd.DataFrame,
    ) -> pd.DataFrame:
        rows = []
        clustered_ids = set()

        for cluster in clusters.to_dict("records"):
            for harvard_index_id in cluster["harvardIndexIDs"]:
                rows.append(
                    {
                        "huhClusterID": cluster["huhClusterID"],
                        "harvardIndexID": harvard_index_id,
                        "canonicalHarvardIndexID": cluster["canonicalHarvardIndexID"],
                        "pkbPersonKey": (
                            f"harvard_index:{cluster['canonicalHarvardIndexID']}"
                        ),
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

    def _deduped_people(
        self,
        work: pd.DataFrame,
        deduplication_map: pd.DataFrame,
    ) -> pd.DataFrame:
        mapped = work.merge(
            deduplication_map[
                ["huhClusterID", "harvardIndexID", "canonicalHarvardIndexID"]
            ],
            on="harvardIndexID",
            how="left",
        )
        rows = []

        for _, group in mapped.groupby("canonicalHarvardIndexID", sort=True):
            canonical_id = group["canonicalHarvardIndexID"].iloc[0]
            canonical = group.sort_values(
                ["informationScore", "harvardIndexID"],
                ascending=[False, True],
            ).iloc[0]
            record = canonical.drop(
                labels=[
                    "huhClusterID",
                    "canonicalHarvardIndexID",
                    "candidateBlock",
                    "surname",
                    "givenNames",
                    "givenNamesText",
                    "initials",
                    "initialsText",
                    "firstInitial",
                    "normalisedName",
                    "birthYearClean",
                    "deathYearClean",
                    "informationScore",
                ],
                errors="ignore",
            ).to_dict()
            record["pkbPersonKey"] = f"harvard_index:{canonical_id}"
            record["harvardIndexID"] = sorted(group["harvardIndexID"].tolist())
            record["deduplicatedHarvardIndexIDs"] = record["harvardIndexID"]
            record["sourceRecordCount"] = len(group)

            for column in ["geographyISO", "herbariaCode", "institutionUUID"]:
                record[column] = _merge_lists(group[column])

            rows.append(record)

        return pd.DataFrame(rows)


def _canonical_harvard_index_id(group: pd.DataFrame) -> str:
    scored = group.copy()
    scored["informationScore"] = scored.apply(_information_score, axis=1)
    return str(
        scored.sort_values(
            ["informationScore", "harvardIndexID"],
            ascending=[False, True],
        )["harvardIndexID"].iloc[0]
    )


def _information_score(row) -> int:
    score = 0
    for column in [
        "name",
        "fullName",
        "standardName",
        "authorAbbreviation",
        "birthYearClean",
        "deathYearClean",
    ]:
        if _has_value(row.get(column)):
            score += 1

    for column in ["geographyISO", "herbariaCode", "institutionUUID"]:
        score += len(_as_list(row.get(column)))

    return score


def _clean_year(value) -> int | None:
    if not _has_value(value):
        return None
    match = re.search(r"\d{4}", str(value))
    if not match:
        return None
    return int(match.group(0))


def _values_match(left, right) -> bool:
    return _has_value(left) and _has_value(right) and left == right


def _values_compatible(left, right) -> bool:
    return not (_has_value(left) and _has_value(right)) or left == right


def _lists_match(left, right) -> bool:
    left_values = set(_as_list(left))
    right_values = set(_as_list(right))
    return bool(left_values and right_values and left_values.intersection(right_values))


def _lists_compatible(left, right) -> bool:
    left_values = set(_as_list(left))
    right_values = set(_as_list(right))
    if not left_values or not right_values:
        return True
    return bool(left_values.intersection(right_values))


def _initials_compatible(left, right) -> bool:
    if not _has_value(left) or not _has_value(right):
        return False

    left = str(left)
    right = str(right)
    return left == right or left.startswith(right) or right.startswith(left)


def _merge_lists(values: pd.Series) -> list[str]:
    merged = []
    for value in values:
        merged.extend(_as_list(value))
    return sorted(set(merged))


def _as_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if _has_value(item)]
    if isinstance(value, tuple):
        return [str(item) for item in value if _has_value(item)]
    if isinstance(value, set):
        return sorted(str(item) for item in value if _has_value(item))
    if not _has_value(value):
        return []
    return [str(value)]


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
