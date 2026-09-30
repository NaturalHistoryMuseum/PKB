from __future__ import annotations

from itertools import combinations

import networkx as nx
import numpy as np
import pandas as pd
import recordlinkage
from sklearn.linear_model import LogisticRegression

from pkb.models.people.names import normalise_person_name_text, parse_person_name
from pkb.utils import country_string_to_iso, parse_literal_collection


IDENTIFIER_COLUMNS = (
    "wikidata_ids",
    "harvard_index_ids",
    "bionomia_ids",
)

PERSON_COLUMNS = (
    "source",
    "source_id",
    "name",
    "name_variants",
    "name_normalised",
    "surname",
    "given_names",
    "initials",
    "birth_date",
    "death_date",
    "geography",
    "institutions",
)

PAIR_COLUMNS = (
    "left_source",
    "left_source_id",
    "right_source",
    "right_source_id",
)

MODEL_COLUMNS = (
    "same_normalised_name",
    "same_first_name",
    "first_names_compatible",
    "same_initials",
    "same_multiple_initials",
    "middle_initials_compatible",
    "same_birth_year",
    "same_death_year",
    "birth_year_conflict",
    "death_year_conflict",
    "same_country",
    "same_institution",
    "common_name",
    "unique_exact_name",
)


def match_people_by_identifier(people: pd.DataFrame) -> pd.DataFrame:
    people = people.reset_index(drop=True)
    pairs = []

    for identifier_column in IDENTIFIER_COLUMNS:
        values = (
            people[["source", identifier_column]]
            .explode(identifier_column)
            .dropna(subset=[identifier_column])
            .reset_index(names="person_index")
        )
        values[identifier_column] = values[identifier_column].astype(str).str.strip()
        values = values[values[identifier_column].ne("")]

        matches = values.merge(values, on=identifier_column, suffixes=("_left", "_right"))
        matches = matches[matches["source_left"] < matches["source_right"]]
        matches = matches.rename(columns={identifier_column: "identifier_value"})
        matches["identifier_type"] = identifier_column.removesuffix("s")
        pairs.append(
            matches[
                [
                    "person_index_left",
                    "person_index_right",
                    "identifier_type",
                    "identifier_value",
                ]
            ]
        )

    pairs = pd.concat(pairs, ignore_index=True).drop_duplicates()
    left = people.loc[pairs["person_index_left"], PERSON_COLUMNS].reset_index(drop=True)
    right = people.loc[pairs["person_index_right"], PERSON_COLUMNS].reset_index(drop=True)
    matches = pd.concat(
        [
            left.add_prefix("left_"),
            right.add_prefix("right_"),
            pairs[["identifier_type", "identifier_value"]].reset_index(drop=True),
        ],
        axis=1,
    )
    return matches.sort_values(
        ["left_source", "left_source_id", "right_source", "right_source_id"],
        ignore_index=True,
    )


def match_people_by_logistic_regression(
    people: pd.DataFrame,
    identifier_matches: pd.DataFrame,
    institutions: pd.DataFrame,
    threshold: float = 0.95,
) -> pd.DataFrame:
    people = _prepare_people(people, institutions)
    candidate_links = _candidate_links(people)
    if candidate_links.empty:
        return pd.DataFrame(columns=[*PAIR_COLUMNS, "probability"])

    features = _comparison_features(people, candidate_links)
    pair_keys = pd.Series(
        [frozenset(link) for link in features.index],
        index=features.index,
    )
    known_pairs = {
        frozenset((left, right))
        for left, right in zip(
            identifier_matches["left_source"].astype(str).str.cat(
                identifier_matches["left_source_id"].astype(str), sep=":"
            ),
            identifier_matches["right_source"].astype(str).str.cat(
                identifier_matches["right_source_id"].astype(str), sep=":"
            ),
        )
    }
    conflicts = pd.Series(
        [
            _identifiers_conflict(people.loc[left], people.loc[right])
            for left, right in features.index
        ],
        index=features.index,
    )
    labelled = pair_keys.isin(known_pairs) | conflicts
    labels = pair_keys.isin(known_pairs).astype(int)
    if labels[labelled].nunique() < 2:
        return pd.DataFrame(columns=[*PAIR_COLUMNS, "probability"])

    model = LogisticRegression(max_iter=1000, class_weight="balanced")
    model.fit(features.loc[labelled], labels.loc[labelled])
    probabilities = pd.Series(
        model.predict_proba(features)[:, 1],
        index=features.index,
    )
    selected = probabilities.ge(threshold) & ~pair_keys.isin(known_pairs) & ~conflicts

    rows = []
    for (left, right), probability in probabilities[selected].items():
        rows.append(
            {
                "left_source": people.at[left, "source"],
                "left_source_id": people.at[left, "source_id"],
                "right_source": people.at[right, "source"],
                "right_source_id": people.at[right, "source_id"],
                "probability": probability,
            }
        )
    return pd.DataFrame(rows, columns=[*PAIR_COLUMNS, "probability"])


def build_people_identifier_map(
    people: pd.DataFrame,
    identifier_matches: pd.DataFrame,
    logistic_matches: pd.DataFrame,
) -> pd.DataFrame:
    people = people[["source", "source_id"]].drop_duplicates().copy()
    people["person_key"] = people["source"].astype(str).str.cat(
        people["source_id"].astype(str), sep=":"
    )
    graph = nx.Graph()
    graph.add_nodes_from(people["person_key"])

    for matches in (identifier_matches, logistic_matches):
        if matches.empty:
            continue
        graph.add_edges_from(
            zip(
                matches["left_source"].astype(str).str.cat(
                    matches["left_source_id"].astype(str), sep=":"
                ),
                matches["right_source"].astype(str).str.cat(
                    matches["right_source_id"].astype(str), sep=":"
                ),
            )
        )

    person_ids = {
        person_key: f"PKBP{index:09d}"
        for index, cluster in enumerate(
            sorted(nx.connected_components(graph), key=min),
            start=1,
        )
        for person_key in cluster
    }
    people["person_id"] = people["person_key"].map(person_ids)
    return people[["person_id", "source", "source_id"]].sort_values(
        ["person_id", "source", "source_id"], ignore_index=True
    )


def _prepare_people(people: pd.DataFrame, institutions: pd.DataFrame) -> pd.DataFrame:
    people = people.copy().drop_duplicates(["source", "source_id"])
    people["source"] = people["source"].astype(str)
    people["source_id"] = people["source_id"].astype(str)
    people["person_key"] = people["source"].str.cat(people["source_id"], sep=":")
    people["birth_year"] = people["birth_date"].astype("string").str.extract(
        r"(\d{4})", expand=False
    )
    people["death_year"] = people["death_date"].astype("string").str.extract(
        r"(\d{4})", expand=False
    )
    people["given_names"] = people["given_names"].astype("string")
    people["initials"] = people["initials"].astype("string")
    people["first_name"] = people["given_names"].str.split().str[0]
    people["middle_initials"] = people["initials"].str[1:]
    people["full_first_name"] = people["first_name"].where(
        people["first_name"].str.len().gt(1)
    )
    people["multiple_initials"] = people["initials"].where(
        people["initials"].str.len().gt(1)
    )
    people["surname_variants"] = [
        tuple(
            sorted(
                {
                    parsed.surname
                    for name in parse_literal_collection(names)
                    if (parsed := parse_person_name(name)) and parsed.surname
                }
                | ({surname} if pd.notna(surname) else set())
            )
        )
        for names, surname in zip(people["name_variants"], people["surname"])
    ]

    for column in (
        "surname",
        "name_normalised",
        "first_name",
        "initials",
        "birth_year",
        "death_year",
    ):
        people[column] = people[column].replace("", pd.NA)

    people["common_name"] = people["first_name"].notna() & people.duplicated(
        ["source", "surname", "first_name"], keep=False
    )
    people["unique_exact_name"] = people["name_normalised"].notna() & ~people.duplicated(
        ["source", "name_normalised"], keep=False
    )
    people["countries"] = people["geography"].map(_countries)

    institution_lookup = institutions.set_index("key")["name"].to_dict()
    people["institution_names"] = [
        _institution_names(source, values, institution_lookup)
        for source, values in zip(people["source"], people["institutions"])
    ]
    return people.set_index("person_key", drop=False)


def _candidate_links(people: pd.DataFrame) -> pd.MultiIndex:
    surnames = (
        people[["person_key", "source", "surname_variants"]]
        .explode("surname_variants")
        .dropna(subset=["surname_variants"])
    )
    pairs = []
    by_source = {
        source: rows[["person_key", "surname_variants"]]
        for source, rows in surnames.groupby("source")
    }
    for left_source, right_source in combinations(sorted(by_source), 2):
        pairs.append(
            by_source[left_source].merge(
                by_source[right_source],
                on="surname_variants",
                suffixes=("_left", "_right"),
            )
        )
    if not pairs:
        return pd.MultiIndex.from_arrays([[], []])
    pairs = pd.concat(pairs, ignore_index=True)
    return pd.MultiIndex.from_frame(
        pairs[["person_key_left", "person_key_right"]].drop_duplicates()
    )


def _comparison_features(
    people: pd.DataFrame,
    candidate_links: pd.MultiIndex,
) -> pd.DataFrame:
    compare = recordlinkage.Compare()
    compare.exact("name_normalised", "name_normalised", label="same_normalised_name")
    compare.exact("full_first_name", "full_first_name", label="same_first_name")
    compare.compare_vectorized(
        _first_names_compatible,
        "first_name",
        "first_name",
        label="first_names_compatible",
    )
    compare.exact("initials", "initials", label="same_initials")
    compare.exact("multiple_initials", "multiple_initials", label="same_multiple_initials")
    compare.compare_vectorized(
        _middle_initials_compatible,
        "middle_initials",
        "middle_initials",
        label="middle_initials_compatible",
    )
    compare.exact("birth_year", "birth_year", label="same_birth_year")
    compare.exact("death_year", "death_year", label="same_death_year")
    compare.compare_vectorized(
        _years_conflict, "birth_year", "birth_year", label="birth_year_conflict"
    )
    compare.compare_vectorized(
        _years_conflict, "death_year", "death_year", label="death_year_conflict"
    )
    compare.compare_vectorized(
        _collections_match, "countries", "countries", label="same_country"
    )
    compare.compare_vectorized(
        _collections_match,
        "institution_names",
        "institution_names",
        label="same_institution",
    )
    features = compare.compute(candidate_links, people)
    left = features.index.get_level_values(0)
    right = features.index.get_level_values(1)
    features["common_name"] = (
        people.loc[left, "common_name"].to_numpy()
        | people.loc[right, "common_name"].to_numpy()
    )
    features["unique_exact_name"] = (
        features["same_normalised_name"].eq(1)
        & people.loc[left, "unique_exact_name"].to_numpy()
        & people.loc[right, "unique_exact_name"].to_numpy()
    )

    availability = {
        "first_name": people["first_name"].notna(),
        "initials": people["initials"].notna(),
        "birth_year": people["birth_year"].notna(),
        "death_year": people["death_year"].notna(),
        "country": people["countries"].map(bool),
        "institution": people["institution_names"].map(bool),
    }
    model_features = features[list(MODEL_COLUMNS)].astype(float)
    for name, available in availability.items():
        left_available = available.loc[left].to_numpy()
        right_available = available.loc[right].to_numpy()
        model_features[f"both_{name}_present"] = left_available & right_available
        model_features[f"one_{name}_missing"] = left_available ^ right_available
    return model_features.astype(float)


def _first_names_compatible(left, right):
    left = left.fillna("")
    right = right.fillna("")
    both_are_names = left.str.len().gt(1) & right.str.len().gt(1)
    return (~both_are_names | left.eq(right)).astype(int)


def _middle_initials_compatible(left, right):
    left = left.fillna("")
    right = right.fillna("")
    return (left.eq("") | right.eq("") | left.eq(right)).astype(int)


def _years_conflict(left, right):
    left = pd.to_numeric(left, errors="coerce")
    right = pd.to_numeric(right, errors="coerce")
    return left.sub(right).abs().gt(1).fillna(False).astype(int)


def _collections_match(left, right):
    return np.fromiter((bool(set(a) & set(b)) for a, b in zip(left, right)), dtype=int)


def _countries(value) -> tuple[str, ...]:
    countries = {
        country_string_to_iso(country)
        for values in parse_literal_collection(value)
        for country in values.split("|")
    }
    return tuple(sorted(country for country in countries if country))


def _institution_names(source, value, lookup) -> tuple[str, ...]:
    values = parse_literal_collection(value)
    if source == "harvard_index":
        values = [lookup.get(item) for item in values]
    return tuple(
        sorted(normalise_person_name_text(item) for item in values if item)
    )


def _identifiers_conflict(left: pd.Series, right: pd.Series) -> bool:
    for column in IDENTIFIER_COLUMNS:
        left_values = set(parse_literal_collection(left[column]))
        right_values = set(parse_literal_collection(right[column]))
        if left_values and right_values and left_values.isdisjoint(right_values):
            return True
    return False
