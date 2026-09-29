from __future__ import annotations

import pandas as pd


IDENTIFIER_COLUMNS = (
    "wikidata_ids",
    "harvard_index_ids",
    "bionomia_ids",
)

PERSON_COLUMNS = (
    "source",
    "source_id",
    "name",
    "name_normalised",
    "surname",
    "given_names",
    "initials",
    "birth_date",
    "death_date",
    "geography",
    "institutions",
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
