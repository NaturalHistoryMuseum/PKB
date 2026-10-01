from types import SimpleNamespace

from pkb.models.places import search_geonames_candidates


COLUMNS = [
    "geoname_id",
    "canonical_name",
    "matched_name",
    "name_type",
    "language",
    "is_preferred",
    "is_historic",
    "country_code",
    "feature_class",
    "feature_code",
    "latitude",
    "longitude",
    "population",
    "exact_match",
    "name_score",
]


class Cursor:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.queries = []
        self.description = [SimpleNamespace(name=name) for name in COLUMNS]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query, parameters=None):
        self.queries.append((query, parameters))
        self.rows = next(self.responses)

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self, responses):
        self.cursor_instance = Cursor(responses)

    def cursor(self):
        return self.cursor_instance


def test_search_geonames_candidates_returns_exact_matches_without_fuzzy_query():
    row = (
        2643743,
        "London",
        "London",
        "canonical",
        None,
        True,
        False,
        "GB",
        "P",
        "PPLC",
        51.50853,
        -0.12574,
        8961989,
        True,
        1.0,
    )
    connection = Connection([[row]])

    candidates = search_geonames_candidates(connection, " London ", "gb", limit=5)

    assert candidates[0]["geoname_id"] == 2643743
    assert candidates[0]["matched_name"] == "London"
    assert len(connection.cursor_instance.queries) == 1
    assert connection.cursor_instance.queries[0][1] == {
        "name": "london",
        "country_code": "GB",
        "limit": 5,
    }


def test_search_geonames_candidates_falls_back_to_fuzzy_search():
    connection = Connection([[], [], []])

    candidates = search_geonames_candidates(connection, "Londn")

    assert candidates == []
    assert len(connection.cursor_instance.queries) == 3
    assert "search_name = %(name)s" in connection.cursor_instance.queries[0][0]
    assert connection.cursor_instance.queries[1][0] == (
        "SET LOCAL pg_trgm.similarity_threshold = 0.6"
    )
    assert "search_name %% %(name)s" in connection.cursor_instance.queries[2][0]
