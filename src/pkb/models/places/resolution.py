from __future__ import annotations


SEARCH_QUERY = """
    WITH matches AS (
        SELECT
            geoname_id,
            canonical_name,
            name AS matched_name,
            name_type,
            language,
            is_preferred,
            is_historic,
            country_code,
            feature_class,
            feature_code,
            latitude,
            longitude,
            population,
            search_name = %(name)s AS exact_match,
            similarity(search_name, %(name)s) AS name_score
        FROM places_geonames_search
        WHERE {predicate}
          AND (
              %(country_code)s::TEXT IS NULL
              OR country_code = %(country_code)s::BPCHAR
          )
    ),
    ranked AS (
        SELECT *,
               row_number() OVER (
                   PARTITION BY geoname_id
                   ORDER BY exact_match DESC,
                            name_score DESC,
                            is_preferred DESC,
                            (name_type = 'canonical') DESC,
                            population DESC NULLS LAST
               ) AS place_rank
        FROM matches
    )
    SELECT geoname_id,
           canonical_name,
           matched_name,
           name_type,
           language,
           is_preferred,
           is_historic,
           country_code,
           feature_class,
           feature_code,
           latitude,
           longitude,
           population,
           exact_match,
           name_score
    FROM ranked
    WHERE place_rank = 1
    ORDER BY exact_match DESC,
             name_score DESC,
             is_preferred DESC,
             (name_type = 'canonical') DESC,
             population DESC NULLS LAST,
             geoname_id
    LIMIT %(limit)s
"""

GB1900_SEARCH_QUERY = """
    SELECT pin_id,
           name,
           nation,
           local_authority,
           parish,
           latitude,
           longitude,
           notes,
           lower(name) = %(name)s AS exact_match,
           similarity(lower(name), %(name)s) AS name_score
    FROM places_gb1900
    WHERE {predicate}
      AND latitude BETWEEN %(min_latitude)s AND %(max_latitude)s
      AND longitude BETWEEN %(min_longitude)s AND %(max_longitude)s
    ORDER BY exact_match DESC,
             name_score DESC,
             pin_id
    LIMIT %(limit)s
"""


def search_geonames_candidates(
    connection,
    name: str,
    country_code: str | None = None,
    limit: int = 10,
) -> list[dict]:
    name = name.strip().lower()
    if not name or limit < 1:
        return []

    parameters = {
        "name": name,
        "country_code": country_code.strip().upper() if country_code else None,
        "limit": limit,
    }
    with connection.cursor() as cursor:
        cursor.execute(
            SEARCH_QUERY.format(predicate="search_name = %(name)s"), parameters
        )
        rows = cursor.fetchall()
        if not rows:
            cursor.execute("SET LOCAL pg_trgm.similarity_threshold = 0.6")
            cursor.execute(
                SEARCH_QUERY.format(predicate="search_name %% %(name)s"), parameters
            )
            rows = cursor.fetchall()

        columns = [column.name for column in cursor.description]
        return [dict(zip(columns, row)) for row in rows]


def search_gb1900_candidates(
    connection,
    name: str,
    bounds: dict[str, float],
    limit: int = 10,
) -> list[dict]:
    name = name.strip().lower()
    if not name or limit < 1:
        return []

    parameters = {"name": name, "limit": limit, **bounds}
    with connection.cursor() as cursor:
        cursor.execute(
            GB1900_SEARCH_QUERY.format(predicate="lower(name) = %(name)s"),
            parameters,
        )
        rows = cursor.fetchall()
        if not rows:
            cursor.execute("SET LOCAL pg_trgm.similarity_threshold = 0.6")
            cursor.execute(
                GB1900_SEARCH_QUERY.format(predicate="lower(name) %% %(name)s"),
                parameters,
            )
            rows = cursor.fetchall()

        columns = [column.name for column in cursor.description]
        return [dict(zip(columns, row)) for row in rows]
