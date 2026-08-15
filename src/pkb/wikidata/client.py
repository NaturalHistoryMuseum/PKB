from __future__ import annotations

import time
from typing import Any, Iterable

import requests_cache
from tqdm import tqdm

from pkb.config import logger, settings


SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
WIKIDATA_API_ENDPOINT = "https://www.wikidata.org/w/api.php"

USER_AGENT = "PKB/0.1 NaturalHistoryMuseum contact: your.email@nhm.ac.uk"


def wikidata_session() -> requests_cache.CachedSession:
    return requests_cache.CachedSession(
        cache_name=str(settings.cache_dir / "wikidata"),
        backend="sqlite",
        expire_after=None,
    )


def chunks(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def run_sparql_query(query: str) -> list[dict[str, Any]]:
    session = wikidata_session()

    logger.info("Running Wikidata SPARQL query")

    response = session.get(
        SPARQL_ENDPOINT,
        params={"query": query, "format": "json"},
        headers={
            "Accept": "application/sparql-results+json",
            "User-Agent": USER_AGENT,
        },
        timeout=120,
    )
    response.raise_for_status()

    rows = response.json()["results"]["bindings"]
    logger.info("Wikidata SPARQL query returned %s rows", len(rows))

    return rows


def fetch_entities(
    qids: list[str],
    *,
    batch_size: int = 50,
    sleep_seconds: float = 0.2,
    label: str = "Wikidata entities",
) -> dict[str, dict[str, Any]]:
    """
    Fetch Wikidata entities in batches using wbgetentities.
    """

    session = wikidata_session()
    entities: dict[str, dict[str, Any]] = {}

    qids = list(dict.fromkeys(qids))
    total_batches = (len(qids) + batch_size - 1) // batch_size

    logger.info(
        "Fetching %s %s in %s batches of up to %s",
        len(qids),
        label,
        total_batches,
        batch_size,
    )

    for i, batch in enumerate(
        tqdm(
            chunks(qids, batch_size),
            total=total_batches,
            desc=f"Fetching {label}",
        ),
        start=1,
    ):
        response = session.get(
            WIKIDATA_API_ENDPOINT,
            params={
                "action": "wbgetentities",
                "ids": "|".join(batch),
                "props": "labels|aliases|claims",
                "languages": "en",
                "format": "json",
            },
            headers={"User-Agent": USER_AGENT},
            timeout=120,
        )

        if response.status_code == 429:
            logger.warning(
                "Wikidata returned 429 on batch %s/%s; sleeping before retry",
                i,
                total_batches,
            )
            time.sleep(10)

            response = session.get(
                WIKIDATA_API_ENDPOINT,
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(batch),
                    "props": "labels|aliases|claims",
                    "languages": "en",
                    "format": "json",
                },
                headers={"User-Agent": USER_AGENT},
                timeout=120,
            )

        response.raise_for_status()

        payload = response.json()
        batch_entities = payload.get("entities", {})
        entities.update(batch_entities)

        if i == 1 or i % 10 == 0 or i == total_batches:
            logger.info(
                "Fetched batch %s/%s; %s entities accumulated",
                i,
                total_batches,
                len(entities),
            )

        time.sleep(sleep_seconds)

    logger.info("Finished fetching %s entities for %s", len(entities), label)

    return entities