from __future__ import annotations

import luigi
import pandas as pd
from tqdm import tqdm

from pkb.config import logger, settings
from pkb.tasks.base import BaseTask
from pkb.wikidata.client import fetch_entities, run_sparql_query
from pkb.wikidata.parsers import (
    label,
    parse_collector_entity,
    property_value,
    property_values,
)


WIKIDATA_PEOPLE_SOURCE_DIR = settings.intermediate_dir / "sources" / "wikidata" / "people"


class WikiDataCollectorIDsSourceTask(BaseTask):
    """Materialise Wikidata item IDs for people relevant to Phase 1."""

    query = """
        SELECT DISTINCT ?item WHERE {
          {
            ?item wdt:P428 ?authorAbbreviation .
          }
          UNION
          {
            ?item wdt:P6264 ?harvardIndexID .
          }
          UNION
          {
            ?item wdt:P6944 ?bionomiaID .
          }
          UNION
          {
            ?item wdt:P106 wd:Q2374149 .
          }
          UNION
          {
            ?item wdt:P106 wd:Q2083925 .
          }
        }
        ORDER BY ?item
    """

    def output(self):
        return luigi.LocalTarget(WIKIDATA_PEOPLE_SOURCE_DIR / "collector_ids.parquet")

    def run(self):
        rows = run_sparql_query(self.query)

        qids = sorted(
            {
                row["item"]["value"].rsplit("/", 1)[-1]
                for row in rows
            }
        )

        logger.info("Found %s Wikidata collector IDs", len(qids))
        logger.info("First 10 Wikidata collector IDs: %s", qids[:10])

        df = pd.DataFrame({"wikidataID": qids})
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote Wikidata collector IDs to %s", self.output().path)


class WikiDataCollectorsSourceTask(BaseTask):
    """Materialise parsed Wikidata collector/person source records."""

    def requires(self):
        return WikiDataCollectorIDsSourceTask()

    def output(self):
        return luigi.LocalTarget(WIKIDATA_PEOPLE_SOURCE_DIR / "collectors.parquet")

    def run(self):
        logger.info("Starting WikiDataCollectorsSourceTask")

        ids = pd.read_parquet(self.input().path)
        qids = ids["wikidataID"].dropna().astype(str).drop_duplicates().tolist()

        logger.info("Loaded %s Wikidata collector IDs", len(qids))
        logger.info("First 10 Wikidata collector IDs: %s", qids[:10])

        entities = fetch_entities(
            qids,
            batch_size=50,
            label="collector entities",
        )

        logger.info("Fetched %s Wikidata collector entities", len(entities))

        related_qids = collect_related_qids_for_collectors(entities)

        logger.info(
            "Found %s related Wikidata IDs from collectors",
            len(related_qids),
        )
        logger.info("First 10 related IDs: %s", sorted(related_qids)[:10])

        related_entities = fetch_entities(
            sorted(related_qids),
            batch_size=50,
            label="related collector entities",
        )

        logger.info("Fetched %s related Wikidata entities", len(related_entities))

        labels_by_qid = {
            qid: entity_label
            for qid, entity in related_entities.items()
            if (entity_label := label(entity))
        }

        logger.info("Built label lookup for %s related entities", len(labels_by_qid))

        records = []

        for qid in tqdm(qids, desc="Parsing Wikidata collectors"):
            entity = entities.get(qid)

            if not entity or entity.get("missing"):
                continue

            records.append(
                parse_collector_entity(
                    qid=qid,
                    entity=entity,
                    labels_by_qid=labels_by_qid,
                )
            )

        logger.info("Parsed %s Wikidata collector source records", len(records))

        df = pd.DataFrame(records)
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote Wikidata collector source records to %s", self.output().path)


def collect_related_qids_for_collectors(
    entities: dict[str, dict],
) -> set[str]:
    related_qids = set()

    for entity in entities.values():
        if not entity or entity.get("missing"):
            continue

        related_qids.update(property_values(entity, "P27"))  # country of citizenship
        related_qids.update(property_values(entity, "P108"))  # employer

        if gender := property_value(entity, "P21"):
            related_qids.add(gender)

    return related_qids


if __name__ == "__main__":
    luigi.build([WikiDataCollectorsSourceTask()], local_scheduler=True)
