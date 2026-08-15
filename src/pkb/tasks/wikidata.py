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
    parse_institution_entity,
    property_value,
    property_values,
)


WIKIDATA_DIR = settings.intermediate_dir / "wikidata"


class WikiDataCollectorIDsTask(BaseTask):
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
        return luigi.LocalTarget(WIKIDATA_DIR / "collector_ids.parquet")

    def run(self):
        rows = run_sparql_query(self.query)

        qids = sorted(
            {
                row["item"]["value"].rsplit("/", 1)[-1]
                for row in rows
            }
        )

        df = pd.DataFrame({"wikidataID": qids})
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote %s Wikidata collector IDs", len(df))


class WikiDataInstitutionIDsTask(BaseTask):
    query = """
        SELECT DISTINCT ?item WHERE {
          {
            ?item p:P31/ps:P31/wdt:P279 wd:Q2982911 .
          }
          UNION
          {
            ?item p:P31/ps:P31/wdt:P279* wd:Q181916 .
          }
        }
        ORDER BY ?item
    """

    def output(self):
        return luigi.LocalTarget(WIKIDATA_DIR / "institution_ids.parquet")

    def run(self):
        rows = run_sparql_query(self.query)

        qids = sorted(
            {
                row["item"]["value"].rsplit("/", 1)[-1]
                for row in rows
            }
        )

        df = pd.DataFrame({"wikidataID": qids})
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote %s Wikidata institution IDs", len(df))


class WikiDataCollectorsTask(BaseTask):
    def requires(self):
        return WikiDataCollectorIDsTask()

    def output(self):
        return luigi.LocalTarget(WIKIDATA_DIR / "collectors.parquet")

    def run(self):
        ids = pd.read_parquet(self.input().path)
        qids = ids["wikidataID"].dropna().astype(str).drop_duplicates().tolist()

        entities = fetch_entities(qids, batch_size=50)

        related_qids = collect_related_qids_for_collectors(entities)
        related_entities = fetch_entities(sorted(related_qids), batch_size=50)

        labels_by_qid = {
            qid: label(entity)
            for qid, entity in related_entities.items()
            if label(entity)
        }

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

        df = pd.DataFrame(records)
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote %s Wikidata collectors", len(df))


class WikiDataInstitutionsTask(BaseTask):
    def requires(self):
        return WikiDataInstitutionIDsTask()

    def output(self):
        return luigi.LocalTarget(WIKIDATA_DIR / "institutions.parquet")

    def run(self):
        ids = pd.read_parquet(self.input().path)
        qids = ids["wikidataID"].dropna().astype(str).drop_duplicates().tolist()

        entities = fetch_entities(qids, batch_size=50)

        records = []

        for qid in tqdm(qids, desc="Parsing Wikidata institutions"):
            entity = entities.get(qid)

            if not entity or entity.get("missing"):
                continue

            records.append(parse_institution_entity(qid, entity))

        df = pd.DataFrame(records)
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote %s Wikidata institutions", len(df))


class WikiDataTask(BaseTask):
    def requires(self):
        return [
            WikiDataCollectorsTask(),
            WikiDataInstitutionsTask(),
        ]

    def output(self):
        return luigi.LocalTarget(WIKIDATA_DIR / "_SUCCESS")

    def run(self):
        with self.output().open("w") as f:
            f.write("ok\n")


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
    luigi.build(
        [WikiDataCollectorIDsTask()],
        local_scheduler=True,
    )