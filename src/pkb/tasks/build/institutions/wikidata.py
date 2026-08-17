from __future__ import annotations

import luigi
import pandas as pd
from tqdm import tqdm

from pkb.config import logger, settings
from pkb.tasks.base import BaseTask
from pkb.wikidata.client import fetch_entities, run_sparql_query
from pkb.wikidata.parsers import parse_institution_entity


INSTITUTIONS_DIR = settings.intermediate_dir / "institutions"


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
        return luigi.LocalTarget(INSTITUTIONS_DIR / "wikidata_institution_ids.parquet")

    def run(self):
        rows = run_sparql_query(self.query)

        qids = sorted(
            {
                row["item"]["value"].rsplit("/", 1)[-1]
                for row in rows
            }
        )

        logger.info("Found %s institution Wikidata IDs", len(qids))
        logger.info("First 10 institution IDs: %s", qids[:10])

        df = pd.DataFrame({"wikidataID": qids})
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote institution IDs to %s", self.output().path)


class WikiDataInstitutionsTask(BaseTask):
    def requires(self):
        return WikiDataInstitutionIDsTask()

    def output(self):
        return luigi.LocalTarget(INSTITUTIONS_DIR / "wikidata_institutions.parquet")

    def run(self):
        logger.info("Starting WikiDataInstitutionsTask")

        ids = pd.read_parquet(self.input().path)
        qids = ids["wikidataID"].dropna().astype(str).drop_duplicates().tolist()

        logger.info("Loaded %s institution Wikidata IDs", len(qids))
        logger.info("First 10 institution IDs: %s", qids[:10])

        entities = fetch_entities(
            qids,
            batch_size=50,
            label="institution entities",
        )

        logger.info("Fetched %s institution entities", len(entities))

        records = []

        for qid in tqdm(qids, desc="Parsing Wikidata institutions"):
            entity = entities.get(qid)

            if not entity or entity.get("missing"):
                continue

            records.append(parse_institution_entity(qid, entity))

        logger.info("Parsed %s institution records", len(records))

        df = pd.DataFrame(records)
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote Wikidata institutions to %s", self.output().path)


if __name__ == "__main__":
    luigi.build([WikiDataInstitutionsTask()], local_scheduler=True)
