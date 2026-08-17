from __future__ import annotations

import luigi
import pandas as pd
from tqdm import tqdm

from pkb.config import logger, settings
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorEntitiesSourceTask,
    WikiDataRelatedCollectorEntitiesSourceTask,
    read_entities_from_manifest,
)
from pkb.wikidata.parsers import label, parse_collector_entity


PEOPLE_DIR = settings.intermediate_dir / "people"


class WikiDataPeopleTask(BaseTask):
    """Build People-domain rows from stored raw Wikidata collector entities."""

    source_name = "wikidata"

    def requires(self):
        return {
            "collectors": WikiDataCollectorEntitiesSourceTask(),
            "related": WikiDataRelatedCollectorEntitiesSourceTask(),
        }

    def output(self):
        return luigi.LocalTarget(PEOPLE_DIR / "wikidata_people.parquet")

    def run(self):
        logger.info("Starting WikiDataPeopleTask")

        collector_entities = read_entities_from_manifest(self.input()["collectors"].path)
        related_entities = read_entities_from_manifest(self.input()["related"].path)
        labels_by_qid = {
            qid: entity_label
            for qid, entity in related_entities.items()
            if (entity_label := label(entity))
        }

        logger.info("Loaded %s Wikidata collector entities", len(collector_entities))
        logger.info("Built label lookup for %s related entities", len(labels_by_qid))

        records = []
        for qid in tqdm(sorted(collector_entities), desc="Parsing Wikidata collectors"):
            entity = collector_entities.get(qid)

            if not entity or entity.get("missing"):
                continue

            records.append(
                parse_collector_entity(
                    qid=qid,
                    entity=entity,
                    labels_by_qid=labels_by_qid,
                )
            )

        logger.info("Parsed %s Wikidata people records", len(records))

        people = pd.DataFrame(records)
        people = people.assign(
            pkbPersonKey=lambda df: self.source_name + ":" + df["wikidataID"].astype(str),
            source=self.source_name,
        )
        people.to_parquet(self.output().path, index=False)

        logger.info("Wrote Wikidata people to %s", self.output().path)


if __name__ == "__main__":
    luigi.build([WikiDataPeopleTask()], local_scheduler=True)
