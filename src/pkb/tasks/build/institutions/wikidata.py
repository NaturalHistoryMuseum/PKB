from __future__ import annotations

import json

import luigi
import pandas as pd
from tqdm import tqdm

from pkb.config import logger, settings
from pkb.storage import fetch_source_object, get_s3_client, read_dataset_version
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.institutions.wikidata import (
    WikiDataInstitutionEntitiesSourceTask,
    WikiDataInstitutionIDsSourceTask,
)
from pkb.wikidata.parsers import parse_institution_entity


INSTITUTIONS_DIR = settings.intermediate_dir / "institutions"


class WikiDataInstitutionsTask(BaseTask):
    def requires(self):
        return WikiDataInstitutionEntitiesSourceTask()

    def output(self):
        return luigi.LocalTarget(INSTITUTIONS_DIR / "wikidata_institutions.parquet")

    def run(self):
        logger.info("Starting WikiDataInstitutionsTask")

        entities = self._read_entities()

        logger.info("Fetched %s institution entities", len(entities))

        records = []

        for qid in tqdm(sorted(entities), desc="Parsing Wikidata institutions"):
            entity = entities.get(qid)

            if not entity or entity.get("missing"):
                continue

            records.append(parse_institution_entity(qid, entity))

        logger.info("Parsed %s institution records", len(records))

        df = pd.DataFrame(records)
        df.to_parquet(self.output().path, index=False)

        logger.info("Wrote Wikidata institutions to %s", self.output().path)

    def _read_entities(self) -> dict[str, dict]:
        dataset_version = read_dataset_version(self.input().path)
        s3 = get_s3_client()
        entities = {}

        for source_object in dataset_version.source_objects or []:
            data = fetch_source_object(s3=s3, source_object=source_object)
            payload = json.loads(data)
            entities.update(payload.get("entities", {}))

        return entities


class WikiDataInstitutionIDsTask(WikiDataInstitutionIDsSourceTask):
    """Compatibility alias for the Wikidata institution source ID task."""


if __name__ == "__main__":
    luigi.build([WikiDataInstitutionsTask()], local_scheduler=True)
