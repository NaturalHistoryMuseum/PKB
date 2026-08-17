from __future__ import annotations

import luigi

from pkb.config import settings
from pkb.tasks.sources.wikidata import (
    WikiDataEntitiesSourceTask,
    WikiDataSPARQLSourceTask,
)


WIKIDATA_INSTITUTION_SOURCE_DIR = (
    settings.intermediate_dir / "sources" / "wikidata" / "institutions"
)


class WikiDataInstitutionIDsSourceTask(WikiDataSPARQLSourceTask):
    """Store raw SPARQL discovery results for Wikidata institutions."""

    dataset = "wikidata_institution_ids"
    output_path = WIKIDATA_INSTITUTION_SOURCE_DIR / "ids.dataset-version.json"
    s3_key = "wikidata/institutions/ids/source.json"
    query = """
        SELECT DISTINCT ?item WHERE {
          {
            ?item p:P31/ps:P31/wdt:P279 wd:Q2982911 .
          }
          UNION
          {
            ?item p:P31/ps:P31/wdt:P279* wd:Q181916 .
          }
          UNION
          {
            ?item wdt:P4090 ?grSciCollID .
          }
          UNION
          {
            ?item wdt:P6782 ?rorID .
          }
          UNION
          {
            ?item wdt:P5858 ?indexHerbariumCode .
          }
        }
        ORDER BY ?item
    """


class WikiDataInstitutionEntitiesSourceTask(WikiDataEntitiesSourceTask):
    """Store raw wbgetentities batches for Wikidata institutions."""

    dataset = "wikidata_institution_entities"
    output_path = WIKIDATA_INSTITUTION_SOURCE_DIR / "entities.dataset-version.json"
    s3_key_prefix = "wikidata/institutions/entities"
    entity_label = "institution entities"

    def requires(self):
        return WikiDataInstitutionIDsSourceTask()


if __name__ == "__main__":
    luigi.build([WikiDataInstitutionEntitiesSourceTask()], local_scheduler=True)
