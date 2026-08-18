from __future__ import annotations

import json

import luigi

from pkb.config import logger, settings
from pkb.storage import fetch_source_object, get_s3_client, read_dataset_version
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.wikidata import (
    WikiDataEntityBatchSourceTask,
    WikiDataEntitiesSourceTask,
    WikiDataSPARQLSourceTask,
)
from pkb.wikidata.parsers import property_value, property_values


WIKIDATA_PEOPLE_SOURCE_DIR = settings.intermediate_dir / "sources" / "wikidata" / "people"


def collect_property_qids_for_collectors(
    entities: dict[str, dict],
) -> set[str]:
    property_qids = set()

    for entity in entities.values():
        if not entity or entity.get("missing"):
            continue

        property_qids.update(property_values(entity, "P27"))  # country of citizenship
        property_qids.update(property_values(entity, "P108"))  # employer

        if gender := property_value(entity, "P21"):
            property_qids.add(gender)

    return property_qids


def read_entities_from_manifest(path: str) -> dict[str, dict]:
    dataset_version = read_dataset_version(path)
    s3 = get_s3_client()
    entities = {}

    for source_object in dataset_version.source_objects or []:
        data = fetch_source_object(s3=s3, source_object=source_object)
        payload = json.loads(data)
        entities.update(payload.get("entities", {}))

    return entities


class WikiDataCollectorIDsSourceTask(WikiDataSPARQLSourceTask):
    """Store raw SPARQL discovery results for Wikidata collectors."""

    dataset = "wikidata_collector_ids"
    output_path = WIKIDATA_PEOPLE_SOURCE_DIR / "collector_ids.dataset-version.json"
    s3_key = "wikidata/people/collector-ids/source.json"
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


class WikiDataCollectorEntityBatchSourceTask(WikiDataEntityBatchSourceTask):
    """Store one raw wbgetentities batch for Wikidata collectors."""

    dataset = "wikidata_collector_entities"
    output_dir = WIKIDATA_PEOPLE_SOURCE_DIR / "collector-entity-batches"
    s3_key_prefix = "wikidata/people/collector-entities"
    entity_label = "collector entities"
    ids_source_task_class = WikiDataCollectorIDsSourceTask


class WikiDataCollectorEntitiesSourceTask(WikiDataEntitiesSourceTask):
    """Store raw wbgetentities batches for Wikidata collectors."""

    dataset = "wikidata_collector_entities"
    output_path = WIKIDATA_PEOPLE_SOURCE_DIR / "collector_entities.dataset-version.json"
    s3_key_prefix = "wikidata/people/collector-entities"
    entity_label = "collector entities"
    batch_task_class = WikiDataCollectorEntityBatchSourceTask

    def requires(self):
        return WikiDataCollectorIDsSourceTask()


class WikiDataCollectorPropertyIDsTask(BaseTask):
    """Derive the Wikidata entity IDs referenced by collector claims."""

    output_path = WIKIDATA_PEOPLE_SOURCE_DIR / "collector_property_ids.json"

    def requires(self):
        return WikiDataCollectorEntitiesSourceTask()

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        collector_entities = read_entities_from_manifest(self.input().path)
        qids = sorted(collect_property_qids_for_collectors(collector_entities))

        with self.output().open("w") as f:
            json.dump(
                {
                    "qids": qids,
                },
                f,
                indent=2,
                sort_keys=True,
            )

        logger.info("Derived %s collector property Wikidata entity IDs", len(qids))


class WikiDataCollectorPropertyEntityBatchSourceTask(WikiDataEntityBatchSourceTask):
    """Store one raw wbgetentities batch for entities referenced by collectors."""

    dataset = "wikidata_collector_property_entities"
    output_dir = WIKIDATA_PEOPLE_SOURCE_DIR / "collector-property-entity-batches"
    s3_key_prefix = "wikidata/people/collector-property-entities"
    entity_label = "collector property entities"
    ids_source_task_class = WikiDataCollectorPropertyIDsTask

    def qids(self) -> list[str]:
        with self.input().open("r") as f:
            return json.load(f)["qids"]


class WikiDataCollectorPropertyEntitiesSourceTask(WikiDataEntitiesSourceTask):
    """Store raw wbgetentities batches for entities referenced by collectors."""

    dataset = "wikidata_collector_property_entities"
    output_path = (
        WIKIDATA_PEOPLE_SOURCE_DIR / "collector_property_entities.dataset-version.json"
    )
    s3_key_prefix = "wikidata/people/collector-property-entities"
    entity_label = "collector property entities"
    batch_task_class = WikiDataCollectorPropertyEntityBatchSourceTask

    def requires(self):
        return WikiDataCollectorPropertyIDsTask()

    def qids(self) -> list[str]:
        with self.input().open("r") as f:
            return json.load(f)["qids"]


if __name__ == "__main__":
    luigi.build([WikiDataCollectorEntitiesSourceTask()], local_scheduler=True)
