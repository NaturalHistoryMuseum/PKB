from __future__ import annotations

import json
import time
from abc import ABCMeta, abstractmethod
from datetime import UTC, datetime

import luigi

from pkb.config import logger, settings
from pkb.storage import (
    DatasetVersion,
    SourceObject,
    dataset_version_to_dict,
    ensure_source_bucket,
    fetch_source_dataset,
    get_s3_client,
    read_dataset_version,
    store_source_dataset,
)
from pkb.tasks.base import BaseTask
from pkb.wikidata.client import (
    chunks,
    fetch_entities_response,
    run_sparql_query_response,
)


WIKIDATA_SOURCE_DIR = settings.intermediate_dir / "sources" / "wikidata"


def qids_from_sparql_response(data: bytes) -> list[str]:
    payload = json.loads(data)
    rows = payload["results"]["bindings"]
    return sorted(
        {
            row["item"]["value"].rsplit("/", 1)[-1]
            for row in rows
        }
    )


class WikiDataSPARQLSourceTask(BaseTask, metaclass=ABCMeta):
    """Store a raw Wikidata SPARQL result as a versioned source object."""

    @property
    @abstractmethod
    def query(self) -> str:
        pass

    @property
    @abstractmethod
    def dataset(self) -> str:
        pass

    @property
    @abstractmethod
    def output_path(self):
        pass

    @property
    @abstractmethod
    def s3_key(self) -> str:
        pass

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def run(self):
        response = run_sparql_query_response(self.query)
        data = response.content
        qids = qids_from_sparql_response(data)

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        dataset_version = store_source_dataset(
            s3=s3,
            dataset=self.dataset,
            source_url=response.url,
            data=data,
            key=self.s3_key,
        )

        with self.output().open("w") as f:
            json.dump(
                dataset_version_to_dict(dataset_version),
                f,
                indent=2,
                sort_keys=True,
            )

        logger.info(
            "Stored %s Wikidata SPARQL IDs for %s at s3://%s/%s version=%s",
            len(qids),
            self.dataset,
            dataset_version.s3_bucket,
            dataset_version.s3_key,
            dataset_version.s3_version_id,
        )


class WikiDataEntitiesSourceTask(BaseTask, metaclass=ABCMeta):
    """Store raw wbgetentities JSON batches for reusable Wikidata entity sources."""

    batch_size = 50
    sleep_seconds = 0.2

    @property
    @abstractmethod
    def dataset(self) -> str:
        pass

    @property
    @abstractmethod
    def output_path(self):
        pass

    @property
    @abstractmethod
    def s3_key_prefix(self) -> str:
        pass

    @property
    @abstractmethod
    def entity_label(self) -> str:
        pass

    def output(self):
        return luigi.LocalTarget(self.output_path)

    def qids(self) -> list[str]:
        dataset_version = read_dataset_version(self.input().path)
        data = fetch_source_dataset(
            s3=get_s3_client(),
            dataset_version=dataset_version,
        )
        return qids_from_sparql_response(data)

    def run(self):
        qids = self.qids()
        total_batches = (len(qids) + self.batch_size - 1) // self.batch_size
        logger.info(
            "Fetching %s %s in %s Wikidata entity batches",
            len(qids),
            self.entity_label,
            total_batches,
        )

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        retrieved_at = datetime.now(UTC)
        source_objects: list[SourceObject] = []
        source_url = None

        for batch_number, batch in enumerate(
            chunks(qids, self.batch_size),
            start=1,
        ):
            response = fetch_entities_response(
                batch,
                label=f"{self.entity_label} batch {batch_number}/{total_batches}",
            )
            source_url = response.url
            batch_version = store_source_dataset(
                s3=s3,
                dataset=self.dataset,
                source_url=response.url,
                data=response.content,
                key=f"{self.s3_key_prefix}/batch-{batch_number:05d}.json",
                retrieved_at=retrieved_at,
            )
            source_objects.extend(batch_version.source_objects or [])

            if batch_number == 1 or batch_number % 10 == 0 or batch_number == total_batches:
                logger.info(
                    "Stored Wikidata entity batch %s/%s for %s",
                    batch_number,
                    total_batches,
                    self.dataset,
                )

            time.sleep(self.sleep_seconds)

        dataset_version = DatasetVersion(
            dataset=self.dataset,
            retrieved_at=retrieved_at,
            source_url=source_url or "https://www.wikidata.org/w/api.php",
            source_objects=source_objects,
        )

        with self.output().open("w") as f:
            json.dump(
                dataset_version_to_dict(dataset_version),
                f,
                indent=2,
                sort_keys=True,
            )

        logger.info(
            "Stored %s Wikidata entity batches for %s",
            len(source_objects),
            self.dataset,
        )
