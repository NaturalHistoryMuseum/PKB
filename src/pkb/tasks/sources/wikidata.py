from __future__ import annotations

import json
import math
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
    """Combine raw wbgetentities batch manifests for a reusable entity source."""

    batch_size = 50

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

    @property
    @abstractmethod
    def batch_task_class(self):
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
        total_batches = math.ceil(len(qids) / self.batch_size)
        logger.info(
            "Materialising %s %s through %s Wikidata entity batch tasks",
            len(qids),
            self.entity_label,
            total_batches,
        )

        batch_tasks = [
            self.batch_task_class(batch_index=batch_index)
            for batch_index in range(total_batches)
        ]
        incomplete_batch_tasks = [
            batch_task for batch_task in batch_tasks if not batch_task.complete()
        ]

        if incomplete_batch_tasks:
            logger.info(
                "Yielding %s incomplete Wikidata entity batch tasks for %s",
                len(incomplete_batch_tasks),
                self.dataset,
            )
            yield incomplete_batch_tasks

        retrieved_at = datetime.now(UTC)
        source_objects: list[SourceObject] = []
        source_url = None

        for batch_task in batch_tasks:
            batch_version = read_dataset_version(batch_task.output().path)
            source_url = batch_version.source_url
            source_objects.extend(batch_version.source_objects or [])

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


class WikiDataEntityBatchSourceTask(BaseTask, metaclass=ABCMeta):
    """Store one raw wbgetentities JSON batch as a versioned source object."""

    batch_index = luigi.IntParameter()
    batch_size = 50

    @property
    @abstractmethod
    def dataset(self) -> str:
        pass

    @property
    @abstractmethod
    def output_dir(self):
        pass

    @property
    @abstractmethod
    def s3_key_prefix(self) -> str:
        pass

    @property
    @abstractmethod
    def entity_label(self) -> str:
        pass

    @property
    @abstractmethod
    def ids_source_task_class(self):
        pass

    def requires(self):
        return self.ids_source_task_class()

    def output(self):
        return luigi.LocalTarget(
            self.output_dir / f"batch-{self.batch_index:05d}.dataset-version.json"
        )

    def qids(self) -> list[str]:
        dataset_version = read_dataset_version(self.input().path)
        data = fetch_source_dataset(
            s3=get_s3_client(),
            dataset_version=dataset_version,
        )
        return qids_from_sparql_response(data)

    def batch_qids(self) -> list[str]:
        start = self.batch_index * self.batch_size
        end = start + self.batch_size
        return self.qids()[start:end]

    def run(self):
        batch = self.batch_qids()

        if not batch:
            raise ValueError(
                f"No QIDs found for {self.entity_label} batch {self.batch_index}"
            )

        response = fetch_entities_response(
            batch,
            label=f"{self.entity_label} batch {self.batch_index}",
        )

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        dataset_version = store_source_dataset(
            s3=s3,
            dataset=self.dataset,
            source_url=response.url,
            data=response.content,
            key=f"{self.s3_key_prefix}/batch-{self.batch_index:05d}.json",
            retrieved_at=datetime.now(UTC),
        )

        with self.output().open("w") as f:
            json.dump(
                dataset_version_to_dict(dataset_version),
                f,
                indent=2,
                sort_keys=True,
            )

        logger.info(
            "Stored %s raw Wikidata entities for %s batch %s at s3://%s/%s version=%s",
            len(batch),
            self.dataset,
            self.batch_index,
            dataset_version.s3_bucket,
            dataset_version.s3_key,
            dataset_version.s3_version_id,
        )
