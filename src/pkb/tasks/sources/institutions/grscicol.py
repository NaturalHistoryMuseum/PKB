from __future__ import annotations

import json
from abc import ABCMeta, abstractmethod

import luigi
import requests

from pkb.config import logger, settings
from pkb.storage import (
    dataset_version_to_dict,
    ensure_source_bucket,
    get_s3_client,
    store_source_dataset,
)
from pkb.tasks.base import BaseTask


GRSCICOLL_SOURCE_DIR = settings.intermediate_dir / "sources" / "grscicol"
GRSCICOLL_API_BASE = "https://api.gbif.org/v1/grscicoll"


class GRSciCollTSVSourceTask(BaseTask, metaclass=ABCMeta):
    """Download a GRSciColl TSV export and store raw bytes in versioned S3."""

    timeout_seconds = 180

    @property
    @abstractmethod
    def dataset(self) -> str:
        pass

    @property
    @abstractmethod
    def export_url(self) -> str:
        pass

    @property
    @abstractmethod
    def s3_key(self) -> str:
        pass

    def output(self):
        return luigi.LocalTarget(
            GRSCICOLL_SOURCE_DIR / f"{self.dataset}.dataset-version.json"
        )

    def run(self):
        logger.info("Downloading GRSciColl TSV export from %s", self.export_url)

        response = requests.get(
            self.export_url,
            params={
                "active": "true",
                "format": "TSV",
            },
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        data = response.content

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        dataset_version = store_source_dataset(
            s3=s3,
            dataset=f"grscicol_{self.dataset}",
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
            "Stored GRSciColl %s source object at s3://%s/%s version=%s",
            self.dataset,
            dataset_version.s3_bucket,
            dataset_version.s3_key,
            dataset_version.s3_version_id,
        )


class GRSciCollInstitutionsTSVSourceTask(GRSciCollTSVSourceTask):
    dataset = "institutions"
    export_url = f"{GRSCICOLL_API_BASE}/institution/export"
    s3_key = "grscicol/institutions/source.tsv"


class GRSciCollCollectionsTSVSourceTask(GRSciCollTSVSourceTask):
    dataset = "collections"
    export_url = f"{GRSCICOLL_API_BASE}/collection/export"
    s3_key = "grscicol/collections/source.tsv"


if __name__ == "__main__":
    luigi.build(
        [
            GRSciCollInstitutionsTSVSourceTask(),
        ],
        local_scheduler=True,
    )
