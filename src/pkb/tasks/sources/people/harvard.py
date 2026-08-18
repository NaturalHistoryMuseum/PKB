from __future__ import annotations

import json

import luigi

from pkb.config import logger, settings
from pkb.storage import (
    dataset_version_to_dict,
    ensure_source_bucket,
    get_s3_client,
    store_source_dataset,
)
from pkb.tasks.base import BaseTask


HARVARD_INDEX_ASSET_DIR = settings.assets_dir / "harvard-index"
HARVARD_INDEX_SOURCE_DIR = (
    settings.intermediate_dir / "sources" / "harvard-index" / "people"
)


class HarvardIndexCollectorsFile(luigi.ExternalTask):
    """Archived output from the historical Harvard Index scraper."""

    def output(self):
        return luigi.LocalTarget(HARVARD_INDEX_ASSET_DIR / "huh-collectors.csv")


class HarvardIndexAgentsFile(luigi.ExternalTask):
    """Input agent list used by the historical Harvard Index scraper."""

    def output(self):
        return luigi.LocalTarget(HARVARD_INDEX_ASSET_DIR / "huh-agents.tab.txt")


class HarvardIndexCollectorsSourceTask(BaseTask):
    """Store the archived Harvard Index collectors CSV in versioned S3."""

    def requires(self):
        return HarvardIndexCollectorsFile()

    def output(self):
        return luigi.LocalTarget(
            HARVARD_INDEX_SOURCE_DIR / "collectors.dataset-version.json"
        )

    def run(self):
        with open(self.input().path, "rb") as f:
            data = f.read()

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        dataset_version = store_source_dataset(
            s3=s3,
            dataset="harvard_index_collectors",
            source_url="archived-local-file:data/assets/harvard-index/huh-collectors.csv",
            data=data,
            key="harvard-index/people/collectors/source.csv",
            upstream_version="historical scrape from src/pkb/tasks/harvard.py",
        )

        with self.output().open("w") as f:
            json.dump(
                dataset_version_to_dict(dataset_version),
                f,
                indent=2,
                sort_keys=True,
            )

        logger.info(
            "Stored archived Harvard Index collectors at s3://%s/%s version=%s",
            dataset_version.s3_bucket,
            dataset_version.s3_key,
            dataset_version.s3_version_id,
        )


if __name__ == "__main__":
    luigi.build([HarvardIndexCollectorsSourceTask()], local_scheduler=True)
