from __future__ import annotations

import json

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


BIONOMIA_PEOPLE_SOURCE_DIR = (
    settings.intermediate_dir / "sources" / "bionomia" / "people"
)
BIONOMIA_PUBLIC_PROFILES_URL = "https://bionomia.net/data/bionomia-public-profiles.csv"


class BionomiaPublicProfilesSourceTask(BaseTask):
    """Download Bionomia public profiles and store raw CSV bytes in versioned S3."""

    timeout_seconds = 180

    def output(self):
        return luigi.LocalTarget(
            BIONOMIA_PEOPLE_SOURCE_DIR / "public_profiles.dataset-version.json"
        )

    def run(self):
        logger.info("Downloading Bionomia public profiles from %s", self.url)

        response = requests.get(self.url, timeout=self.timeout_seconds)
        response.raise_for_status()

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        dataset_version = store_source_dataset(
            s3=s3,
            dataset="bionomia_public_profiles",
            source_url=response.url,
            data=response.content,
            key="bionomia/people/public-profiles/source.csv",
        )

        with self.output().open("w") as f:
            json.dump(
                dataset_version_to_dict(dataset_version),
                f,
                indent=2,
                sort_keys=True,
            )

        logger.info(
            "Stored Bionomia public profiles at s3://%s/%s version=%s",
            dataset_version.s3_bucket,
            dataset_version.s3_key,
            dataset_version.s3_version_id,
        )

    @property
    def url(self) -> str:
        return BIONOMIA_PUBLIC_PROFILES_URL


if __name__ == "__main__":
    luigi.build([BionomiaPublicProfilesSourceTask()], local_scheduler=True)
