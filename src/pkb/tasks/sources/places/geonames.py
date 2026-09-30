from __future__ import annotations

import json

import luigi
import requests
from tqdm import tqdm

from pkb.config import logger, settings
from pkb.storage import (
    dataset_version_to_dict,
    ensure_source_bucket,
    get_s3_client,
    store_source_dataset,
)
from pkb.tasks.base import BaseTask


GEONAMES_URL = "https://download.geonames.org/export/dump"
GEONAMES_SOURCE_DIR = settings.intermediate_dir / "sources" / "geonames"


class GeoNamesSourceTask(BaseTask):
    timeout_seconds = 1800
    filename: str
    dataset: str

    def output(self):
        return luigi.LocalTarget(
            GEONAMES_SOURCE_DIR / f"{self.dataset}.dataset-version.json"
        )

    def run(self):
        url = f"{GEONAMES_URL}/{self.filename}"
        logger.info("Downloading GeoNames %s from %s", self.dataset, url)
        response = requests.get(url, timeout=self.timeout_seconds, stream=True)
        data = bytearray()
        try:
            response.raise_for_status()
            with tqdm(
                total=int(response.headers.get("content-length", 0)) or None,
                desc=f"Downloading GeoNames {self.dataset}",
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            ) as progress:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        data.extend(chunk)
                        progress.update(len(chunk))
        finally:
            response.close()

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        dataset_version = store_source_dataset(
            s3=s3,
            dataset=f"geonames_{self.dataset}",
            source_url=response.url,
            data=data,
            key=f"geonames/places/{self.filename}",
        )

        with self.output().open("w") as manifest:
            json.dump(
                dataset_version_to_dict(dataset_version),
                manifest,
                indent=2,
                sort_keys=True,
            )

        logger.info(
            "Stored GeoNames %s at s3://%s/%s version=%s",
            self.dataset,
            dataset_version.s3_bucket,
            dataset_version.s3_key,
            dataset_version.s3_version_id,
        )


class GeoNamesPlacesSourceTask(GeoNamesSourceTask):
    filename = "allCountries.zip"
    dataset = "places"


class GeoNamesAlternateNamesSourceTask(GeoNamesSourceTask):
    filename = "alternateNamesV2.zip"
    dataset = "alternate_names"
