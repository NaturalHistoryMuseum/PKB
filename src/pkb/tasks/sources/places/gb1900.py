from __future__ import annotations

import json
import warnings

import luigi
import requests
from tqdm import tqdm
from urllib3.exceptions import InsecureRequestWarning

from pkb.config import logger, settings
from pkb.storage import (
    dataset_version_to_dict,
    ensure_source_bucket,
    get_s3_client,
    store_source_dataset,
)
from pkb.storage.s3 import sha256_bytes
from pkb.tasks.base import BaseTask


GB1900_FILENAME = "GB1900_gazetteer_complete_july_2018.zip"
GB1900_URL = f"https://www.visionofbritain.org.uk/downloads/{GB1900_FILENAME}"
GB1900_SHA256 = "aa19b6f2d665e30c2e28a033399f4e8c2308d092e5f200cba714b85d664d2ac2"
GB1900_SOURCE_DIR = settings.intermediate_dir / "sources" / "gb1900"


class GB1900PlacesSourceTask(BaseTask):
    timeout_seconds = 1800

    def output(self):
        return luigi.LocalTarget(GB1900_SOURCE_DIR / "places.dataset-version.json")

    def run(self):
        logger.info("Downloading GB1900 places from %s", GB1900_URL)
        logger.warning(
            "The GB1900 host has an incomplete TLS chain; validating the fixed "
            "archive against its pinned SHA-256"
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", InsecureRequestWarning)
            response = requests.get(
                GB1900_URL,
                timeout=self.timeout_seconds,
                stream=True,
                verify=False,
            )

        data = bytearray()
        try:
            response.raise_for_status()
            with tqdm(
                total=int(response.headers.get("content-length", 0)) or None,
                desc="Downloading GB1900 places",
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

        checksum = sha256_bytes(data)
        if checksum != GB1900_SHA256:
            raise ValueError(
                f"Unexpected GB1900 archive SHA-256: {checksum}"
            )

        s3 = get_s3_client()
        ensure_source_bucket(s3=s3)
        dataset_version = store_source_dataset(
            s3=s3,
            dataset="gb1900_places",
            source_url=response.url,
            data=data,
            key=f"gb1900/places/{GB1900_FILENAME}",
            upstream_version="July 2018",
        )

        with self.output().open("w") as manifest:
            json.dump(
                dataset_version_to_dict(dataset_version),
                manifest,
                indent=2,
                sort_keys=True,
            )

        logger.info(
            "Stored GB1900 places at s3://%s/%s version=%s",
            dataset_version.s3_bucket,
            dataset_version.s3_key,
            dataset_version.s3_version_id,
        )
