from __future__ import annotations

from io import BytesIO

import luigi
import pandas as pd

from pkb.config import logger, settings
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.people.bionomia import BionomiaPublicProfilesSourceTask


PEOPLE_DIR = settings.intermediate_dir / "people"


def _clean(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    return value or None


def _split_pipe(value) -> list[str]:
    value = _clean(value)
    if not value:
        return []
    return [part.strip() for part in value.split("|") if part.strip()]


def _person_name(row) -> str | None:
    if label_name := _clean(row.get("LabelName")):
        return label_name

    parts = [
        _clean(row.get("Given")),
        _clean(row.get("Particle")),
        _clean(row.get("Family")),
    ]
    return " ".join(part for part in parts if part) or None


def _bionomia_id(row) -> str | None:
    if orcid := _clean(row.get("ORCID")):
        return orcid
    if url := _clean(row.get("URL")):
        return url.rstrip("/").rsplit("/", 1)[-1]
    return None


class BionomiaPeopleTask(BaseTask):
    """Build People-domain rows from stored raw Bionomia public profiles."""

    source_name = "bionomia"

    def requires(self):
        return BionomiaPublicProfilesSourceTask()

    def output(self):
        return luigi.LocalTarget(PEOPLE_DIR / "bionomia_people.parquet")

    def run(self):
        dataset_version = read_dataset_version(self.input().path)
        data = fetch_source_dataset(
            s3=get_s3_client(),
            dataset_version=dataset_version,
        )
        source = pd.read_csv(BytesIO(data), dtype=str, keep_default_na=False)

        records = []
        for _, row in source.iterrows():
            bionomia_id = _bionomia_id(row)
            if not bionomia_id:
                continue

            records.append(
                {
                    "pkbPersonKey": f"{self.source_name}:{bionomia_id}",
                    "source": self.source_name,
                    "bionomiaID": bionomia_id,
                    "orcidID": _clean(row.get("ORCID")),
                    "wikidataID": _clean(row.get("wikidata")),
                    "name": _person_name(row),
                    "familyName": _clean(row.get("Family")),
                    "givenName": _clean(row.get("Given")),
                    "particle": _clean(row.get("Particle")),
                    "aliases": _split_pipe(row.get("OtherNames")),
                    "country": _clean(row.get("Country")),
                    "keywords": _split_pipe(row.get("Keywords")),
                    "url": _clean(row.get("URL")),
                }
            )

        people = pd.DataFrame(records)
        people.to_parquet(self.output().path, index=False)

        logger.info("Wrote %s Bionomia people to %s", len(people), self.output().path)


if __name__ == "__main__":
    luigi.build([BionomiaPeopleTask()], local_scheduler=True)
