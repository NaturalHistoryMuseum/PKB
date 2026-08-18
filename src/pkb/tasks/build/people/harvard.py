from __future__ import annotations

import ast
from io import BytesIO

import luigi
import pandas as pd

from pkb.config import logger, settings
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.people.harvard import HarvardIndexCollectorsSourceTask


PEOPLE_DIR = settings.intermediate_dir / "people"


def _clean(value) -> str | None:
    if value is None or pd.isna(value):
        return None
    value = str(value).strip()
    if not value or value == "nan":
        return None
    return value


def _parse_literal_collection(value) -> list[str]:
    value = _clean(value)
    if not value:
        return []

    try:
        parsed = ast.literal_eval(value)
    except (SyntaxError, ValueError):
        return [value]

    if isinstance(parsed, (set, list, tuple)):
        return sorted(str(item) for item in parsed if _clean(item))

    return [str(parsed)]


def _name(row) -> str | None:
    return (
        _clean(row.get("Standard/Label Name"))
        or _clean(row.get("Full Name"))
        or _clean(row.get("Name"))
    )


class HarvardIndexPeopleTask(BaseTask):
    """Build People-domain rows from the archived Harvard Index scrape."""

    source_name = "harvard_index"

    def requires(self):
        return HarvardIndexCollectorsSourceTask()

    def output(self):
        return luigi.LocalTarget(PEOPLE_DIR / "harvard_index_people.parquet")

    def run(self):
        dataset_version = read_dataset_version(self.input().path)
        data = fetch_source_dataset(
            s3=get_s3_client(),
            dataset_version=dataset_version,
        )
        source = pd.read_csv(BytesIO(data), dtype=str)

        records = []
        for _, row in source.iterrows():
            harvard_index_id = _clean(row.get("id"))
            if not harvard_index_id:
                continue

            records.append(
                {
                    "pkbPersonKey": f"{self.source_name}:{harvard_index_id}",
                    "source": self.source_name,
                    "harvardIndexID": harvard_index_id,
                    "name": _name(row),
                    "fullName": _clean(row.get("Full Name")),
                    "standardName": _clean(row.get("Standard/Label Name")),
                    "authorAbbreviation": _clean(row.get("B & P Author Abbrev.")),
                    "dateOfBirth": _clean(row.get("Date of birth")),
                    "dateOfDeath": _clean(row.get("Date of death")),
                    "birthYear": _clean(row.get("birthYear")),
                    "deathYear": _clean(row.get("deathYear")),
                    "birthYearIsApprox": _clean(row.get("birthYearIsApprox")),
                    "deathYearIsApprox": _clean(row.get("deathYearIsApprox")),
                    "geographyISO": _parse_literal_collection(row.get("geographyISO")),
                    "herbariaCode": _parse_literal_collection(row.get("herbariaCode")),
                    "institutionUUID": _parse_literal_collection(row.get("institutionUUID")),
                    "agentType": _clean(row.get("Agent type")),
                    "url": _clean(row.get("URL")),
                }
            )

        people = pd.DataFrame(records)
        people.to_parquet(self.output().path, index=False)

        logger.info(
            "Wrote %s Harvard Index people to %s",
            len(people),
            self.output().path,
        )


if __name__ == "__main__":
    luigi.build([HarvardIndexPeopleTask()], local_scheduler=True)
