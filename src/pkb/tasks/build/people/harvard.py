from __future__ import annotations

from io import BytesIO
from pathlib import Path

import luigi
import pandas as pd

from pkb.config import logger, settings
from pkb.models.people import HUHPersonDeduplicator
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.people.harvard import HarvardIndexCollectorsSourceTask
from pkb.utils import clean_scalar, parse_literal_collection


PEOPLE_DIR = settings.intermediate_dir / "people"


def _name(row) -> str | None:
    return (
        clean_scalar(row.get("Standard/Label Name"))
        or clean_scalar(row.get("Full Name"))
        or clean_scalar(row.get("Name"))
    )


class HarvardIndexPeopleBuildTask(BaseTask):
    """Build deduplicated People-domain rows from the archived Harvard Index scrape."""

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
            harvard_index_id = clean_scalar(row.get("id"))
            if not harvard_index_id:
                continue

            records.append(
                {
                    "pkbPersonKey": f"{self.source_name}:{harvard_index_id}",
                    "source": self.source_name,
                    "harvardIndexID": harvard_index_id,
                    "name": _name(row),
                    "fullName": clean_scalar(row.get("Full Name")),
                    "standardName": clean_scalar(row.get("Standard/Label Name")),
                    "authorAbbreviation": clean_scalar(row.get("B & P Author Abbrev.")),
                    "dateOfBirth": clean_scalar(row.get("Date of birth")),
                    "dateOfDeath": clean_scalar(row.get("Date of death")),
                    "birthYear": clean_scalar(row.get("birthYear")),
                    "deathYear": clean_scalar(row.get("deathYear")),
                    "birthYearIsApprox": clean_scalar(row.get("birthYearIsApprox")),
                    "deathYearIsApprox": clean_scalar(row.get("deathYearIsApprox")),
                    "geographyISO": parse_literal_collection(row.get("geographyISO")),
                    "herbariaCode": parse_literal_collection(row.get("herbariaCode")),
                    "institutionUUID": parse_literal_collection(row.get("institutionUUID")),
                    "agentType": clean_scalar(row.get("Agent type")),
                    "url": clean_scalar(row.get("URL")),
                }
            )

        people = pd.DataFrame(records)
        result = HUHPersonDeduplicator().deduplicate(people)
        deduplicated_people = _deduplicated_people(
            people,
            result.deduplication_map,
        )
        output = self.output()
        output_path = Path(output.path)

        result.deduplication_map.to_parquet(
            output_path.with_name("harvard_index_person_deduplication_map.parquet"),
            index=False,
        )
        result.clusters.to_parquet(
            output_path.with_name("harvard_index_person_deduplication_clusters.parquet"),
            index=False,
        )
        deduplicated_people.to_parquet(output.path, index=False)

        duplicates_removed = len(people) - len(deduplicated_people)
        logger.info(
            "Harvard Index deduplication: %s source records -> %s people "
            "(%s duplicates removed across %s clusters)",
            len(people),
            len(deduplicated_people),
            duplicates_removed,
            len(result.clusters),
        )
        logger.info(
            "Wrote %s deduplicated Harvard Index people to %s",
            len(deduplicated_people),
            output.path,
        )


def _deduplicated_people(
    people: pd.DataFrame,
    deduplication_map: pd.DataFrame,
) -> pd.DataFrame:
    mapped = people.merge(
        deduplication_map[
            ["huhClusterID", "harvardIndexID", "canonicalHarvardIndexID"]
        ],
        on="harvardIndexID",
        how="left",
    )
    records = []

    for canonical_id, group in mapped.groupby("canonicalHarvardIndexID", sort=True):
        canonical = group[group["harvardIndexID"].eq(canonical_id)].iloc[0]
        record = canonical.drop(
            labels=["huhClusterID", "canonicalHarvardIndexID"],
            errors="ignore",
        ).to_dict()
        harvard_ids = sorted(group["harvardIndexID"].astype(str).tolist())

        record["pkbPersonKey"] = f"harvard_index:{canonical_id}"
        record["harvardIndexID"] = harvard_ids
        record["deduplicatedHarvardIndexIDs"] = harvard_ids
        record["sourceRecordCount"] = len(group)

        for column in ["geographyISO", "herbariaCode", "institutionUUID"]:
            record[column] = _merge_lists(group[column])

        records.append(record)

    return pd.DataFrame(records)


def _merge_lists(values: pd.Series) -> list[str]:
    merged = []
    for value in values:
        if isinstance(value, list):
            merged.extend(str(item) for item in value if clean_scalar(item))
        elif clean_scalar(value):
            merged.append(str(value))
    return sorted(set(merged))


if __name__ == "__main__":
    luigi.build([HarvardIndexPeopleBuildTask()], local_scheduler=True)
