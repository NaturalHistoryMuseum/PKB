from __future__ import annotations

from abc import ABCMeta, abstractmethod
from io import BytesIO

import country_converter as coco
import luigi
import pandas as pd
import requests_cache

from pkb.config import logger, settings
from pkb.storage import fetch_source_dataset, get_s3_client, read_dataset_version
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.institutions.grscicol import (
    GRSciCollCollectionsTSVSourceTask,
    GRSciCollInstitutionsTSVSourceTask,
)


def _first_existing_column(df: pd.DataFrame, *columns: str) -> pd.Series:
    for column in columns:
        if column in df.columns:
            return df[column]
    return pd.Series([None] * len(df), index=df.index)


def _add_missing_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df


def _read_tsv_source_from_manifest(input_target) -> pd.DataFrame:
    dataset_version = read_dataset_version(input_target.path)
    data = fetch_source_dataset(
        s3=get_s3_client(),
        dataset_version=dataset_version,
    )
    return pd.read_csv(BytesIO(data), sep="\t", dtype=str)


class GRSciCollCollectionsTSVTask(BaseTask):
    """Build Collection rows from the GRSciColl TSV export."""

    def requires(self):
        return GRSciCollCollectionsTSVSourceTask()

    def output(self):
        return luigi.LocalTarget(
            settings.intermediate_dir / "institutions" / "grscicol_collections.parquet"
        )

    def run(self):
        source = _read_tsv_source_from_manifest(self.input())

        df = pd.DataFrame(
            {
                "key": _first_existing_column(source, "key"),
                "code": _first_existing_column(source, "code"),
                "institutionKey": _first_existing_column(
                    source,
                    "institution_key",
                    "institutionKey",
                ),
                "name": _first_existing_column(source, "name"),
            }
        )

        df.to_parquet(self.output().path, index=False)


class GRSciCollInstitutionsTSVTask(BaseTask):
    """Build Institution rows from the GRSciColl TSV export."""

    identifier_codes = ["ROR", "LSID", "CITES", "VIAF", "ISNI"]

    def requires(self):
        return GRSciCollInstitutionsTSVSourceTask()

    def output(self):
        return luigi.LocalTarget(
            settings.intermediate_dir / "institutions" / "grscicol_institutions.parquet"
        )

    def run(self):
        source = _read_tsv_source_from_manifest(self.input())

        df = pd.DataFrame(
            {
                "key": _first_existing_column(source, "key"),
                "code": _first_existing_column(source, "code"),
                "name": _first_existing_column(source, "name"),
                "country": _first_existing_column(source, "country"),
            }
        )
        df = self._add_country_iso3(df)
        df = _add_missing_columns(df, self.identifier_codes)

        columns = ["key", "code", "name", "countryISO3"] + self.identifier_codes
        df = df[columns]
        df.to_parquet(self.output().path, index=False)

    def _add_country_iso3(self, df):
        countries = df["country"].dropna().unique()
        cc = coco.CountryConverter()

        mapping = {
            country: cc.convert(names=country, to="ISO3", not_found=None)
            for country in countries
        }

        df["countryISO3"] = df["country"].map(mapping)

        return df


class GRSciCollAPITask(BaseTask, metaclass=ABCMeta):
    per_request_num = 1000

    @property
    @abstractmethod
    def url(self) -> str:
        pass

    @property
    def session(self):
        return requests_cache.CachedSession(
            settings.cache_dir / "grscicol",
            expire_after=60 * 60 * 24 * 7,
        )

    def api_get_records(self):
        offset = 0
        # GBIF currently hangs for offsets above this value.
        max_offset = 9999
        total_records = 0

        while True:
            response = self.session.get(
                self.url,
                params={
                    "limit": self.per_request_num,
                    "offset": offset,
                },
                timeout=30,
            )

            logger.info(
                "Response status=%s cached=%s url=%s",
                response.status_code,
                getattr(response, "from_cache", False),
                response.url,
            )

            response.raise_for_status()
            payload = response.json()
            results = payload.get("results", [])

            total_records += len(results)

            logger.info(
                "Fetched %s records at offset=%s; total=%s; endOfRecords=%s; count=%s",
                len(results),
                offset,
                total_records,
                payload.get("endOfRecords"),
                payload.get("count"),
            )

            yield from results

            if payload.get("endOfRecords", True):
                break

            if offset == max_offset:
                logger.warning(
                    "Reached max offset %s and not endOfRecords - test GBIF API to see if bug is fixed",
                    offset,
                )
                break

            if not results:
                logger.warning("No results returned at offset %s; stopping", offset)
                break

            offset += self.per_request_num

            if offset >= max_offset:
                offset = max_offset


class GRSciCollCollectionsAPITask(GRSciCollAPITask):
    url = "https://api.gbif.org/v1/grscicoll/collection"

    def run(self):
        df = pd.DataFrame(self.api_get_records())
        logger.info("Fetched %s GRSciColl collections", len(df))
        df = df[["key", "code", "institutionKey", "name"]]
        df.to_parquet(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            settings.intermediate_dir / "institutions" / "grscicol_collections.parquet"
        )


class GRSciCollInstitutionsAPITask(GRSciCollAPITask):
    url = "https://api.gbif.org/v1/grscicoll/institution"
    identifier_codes = ["ROR", "LSID", "CITES", "VIAF", "ISNI"]

    def run(self):
        df = pd.DataFrame(self.api_get_records())
        df = self._unpack_identifiers(df)
        df = self._add_country(df)
        columns = ["key", "code", "name", "countryISO3"] + self.identifier_codes
        df = df[columns]
        df.to_parquet(self.output().path)

    def _unpack_identifiers(self, df):
        def _normalise(identifier):
            return identifier.split("/")[-1]

        def _unpack(row, code):
            for identifier in row.identifiers:
                if identifier["type"] == code:
                    return _normalise(identifier["identifier"])
            for alt_code in row.alternativeCodes:
                if alt_code.get("description") == code:
                    return _normalise(alt_code["code"])

        for id_code in self.identifier_codes:
            df[id_code] = df.apply(_unpack, args=(id_code,), axis=1)

        return df

    def _add_country(self, df):
        def get_country(row):
            if row["address"] and not pd.isna(row["address"]):
                return row["address"].get("country")
            if row["mailingAddress"] and not pd.isna(row["mailingAddress"]):
                return row["mailingAddress"].get("country")

        df["country"] = df.apply(get_country, axis=1)
        countries = df["country"].dropna().unique()

        cc = coco.CountryConverter()

        mapping = {
            country: cc.convert(names=country, to="ISO3", not_found=None)
            for country in countries
        }

        df["countryISO3"] = df["country"].map(mapping)

        return df

    def output(self):
        return luigi.LocalTarget(
            settings.intermediate_dir / "institutions" / "grscicol_institutions.parquet"
        )


if __name__ == "__main__":
    luigi.build([GRSciCollInstitutionsTSVTask(force=True)], local_scheduler=True)
