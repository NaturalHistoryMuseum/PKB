from __future__ import annotations

import luigi
import pandas as pd
import requests_cache
import yaml
from bs4 import BeautifulSoup, SoupStrainer
from typing import Any

from pkb.config import logger, settings
from pkb.tasks.base import BaseTask
from pkb.utils.text import normalize_unicode


PEOPLE_DIR = settings.intermediate_dir / "people"
HARVARD_INDEX_URL = "https://kiki.huh.harvard.edu/databases/botanist_search.php"


def harvard_session() -> requests_cache.CachedSession:
    return requests_cache.CachedSession(
        cache_name=str(settings.cache_dir / "harvard"),
        backend="sqlite",
        expire_after=None,
    )


class HarvardAgentsFile(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(settings.assets_dir / "huh-agents.tab.txt")


class HarvardIndexDetailTask(BaseTask):
    """Fetch and parse detail pages for Harvard Index IDs."""

    def requires(self):
        return HarvardAgentsFile()

    def output(self):
        return luigi.LocalTarget(PEOPLE_DIR / "harvard_index_details.yaml")

    def run(self):
        session = harvard_session()
        agents = pd.read_csv(self.input().path, sep="\t", dtype=str)

        if "harvardIndexID" not in agents.columns:
            raise ValueError("Harvard agents file must contain a harvardIndexID column")

        with self.output().open("w") as outf:
            for harvard_index_id in agents.harvardIndexID.dropna().drop_duplicates():
                detail = self._parse_detail_page(session, harvard_index_id)

                if not detail:
                    continue

                if botanist_id := detail.get("ASA Botanist ID"):
                    try:
                        _, asa_category = botanist_id.split()
                    except ValueError:
                        pass
                    else:
                        detail["asa_category"] = asa_category

                yaml.safe_dump(
                    detail,
                    outf,
                    default_flow_style=False,
                    explicit_start=True,
                    allow_unicode=True,
                )

    def _parse_detail_page(
        self,
        session: requests_cache.CachedSession,
        collector_id: str,
    ) -> dict[str, Any] | None:
        response = session.get(
            HARVARD_INDEX_URL,
            params={"mode": "details", "id": collector_id},
        )
        response.raise_for_status()

        strainer = SoupStrainer("div", attrs={"id": "main_text_wide"})
        soup = BeautifulSoup(response.content, "lxml", parse_only=strainer)

        table = soup.find("table")
        if table is None:
            logger.error("Could not parse Harvard detail page for %s", collector_id)
            return None

        collector: dict[str, Any] = {"id": collector_id}

        for tr in table.find_all("tr"):
            label_cell = tr.find("td", {"class": "cap"})
            value_cell = tr.find("td", {"class": "val"})

            if label_cell is None or value_cell is None:
                continue

            label = normalize_unicode(label_cell.get_text(strip=True))
            value = normalize_unicode(value_cell.get_text(strip=True))

            if label == "Variant name":
                label = "Name"

            if label not in collector:
                collector[label] = value
            elif isinstance(collector[label], list):
                collector[label].append(value)
            else:
                collector[label] = [collector[label], value]

        return collector


if __name__ == "__main__":
    luigi.build([HarvardIndexDetailTask()], local_scheduler=True)
