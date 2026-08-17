from __future__ import annotations

import luigi
import pandas as pd

from pkb.config import settings
from pkb.tasks.base import BaseTask
from pkb.tasks.sources.people.wikidata import (
    WikiDataCollectorIDsSourceTask,
    WikiDataCollectorsSourceTask,
    collect_related_qids_for_collectors,
)


PEOPLE_DIR = settings.intermediate_dir / "people"


class WikiDataPeopleTask(BaseTask):
    """Build People-domain rows from Wikidata source records."""

    source_name = "wikidata"

    def requires(self):
        return WikiDataCollectorsSourceTask()

    def output(self):
        return luigi.LocalTarget(PEOPLE_DIR / "wikidata_people.parquet")

    def run(self):
        people = pd.read_parquet(self.input().path)
        people = people.assign(
            pkbPersonKey=lambda df: self.source_name + ":" + df["wikidataID"].astype(str),
            source=self.source_name,
        )
        people.to_parquet(self.output().path, index=False)


class WikiDataCollectorIDsTask(WikiDataCollectorIDsSourceTask):
    """Compatibility alias for the Wikidata people source ID task."""


class WikiDataCollectorsTask(WikiDataPeopleTask):
    """Compatibility alias for the Wikidata people build task."""


if __name__ == "__main__":
    luigi.build([WikiDataPeopleTask()], local_scheduler=True)
