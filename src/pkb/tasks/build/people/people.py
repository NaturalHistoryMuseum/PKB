from __future__ import annotations

import luigi
import pandas as pd

from pkb.config import settings
from pkb.tasks.base import BaseTask
from pkb.tasks.build.people.bionomia import BionomiaPeopleTask
from pkb.tasks.build.people.wikidata import WikiDataPeopleTask


class PeopleTask(BaseTask):
    """Build the current Phase 1 Person backbone table.

    This is intentionally conservative while person aggregation is being
    refactored: Wikidata collector records are exposed as the first source of
    person identities, with additional sources joining here as resolution work
    moves out of notebooks.
    """

    def requires(self):
        return [
            WikiDataPeopleTask(),
            BionomiaPeopleTask(),
            # HarvardIndexDetailTask(),
            # IPNIAuthorSourceTask(),
        ]

    def output(self):
        return luigi.LocalTarget(settings.intermediate_dir / "people" / "people.parquet")

    def run(self):
        wikidata_people = pd.read_parquet(WikiDataPeopleTask().output().path)
        bionomia_people = pd.read_parquet(BionomiaPeopleTask().output().path)
        people = pd.concat(
            [
                wikidata_people,
                bionomia_people,
            ],
            ignore_index=True,
            sort=False,
        )
        people.to_parquet(self.output().path, index=False)


if __name__ == "__main__":
    luigi.build([PeopleTask()], local_scheduler=True)
