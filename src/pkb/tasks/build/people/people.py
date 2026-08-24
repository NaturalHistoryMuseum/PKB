from __future__ import annotations

import luigi
import pandas as pd

from pkb.config import settings
from pkb.models.people import PersonAggregator
from pkb.tasks.base import BaseTask
from pkb.tasks.build.people.bionomia import BionomiaPeopleTask
from pkb.tasks.build.people.harvard import HarvardIndexPeopleBuildTask
from pkb.tasks.build.people.wikidata import WikiDataPeopleTask


class PeopleTask(BaseTask):
    """Build the current Phase 1 Person backbone table."""

    def requires(self):
        return {
            "wikidata": WikiDataPeopleTask(),
            "bionomia": BionomiaPeopleTask(),
            "harvard_index": HarvardIndexPeopleBuildTask(),
            # "ipni_authors": IPNIAuthorSourceTask(),
        }

    def output(self):
        people_dir = settings.intermediate_dir / "people"
        return {
            "people": luigi.LocalTarget(people_dir / "people.parquet"),
            "source_links": luigi.LocalTarget(
                people_dir / "person_source_links.parquet"
            ),
        }

    def run(self):
        source_people = pd.concat(
            [
                pd.read_parquet(target.path)
                for target in self.input().values()
            ],
            ignore_index=True,
            sort=False,
        )
        result = PersonAggregator().aggregate(source_people)
        output = self.output()

        result.people.to_parquet(output["people"].path, index=False)
        result.source_links.to_parquet(
            output["source_links"].path,
            index=False,
        )


if __name__ == "__main__":
    luigi.build([PeopleTask()], local_scheduler=True)
