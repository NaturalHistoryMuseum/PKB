from __future__ import annotations

import luigi
import pandas as pd

from pkb.config import settings
from pkb.tasks.base import BaseTask
from pkb.tasks.build.institutions.grscicol import GRSciCollInstitutionsTSVTask


class InstitutionsTask(BaseTask):
    """Build the Phase 1 Institution backbone table."""

    def requires(self):
        return [
            GRSciCollInstitutionsTSVTask(),
            # WikiDataInstitutionSourceTask belongs here when institution aggregation is ready.
        ]

    def run(self):
        df = pd.read_parquet(GRSciCollInstitutionsTSVTask().output().path)
        df.to_parquet(self.output().path)

    def output(self):
        return luigi.LocalTarget(
            settings.intermediate_dir / "institutions" / "institutions.parquet"
        )


if __name__ == "__main__":
    luigi.build([InstitutionsTask(force=True)], local_scheduler=True)
