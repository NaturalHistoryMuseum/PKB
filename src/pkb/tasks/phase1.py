from __future__ import annotations

import luigi

from pkb.tasks.build.institutions import InstitutionsTask
from pkb.tasks.build.people import PeopleTask
from pkb.tasks.load.domains.institutions import LoadInstitutionsDomainTask
from pkb.tasks.load.domains.people import LoadPeopleDomainTask


class BuildPhase1BackboneTask(luigi.WrapperTask):
    """Build currently implemented Phase 1 knowledge-domain datasets."""

    def requires(self):
        return [
            InstitutionsTask(),
            PeopleTask(),
        ]


class LoadPhase1BackboneToNeo4jTask(luigi.WrapperTask):
    """Load currently implemented Phase 1 knowledge domains into Neo4j."""

    def requires(self):
        return [
            LoadInstitutionsDomainTask(),
            LoadPeopleDomainTask(),
        ]


if __name__ == "__main__":
    luigi.build([LoadPhase1BackboneToNeo4jTask()], local_scheduler=True)
