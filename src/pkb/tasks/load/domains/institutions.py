from __future__ import annotations

import luigi

from pkb.tasks.load.edges.collection_institution_edges import (
    LinkCollectionsToInstitutions,
)
from pkb.tasks.load.nodes.institution_collections import (
    LoadInstitutionCollectionNodes,
)
from pkb.tasks.load.nodes.institutions import LoadInstitutionNodes


class LoadInstitutionsDomainTask(luigi.WrapperTask):
    """Load the Phase 1 Institutions and Collections domain into Neo4j."""

    def requires(self):
        return [
            LoadInstitutionNodes(),
            LoadInstitutionCollectionNodes(),
            LinkCollectionsToInstitutions(),
        ]


if __name__ == "__main__":
    luigi.build([LoadInstitutionsDomainTask()], local_scheduler=True)
