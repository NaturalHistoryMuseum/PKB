from __future__ import annotations

import luigi

from pkb.tasks.load.nodes.people import LoadPersonNodes


class LoadPeopleDomainTask(luigi.WrapperTask):
    """Load the current Phase 1 People domain into Neo4j."""

    def requires(self):
        return [
            LoadPersonNodes(),
        ]


if __name__ == "__main__":
    luigi.build([LoadPeopleDomainTask()], local_scheduler=True)
