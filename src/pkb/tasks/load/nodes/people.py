from __future__ import annotations

import luigi

from pkb.graph.schema import Labels
from pkb.tasks.build.people import PeopleTask
from pkb.tasks.load.nodes.base import LoadParquetNodesToGraph


class LoadPersonNodes(LoadParquetNodesToGraph):
    label = Labels.PERSON
    key_field = "pkbPersonKey"

    def requires(self):
        return PeopleTask()


if __name__ == "__main__":
    luigi.build([LoadPersonNodes()], local_scheduler=True)
