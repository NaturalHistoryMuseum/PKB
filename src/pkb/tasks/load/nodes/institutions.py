import luigi

from pkb.graph.schema import Labels
from pkb.tasks.build.institutions import InstitutionsTask
from pkb.tasks.load.nodes.base import LoadParquetNodesToGraph


class LoadInstitutionNodes(LoadParquetNodesToGraph):
    label = Labels.INSTITUTION
    key_field = "key"

    def requires(self):
        return InstitutionsTask()
    
if __name__ == "__main__":
    luigi.build([LoadInstitutionNodes()], local_scheduler=True)
