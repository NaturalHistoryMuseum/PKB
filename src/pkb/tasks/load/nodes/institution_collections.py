import luigi


from pkb.graph.schema import Labels
from pkb.tasks.build.institutions.grscicol import GRSciCollCollectionsTSVTask
from pkb.tasks.load.nodes.base import LoadParquetNodesToGraph


class LoadInstitutionCollectionNodes(LoadParquetNodesToGraph):
    label = Labels.COLLECTION
    key_field = "key"
    exclude_fields = ("institutionKey",)

    def requires(self):
        return GRSciCollCollectionsTSVTask()
    

if __name__ == "__main__":
    luigi.build([LoadInstitutionCollectionNodes()], local_scheduler=True)    
