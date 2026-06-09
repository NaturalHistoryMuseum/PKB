import luigi


from pkb.tasks.build.grscicol import GRSciCollCollectionsAPITask
from pkb.tasks.load.nodes.base import LoadParquetNodesToGraph


class LoadInstitutionCollectionNodes(LoadParquetNodesToGraph):
    label = "Collection"
    key_field = "key"
    exclude_fields = ("institutionKey",)

    def requires(self):
        return GRSciCollCollectionsAPITask()
    

if __name__ == "__main__":
    luigi.build([LoadInstitutionCollectionNodes()], local_scheduler=True)    