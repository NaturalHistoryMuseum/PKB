
import luigi
import pandas as pd

from graph_writers import Neo4jWriter

class LoadInstitutionsToGraph(luigi.Task):

    input_path: str = luigi.Parameter()
    backend: str = luigi.Parameter(default="neo4j")
    batch_size: int = luigi.IntParameter(default=1000)

    def output(self):
        return luigi.LocalTarget(
            
        )    
    
    def get_writer(self):
        if self.backend == "neo4j":
            return Neo4jWriter(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="your-password",
                database="neo4j",
            )

        raise ValueError(f"Unsupported graph backend: {self.backend}")
    

if __name__ == "__main__":
    luigi.build([LoadInstitutionsToGraph(force=True)], local_scheduler=True)    