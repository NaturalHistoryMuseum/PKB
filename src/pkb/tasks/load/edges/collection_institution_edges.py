import luigi
import pandas as pd
from pkb.config import logger, settings


from pkb.graph.schema import Labels, Relationships
from pkb.tasks.build.institutions.grscicol import GRSciCollCollectionsTSVTask
from pkb.tasks.load.edges.base import (
    LoadParquetEdgesToGraph,
)
from pkb.tasks.load.nodes.institution_collections import (
    LoadInstitutionCollectionNodes,
)
from pkb.tasks.load.nodes.institutions import (
    LoadInstitutionNodes
)


class LinkCollectionsToInstitutions(
    LoadParquetEdgesToGraph
):

    source_label = Labels.COLLECTION
    target_label = Labels.INSTITUTION

    rel_type = Relationships.HELD_BY

    source_id_field = "key"              # field in collection parquet
    target_id_field = "institutionKey"   # field in collection parquet

    source_node_key = "key"              # property on Collection node
    target_node_key = "key"              # property on Institution node    

    def requires(self):
        return {
            "source_data": GRSciCollCollectionsTSVTask(),
            "collections": LoadInstitutionCollectionNodes(),
            "institutions": LoadInstitutionNodes(),
        }
    
    def prepare_source_dataframe(
        self,
        source_df: pd.DataFrame,
        inputs,
    ) -> pd.DataFrame:
        

        institution_ids_df = pd.read_parquet(inputs["institutions"].path)

        invalid_mask = (
            source_df["institutionKey"].notna()
            &
            ~source_df["institutionKey"].isin(institution_ids_df.id)
        )

        missing_df = source_df[invalid_mask]

        if not missing_df.empty:
            logger.warning(
                "Filtered out %s Collection → Institution relationships "
                "with unknown institutionKey",
                len(missing_df),
            )

            logger.warning(
                "Examples of missing collection → institution references:\n%s",
                missing_df[
                    ["key", "institutionKey"]
                ].head(20),
            )

            missing_output = (
                settings.intermediate_dir
                / "graph"
                / "missing_collection_institution_edges.parquet"
            )
            missing_output.parent.mkdir(parents=True, exist_ok=True)

            missing_df.to_parquet(missing_output)

        return source_df[~invalid_mask]

if __name__ == "__main__":
    luigi.build([LinkCollectionsToInstitutions()], local_scheduler=True)        
