from __future__ import annotations

from pathlib import Path

import luigi
import pandas as pd

from pkb.config import logger, settings
from pkb.graph.factory import get_graph_writer


class LoadParquetNodesToGraph(luigi.Task):
    label = luigi.Parameter()
    key_field = luigi.Parameter()
    batch_size = luigi.IntParameter(default=1000)
    
    exclude_fields: tuple[str, ...] = ()

    def requires(self):
        raise NotImplementedError(
            "Subclasses must define the task that produces the parquet file."
        )

    def output(self) -> luigi.LocalTarget:
        input_path = Path(self.input().path)

        output_file = (
            settings.intermediate_dir
            / "graph"
            / f"{self.label}_loaded_ids.parquet"
        )

        output_file.parent.mkdir(parents=True, exist_ok=True)

        return luigi.LocalTarget(str(output_file))

    def run(self) -> None:
        input_path = self.input().path

        logger.info(
            "Loading %s into graph as %s nodes",
            input_path,
            self.label,
        )

        df = pd.read_parquet(input_path)

        if self.key_field not in df.columns:
            raise ValueError(
                f"Key field '{self.key_field}' not found in dataframe"
            )

        original_rows = len(df)

        df = (
            df.where(pd.notna(df), None)
            .dropna(subset=[self.key_field])
            .drop_duplicates(subset=[self.key_field])
        )

        logger.info(
            "Reduced %s rows to %s unique nodes",
            original_rows,
            len(df),
        )

        if self.exclude_fields:
            df = df.drop(
                columns=[c for c in self.exclude_fields if c in df.columns]
            )
            logger.info(
                "Drop columns %s - remaining columns %s",
                self.exclude_fields,
                df.columns,
            )                    

        records = df.to_dict("records")

        writer = get_graph_writer()
        writer.connect()

        try:
            writer.create_node_constraint(
                label=self.label,
                key_field=self.key_field,
            )

            writer.write_nodes(
                label=self.label,
                rows=records,
                key_field=self.key_field,
                batch_size=self.batch_size,
            )

            loaded_ids = (
                df[[self.key_field]]
                .dropna()
                .drop_duplicates()
                .rename(
                    columns={
                        self.key_field: "id",
                    }
                )                
            )

            loaded_ids.to_parquet(self.output().path, index=False)            

            logger.info(
                "Loaded %s %s nodes",
                len(records),
                self.label,
            )

        finally:
            writer.close()