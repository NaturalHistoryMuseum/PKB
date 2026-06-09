from __future__ import annotations

from pathlib import Path

import luigi
import pandas as pd

from pkb.config import logger, settings
from pkb.graph.factory import get_graph_writer


class LoadParquetEdgesToGraph(luigi.Task):
    source_label: str | None = None
    target_label: str | None = None
    rel_type: str | None = None

    # Columns in the source parquet
    source_id_field: str | None = None
    target_id_field: str | None = None

    # Properties on the graph nodes
    source_node_key: str | None = None
    target_node_key: str | None = None

    batch_size = luigi.IntParameter(default=1000)

    def requires(self):
        raise NotImplementedError

    def output(self) -> luigi.LocalTarget:
        input_path = Path(self.input()["source_data"].path)

        output_file = (
            settings.output_dir
            / "graph"
            / f"{input_path.stem}_{self.rel_type}.done"
        )

        output_file.parent.mkdir(parents=True, exist_ok=True)

        return luigi.LocalTarget(str(output_file))
    
    def prepare_source_dataframe(
        self,
        source_df: pd.DataFrame,
        inputs,
    ) -> pd.DataFrame:
        return source_df    

    def run(self) -> None:
        self._validate()

        inputs = self.input()
        input_path = inputs["source_data"].path

        df = pd.read_parquet(input_path)

        df = self.prepare_source_dataframe(
            source_df=df,
            inputs=inputs,
        )

        missing_fields = [
            field
            for field in [
                self.source_id_field,
                self.target_id_field,
            ]
            if field not in df.columns
        ]

        if missing_fields:
            raise ValueError(
                f"Missing required edge field(s): {missing_fields}"
            )

        edges = (
            df[
                [
                    self.source_id_field,
                    self.target_id_field,
                ]
            ]
            .dropna(subset=[self.target_id_field])
            .drop_duplicates()
            .rename(
                columns={
                    self.source_id_field: "source_id",
                    self.target_id_field: "target_id",
                }
            )
            .to_dict("records")
        )

        logger.info(
            "Creating %s %s relationships from %s",
            len(edges),
            self.rel_type,
            input_path,
        )

        writer = get_graph_writer()
        writer.connect()

        try:
            writer.write_edges(
                source_label=self.source_label,
                target_label=self.target_label,
                rel_type=self.rel_type,
                rows=edges,
                source_node_key=self.source_node_key,
                target_node_key=self.target_node_key,
                batch_size=self.batch_size,
            )

            with self.output().open("w") as f:
                f.write(f"Loaded {len(edges)} {self.rel_type} relationships\n")
                f.write(f"source={input_path}\n")
                f.write(f"source_label={self.source_label}\n")
                f.write(f"target_label={self.target_label}\n")
                f.write(f"source_id_field={self.source_id_field}\n")
                f.write(f"target_id_field={self.target_id_field}\n")

        finally:
            writer.close()

    def _validate(self) -> None:
        required = {
            "source_label": self.source_label,
            "target_label": self.target_label,
            "rel_type": self.rel_type,
            "source_id_field": self.source_id_field,
            "target_id_field": self.target_id_field,
        }

        missing = [name for name, value in required.items() if value is None]

        if missing:
            raise ValueError(
                f"{self.__class__.__name__} must define: {', '.join(missing)}"
            )