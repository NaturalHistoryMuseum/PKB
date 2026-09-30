from pathlib import Path

import luigi
from luigi.contrib.postgres import CopyToTable


class BaseTask(luigi.Task):
    force = luigi.BoolParameter(default=False, significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ensure_output_dir()

    def complete(self):
        if self.force:
            return False

        return super().complete()

    def ensure_output_dir(self):
        for output in self._flatten_outputs(self.output()):
            Path(output.path).parent.mkdir(parents=True, exist_ok=True)

    def _flatten_outputs(self, output):
        if not output:
            return []
        if isinstance(output, dict):
            outputs = []
            for value in output.values():
                outputs.extend(self._flatten_outputs(value))
            return outputs
        if isinstance(output, (list, tuple)):
            outputs = []
            for value in output:
                outputs.extend(self._flatten_outputs(value))
            return outputs
        return [output]


class PostgresCopyToTableTask(CopyToTable):
    force = luigi.BoolParameter(default=False, significant=False)

    def complete(self):
        if self.force:
            return False
        return super().complete()

    def run(self):
        if self.force:
            target = self.output()
            connection = target.connect()
            connection.autocommit = True
            try:
                if target.exists(connection):
                    connection.cursor().execute(
                        f"DELETE FROM {target.marker_table} WHERE update_id = %s",
                        (target.update_id,),
                    )
            finally:
                connection.close()
        return super().run()
