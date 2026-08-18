import luigi
from pathlib import Path


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
