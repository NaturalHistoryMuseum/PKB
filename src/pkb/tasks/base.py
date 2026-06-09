import luigi
from pathlib import Path

class BaseTask(luigi.Task):

    force = luigi.BoolParameter(default=False, significant=False)
    
    def __init__(self, *args, **kwargs):
        self.ensure_output_dir()
        super().__init__(*args, **kwargs)
            
    def complete(self):
        if self.force:
            return False
        
        return super().complete()  
    
    def ensure_output_dir(self):
        if self.output():            
            print(Path(self.output().path).parent)
            return Path(self.output().path).parent.mkdir(parents=True, exist_ok=True)
        