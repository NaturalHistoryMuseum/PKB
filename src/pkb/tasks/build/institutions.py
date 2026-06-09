import pandas as pd
import luigi

from pkb.config import settings, logger
from pkb.tasks.base import BaseTask
from pkb.tasks.build.grscicol import GRSciCollInstitutionsAPITask

         
class InstitutionsTask(BaseTask):
    """
    Aggregate insitutions
    """
    
    def requires(self):
        return [
            GRSciCollInstitutionsAPITask(),
            # WikiDataInstitutionsTask(),
        ]
    
    def run(self):
        
        df = pd.read_parquet(GRSciCollInstitutionsAPITask().output().path)
        df.to_parquet(self.output().path)
        
    def output(self): 
        return luigi.LocalTarget(settings.intermediate_dir / 'institutions.parquet')                  

    
    
if __name__ == "__main__":
    luigi.build([InstitutionsTask(force=True)], local_scheduler=True)