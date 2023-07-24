import pandas as pd
import requests
import requests_cache
import numpy as np
import itertools
import yaml
from yaml import SafeLoader, load
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import luigi
from tqdm import tqdm

from pkb.config import INTERMEDIATE_DIR, OUTPUT_DIR, logger
from pkb.tasks.base import BaseTask
from pkb.tasks.harvard import HarvardIndexCollectorsTask
from pkb.tasks.wikidata import WikiDataCollectorsTask
from pkb.tasks.bionomia import BionomiaCollectorsTask

tqdm.pandas()

         
class InstitutionsTask(BaseTask):
    """
    Look up index herbarium code
    """
    
    def requires(self):
        return [
            BionomiaCollectorsTask(),
            WikiDataCollectorsTask(),
            HarvardIndexCollectorsTask()
        ]
    
    def run(self):
        pass
        # TODO: Add Hiris' collectors aggregation code   

    def output(self): 
        return luigi.LocalTarget(OUTPUT_DIR / 'collectors.csv')                  

    
    
if __name__ == "__main__":
    # luigi.build([ProcessSpecimenTask(image_id='011244568', force=True)], local_scheduler=True)
    luigi.build([InstitutionsTask(force=True)], local_scheduler=True)