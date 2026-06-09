import luigi
import requests
import requests_cache
import wget
import tempfile
import shutil
from pathlib import Path
from bs4 import BeautifulSoup
import dask.dataframe as dd
import csv
import pandas as pd
from tqdm import tqdm

from pkb.config import INTERMEDIATE_DIR, CACHE_DIR, OUTPUT_DIR, logger
from pkb.tasks.institutions import InstitutionsTask
from pkb.tasks.base import BaseTask
from pkb.tasks.gbif import GBIFOccurrencesTask


tqdm.pandas()
requests_cache.install_cache(CACHE_DIR / 'edges')

    
class OccurrenceInstitutionEdges(BaseTask):

    def requires(self):
        return [
            GBIFOccurrencesTask(),
            InstitutionsTask()
        ]    
    
    def run(self):
        cols = ['gbifID','institutionCode', 'collectionCode', 'issue', 'datasetKey']
        
        specimens = dd.read_parquet(GBIFOccurrencesTask().output().path, 
            columns = cols,
            dtype='str'
        )  
                        
        institutions = pd.read_parquet(InstitutionsTask().output().path)
        # Assign index to uuid field
        institutions = institutions.assign(institutionUUID=institutions.index)

        # Merge institution, assigning institutionUUID 
        specimens = specimens.merge(institutions[['code', 'institutionUUID']], left_on='institutionCode', right_on='code', how='left')

        specimens_without_inst = specimens[specimens.institutionUUID.isna()]
        specimens_without_inst = specimens_without_inst.drop(columns=['institutionUUID'])
        
        logger.info('%s specimens without matching institution', len(specimens_without_inst))
        
        without_inst_unique = specimens_without_inst.drop_duplicates(subset=['institutionCode', 'datasetKey'], keep='last')
        logger.info('Searching GBIF API for %s unique institutionCode & datasetKey', len(without_inst_unique))
        without_inst_unique['institutionUUID'] = without_inst_unique['gbifID'].apply(self.gbif_api_get_occurrence_institution_code, meta='str')
        
        # Merge institutionUUID into specimens_without_inst
        specimens_without_inst = specimens_without_inst.merge(without_inst_unique[['institutionUUID', 'institutionCode', 'datasetKey']], on=['institutionCode', 'datasetKey'], how="left")
        combined = dd.concat([
            specimens_without_inst.set_index('gbifID').institutionUUID, 
            # Specimens with institution
            specimens[~specimens.institutionUUID.isna()].set_index('gbifID').institutionUUID
        ])
        
        combined.to_parquet(self.output().path)

    def gbif_api_get_occurrence_institution_code(self, occurrence_id):
        url = f'https://api.gbif.org/v1/occurrence/{occurrence_id}'
        r = requests.get(url)
        data = r.json()
        return data.get('institutionKey')        
    
    def output(self): 
        return luigi.LocalTarget(OUTPUT_DIR / 'occurrence-institution.edges.parquet')    
    
    
# class BionomiaAttributions(BaseTask):    
    
    
if __name__ == "__main__":
    luigi.build([OccurrenceInstitutionEdges()], local_scheduler=True)    