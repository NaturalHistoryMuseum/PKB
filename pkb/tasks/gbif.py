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

from pkb.config import INTERMEDIATE_DIR, CACHE_DIR, OUTPUT_DIR, logger
from pkb.tasks.base import BaseTask


requests_cache.install_cache(CACHE_DIR / 'gbif')


class GBIFOccurrencesDownloadTask(luigi.ExternalTask):
    
    doi = 'https://doi.org/10.15468/dl.5qqpak'
    
    @property
    def filename(self):
        url = self.resolve()
        file_id = Path(url).stem
        # file_id = '0104034-230530130749713'
        return f'{file_id}.csv'
    
    def resolve(self):
        r = requests.get(self.doi)
        soup = BeautifulSoup(r.text, 'xml')
        a = soup.find('a', string="Download")
        return a['href']
    
    def run(self):
        url = self.resolve()
        
        with tempfile.TemporaryDirectory(dir=INTERMEDIATE_DIR) as tmp_dir:
            tmp = str(Path(tmp_dir) / 'download.zip')
            wget.download(url, tmp)
            logger.info('Unpackign archive to %s', INTERMEDIATE_DIR)
            shutil.unpack_archive(tmp, INTERMEDIATE_DIR)
    
    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'gbif' / self.filename)
    
class GBIFOccurrencesTask(BaseTask):    
    """
    Call download and filter out issues etc.,
    """
    
    def requires(self):
        return GBIFOccurrencesDownloadTask()
    
    @property
    def filename(self):
        p = Path(self.input().path)
        return f'{p.stem}.parquet'
    
    def run(self):        
        df = dd.read_csv(self.input().path, 
            delimiter='\t', 
            quoting=csv.QUOTE_NONE, 
            encoding='utf-8',
            dtype='str'
        )
            
        logger.info('Filtering and writing output to %s', self.filename)
        df = df[(~df.issue.str.contains('INSTITUTION_MATCH_NONE|DIFFERENT_OWNER_INSTITUTION|AMBIGUOUS_INSTITUTION', regex=True, na=False)) & (df.institutionCode.notnull())]
        df.to_parquet(self.output().path)
            
    def output(self): 
        return luigi.LocalTarget(OUTPUT_DIR / f'gbif-occurrences-{self.filename}')
    
    
if __name__ == "__main__":
    luigi.build([GBIFOccurrencesTask()], local_scheduler=True)    