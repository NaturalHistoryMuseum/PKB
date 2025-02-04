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
import urllib.parse


from pkb.config import INTERMEDIATE_DIR, CACHE_DIR, OUTPUT_DIR, logger
from pkb.tasks.base import BaseTask


requests_cache.install_cache(CACHE_DIR / 'gbif')


class GBIFOccurrencesDownloadTask(luigi.ExternalTask):

    doi = luigi.Parameter(default='https://doi.org/10.15468/dl.cazy3b')
            
    def resolve_doi(self):
        r = requests.get(self.doi)
        soup = BeautifulSoup(r.text, 'xml')
        a = soup.find('a', string="Download")
        return a['href']
    
    def run(self):

        url = self.resolve_doi()
        
        with tempfile.TemporaryDirectory(dir=INTERMEDIATE_DIR) as tmp_dir:
            tmp = str(Path(tmp_dir) / 'download.zip')
            wget.download(url, tmp)
            extract_dir = INTERMEDIATE_DIR / 'gbif'
            logger.info(f'Unpacking archive to {extract_dir}')
            shutil.unpack_archive(tmp, extract_dir)

            # Is this a GBIF simple CSV or DWCA
            if (extract_dir / 'occurrence.txt').exists():
                path = extract_dir / 'occurrence.txt'                
            else:
                path = next(extract_dir.glob("*.csv"))

            shutil.copy(path, self.output().path)


    
    def output(self): 
        path = urllib.parse.urlparse(self.doi).path
        # Get the last part of the DOI
        file_id = path.split('/')[-1].replace('.', '-')
        return luigi.LocalTarget(INTERMEDIATE_DIR / f'gbif-{file_id}.csv')
    
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
            
        logger.info('Filtering and writing output to %s', self.output().path)
        df = df[(~df.issue.str.contains('INSTITUTION_MATCH_NONE|DIFFERENT_OWNER_INSTITUTION|AMBIGUOUS_INSTITUTION', regex=True, na=False)) & (df.institutionCode.notnull())]
        df.to_parquet(self.output().path)
            
    def output(self): 
        return luigi.LocalTarget(OUTPUT_DIR / f'occurrences-{self.filename}')
    
    
if __name__ == "__main__":
    luigi.build([GBIFOccurrencesTask()], local_scheduler=True)    