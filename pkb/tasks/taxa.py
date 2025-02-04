import luigi
import requests
import requests_cache
import wget
import tempfile
import shutil
import numpy as np
from pathlib import Path
from bs4 import BeautifulSoup
import dask.dataframe as dd
import csv
import pandas as pd
import urllib.parse
import dask.dataframe as dd


from pkb.config import INTERMEDIATE_DIR, CACHE_DIR, OUTPUT_DIR, logger
from pkb.tasks.base import BaseTask


requests_cache.install_cache(CACHE_DIR / 'gbif')


class TaxaGBIFBackboneDownloadTask(luigi.ExternalTask):

    url = 'https://hosted-datasets.gbif.org/datasets/backbone/current/backbone.zip'

    def run(self):
        wget.download(self.url, self.output().path)

    
    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'backbone.zip')
    
class TaxaGBIFBackboneUnpackTask(BaseTask):    
    """
    Call download and filter out issues etc.,
    """
    
    def requires(self):
        return TaxaGBIFBackboneDownloadTask()
    
    def run(self):
        extract_dir = Path(self.output().path).parents[1]
        logger.info(f'Unpacking archive to {extract_dir}')
        shutil.unpack_archive(self.input().path, extract_dir) 

    def output(self):
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'backbone' / 'Taxon.tsv')
    

class TaxaGBIFBackboneTask(BaseTask):    
    """
    Call download and filter out issues etc.,
    """
    
    def requires(self):
        return TaxaGBIFBackboneUnpackTask()
    
    def run(self):        
        logger.info('Filtering taxa and writing output to %s', self.output().path)
        df = dd.read_csv(self.input().path, sep='\t', on_bad_lines='skip', dtype='str')
        # Remove unranked taxa
        df['taxonRank'] = df.taxonRank.astype('category')
        df = df[df.taxonRank != 'unranked']

        # Ensure all parent terms exist in the same dataset, or set to Null
        # df['parentNameUsageID'] = df['parentNameUsageID'].where(df['parentNameUsageID'].isin(df['taxonID']), np.nan)

        # FIXME: Hardcode filter on solanaceae family
        df['family'] = df.family.astype('category')
        df = df[df.family == 'Solanaceae']
        
        def set_a_to_nan(df):
            df['parentNameUsageID'] = df['parentNameUsageID'].where(df['parentNameUsageID'].isin(df['taxonID']), np.nan)
            return df

        # Apply the function on each partition
        df = df.map_partitions(set_a_to_nan, meta=df)

        logger.info('%s taxa after filtering', df.shape[0].compute())

        df.to_csv(
            self.output().path, 
            columns=['taxonID', 'parentNameUsageID', 'acceptedNameUsageID', 'canonicalName', 'scientificNameAuthorship', 'taxonRank', 'taxonomicStatus'], 
            header=['taxonID', 'parent', 'synonymOf', 'name', 'authorship', 'taxonRank', 'status'],
            single_file = True, 
            index=False
        )
            
    def output(self): 
        return luigi.LocalTarget(OUTPUT_DIR / f'solanaceae.csv')
    
    
if __name__ == "__main__":
    luigi.build([TaxaGBIFBackboneTask()], local_scheduler=True)    