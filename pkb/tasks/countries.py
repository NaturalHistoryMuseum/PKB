import luigi
import requests
import requests_cache
from bs4 import BeautifulSoup
from pkb.config import INTERMEDIATE_DIR, logger, CACHE_DIR, OUTPUT_DIR
from pkb.tasks.base import BaseTask
from pkb.utils import country_string_to_iso
import pandas as pd
import wget
import string
import itertools
from abc import ABCMeta, abstractmethod
from tqdm import tqdm
from pathlib import Path
import re
import numpy as np
import dask.dataframe as dd
import csv
import pandas as pd

from pkb.wikidata import wikidata_api_get_entities
from pkb.tasks.wikidata import WikiDataInstitutionsTask
from pkb.tasks.gbif import GBIFOccurrencesTask

class GeonamesDownloadTask(luigi.ExternalTask):

    url = 'https://download.geonames.org/export/dump/countryInfo.txt'

    def run(self):
        wget.download(self.url, self.output().path)

    def output(self): 
        file_name = self.url.split('/')[-1]
        return luigi.LocalTarget(INTERMEDIATE_DIR / file_name)
    
class CountriesTask(BaseTask):    

    def requires(self):
        return GeonamesDownloadTask()
    
    @staticmethod
    def read_file_headers(file_path):
        with file_path.open('r') as f:
            for line in f:
                if line.startswith('#ISO'):
                    header = line[1:].strip().split('\t')
                    return header

    
    def run(self):
        file_path = Path(self.input().path)
        headers = self.read_file_headers(file_path)
        df = pd.read_csv(file_path, sep='\t', names=headers, comment='#')
        df[['ISO', 'ISO3', 'Country', 'fips', 'Continent', 'geonameid']].to_csv(self.output().path)

    def output(self): 
        return luigi.LocalTarget(OUTPUT_DIR / 'countries.csv')

            
if __name__ == "__main__":
    # luigi.build([ProcessSpecimenTask(image_id='011244568', force=True)], local_scheduler=True)
    luigi.build([CountriesTask()], local_scheduler=True)     