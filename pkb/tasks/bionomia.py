import luigi
import requests
import requests_cache
from bs4 import BeautifulSoup
from pkb.config import INTERMEDIATE_DIR, logger, CACHE_DIR
from pkb.tasks.base import BaseTask
from pkb.utils import country_string_to_iso
import pandas as pd
import wget
import string
import itertools
from abc import ABCMeta, abstractmethod
from tqdm import tqdm
import re
import numpy as np
import dask.dataframe as dd
import csv
import pandas as pd

from pkb.wikidata import wikidata_api_get_entities
from pkb.tasks.wikidata import WikiDataInstitutionsTask
from pkb.tasks.gbif import GBIFOccurrencesTask

tqdm.pandas()
# from typing import deprecated

requests_cache.install_cache(CACHE_DIR / 'bionomia')

# @deprecated("Use BionomiaPublicSearchTask")
class BionomiaPublicProfilesTask(BaseTask):
    """
    Download public profiles from https://bionomia.net/downloads
    
    Deprecated: BionomiaPublicSearchTask includes same info + lifespan + has_occurrences
    
    """

    def output(self):
        return luigi.LocalTarget(INTERMEDIATE_DIR / "bionomia-public-profiles.csv")

    def run(self):
        logger.info('Downloading bionomia-public-claims.csv.gz')
        url = "https://bionomia.net/data/bionomia-public-profiles.csv"
        # urllib.request.urlretrieve(url, self.output().path)
        wget.download(url, self.output().path) 

class BionomiaSearchTask(BaseTask, metaclass=ABCMeta):   
    
    """
    Abstract task to loop through bionomia search
    
    Child classes can define is_public true/false
    """ 
    
    request_limit = 10000
    url = 'https://api.bionomia.net/user.json'
    
    @property
    @abstractmethod
    def is_public(self):
        pass
    
    @property
    @abstractmethod
    def filename(self):
        pass    
    
    def _request(self, alpha):        
        params = {
            'is_public': str(self.is_public).lower(),
            'q': alpha,
            'limit': self.request_limit
        }
        return requests.get(self.url, params=params)
        
    def run(self):

        collectors = {}
        
        for alpha in string.ascii_lowercase:
            results = self._request(alpha).json()
            logger.info(f'{len(results)} collected in search for letter {alpha}')
            for person in results:
                identifier = person.get('orcid') or person.get('wikidata')
                collectors[identifier] = person
                
        df = pd.DataFrame(collectors.values())
        logger.info(f'{len(df.index)} collectors output to {self.filename}')
        df.astype(str).to_parquet(self.output().path, index=False)   
        
    def output(self):        
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'bionomia' / self.filename)              
        
            
class BionomiaNonPublicSearchTask(BionomiaSearchTask):        
    """
    These are available through the search interface
    """        
    is_public = False
    filename = 'bionomia-nonpublic-search.parquet'
    
class BionomiaPublicSearchTask(BionomiaSearchTask):        
    """
    These are available through the search interface
    """        
    is_public = True
    filename = 'bionomia-public-search.parquet'    
    
class BionomiaAggregateTask(BaseTask):   

    re_org = re.compile(r'https://bionomia.net/organization*')
    re_country = re.compile(r'https://bionomia.net/country*')

    def requires(self):
        return [
            BionomiaPublicSearchTask(),
            BionomiaNonPublicSearchTask(),
        ]
        
    def run(self):        
        df = pd.concat([pd.read_parquet(f.path) for f in self.input()])
        df = df.reset_index()

        # Dedupe - remove wikidata duplicates (only 2)
        df = df[~((df.wikidata.notna()) & (df.duplicated(subset=['wikidata'], keep='last')))]   
        # There are no duplciates on ORCID!   

        df[["orgs", "countries"]] = df[df.orcid.notna()].orcid.progress_apply(self.parse_collector_page)            
        logger.info(f'Added {len(df[df.orgs.notna()])} organisations and {len(df[df.countries.notna()])} countries')             
        logger.info(f'Writing {len(df)} bionomia collectors')   
        # Need to replace nan before casting to string for parquet  
        df = df.replace(np.nan, None)   
        df.astype(str).to_parquet(self.output().path, index=False)                
        
    def output(self):        
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'bionomia' / 'aggregated.parquet')         
    
    # @staticmethod
    def parse_collector_page(self, orcid):
        url = f'https://bionomia.net/{orcid}'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        if div := soup.find("div", {"itemtype": "http://schema.org/Person"}):
            orgs = '|'.join([a['href'].replace('https://bionomia.net/organization/', '') for a in div.findAll('a', href=self.re_org)])
            countries = '|'.join([a['href'].replace('https://bionomia.net/country/', '') for a in div.findAll('a', href=self.re_country)])
        else:
            orgs = None
            countries = None
            
        return pd.Series({
                'orgs': orgs,
                'countries': countries
            })  
        

class BionomiaInsitutionsTask(BaseTask):  
    
    def requires(self):
        return BionomiaAggregateTask()
        
    def run(self):
        df = pd.read_parquet(self.input().path)
        df = df.replace('nan', None)
        bionomia_ids = set(itertools.chain.from_iterable([x.split('|') for x in df[df.orgs.notna()].orgs if x and not x.startswith('Q')]))
        organisations = []
        for bionomia_id in tqdm(bionomia_ids):
            url = f'https://bionomia.net/organization/{bionomia_id}'    
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            
            if div := soup.find("div", {"itemtype": "http://schema.org/Organization"}):
                name = div.find("h1", {"itemprop": "name"}).text
                address = div.find("p", {"itemprop": "address"}).text
                m = re.search(r'\s([A-Z]{2})$', address)        
                iso = m.group(0)
                
                organisation = {
                    'id': bionomia_id,
                    'name': name,
                    'address': address,
                    'country': iso.strip()
                } 
                
                organisations.append(organisation)
                 
    
        df = pd.DataFrame(organisations)                
        df.astype(str).to_parquet(self.output().path, index=False) 
                
    def output(self):        
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'bionomia' / 'institutions.parquet')                        

class BionomiaWikiDataInsitutionsTask(BaseTask):  
    
    def requires(self):
        return BionomiaAggregateTask()
        
    def run(self):
        df = pd.read_parquet(self.input().path)
        # FIXME: Remove this:
        df = df.replace('nan', None)
        qids = set(itertools.chain.from_iterable([x.split('|') for x in df[df.orgs.notna()].orgs if x and x.startswith('Q')]))
        institutions = []
        with tqdm(total=len(qids)) as pbar:            
            for entity in wikidata_api_get_entities(list(qids)):
                institutions.append(WikiDataInstitutionsTask.parse_entity(entity))
                pbar.update(1)

        df = pd.DataFrame(institutions)                
        df.replace(np.nan, None).astype(str).to_parquet(self.output().path, index=False) 
                
    def output(self):        
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'bionomia' / 'wikidata-institutions.parquet') 
            
           
class BionomiaCollectorsTask(BaseTask): 
    """
    Set collectors country, based on the institions they've worked in
    """
    def requires(self):
        return [
            BionomiaAggregateTask(),
            BionomiaWikiDataInsitutionsTask(),
            BionomiaInsitutionsTask(),
        ]
        
    def run(self):
        
        collectors = pd.read_parquet(BionomiaAggregateTask().output().path)
        wikidata_inst = pd.read_parquet(BionomiaWikiDataInsitutionsTask().output().path)
        inst = pd.read_parquet(BionomiaInsitutionsTask().output().path)
        
        wd = wd.rename(columns={'qid': 'id'})
        institutions = pd.concat([wd[['id', 'country']], inst[['id', 'country']]])
        institutions = institutions.set_index('id')
        
        
        pass
        
class BionomiaAttributionsTask(BaseTask):  
    """
    We do not use 
    """ 
    def requires(self):
        return [
            BionomiaAggregateTask(),
            # GBIFOccurrencesTask()
        ]    
    
    def run(self):
        
        print('YAY')
        
        # occurrences = dd.read_parquet(GBIFOccurrences().output().path, 
        #     columns = cols,
        #     dtype='str'
        # )          
                
            
if __name__ == "__main__":
    # luigi.build([ProcessSpecimenTask(image_id='011244568', force=True)], local_scheduler=True)
    luigi.build([BionomiaAggregateTask(force=True)], local_scheduler=True)     