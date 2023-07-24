import luigi
import requests
import requests_cache
from bs4 import BeautifulSoup
from pkb.config import INTERMEDIATE_DIR, logger, CACHE_DIR
from pkb.tasks.base import BaseTask
from pkb.utils import country_string_to_iso
import pandas as pd
from tqdm import tqdm
from abc import ABCMeta, abstractmethod
import numpy as np
from pathlib import Path


tqdm.pandas()

requests_cache.install_cache(CACHE_DIR / 'grscicol')


class GRSciCollAPITask(BaseTask, metaclass=ABCMeta):
    
    per_request_num = 1000

    @property
    @abstractmethod
    def url(self):
        pass
    
    def api_get_records(self):

        params = {'limit': self.per_request_num}
        i = 0

        while True:    
            params['offset'] = i * params['limit']
            r = requests.get(self.url, params)
            results = r.json()
            yield from results['results']            
            if results['endOfRecords']:
                break

            i += 1    

    def _add_index_herbaria(self, df):  
        
        def get_index_herb(identifiers):
            return [i['identifier'].replace('gbif:ih:irn:', '') for i in identifiers if i['type'] == 'IH_IRN'] or None

        df['indexHerbID'] = df[df['identifiers'].notna()].identifiers.apply(get_index_herb)
        return df

class GRSciCollCollectionsAPITask(GRSciCollAPITask):
    
    url = 'https://api.gbif.org/v1/grscicoll/collection'
    
    def run(self):                
        df = pd.DataFrame(self.api_get_records())   
        df = self._add_index_herbaria(df)   
        df = df[['key', 'code', 'institutionKey', 'name', 'indexHerbID']] 
        df.to_parquet(self.output().path)
        
    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'grscicol' / 'collections.parquet')        
    

class GRSciCollInstitutionsAPITask(GRSciCollAPITask):
    
    url = 'https://api.gbif.org/v1/grscicoll/institution'
    identifier_codes = ['ROR', 'LSID', 'CITES', 'VIAF', 'ISNI']
    
    def run(self):        
        
        df = pd.DataFrame(self.api_get_records())
        df = self._add_index_herbaria(df)    
        df = self._unpack_identifiers(df)    
        df = self._add_country(df)            
        columns = ['key', 'code', 'name', 'indexHerbID', 'countryISO'] + self.identifier_codes        
        df = df[columns]        
        df.to_parquet(self.output().path)
    
    def _unpack_identifiers(self, df):  
        
        def _normalise(identifier):
            # If we have a URL, just take the last part
            # https://ror.org/04aha0598 => 04aha0598
            return identifier.split('/')[-1]
            
        def _unpack(row, code):
            for i in row.identifiers:
                if i['type'] == code:
                    return _normalise(i['identifier'])
            for c in row.alternativeCodes:
                if c.get('description') == code:
                    return _normalise(c['code'])
                
        for id_code in self.identifier_codes:
            df[id_code] = df.apply(_unpack, args = (id_code,), axis=1)                
        
        return df    
    
    def _add_country(self, df):  
        def get_country(row):
            if row['address'] and not pd.isna(row['address']):
                if country := row['address'].get('country'):
                    return country_string_to_iso(country)                
            if row['mailingAddress'] and not pd.isna(row['mailingAddress']):
                if country := row['mailingAddress'].get('country'):
                    return country_string_to_iso(country)
                
        df['countryISO'] = df.apply(get_country, axis=1)        
        return df
                

    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'grscicol' / 'institutions.parquet')  
    
                    
class GRSciCollIndexHerbariumCodeTask(BaseTask):
    """
    Scrape index herbarium code - as GRSCICOL just has the ID, not the code
    """
    
    url = 'https://sweetgum.nybg.org/science/ih/herbarium-details'
    
    def requires(self):
        
        return [
            GRSciCollCollectionsAPITask(),
            GRSciCollInstitutionsAPITask()
        ]
    
    def parse_code_herbarium_details_page(self, index_herb_id):
        try:
            r = requests.get(self.url, params={'irn': index_herb_id})
        except requests.ConnectionError as e:
            logger.error(e)
            return
        
        try:
            soup = BeautifulSoup(r.text, 'html.parser')
            h5 = soup.find('h5', string = 'Herbarium Code') 
            return h5.find_next_sibling('p').text      
        except AttributeError:
            logger.error('Could not parse herbarium code from %s', index_herb_id)        
    
    def run(self):
        df =  pd.concat([pd.read_parquet(i.path) for i in self.input()])        
        df = df[df.indexHerbID.notna()]      
        herb_ids = np.unique(np.hstack(df.indexHerbID)).tolist()      
        herb_id_codes = [(herb_id, self.parse_code_herbarium_details_page(herb_id)) for herb_id in tqdm(herb_ids)]        
        df= pd.DataFrame(herb_id_codes, columns=['herbID', 'code'])
        df.to_parquet(self.output().path)

        
    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'grscicol' / 'herb-codes.parquet')                  

class GRSciCollAggregatedTask(BaseTask):
    
    def requires(self):
        
        return [
            GRSciCollCollectionsAPITask(),
            GRSciCollInstitutionsAPITask(),
            GRSciCollIndexHerbariumCodeTask()
        ]   
        
    def run(self):        
        collections = pd.read_parquet(GRSciCollCollectionsAPITask().output().path)
        institutions = pd.read_parquet(GRSciCollInstitutionsAPITask().output().path)
        codes = pd.read_parquet(GRSciCollIndexHerbariumCodeTask().output().path)

        def _index_herb_code(index_herb):
            return codes[codes.herbID.isin(index_herb)].code.unique().tolist()    
        
        institutions['indexHerbCode'] = institutions[institutions.indexHerbID.notna()]['indexHerbID'].apply(_index_herb_code)
        collections['indexHerbCode'] = collections[collections.indexHerbID.notna()]['indexHerbID'].apply(_index_herb_code)
        
        def _group_index_herb_code(herb_codes):
            herb_codes = herb_codes.dropna()
            if not herb_codes.empty:
                stacked = np.hstack(herb_codes)
                codes = stacked[~pd.isnull(stacked)]
                if len(codes): return codes

        grouped_collections = collections.groupby('institutionKey')[['code', 'indexHerbCode']].agg({
            'code': lambda codes: {c for c in codes if c} or None, 
            'indexHerbCode': _group_index_herb_code
        }).rename(columns= {'code':'collectionsCode', 'indexHerbCode':'collectionsindexHerbCode'})        
        
        combined = institutions.set_index('key').join(grouped_collections)             
        combined.to_parquet(self.output().path)
        



    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'grscicol' / 'aggregated.parquet') 
       
    
    
if __name__ == "__main__":
    luigi.build([GRSciCollAggregatedTask(force=True)], local_scheduler=True)