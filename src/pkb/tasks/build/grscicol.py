import luigi
import requests
import requests_cache
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from abc import ABCMeta, abstractmethod
import numpy as np
from pathlib import Path
from functools import cached_property
import country_converter as coco

from pkb.config import settings, logger
from pkb.tasks.base import BaseTask


class GRSciCollAPITask(BaseTask, metaclass=ABCMeta):
    per_request_num = 1000

    @property
    @abstractmethod
    def url(self) -> str:
        pass

    @property
    def session(self):
        return requests_cache.CachedSession(
            settings.cache_dir / "grscicol",
            expire_after=60 * 60 * 24 * 7,
        )

    def api_get_records(self):
        offset = 0
        # There is a bug in GBIF API - if the offset is set above this amount, the 
        # API hangs
        max_offset = 9999 
        total_records = 0

        while True:

            response = self.session.get(
                self.url,
                params={
                    "limit": self.per_request_num,
                    "offset": offset,
                },
                timeout=30,
            )

            logger.info(
                "Response status=%s cached=%s url=%s",
                response.status_code,
                getattr(response, "from_cache", False),
                response.url,
            )

            response.raise_for_status()
            payload = response.json()
            results = payload.get("results", [])

            total_records += len(results)

            logger.info(
                "Fetched %s records at offset=%s; total=%s; endOfRecords=%s; count=%s",
                len(results),
                offset,
                total_records,
                payload.get("endOfRecords"),
                payload.get("count"),
            )

            yield from results

            if payload.get("endOfRecords", True):
                break

            if offset == max_offset:
                logger.warning("Reached max offset %s and not endOfRecords - test GBIF API to see if bug is fixed", offset)
                break                

            if not results:
                logger.warning("No results returned at offset %s; stopping", offset)
                break

            offset += self.per_request_num

            # if offset has reached maximum offset (GBIF bug), then use max offset in final loop
            if offset >= max_offset:
                offset = max_offset





class GRSciCollCollectionsAPITask(GRSciCollAPITask):
    
    url = 'https://api.gbif.org/v1/grscicoll/collection'
    
    def run(self):                
        df = pd.DataFrame(self.api_get_records())
        logger.info("Fetched %s GRSciColl collections", len(df))
        df = df[['key', 'code', 'institutionKey', 'name']]
        df.to_parquet(self.output().path)

    def output(self): 
        return luigi.LocalTarget(settings.intermediate_dir / 'grscicol' / 'collections.parquet')          

class GRSciCollInstitutionsAPITask(GRSciCollAPITask):
    
    url = 'https://api.gbif.org/v1/grscicoll/institution'
    identifier_codes = ['ROR', 'LSID', 'CITES', 'VIAF', 'ISNI']
    
    def run(self):        
        df = pd.DataFrame(self.api_get_records()) 
        df = self._unpack_identifiers(df)    
        df = self._add_country(df)            
        columns = ['key', 'code', 'name', 'countryISO3'] + self.identifier_codes        
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
                return row['address'].get('country')
            if row['mailingAddress'] and not pd.isna(row['mailingAddress']):
                return row['mailingAddress'].get('country')
                
        df['country'] = df.apply(get_country, axis=1)     
        countries = df["country"].dropna().unique()

        cc = coco.CountryConverter()

        mapping = {
            country: cc.convert(names=country, to="ISO3", not_found=None)
            for country in countries
            }        

        df["countryISO3"] = df["country"].map(mapping)

        return df
                
    def output(self): 
        return luigi.LocalTarget(settings.intermediate_dir / 'grscicol' / 'institutions.parquet')          
   
                    
# class GRSciCollIndexHerbariumCodeTask(BaseTask):
#     """
#     Scrape index herbarium code - as GRSCICOL just has the ID, not the code
#     """
    
#     url = 'https://sweetgum.nybg.org/science/ih/herbarium-details'
    
#     def requires(self):
        
#         return [
#             GRSciCollCollectionsAPITask(),
#             GRSciCollInstitutionsAPITask()
#         ]
    
#     def parse_code_herbarium_details_page(self, index_herb_id):
#         try:
#             r = requests.get(self.url, params={'irn': index_herb_id})
#         except requests.ConnectionError as e:
#             logger.error(e)
#             return
        
#         try:
#             soup = BeautifulSoup(r.text, 'html.parser')
#             h5 = soup.find('h5', string = 'Herbarium Code') 
#             return h5.find_next_sibling('p').text      
#         except AttributeError:
#             logger.error('Could not parse herbarium code from %s', index_herb_id)        
    
#     def run(self):
#         df =  pd.concat([pd.read_parquet(i.path) for i in self.input()])        
#         df = df[df.indexHerbID.notna()]      
#         herb_ids = np.unique(np.hstack(df.indexHerbID)).tolist()      
#         herb_id_codes = [(herb_id, self.parse_code_herbarium_details_page(herb_id)) for herb_id in tqdm(herb_ids)]        
#         df= pd.DataFrame(herb_id_codes, columns=['herbID', 'code'])
#         df.to_parquet(self.output().path)

        
#     def output(self): 
#         return luigi.LocalTarget(INTERMEDIATE_DIR / 'grscicol' / 'herb-codes.parquet')                  

# class GRSciCollAggregatedTask(BaseTask):
    
#     def requires(self):
        
#         return [
#             GRSciCollCollectionsAPITask(),
#             GRSciCollInstitutionsAPITask(),
#             GRSciCollIndexHerbariumCodeTask()
#         ]   
        
#     def run(self):        
#         collections = pd.read_parquet(GRSciCollCollectionsAPITask().output().path)
#         institutions = pd.read_parquet(GRSciCollInstitutionsAPITask().output().path)
#         codes = pd.read_parquet(GRSciCollIndexHerbariumCodeTask().output().path)

#         def _index_herb_code(index_herb):
#             return codes[codes.herbID.isin(index_herb)].code.unique().tolist()    
        
#         institutions['indexHerbCode'] = institutions[institutions.indexHerbID.notna()]['indexHerbID'].apply(_index_herb_code)
#         collections['indexHerbCode'] = collections[collections.indexHerbID.notna()]['indexHerbID'].apply(_index_herb_code)
        
#         def _group_index_herb_code(herb_codes):
#             herb_codes = herb_codes.dropna()
#             if not herb_codes.empty:
#                 stacked = np.hstack(herb_codes)
#                 codes = stacked[~pd.isnull(stacked)]
#                 if len(codes): return codes

#         grouped_collections = collections.groupby('institutionKey')[['code', 'indexHerbCode']].agg({
#             'code': lambda codes: {c for c in codes if c} or None, 
#             'indexHerbCode': _group_index_herb_code
#         }).rename(columns= {'code':'collectionsCode', 'indexHerbCode':'collectionsindexHerbCode'})        
        
#         combined = institutions.set_index('key').join(grouped_collections)             
#         combined.to_parquet(self.output().path)
        



#     def output(self): 
#         return luigi.LocalTarget(INTERMEDIATE_DIR / 'grscicol' / 'aggregated.parquet') 
       
    
    
if __name__ == "__main__":
    luigi.build([GRSciCollInstitutionsAPITask(force=True)], local_scheduler=True)