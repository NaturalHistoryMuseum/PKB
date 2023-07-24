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
from pkb.tasks.grscicol import GRSciCollAggregatedTask
from pkb.tasks.wikidata import WikiDataInstitutionsTask
from pkb.tasks.bionomia import BionomiaInsitutionsTask, BionomiaWikiDataInsitutionsTask     
# from pkb.utils import to_parquet

tqdm.pandas()

         
class InstitutionsTask(BaseTask):
    """
    Look up index herbarium code
    """
    
    def requires(self):
        return [
            GRSciCollAggregatedTask(),
            WikiDataInstitutionsTask(),
        ]
    
    def run(self):
        
        self.df = pd.read_parquet(GRSciCollAggregatedTask().output().path)
        
        with WikiDataInstitutionsTask().output().open() as f:
            wikidata = pd.json_normalize(yaml.load_all(f, yaml.FullLoader))     
            
        wikidata['grscicol'] = wikidata.progress_apply(self.get_grscicol_uuid, axis=1)           
        wikidata = wikidata[wikidata.grscicol.notna()][['qid', 'grscicol']].explode("grscicol").set_index("grscicol")        
        logger.info('Mapped %s wikidata entites to GrSciCol', len(wikidata))
        
        df = self.df.join(wikidata)
        df.rename(columns={'qid': 'wikidata'}, inplace=True)   
        df.to_parquet(self.output().path)

    def get_grscicol_uuid(self, row):
        
        if match := self.match_identifier(row['grSciCollID'], 'code'):
            return match 

        if pd.notnull(row.indexHerb):
            if code := self.match_index_herbarium_code(row.indexHerb):
                return code        
            
        for identifier in ['ISNI', 'VIAF', 'ROR']:
            if match := self.match_identifier(row[identifier], identifier):
                return match

        if pd.notnull(row['name']) and pd.notnull(row.country):
            if code := self.fuzzy_match_insitution_name(row['name'], row.country, row.aliases):
                return code        
        
    def match_index_herbarium_code(self, herb_code):
        for col in ['indexHerbCode', 'collectionsindexHerbCode']:
            matches = self.df[self.df[col].str.contains(herb_code, regex=False, na=False)]
            if matches.empty: continue
            return matches.index.tolist()

    def fuzzy_match_insitution_name(self, name, country, aliases):

        match_threshold = 90

        grscicol_filtered_by_country = self.df[self.df.countryISO == country]['name']
        grscicol_names = {n: k for k, n in grscicol_filtered_by_country.items()}

        insitution_names = aliases
        insitution_names.append(name)

        # Remove any acronyms so levenstein doesn't mis-match
        insitution_names = [n for n in insitution_names if len(list(filter(str.islower, n))) > 3]

        for name in insitution_names:
            if matches := process.extract(name, grscicol_names.keys(), limit=1, scorer=fuzz.token_set_ratio):
                m,s = matches[0]
                if s > match_threshold:
                    return grscicol_names[m]    

    def match_identifier(self, value, col):
        if value is None: return

        if isinstance(value, list):
            mask = self.df[col].isin(value)
        else:
            mask = self.df[col] == value

        result = self.df[mask]

        if not result.empty:
            return result.index.tolist()        
        
    def output(self): 
        return luigi.LocalTarget(OUTPUT_DIR / 'institutions.parquet')                  

    
    
if __name__ == "__main__":
    # luigi.build([ProcessSpecimenTask(image_id='011244568', force=True)], local_scheduler=True)
    luigi.build([InstitutionsTask(force=True)], local_scheduler=True)