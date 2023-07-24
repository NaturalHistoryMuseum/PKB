import luigi
import yaml
import pandas as pd
from PIL import Image
from pkb.config import (
    INTERMEDIATE_DIR,
    logger,
    CACHE_DIR
)
from bs4 import BeautifulSoup
import requests
import requests_cache

from pkb.tasks.base import BaseTask
from pkb.wikidata import WikiDataQuery, wikidata_get_entity   
from pkb.utils import dict_to_parquet

requests_cache.install_cache(CACHE_DIR / 'bionomia')

         
class WikiSpeciesInstitutionsTask(BaseTask):
    
    def run(self):
        
        urls = [
            'https://species.wikimedia.org/wiki/Repositories_(A%E2%80%93M)',
            'https://species.wikimedia.org/wiki/Repositories_(N%E2%80%93Z)'
        ]
        
        insitutions = []
        columns = ['acroynym', 'name', 'wikidata']
        
        for url in urls:
            r = requests.get(url)
            soup = BeautifulSoup(r.text, 'html.parser')
            content = soup.find("div", {"id": "mw-content-text"})
            for table in content.findAll('table'):    
                trs =  table.findAll('tr')
                # Check this is an acronym table - filter out directory list
                if not 'acronym' in trs[0].text.lower():
                    continue
                for tr in trs[1:]:
                    values = 
                    insitution = dict(zip(columns, [td.text for td in tr.findAll('td')]))  
                    insitutions.append(insitution)
                            
        dict_to_parquet(insitutions, self.output().path)                
        logger.info(f'Parsed {len(insitutions)} institutions from wikispecies')
        
    def output(self):        
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'wikispecies-insitutions.parquet')         
        
if __name__ == "__main__":
    luigi.build([WikiSpeciesInstitutionsTask(force=True)], local_scheduler=True)           