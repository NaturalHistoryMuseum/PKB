import luigi
import yaml
import requests_cache
from tqdm import tqdm

from pkb.config import INTERMEDIATE_DIR, CACHE_DIR, logger
from pkb.tasks.base import BaseTask
from pkb.wikidata import WikiDataQuery, wikidata_get_entity   

         
requests_cache.install_cache(CACHE_DIR / 'wikidata')         
         
         
class WikiDataInstitutionsTask(BaseTask):

    def run(self):
        # Natural history collection or herbarium
        query = """
            SELECT DISTINCT ?item WHERE {
            {OPTIONAL { ?item p:P31/ps:P31/wdt:P279 wd:Q2982911 . }} 
                UNION 
            {OPTIONAL { ?item p:P31/ps:P31/wdt:P279* wd:Q181916 . }} 
            } ORDER BY ?item
        """        
        query_results = WikiDataQuery(query)
        with self.output().open('w') as f:                          
            for entity in tqdm(query_results):      
                institution = self.parse_entity(entity)                    
                yaml.dump(institution, f, default_flow_style=False, explicit_start=True)
                         
    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'wikidata' / 'insitutions.yaml') 
    
    @staticmethod
    def parse_entity(entity):
        institution = {
            'qid': entity.qid,
            'name': entity.label,
            'aliases': entity.aliases,
            'indexHerb': entity.get_property_value('P5858'),
            'harvardIndex': entity.get_property_value('P6264'),
            'VIAF': entity.get_property_value('P214'),
            'ISNI': entity.get_property_value('P213'),
            'grSciCollID': entity.get_property_values('P4090'),
            'ROR': entity.get_property_values('P6782') 
        }
        
        if entity.country:
            institution['country'] = entity.country
        elif location := entity.get_property_value('P276'):
            location_entity = wikidata_get_entity(location)
            institution['country'] = location_entity.country 
              
        return institution      

    
class WikiDataCollectorsTask(BaseTask):    
    
    def requires(self):
        return WikiDataInstitutionsTask()    
    
    def run(self):
        # Natural history collection or herbarium
        query = """
            SELECT DISTINCT ?item WHERE {
            {OPTIONAL { ?item wdt:P428 ?authorAbbrv . }} 
                UNION 
            {OPTIONAL {?item wdt:P6264 ?harvardIndex . }} 
                UNION 
            {OPTIONAL {?item wdt:P6944 ?biomomia . }} 
                UNION 
            {OPTIONAL {?item wdt:P106 wd:Q2374149 . }}
                UNION
            {OPTIONAL {?item wdt:P106 wd:Q2083925 . }}
            } ORDER BY ?item
        """        
        query_results = WikiDataQuery(query)
        with self.output().open('w') as f:  
            for entity in tqdm(query_results):      
                collector = {
                    'qid': entity.qid,
                    'name': entity.label,
                    'aliases': entity.aliases,
                    'birthName': entity.get_property_value('P1477'),
                    'dateOfBirth': entity.get_property_value('P569'),
                    'dateOfDeath': entity.get_property_value('P570'),
                    'VIAF': entity.get_property_value('P214'),
                    'authorAbbrv': entity.get_property_value('P428'), # TL2
                    'harvardIndex': entity.get_property_value('P6264'), 
                    'bionomia': entity.get_property_value('P6944'), 
                    'IPNI': entity.get_property_value('P586'),
                    'ISNI': entity.get_property_value('P213'),     
                    'employers': entity.get_property_values('P108'),
                    }         
                
                if countriesOfCitizenship := entity.get_property_values('P27'):
                    collector['countriesOfCitizenship'] = [wikidata_get_entity(c).country for c in countriesOfCitizenship]
                    
                if employers := entity.get_property_values('P108'):
                    collector['employerCountries'] = {wikidata_get_entity(e).country for e in employers}
                    
                if gender := entity.get_property_value('P21'):
                    collector['sexOrGender'] = wikidata_get_entity(gender).label
                    
                yaml.dump(collector, f, default_flow_style=False, explicit_start=True)
                       
    def output(self): 
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'wikidata' / 'collectors.yaml')      
    
if __name__ == "__main__":
    # luigi.build([ProcessSpecimenTask(image_id='011244568', force=True)], local_scheduler=True)
    luigi.build([WikiDataCollectorsTask(force=True)], local_scheduler=True)