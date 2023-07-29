import luigi
import requests
import requests_cache
import lxml
from bs4 import BeautifulSoup, SoupStrainer
from pkb.config import INTERMEDIATE_DIR, CACHE_DIR, logger, INPUT_DIR
from pkb.tasks.base import BaseTask
from pkb.tasks.grscicol import GRSciCollAggregatedTask
from pkb.utils import is_alpha
from dateutil.parser import parse
from nameparser import HumanName
import yaml
import time
import pandas as pd
import re
import unicodedata
import numpy as np
from tqdm import tqdm


requests_cache.install_cache(CACHE_DIR / 'harvard')


class HarvardIndexSearchTask(BaseTask):
    
    url = 'https://kiki.huh.harvard.edu/databases/botanist_search.php'
    
    def _search(self, params):
        default_params = {'start': 1, 'name': None, 'id': None, 'remarks': None, 'specilaity': None, 'country': None}        
        params.update(default_params)
        r = requests.get(self.url, params=params)          
        logger.info('Using cached search: %s', r.from_cache)         
        soup = BeautifulSoup(r.text, 'lxml')
        inputs = soup.find_all('input', {'name': 'id[]'})    
        for i in inputs:
            yield i['value']
            
    def _search_individuals(self):            
        return self._search({'individual': 'on'})

    def _search_collectors(self):            
        return self._search({'is_collector': 'on'})
            
    def run(self):
        logger.info(f'Parsing Harvard Index of individuals')         
        individuals = set(self._search_individuals()) 
        logger.info(f'%s individuals retrieved from Harvard Index', len(individuals)) 
        logger.info(f'Parsing Harvard Index of collectors')         
        collectors = set(self._search_collectors()) 
        logger.info(f'%s collectors retrieved from Harvard Index', len(collectors))
        collector_ids = individuals | collectors
        with self.output().open('w') as f:                  
            f.write(yaml.dump(list(collector_ids),  default_flow_style=False))         
        
    def output(self):
        return luigi.LocalTarget(INTERMEDIATE_DIR / 'harvard-index' / f'search.yaml')           
        
class HarvardIndexDetailTask(BaseTask):
    
    url = 'https://kiki.huh.harvard.edu/databases/botanist_search.php'
    
    def requires(self):
        return HarvardIndexSearchTask()
    
    def run(self):
        with self.input().open('r') as f: 
            collector_ids = yaml.full_load(f)  
            
        with self.output().open('w') as outf:  
            
            start_time = time.time()  
            # collector_ids.reverse()
            
            # collector_ids = [42819]
            
            for collector_id in tqdm(collector_ids):              
                collector = self._parse_detail_page(collector_id)
                if botanist_id := collector.get('ASA Botanist ID'):                
                    try:
                        _, asa_cat = botanist_id.split()
                    except ValueError:
                        pass
                    else:
                        collector['asa_category'] = asa_cat
                
                yaml.dump(collector, outf, default_flow_style=False, explicit_start=True, allow_unicode=True)
                
            logger.info("Total Running time = {:.3f} seconds".format(time.time() - start_time))

            
    def _parse_detail_page(self, collector_id):
        params = {
           'mode': 'details',
           'id': collector_id
        }
        r = requests.get(self.url, params)
        
        strainer = SoupStrainer('div', attrs={'id': 'main_text_wide'})        
        soup = BeautifulSoup(r.content, 'lxml', parse_only=strainer)

        try:
            table = soup.find('table')
            trs = table.find_all('tr')
        except AttributeError:       
            logger.error('Could not parse harvard detail page for %s', collector_id)
            return
        
        collector = {
            'id': collector_id
        }
        
        for tr in trs:

            label = self.normalise(tr.find('td', {"class":"cap"}).get_text(strip=True))
            val = self.normalise(tr.find('td', {"class":"val"}).get_text(strip=True))            
            if label == 'Variant name': label = 'Name'   
            
            try:            
                collector[label].append(val)
            # Label does not exist so set it to the str value 
            except KeyError:
                collector[label] = val
            # Label already exists, but is a string
            except AttributeError:
                collector[label] = [collector[label], val]  

        return collector
    
    @staticmethod
    def normalise(text):
        return unicodedata.normalize("NFKD", text)
        
    
    def output(self):
        return luigi.LocalTarget(INTERMEDIATE_DIR /  'harvard-index' / 'detail-4.yaml')     
            
            
class HarvardIndexCollectorsTask(BaseTask):
    
    # Match space/start A or A-Z space/end/comma
    herb_code_regex = re.compile('(?:\s|^|\[)([A-Z]+|[A-Z]+\-[A-Z]+)(?:\s+|$|,|\])')
    
    def requires(self):
        return [
            HarvardIndexDetailTask(),
            GRSciCollAggregatedTask(),
        ]
        
    def run(self):
        logger.info('Loading harvard index botanists')
        
        with HarvardIndexDetailTask().output().open('r') as f:             
            harvard_index = yaml.load_all(f, yaml.FullLoader)
            df = pd.json_normalize(harvard_index)
            
        grscicol = pd.read_parquet(GRSciCollAggregatedTask().output().path)
            
        logger.info('%s records in Harvard Index', len(df))
            
        # FIXME: Should be a task
        countries = pd.read_csv(INPUT_DIR / 'countries.csv')
        alt_countries = pd.read_csv(INPUT_DIR / 'alternateCountryNames.csv')  
        
        # Create a dict keyed by name - easy lookup later for ISO  
        country_dict = dict(alt_countries[alt_countries.isolanguage.isin(['en', 'en-GB'])][['altName', 'ISO']].to_records(index=False))
        country_dict.update(dict(countries[['countryName', 'ISO']].to_records(index=False)))        
        country_names = [n for n in country_dict.keys() if is_alpha(n)]
        # Sorted so longer matches are found first
        sorted_country_names = sorted(country_names, key=len, reverse=True)
        pattern = re.compile("|".join(sorted_country_names))
        
        def string_to_country_code(combined_geog):
            matches = set(pattern.findall(combined_geog))
            iso = [country_dict.get(m) for m in matches]
            return iso        

        # Ensure these are individual botanists (list includes insitutions and groups)
        df = df[(df['asa_category'] == 'botanist') & (df['Agent type'] != 'Team/Group')]
        
        logger.info('%s botanists identifed in Harvard Index', len(df))
        
        df[['birthYear', 'birthYearIsApprox']] = df[df['Date of birth'].notna()].apply(self.get_year, axis=1, result_type='expand', args = ('Date of birth',))
        df[['deathYear', 'deathYearIsApprox']] = df[df['Date of death'].notna()].apply(self.get_year, axis=1, result_type='expand', args = ('Date of death',))

        geography_cols = ['Geography Author', 'Geography Collector', 'Geography Determiner', 'Geography']
        df['geographyCombined'] = df[geography_cols].apply(lambda x: x.str.cat(sep='|'), axis=1)        
        df['geographyISO'] = df[df['geographyCombined'] != '']['geographyCombined'].apply(string_to_country_code)
        
        df[['title', 'firstName', 'middleName', 'lastName']] = df.apply(self.parse_name, axis=1, result_type='expand')

        # Extract herbaria codes from remarks - needs tidying up / mering with Q's code
        df['herbariaCode'] = df[df['Remarks'].notna()]['Remarks'].apply(self.extract_herbarium_codes)    
        
        # Add insitution UUID for herbaria codes
        herb_code = pd.concat([
            grscicol[grscicol.indexHerbCode.notna()].explode('indexHerbCode').indexHerbCode,
            grscicol[grscicol.collectionsindexHerbCode.notna()].explode('collectionsindexHerbCode').collectionsindexHerbCode
        ]).drop_duplicates()
        
        df['institutionUUID'] = df[df.herbariaCode.notna()].herbariaCode.apply(lambda codes: herb_code[herb_code.isin(codes)].index.to_list())
        logger.info('%s botanists matched to insitution ID', len(df[df['institutionUUID'].notna()]))
        
        # Contains lists so lets use a CSV
        df.to_csv(self.output().path, index=False)

    def output(self):
        return luigi.LocalTarget(INTERMEDIATE_DIR /  'harvard-index' / 'collectorss.csv') 
    
    @staticmethod
    def get_year(row, field_name):
        s = row[field_name]
        year = parse(s, fuzzy=True).year
        # Match ? ~ ca - denotes approx
        is_approx = bool(re.search(r'\?|~|ca', s))
        return year, is_approx  
    
    @staticmethod    
    def parse_name(row):
        full_name = row['Name'] if pd.isnull(row['Full Name']) else row['Full Name']
        # If this is a list, we use the longest name  
        if isinstance(full_name, list):
            full_name = sorted(full_name, key=len)[-1]
        name = HumanName(full_name)
        return name.title, name.first, name.middle, name.last 
    
    def extract_herbarium_codes(self, remarks):
        return set(self.herb_code_regex.findall(remarks)) or np.nan
        
if __name__ == "__main__":
    luigi.build([HarvardIndexCollectorsTask(force=True)], local_scheduler=True)        