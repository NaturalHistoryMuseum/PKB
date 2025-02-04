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
from tqdm import tqdm
from neo4j import GraphDatabase, RoutingControl
from abc import ABCMeta, abstractmethod

from pkb.config import INTERMEDIATE_DIR, CACHE_DIR, OUTPUT_DIR, logger, NEO4J
from pkb.tasks.base import BaseTask
from pkb.tasks.graph.base import GraphBaseTask
from pkb.tasks.countries import CountriesTask

tqdm.pandas()

class GraphCountriesTask(GraphBaseTask):

    def requires(self):
        return CountriesTask()    

    @property
    def query(self):               
        return f'''
            LOAD CSV WITH HEADERS FROM 'file://{self.input().path}' AS row with row where row.ISO is not null
            MERGE(t:Country {{
                ISO: row.ISO, 
                ISO3: row.ISO3,
                name: row.Country,
                continent: coalesce(row.Continent, '')
            }})  
        '''        


    
    
if __name__ == "__main__":
    luigi.build([GraphCountriesTask()], local_scheduler=True)  
    # luigi.build([GraphTaxaParentTask()], local_scheduler=True)  

