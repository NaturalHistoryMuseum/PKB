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
from pkb.tasks.taxa import TaxaGBIFBackboneTask

tqdm.pandas()



class GraphTaxaTask(GraphBaseTask):

    def requires(self):
        return TaxaGBIFBackboneTask()    

    @property
    def query(self):               
        return f'''
            LOAD CSV WITH HEADERS FROM 'file://{self.input().path}' AS row with row where row.name is not null
            MERGE(t:Taxon {{
                taxonID:row.taxonID,
                name:row.name,
                authorship: coalesce(row.authorship, ''),
                taxonRank:row.taxonRank,
                status:row.status
            }})  
        '''        

class GraphTaxaSynonymyTask(GraphBaseTask):

    def requires(self):
        return TaxaGBIFBackboneTask()    

    @property
    def query(self):   
        return f'''
            LOAD CSV WITH HEADERS FROM 'file://{self.input().path}' AS row with row where row.synonymOf is not null
            MATCH (s:Taxon {{taxonID: row.taxonID}})
            MATCH (t:Taxon {{taxonID: row.synonymOf}})
            MERGE (s)-[:synonym_of]->(t)            
            return count(t)          
        '''  

class GraphTaxaParentTask(GraphBaseTask):

    def requires(self):
        return TaxaGBIFBackboneTask()    

    @property
    def query(self):   
        return f'''
            LOAD CSV WITH HEADERS FROM 'file://{self.input().path}' AS row with row where row.synonymOf is not null
            MATCH (t:Taxon {{taxonID: row.taxonID}})
            MATCH (p:Taxon {{taxonID: row.parent}})
            MERGE (p)-[:parent_of]->(t)                     
        ''' 
    
    
if __name__ == "__main__":
    luigi.build([GraphTaxaTask(),GraphTaxaSynonymyTask(),GraphTaxaParentTask()], local_scheduler=True)  
    # luigi.build([GraphTaxaParentTask()], local_scheduler=True)  

