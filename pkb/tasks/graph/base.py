from neo4j import GraphDatabase
from abc import ABCMeta, abstractmethod

from pkb.config import NEO4J
from pkb.tasks.base import BaseTask



class GraphBaseTask(BaseTask):

    __metaclass__ = ABCMeta

    @property
    @abstractmethod
    def query(self):    
        return None

    def run(self): 

        with GraphDatabase.driver(NEO4J.URI.value, auth=(NEO4J.USER.value, NEO4J.PASSWORD.value)) as driver:
            driver.verify_connectivity()
            records, summary, keys = driver.execute_query(
                self.query,
                database_="neo4j",
            ) 

            print(summary.counters)
            print(records)
            print('--')