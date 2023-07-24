from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
import requests


def wikidata_get_entity(qid):
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{qid}.json'
    r = requests.get(url)
    r.raise_for_status()
    return WikiDataEntity(list(r.json().get('entities').values())[0])    

class WikiDataEntity:
    
    def __init__(self, data):
        self._data = data

    @property
    def qid(self):        
        return self._data['id']
        
    @property
    def label(self):
        try:
            return self._data['labels']['en']['value']
        except KeyError:
            # No english label - skip
            print('No english label')        

    @property
    def aliases(self):            
        return [a['value'] for a in self._data['aliases'].get('en', [])]
    
    @property
    def country(self):            
        if country := self.get_property_value('P17'):
            entity = wikidata_get_entity(country)
            return entity.get_property_value('P297')
        
                
    @staticmethod
    def _get_property(claim):
        if value := claim['mainsnak'].get('datavalue'):
            v = value['value']

            if isinstance(v, dict):
                if v.get('latitude'):
                    return f"{v['latitude']} {v['longitude']}"

                return v.get('time') or v.get('text') or v.get('id')

            return v    

    def get_property_values(self, prop):
        values = list({v for claim in self._data['claims'].get(prop, []) if (v := self._get_property(claim))})   
        if values:
            return values

    def get_property_value(self, prop):    
        if prop_value := self._data['claims'].get(prop):
            return self._get_property(prop_value[0])        
        
class WikiDataQuery:
    
    sparql = SPARQLWrapper("https://query.wikidata.org/sparql")
    api_subset_size = 50
    
    def __init__(self, query):
        self._qids = self._query(query)
            
    def _query(self, query):
        self.sparql.setQuery(query)
        self.sparql.setReturnFormat(JSON)
        results = self.sparql.query().convert()
        return [result['item']['value'].split('/')[-1] for result in results['results']['bindings']]    
        
    def __iter__(self):
        yield from wikidata_api_get_entities(self._qids)
            
    def __len__(self):
        return len(self._qids)
    
def wikidata_api_get_entities(qids):    
    api_max_ids = 50
    for i in range(0, len(qids), api_max_ids):
        qid_str = '|'.join(qids[i:i+api_max_ids])
        url = f'https://www.wikidata.org/w/api.php?action=wbgetentities&props=claims|aliases|labels&ids={qid_str}&format=json&languages=en'
        r = requests.get(url)
        r.raise_for_status()
        for entity in r.json().get('entities').values():
            yield WikiDataEntity(entity) 
            