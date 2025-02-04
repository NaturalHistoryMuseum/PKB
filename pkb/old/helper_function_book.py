import argparse
import torch
import torch.nn as nn
import dgl
import torch.optim as optim
from dgl.dataloading import MultiLayerFullNeighborSampler, EdgeDataLoader
from dgl.dataloading.negative_sampler import Uniform
import numpy as np
import pandas as pd
import itertools
import os
import tqdm
import matplotlib.pyplot as plt
from dgl import save_graphs, load_graphs
import dgl.function as fn
import torch.nn.functional as F
from dgl.nn.pytorch import GraphConv, SAGEConv, HeteroGraphConv
from dgl.utils import expand_as_pair
from collections import defaultdict
import torch as th
import dgl.nn as dglnn
from dgl.data.utils import makedirs, save_info, load_info
from sklearn.metrics import roc_auc_score
import gc
from thefuzz import fuzz
from thefuzz import process
import time
import re

gc.collect()

# Method to compute and plot the data distribution of a given dataframe
'''
Example:
distribution = get_distribution(wiki_data, wiki_columns)
temp = distribution.T.sort_values(by=['sum'], ascending=False)
print(temp)
temp.plot.bar(figsize=(15,10), title = 'WikiData')
'''
def get_distribution(data, col):
    sum_count = 0
    data_distribution = {} ## a dictionary to store the distribution of individual entity
    data_distribution_sum = pd.DataFrame(columns = col, index=['sum'])
    for (columnName, columnData) in data.iteritems():
        temp = data[columnName].value_counts()
        data_distribution_sum.at['sum', columnName]=sum(temp)
    return data_distribution_sum

'''
# this function will remove all special charaters -- including spaces
# used process_time() for evaluation
Example:
newCol = remove_spec_in_col(wiki_data,'aliases')
'''
# Remove square blankets auto generated during data alignment process
def clean_text(text): # fb
    text = text.replace('[', '').replace(']','').replace("'", '')
    return text

def remove_spec_in_col(df, col):
    newCol = []
    for index, rowValue in df[col].iteritems():
        if pd.notnull(rowValue):
            newCol.append(clean_text(rowValue))
        else:
            newCol.append(np.nan)
    return newCol


'''
For Harvard Index data preprocessing
Example:
text1 = "[author note: Types at: MT] [collector note: col. with M. St-Arnaud; MT; BO]"
text2 = "[collector note: Herbarium (Piedmont (Italy): TO]"
text3 = "[collector note: SAFB] plant pathologist; short biography and photo: Can. J. Plant Pathol. Vol. 28: S21-S22. 2006."
text4 = "[collector note: BM-SL, OXF.]"
text5 = "[collector note: Herbarium and types, US, additional m,erial , B, C, DBN, GH, K, MIN, MO, NA, NY, PH"
text = "[author note: LE; temperate Asian Polygonaceae] [collector note: LE] "
temp = text4.split("[")

s1 = get_author_notes(temp)
s2 = get_collector_notes(temp)
print("\nFunction version:")
print("author note: ", s1)
for i in s1: print("\nHerbarium List: ", get_herbarium_codes(i))
print("collector note: ", s2)
for i in s2: print("\nHerbarium List: ", get_herbarium_codes(i))

a, b = get_author_collector_notes(harvard_data, 'Remarks')
'''
# Functions to extract the herbarium institution codes from Remarks in Harvard Index
def get_herbarium_codes(string):
    herbarium_codes = []
    for s in string.split(","):
        if s.isupper():
            herbarium_codes.append(re.sub('[^A-Z]', ',', s).replace(",",""))
    return herbarium_codes
            
def get_author_notes(string):
    authorNotes = []
    for s in string:
        # s = clean_text(s)
        # authorNotes.append(s.partition("author note: ")[2].partition(" ")[0].replace(";", ''))
        authorNotes.append(s.partition("author note: ")[2].partition("]")[0].replace(";", ',').replace(":", ',').replace("(",",").replace(")",",").replace("at",","))
    authorNotes = list(filter(None, authorNotes))
    return authorNotes

def get_collector_notes(string):
    collectorNotes = []
    for s in string:
        # s = clean_text(s)
        # collectorNotes.append(s.partition("collector note: ")[2].partition(" ")[0].replace(";", ''))
        collectorNotes.append(s.partition("collector note: ")[2].partition("]")[0].replace(";", ',').replace(":", ',').replace("(",",").replace(")",",").replace("at",","))
    collectorNotes = list(filter(None, collectorNotes))
    return collectorNotes

def get_author_collector_notes(df, col):
    authorNoteCol = []
    collectorNoteCol = []
    for index, rowValue in df[col].iteritems():
        if pd.notnull(rowValue):
            string = rowValue.split("[")
            authorNoteCol.append(get_author_notes(string))
            collectorNoteCol.append(get_collector_notes(string))
        else:
            authorNoteCol.append([]) # use empty to reduce runtime exception while condition checking
            collectorNoteCol.append([])
    return authorNoteCol, collectorNoteCol

def extract_herbariums(df, col):
    newCol = []
    for index, rowValue in df[col].iteritems():
        if rowValue:
            temp = []
            # for i in rowValue: temp = get_herbarium_codes(i)
            for i in rowValue: 
                temp += get_herbarium_codes(i)
            newCol.append(temp)
        else:
            newCol.append([])
    return newCol


'''
Return the cleaned year value of Wikidata
Example:
new_df['dateOfBirthYear'] = convert_date2year(wiki_data,'dateOfBirth')
'''
def get_year(date_str):
    # Remove + sign
    if date_str[0] == '+':
        date_str = date_str[1:]
    return int(date_str[0:4])

def convert_date2year(df, col):
    newCol = []
    for index, rowValue in df[col].iteritems():
        if pd.notnull(rowValue):
            newCol.append(get_year(rowValue))
        else:
            newCol.append(np.NaN)
    return newCol

# Not used in the data cleaning process
def get_timestamp(date_str):
    # Probably not necessary
    date_str = date_str.strip()
    # Remove + sign
    if date_str[0] == '+':
        date_str = date_str[1:]
    # Remove missing month/day
    date_str = date_str.split('-00', maxsplit=1)[0]
    # Parse date
    dt = np.datetime64(date_str)
    # As Unix timestamp (choose preferred datetype)
    return dt.astype('<M8[s]').astype(np.int64)


'''
For Harvard Index data preprocessing
Example:
temp = harvard_data[['id','Name','Specialty Author', 'Specialty Collector', 'Specialty Determiner', 'Specialty']].copy()
temp['combine_specialty'] = combine_specialty(temp)
# temp['combine_specialty'].value_counts().index.tolist()
'''
# Extract specialty areas of collectors
def combine_specialty(df):
    cols = ['Specialty Author', 'Specialty Collector', 'Specialty Determiner', 'Specialty']
    newCol = df[cols].apply(lambda row: ','.join(row.dropna().unique()), axis=1)
    for index, rowValue in newCol.iteritems():
        if pd.notnull(rowValue):
            newCol[index] = set(clean_text(rowValue).replace('And',',').replace(' and ',',').replace(' ','').split(','))
        else:
            newCol[index] = np.nan
    return newCol


'''
For Harvard Index data preprocessing
Example:
temp = harvard_data[['id','Name','Geography Author', 'Geography Collector', 'Geography Determiner', 'Geography']].copy()
temp['combine_geography'] = combine_geography(temp)
# temp['combine_geography'].value_counts().index.tolist()
'''
# Extract geography locations and travel history related to collectors
def combine_geography(df):
    cols = ['Geography Author', 'Geography Collector', 'Geography Determiner', 'Geography']
    newCol = df[cols].apply(lambda row: ','.join(row.dropna().unique()), axis=1)
    for index, rowValue in newCol.iteritems():
        if pd.notnull(rowValue):
            newCol[index] = set(clean_text(rowValue).replace('\xa0 ','').split(','))
        else:
            newCol[index] = np.nan
    return newCol


# Replace empty list as np.nan
def remove_empty(df, col):
    newCol = []
    for index, rowValue in df[col].iteritems():
        if not rowValue:
            newCol.append(np.nan)
        else:
            newCol.append(rowValue)
    return newCol


'''
For Combining Wikidata and Harvard Index data
Example:
wiki_data['harvardIndex'] = return_numeric(wiki_data, 'harvardIndex')

# But it's the same as the below inbuilt function
wiki_data['harvardIndex'] = pd.to_numeric(wiki_data['harvardIndex'],errors='coerce') ## wrap wiki id to int64
'''
# Helper function to extract numerical numbers from a data column
def return_numeric(df, col):
    newCol = []
    for index, rowValue in df[col].iteritems():
        if pd.notnull(rowValue):
            newCol.append(re.sub("[^0-9|.]", "", str(rowValue)))
        else:
            newCol.append(rowValue)
    return newCol

'''
import unicodedata as ud

latin_letters= {}

def is_latin(uchr):
    try: return latin_letters[uchr]
    except KeyError:
         return latin_letters.setdefault(uchr, 'LATIN' in ud.name(uchr))

def only_roman_chars(unistr):
    return all(is_latin(uchr)
           for uchr in unistr
           if uchr.isalpha()) 
           
Example:
clean_encode_characters_col(df,'Name')
clean_encode_characters(df)
'''
#Clean the latin encoded characters - This is so silly...but it works
def clean_encode_characters(df): # clean all the dataframe
    df = df.replace(to_replace ="Ã¶", value ="ö", regex=True) 
    df = df.replace(to_replace ="Ã©", value ="é", regex=True)
    df = df.replace(to_replace ="Ã¨", value ="è", regex=True)
    df = df.replace(to_replace ="Ã¼", value ="ü", regex=True)
    
def clean_encode_characters_col(df,col): # clean specific column of the dataframe
    df[col] = df[col].replace(to_replace ="Ã¶", value ="ö", regex=True) 
    df[col] = df[col].replace(to_replace ="Ã©", value ="é", regex=True)
    df[col] = df[col].replace(to_replace ="Ã¨", value ="è", regex=True)
    df[col] = df[col].replace(to_replace ="Ã¼", value ="ü", regex=True)

## Helper function to check config in name list
def remove_name_config(nameList):
    i = 0
    while i < len(nameList):
        j = i + 1
        while j < len(nameList):
            if fuzz.token_set_ratio(nameList[i], nameList[j]) == 100:
                del nameList[j]
            else:
                j += 1
        i += 1
    return nameList

# function to get firstnames and lastnames from wiki
def get_firstname_lastname_wiki(df):
    colName = 'label'
    df['firstnames_wiki'] = df[colName].str.split('\s+').str[0]
    df['lastnames_wiki'] = df[colName].str.split('\s+').str[-1]

# function to get firstnames and lastnames from harvard
def get_firstname_lastname_harvard(df):
    colName = 'Name'
    df['firstnames_harvard'] = df[colName].str.split(', ').str[-1]
    df['lastnames_harvard'] = df[colName].str.split(', ').str[0]
    
# function to get firstnames 
def get_firstname(df):
    firstNameList = []
    temp = []
    colNames = ['firstnames_wiki','firstnames_harvard']
    firstNameList = df[colNames].apply(lambda row: ','.join(row.dropna().unique()), axis=1)
    for index, rowValue in firstNameList.iteritems():
        temp = rowValue.split(',')
        if isinstance(temp, str):
            firstNameList[index] = rowValue
        else:
            temp = remove_name_config(temp)
            firstNameList[index] = temp[0]
    return firstNameList

# function to get lastnames
def get_lastname(df):
    lastNameList = []
    temp = []
    colNames = ['lastnames_wiki','lastnames_harvard'] # add the columns you wanna combine here
    lastNameList = df[colNames].apply(lambda row: ','.join(row.dropna().unique()), axis=1)
    for index, rowValue in lastNameList.iteritems():
        temp = rowValue.split(',')
        if isinstance(temp, str):
            lastNameList[index] = rowValue
        else:
            temp = remove_name_config(temp)
            lastNameList[index] = temp[0]
    return lastNameList

# function to return all possible author abbreviations
def get_authorAbbrv(df):
    newCol = []
    temp = []
    colNames = ['authorAbbrv','B\xa0&\xa0P\xa0Author\xa0Abbrev.'] # add the columns you wanna combine here
    newCol = df[colNames].apply(lambda row: ','.join(row.dropna().unique()), axis=1)
    for index, rowValue in newCol.iteritems():
        temp = rowValue.split(',')
        if isinstance(temp, str):
            newCol[index] = rowValue
        else:
            temp = remove_name_config(temp)
            newCol[index] = temp
    return newCol

# function to return the collector's fullname
def get_fullName(df):
    newCol = []
    colNames_w = ['firstnames_wiki','lastnames_wiki']
    colNames_h = ['firstnames_harvard','lastnames_harvard']
    fullname_w = df[colNames_w].apply(lambda row: ' '.join(row.dropna().unique()), axis=1)
    fullname_h = df[colNames_h].apply(lambda row: ' '.join(row.dropna().unique()), axis=1)
    newCol = fullname_w
    for index, rowValue in fullname_w.iteritems():
        if len(rowValue) > len(fullname_h[index]):
            newCol[index] = rowValue
        else:
            newCol[index] = fullname_h[index]
    return newCol

# Extract specialty areas of collectors - for Harvard Index data preprocessing
def combine_name_list(df):
    nameList = []
    cols_name_to_combine = ['aliases','label','authorAbbrv','birthName',
                            'Name','labelName','Full Name','Variant name','Author name'
                            ,'B\xa0&\xa0P\xa0Author\xa0Abbrev.']
    nameList = df[cols_name_to_combine].apply(lambda row: ','.join(row.dropna().unique()), axis=1)
    return nameList

def drop_dump_names(df):
    cols_to_drop = ['aliases','label','authorAbbrv','birthName','Variant name',
                    'Name','labelName','Full Name','Author name','B\xa0&\xa0P\xa0Author\xa0Abbrev.',
                   'firstnames_wiki','lastnames_wiki','firstnames_harvard','lastnames_harvard']

    df.drop(cols_to_drop, axis=1, inplace=True)

# collector name cleaning process
def name_cleaning(df):
    
    result['acceptedNames'] = combine_name_list(result) # return all accepted names of a collector
    result['acceptedNames'] = remove_spec_in_col(result,'acceptedNames')
    
    get_firstname_lastname_wiki(df)
    get_firstname_lastname_harvard(df)
    df['firstname'] = get_firstname(df)
    df['lastname'] = get_lastname(df)
    df['firstname'] = remove_spec_in_col(df,'firstname') # return firstname of a collector
    df['lastname'] = remove_spec_in_col(df,'lastname') # return lastname of a collector
    
    df['authorAbbrv'] = get_authorAbbrv(df) # return all possible author abbreviations of a collector
    
    df['fullName'] = get_fullName(df)
    df['fullName'] = remove_spec_in_col(df,'fullName') # return fullname of a collector
    
    clean_encode_characters_col(df,'firstname')
    clean_encode_characters_col(df,'lastname')
    clean_encode_characters_col(df,'fullName')
    
    drop_dump_names(df)
