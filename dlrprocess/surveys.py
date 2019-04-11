#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 09:34:08 2017

@author: Wiebke Toussaint

Answer query script: This script contains functions to query and manipulate DLR survey answer sets. It references datasets that must be stored in a /data/tables subdirectory in the parent directory.

"""

import numpy as np
import pandas as pd
import os
from glob import glob
import json
import feather

from .support import usr_dir, fdata_dir, table_dir, InputError, validYears, geoMeta, writeLog
   
def loadTable(name, columns=None):
    """
    This function loads all feather tables in filepath into workspace.
    
    """
    #TODO should also be able to read csv files
    
    dir_path = os.path.join(table_dir, 'feather')

    file = os.path.join(dir_path, name +'.feather')
    d = feather.read_dataframe(file)
    if columns is None:
        table = d
    else:
        table = d[columns]
            
    try: 
        return table

    except UnboundLocalError:
        return('Could not find table with name '+name)    

def loadID():
    """
    This function matches all ProfileIDs of observational electricity data with AnswerIDs of the corresponding survey 
    responses. Namibian households are removed. The following geographic information is added for each location:
        - Latitude
        - Longitude
        - Province
        - Municipality
        - District
    """
    this_dir = os.path.dirname(__file__)
    groups = loadTable('groups')
    links = loadTable('links')
    profiles = loadTable('profiles')
    
#    a_id = links[(links.GroupID != 0) & (links['AnswerID'] != 0)].drop(columns=['ConsumerID','lock','ProfileID'])
    p_id = links[(links.GroupID != 0) & (links['ProfileID'] != 0)].drop(labels=['ConsumerID','lock','AnswerID'], axis=1)
    profile_meta = profiles.merge(p_id, how='left', left_on='ProfileId', right_on='ProfileID').drop(labels=['ProfileId','lock'], axis=1)

    ap = links[links.GroupID==0].drop(labels=['ConsumerID','lock','GroupID'], axis=1)
    
    x = profile_meta.merge(ap, how='outer', on = 'ProfileID')    
    join = x.merge(groups, on='GroupID', how='left')

    #Wrangling data into right format    
    all_ids = join[join['Survey'] != 'Namibia'] # remove Namibian households 
    all_ids = all_ids.dropna(subset=['GroupID','Year'])
    all_ids.Year = all_ids.Year.astype(int)
    all_ids.GroupID = all_ids.GroupID.astype(int)
    all_ids.AnswerID.fillna(0, inplace=True)
    all_ids.AnswerID = all_ids.AnswerID.astype(int)
    all_ids.ProfileID = all_ids.ProfileID.astype(int)

    try:
        geo_meta = pd.read_csv(os.path.join(this_dir,'data', 'geometa', 'site_geo.csv'))
    except:
        geoMeta()
        geo_meta = pd.read_csv(os.path.join(this_dir,'data', 'geometa', 'site_geo.csv'))

    output = all_ids.merge(geo_meta[['GPSName','Lat','Long','Province','Municipality',
                                     'District']], left_on='LocName', right_on='GPSName', how='left')
    output.drop(labels='GPSName', axis=1, inplace=True)
        
    return all_ids


def duplicateIDs():
    """
    This function returns duplicate ProfileIDs and the corresponding AnswerIDs in which they are duplicated.
    """
    ids = loadID()
    i = ids[(ids.duplicated('ProfileID')==True)&(ids['ProfileID']!=0)]
    ip = i.pivot_table(index='Year',columns='AnswerID',values='ProfileID',aggfunc='count')

    print('Duplicate ProfileIDs:', i.ProfileID.unique())
    print(ip)
    return 


def loadQuestions(dtype = None):
    """
    This function gets all survey questions.
    
    *input*
    -------
    dtype (str): default = None, or one of 'blob', 'char', 'num'
    
    """
    qu = loadTable('questions').drop(labels='lock', axis=1)
    qu.Datatype = qu.Datatype.astype('category')
    qu.Datatype.cat.categories = ['blob','char','num']
    qu['ColumnAlias'] = [x.strip() for x in qu['ColumnAlias']]
    if dtype is None:
        pass
    else: 
        qu = qu[qu.Datatype == dtype]
    return qu

def loadAnswers():
    """
    This function returns all survey responses as a dict constructed by data type with keys = [blob, char, num].
    
    """
    answer_meta = loadTable('answers', columns=['AnswerID', 'QuestionaireID'])

    blob = loadTable('answers_blob_anonymised').drop(labels='lock', axis=1)
    blob = blob.merge(answer_meta, how='left', on='AnswerID')
    blob.fillna(np.nan, inplace = True)
    
    char = loadTable('answers_char_anonymised').drop(labels='lock', axis=1)
    char = char.merge(answer_meta, how='left', on='AnswerID')
    char.fillna(np.nan, inplace = True)

    num = loadTable('answers_number_anonymised').drop(labels='lock', axis=1)
    num = num.merge(answer_meta, how='left', on='AnswerID')
    num.fillna(np.nan, inplace = True)

    return {'blob':blob, 'char':char, 'num':num}

def searchQuestions(search = None):
    """
    Searches questions for a search term, taking questionaire ID and question data type (num, blob, char) as input. 
    A single search term can be specified as a string, or a list of search terms as list.
    
    """       
    questions = loadTable('questions').drop(labels='lock', axis=1)
    questions.Datatype = questions.Datatype.astype('category')
    questions.Datatype.cat.categories = ['blob','char','num']
        
    if search is None:
        searchterm = ''
    else:
        searchterm = search.replace(' ', '+')

    trantab = str.maketrans({'(':'', ')':'', ' ':'', '/':''})
    
    result = questions.loc[questions.Question.str.translate(trantab).str.contains(searchterm, case=False), ['Question', 'Datatype','QuestionaireID', 'ColumnNo']]
    return result

def searchAnswers(search):
    """
    This function returns the answer IDs and responses for a list of search terms
    
    """
    answers = loadAnswers()

    questions = searchQuestions(search) #get column numbers for query
    
    result = pd.DataFrame(columns=['AnswerID','QuestionaireID'])
    for dt in questions.Datatype.unique():
        ans = answers[dt]
        for i in questions.QuestionaireID.unique():            
            select = questions.loc[(questions.Datatype == dt)&(questions.QuestionaireID==i)]            
            fetchcolumns=['AnswerID'] + ['QuestionaireID'] + list(select.ColumnNo.astype(str))
            newcolumns = ['AnswerID'] + ['QuestionaireID'] + list(select.Question.astype(str).str.lower())
            
            df = ans.loc[ans['QuestionaireID']==i,fetchcolumns]           
            df.columns = newcolumns
            
            result = result.merge(df, how='outer')
            
    return result


def extractSocios(searchlist, year=None, col_names=None, geo=None):
    """
    This function extracts a set of survey responses for a given year, based on a pre-defined list of search terms.
    
    *input*
    -------
    dtype (str): default
    """

    if isinstance(searchlist, list):
        pass
    else:
        searchlist = [searchlist]
        
    if col_names is None:
        search = dict(zip(searchlist, searchlist))
    else:
        search = dict(zip(searchlist, col_names))
    
    #filter AnswerIDs by year          
    ids = loadID()
    if year is None:
        sub_ids = ids[ids.AnswerID!=0]
    else:
        sub_ids = ids[(ids.AnswerID!=0)&(ids.Year==year)]
        sub_ids = sub_ids.drop_duplicates(subset='AnswerID')
    
    #generate feature frame
    result = pd.DataFrame(columns=['AnswerID','QuestionaireID'])        
    for s in search.keys():
        d = searchAnswers(s)
        ans = d[(d.AnswerID.isin(sub_ids.AnswerID)) & (d.QuestionaireID < 10)] # remove non-domestic results 
        ans = ans.dropna(axis=1, how='all')
    #set feature frame column names
        if len(ans.columns[2:])==1:
            ans.columns = ['AnswerID','QuestionaireID'] + [search.get(s)]

        try:    
            result = result.merge(ans, how='outer')
        except Exception:
            pass
        
    if geo is None:
        result = result.merge(sub_ids[['AnswerID', 'ProfileID']], how='left')
    else:
        result = result.merge(sub_ids[['AnswerID', 'ProfileID', geo]], how='left')
                          
    return result

def generateSociosSetSingle(year, spec_file, set_id='ProfileID'):
    """
    This function generates a json formatted evidence text file compatible with 
    the syntax for providing evidence to the python library libpgm for the specified 
    year. The function requires a json formatted text file with feature specifications as input.
    
    """
    #Get feature specficiations
    files = glob(os.path.join(usr_dir, 'specs', spec_file + '*.txt'))

    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                featurespec = json.load(f)        
            year_range = featurespec['year_range']
        except:
            raise InputError(year, 'Problem reading the spec file.')
            
        if year >= int(year_range[0]) and year <= int(year_range[1]):            
            validYears(year) #check if year input is valid
            break
        else:
            continue
            
    searchlist = featurespec['searchlist']
    features = featurespec['features']
    transform = featurespec['transform']
    bins = featurespec['bins']
    labels = featurespec['labels']
    cut = featurespec['cut']
    replace = featurespec['replace']
    if len(featurespec['geo'])==0:
        geo = None
    else:
        geo = featurespec['geo']
    
    #Get data and questions from socio-demographic survey responses
    data = extractSocios(searchlist, year, col_names=searchlist, geo=geo)
    missing_cols = list(set(searchlist) - set(data.columns))
    data = data.append(pd.DataFrame(columns=missing_cols), sort=True) #add columns dropped during feature extraction
    data.fillna(0, inplace=True) #fill na with 0 to allow for further processing
    data['AnswerID'] = data.AnswerID.astype(int)
    data['ProfileID'] = data.ProfileID.astype(int)

    if len(data) is 0:
        raise InputError(year, 'No survey data collected for this year')
    
    else:
        #Transform and select BN nodes from dataframe 
        for k, v in transform.items():
            data[k] = data.apply(lambda x: eval(v), axis=1)
        try:
            data = data[[set_id, geo] + features]
        except:
            data = data[[set_id] + features]
            
    #adjust monthly income for inflation: baselined to Stats SA December 2016 values. 
    #Important that this happens here, after columns have been renamed and before income data is binned
    if 'monthly_income' in features:
        cpi_percentage=(0.265,0.288,0.309,0.336,0.359,0.377,0.398,0.42,0.459,0.485,0.492,0.509,
                    0.532,0.57,0.636,0.678,0.707,0.742, 0.784, 0.829,0.88,0.92,0.979,1.03)
        cpi = dict(zip(list(range(1994,2015)),cpi_percentage))
        data['monthly_income'] = data['monthly_income']/cpi[year]
    
    #Cut columns into datatypes that match factors of BN node variables    
    for k, v in bins.items():
        bin_vals = [int(b) for b in v]
        try:
            data[k] = pd.cut(data[k], bins = bin_vals, labels = labels[k], 
                right=eval(cut[k]['right']), include_lowest=eval(cut[k]['include_lowest']))
            data[k].cat.reorder_categories(labels[k], inplace=True)
        except KeyError:
            data[k] = pd.cut(data[k], bins = bin_vals, labels = labels[k])

    for y, z in replace.items():
        data[y].replace([int(a) for a in z.keys()], z.values(),inplace=True)                                  
        data[y].where(data[y]!=0, inplace=True)  
            
    data.set_index(set_id, inplace=True) #set ID column as index

    return data
                 
def generateSociosSetMulti(spec_files, year_start=1994, year_end=2014):

    if isinstance(spec_files, list):
        pass
    else:
        spec_files = [spec_files]
    
    ff = pd.DataFrame()    
    for spec in spec_files:
        gg = pd.DataFrame()
        for year in range(year_start, year_end+1):
            try:
                gg = gg.append(generateSociosSetSingle(year, spec))
            except Exception:
                ## TODO this should be logged
                print('Could not extract features for '+str(year)+' with spec '+spec)
            pass
        ff = ff.merge(gg, left_index=True, right_index=True, how='outer')
        del gg #clear memory

    #Some data wrangling to sort out data types
    for c in ff.columns:
        if ff[c].dtype == np.float64:
            if sum(ff[c]%1) == 0.0: #check for columns that should be integers
                ff[c] = ff[c].astype(int)
                #TODO also check for nan
                
    ff = ff[~ff.index.duplicated(keep='first')] #problem with profile_id 8396, answer id 2000458
    
    return ff

def genS(spec_files, year_start, year_end, filetype='csv'):
    """
    This function saves an evidence dataset with observations in the data directory.
    
    """
    loglines = []

    if isinstance(spec_files, list):
        pass
    
    else:
        spec_files = [spec_files]
        
    #Save data to disk
    root_name = '_'.join(spec_files)
    file_name =  root_name+'_'+str(year_start)+'+'+str(year_end-year_start)+'.'+filetype
    dir_path = os.path.join(fdata_dir, root_name)
    os.makedirs(dir_path , exist_ok=True)
    file_path = os.path.join(dir_path, file_name)
    
    try:
        try:
            evidence = feather.read_dataframe(file_path)
            evidence.set_index('ProfileID',inplace=True)
        except:
            evidence = pd.read_csv(file_path).set_index('ProfileID')
    
    except:
        #Generate evidence data
        evidence = generateSociosSetMulti(spec_files, year_start, year_end)
        status = 1      
        message = 'Success!'
        if filetype == 'feather':
            feather.write_dataframe(evidence.reset_index(), file_path)
            print('Success! Saved to data/feature_data/'+root_name+'/'+file_name)
        elif filetype == 'csv':
            evidence.to_csv(file_path, index=False)
            print('Success! Saved to data/feature_data/'+root_name+'/'+file_name)
        else:
            status = 0
            message = 'Cannot save to specified file type'
            print(message)
    
    		#TODO errors are a MESS! 
            #save errors to logs
		    #l = ['featureExtraction', year_start, year_end, status, message, spec_files, file_name]
		    #loglines.append(l)            
		    #logs = pd.DataFrame(loglines, columns = ['process','from year','to year','status','message','features', 'output_file'])
		    #writeLog(logs,'log_generateData')
 
    evidence.sort_index(inplace=True)
           
    return evidence

def checkAnswer(answerid, features):
    """
    This function returns the survey responses for an individuals answer ID and list of search terms.
    
    """
    links = loadTable('links')
    groupid = links.loc[links['AnswerID']==answerid].reset_index(drop=True).get_value(0, 'GroupID')
    groups = loadTable('groups')
    year = int(groups.loc[groups.GroupID == groupid, 'Year'].reset_index(drop=True)[0])
    
    ans = extractSocios(features, year).loc[extractSocios(features, year)['AnswerID']==answerid]
    return ans

def recorderLocations(year = 2014):
    """
    This function returns all survey locations and recorder abbreviations for a given year. Only valid from 2009 onwards.
    
    """
    if year > 2009:
        stryear = str(year)
        groups = loadTable('groups')
        recorderids = loadTable('recorderinstall')
        
        reclocs = groups.merge(recorderids, left_on='GroupID', right_on='GROUP_ID')
        reclocs['recorder_abrv'] = reclocs['RECORDER_ID'].apply(lambda x:x[:3])
        yearlocs = reclocs.loc[reclocs['Year']== stryear,['GroupID','LocName','recorder_abrv']].drop_duplicates()
        
        locations = yearlocs.sort_values('LocName')
        return locations 
    
    else:
        print('Recorder locations can only be returned for years after 2009.')

def lang(code = None):
    """
    This function returns the language categories.
    
    """
    language = dict(zip(searchAnswers(qnairid=5)[0].iloc[:,1], searchAnswers(qnairid=5,dtype='char')[0].iloc[:,1]))
    if code is None:
        pass
    else:
        language = language[code]
    return language

def altE(code = None):
    """
    This function returns the alternative fuel categories.
    
    """
    altenergy = dict(zip(searchAnswers(qnairid=8)[0].iloc[:,1], searchAnswers(qnairid=8,dtype='char')[0].iloc[:,1]))
    if code is None:
        pass
    else:
        altenergy = altenergy[code]
    return altenergy
