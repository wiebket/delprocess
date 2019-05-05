#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 09:34:08 2017
@author: Wiebke Toussaint

This module contains functions to query and manipulate DEL Survey data.

Updated: 4 May 2019
"""

import numpy as np
import pandas as pd
import os
from glob import glob
import json

from .support import usr_dir, fdata_dir, table_dir, InputError, validYears, geoMeta#, writeLog
   

def loadTable(name):
    """Loads a table into the workspace.
    
    Parameters:
        name (str): Table name. Must be saved as csv file in USER_data_path/tables.
    
    Returns:
        pandas dataframe: Data in csv file (ie table).
    """
    file = os.path.join(table_dir, name +'.csv')
    try: 
        table = pd.read_csv(file)        
        return table
    except FileNotFoundError:
        return('Could not find table "{}" in {}'.format(name, table_dir))


def loadID():
    """Matches all electricity ProfileIDs with household survey AnswerIDs. 
    
    The following geographic information is added:
        - Latitude
        - Longitude
        - Province
        - Municipality
        - District
    Namibian survey respondents are removed. 

    Returns:
        pandas dataframe with columns [
                'RecorderID', 'ChannelNo', 'Type', 'Unit of measurement', 
                'Active', 'aux', 'GroupID', 'ProfileID', 'AnswerID', 
                'ContextID', 'Dom_NonDom', 'Survey', 'Year', 'Location', 
                'LocName', 'Lat', 'Long', 'Province', 'Municipality', 'District']   
    """
    this_dir = os.path.dirname(__file__)
    groups = loadTable('groups')
    links = loadTable('links')
    profiles = loadTable('profiles')
    
    p_id = links[(links.GroupID != 0) & (links['ProfileID'] != 0)
                ].drop(labels=['ConsumerID','lock','AnswerID'], axis=1)
    profile_meta = profiles.merge(p_id, how='left', left_on='ProfileId', right_on='ProfileID'
                                  ).drop(labels=['ProfileId','lock'], axis=1)
    ap = links[links.GroupID==0].drop(labels=['ConsumerID','lock','GroupID'], axis=1)
    x = profile_meta.merge(ap, how='outer', on = 'ProfileID')    
    join = x.merge(groups, on='GroupID', how='left')

    # Wrangling data into right format   
    # Remove Namibian households 
    all_ids = join[join['Survey'] != 'Namibia'] 
    all_ids = all_ids.dropna(subset=['GroupID','Year'])
    all_ids.Year = all_ids.Year.astype(int)
    all_ids.GroupID = all_ids.GroupID.astype(int)
    all_ids.AnswerID.fillna(0, inplace=True)
    all_ids.AnswerID = all_ids.AnswerID.astype(int)
    all_ids.ProfileID = all_ids.ProfileID.astype(int)

    try:
        geo_meta = pd.read_csv(os.path.join(this_dir,'data', 'geometa', 'site_geo.csv'))
    except:
        # Merge boundary data to site locations if not yet done
        geoMeta() 
        geo_meta = pd.read_csv(os.path.join(this_dir,'data', 'geometa', 'site_geo.csv'))

    output = all_ids.merge(
            geo_meta[['GPSName','Lat','Long','Province','Municipality','District']], 
            left_on='LocName', right_on='GPSName', how='left')
    output.drop(labels='GPSName', axis=1, inplace=True)
        
    return output


def loadQuestions(dtype = None):
    """Returns all survey questions.
    
    Parameters:
        dtype (str): One of 'blob', 'char', 'num'. Defaults to None.
    
    Returns:
        pandas dataframe with columns [
                'QuestionID', 'QuestionaireID', 'Question', 'Datatype', 
                'ColumnNo', 'ColumnAlias']    
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
    """Returns all anonymised survey responses.
        
    Returns:
        dict of pandas dataframes: keys = ['blob', 'char', 'num']
    """
    answer_meta = loadTable('answers').loc[:,['AnswerID', 'QuestionaireID']]

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
    """Searches questions for a single search criteria. 
       
    The search criteria must be a single string that can consist of multiple words 
    separated by whitespace. The order of words in the search criteria is important, 
    as the whitespace will be removed and words joined during search. 
    
    The search is not case sensitive and has been implemented as a simple 
    `str.contains(searchterm, case=False)`, searching all the strings of all 
    the `Question` column entries in the `questions.csv` data file. 
    
    For example, 'hot water' and 'HoT wAtER' will yield the same results, 
    but 'water hot' will yield no results!
    
    Parameters:
        search (str): String of words separated by whitespace. Defaults to None (returns all).
    
    Returns:
        pandas dataframe with columns [
            'Question', 'Datatype', 'QuestionaireID', 'ColumnNo']
    """       
    questions = loadTable('questions').drop(labels='lock', axis=1)
    questions.Datatype = questions.Datatype.astype('category')
    questions.Datatype.cat.categories = ['blob','char','num']
        
    if search is None:
        searchterm = ''
    else:
        searchterm = search.replace(' ', '+')

    trantab = str.maketrans({'(':'', ')':'', ' ':'', '/':''})
    result = questions.loc[
            questions.Question.str.translate(trantab).str.contains(searchterm, case=False), 
            ['Question', 'Datatype','QuestionaireID', 'ColumnNo']]
    
    if len(result) is 0:
        raise InputError(search, 'Not contained in any question. Try something else.')
#        print('Search term "{}" is not contained in any question. Try something else.'.format(search))
    else:
        return result


def searchAnswers(search):
    """Returns the AnswerIDs and responses for a single search criteria.
    
    Parameters:
        search (str): String of words separated by whitespace. Defaults to None (returns all).
    
    Returns:
        pandas dataframe with columns [
            'AnswerID', 'QuestionaireID', Questions corresonding to search]    
    """
    answers = loadAnswers()
    # Get column numbers for query
    try:
        questions = searchQuestions(search)
    except InputError:
        raise
    result = pd.DataFrame(columns=['AnswerID','QuestionaireID'])
    
    for dt in questions.Datatype.unique():
        ans = answers[dt]
        for i in questions.QuestionaireID.unique():            
            select = questions.loc[(questions.Datatype == dt) &
                                   (questions.QuestionaireID==i)]            
            fetchcolumns=['AnswerID'] + ['QuestionaireID'] + list(select.ColumnNo.astype(str))
            newcolumns = ['AnswerID'] + ['QuestionaireID'] + list(select.Question.astype(str).str.lower())
            df = ans.loc[ans['QuestionaireID']==i,fetchcolumns]           
            df.columns = newcolumns
            result = result.merge(df, how='outer')
            
    return result


def extractSocios(searchlist, year=None, col_names=None, geo=None):
    """Extracts survey responses based on a list of search criteria
    
    The result are joined with ProfileIDs and geographic metadata.
    Non-domestic survey respondents are removed.
    
    Parameters:
        searchlist (list): List of valid search criteria.
        year (int): 1994 <= year <= 2014. Defaults to None (returns all).
        col_names (list): Renames new column headers. Defaults to None (uses searchlist).
        geo (str): Can be 'Province', 'District' or 'Municipality'. Defaults to None (returns none).
    
    len(searchlist) == len(col_names)
    
    Returns:
        pandas dataframe with columns [
            'AnswerID','QuestionaireID', Questions corresonding to searchlist,
            'ProfileID','Unit of measurement','Survey','Year', geo, 'LocName']
    """
    if isinstance(searchlist, list):
        pass
    else:
        searchlist = [searchlist]
        
    if col_names is None:
        search = dict(zip(searchlist, searchlist))
    else:
        search = dict(zip(searchlist, col_names))
    
    # Filter AnswerIDs by year          
    ids = loadID()
    if year is None:
        sub_ids = ids[ids.AnswerID!=0]
    else:
        sub_ids = ids[(ids.AnswerID!=0)&(ids.Year==year)]
    
    # Generate feature frame
    result = pd.DataFrame(columns=['AnswerID','QuestionaireID'])        
    for s in search.keys():
        d = searchAnswers(s)
        # Remove non-domestic results
        ans = d[(d.AnswerID.isin(sub_ids.AnswerID)) & (d.QuestionaireID < 10)]  
        ans = ans.dropna(axis=1, how='all')
        # Set feature frame column names
        if len(ans.columns[2:])==1:
            ans.columns = ['AnswerID','QuestionaireID'] + [search.get(s)]
        try:    
            result = result.merge(ans, how='outer')
        except Exception:
            raise InputError(searchlist, 'Not contained in any question. Try something else.')
    
    try:    
        if geo is None:
            result = result.merge(sub_ids[['AnswerID','ProfileID','Unit of measurement',
                                           'Survey','Year','LocName']], how='left')
        else:
            result = result.merge(sub_ids[['AnswerID', 'ProfileID','Unit of measurement',
                                           'Survey','Year', geo,'LocName']], how='left')
        return result
    except:
        raise InputError(year, 'No survey data collected for this year.')


def generateSociosSetSingle(year, spec_file):
    """Filters and transforms survey responses for a single year.

    The survey features are filtered and transformed based on requirements 
    specified in a spec_files. The formatting of the spec_file must be 
    exactly like the templates in `delprocess/data/specs/`. 
    Consult the `README` file for more information.

    NB: The function adjusts monthly income values for inflation, 
    baselined to Stats SA December 2016.
    
    Parameters:
        year (int): 1994 <= year <= 2014
        spec_file (str): Name of feature specification file.
            spec_file naming convention: root_94.txt or root_00.txt - only change root
        set_id (str): Selects column to set as index. Either 'ProfileID' or 'AnswerID'. 
    
    Returns:
        pandas dataframe with columns [
            'AnswerID','QuestionaireID', 'features' specified in spec_file,
            'ProfileID','Unit of measurement','Survey','Year', geo, 'LocName']
    """
    # Get feature specficiations
    files = glob(os.path.join(usr_dir, 'specs', spec_file + '*.txt'))

    for file_path in files:
        try:
            with open(file_path, 'r') as f:
                featurespec = json.load(f)        
            year_range = featurespec['year_range']
        except:
            raise InputError(year, 'Problem reading the spec file.')
            
        if year >= int(year_range[0]) and year <= int(year_range[1]):
            # Check if year input is valid            
            validYears(year) 
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
    
    # Get data and questions from socio-demographic survey responses
    data = extractSocios(searchlist, year, col_names=searchlist, geo=geo)
    # Add missing columns dropped during feature extraction
    missing_cols = list(set(searchlist) - set(data.columns))
    data = data.append(pd.DataFrame(columns=missing_cols), sort=True)
    data['AnswerID'] = data.AnswerID.astype(int)
    data['ProfileID'] = data.ProfileID.astype(int)

    for k, v in transform.items():
        data[k] = data.apply(lambda x: eval(v), axis=1)
        
    data.drop(columns = searchlist, inplace=True, axis=1)
            
    # Adjust monthly income for inflation: baselined to 
    # Stats SA December 2016 values. Important that this happens here, 
    # after columns have been renamed and before income data is binned.
    if 'monthly_income' in features:
        cpi_percentage=(0.265,0.288,0.309,0.336,0.359,0.377,0.398,0.42,
                        0.459,0.485,0.492,0.509,0.532,0.57,0.636,0.678,
                        0.707,0.742, 0.784, 0.829,0.88,0.92,0.979,1.03)
        cpi = dict(zip(list(range(1994,2015)),cpi_percentage))
        data['monthly_income'] = np.round(data['monthly_income']/cpi[year], 2)
    
    # Bin features    
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

    return data
                 

def generateSociosSetMulti(spec_files, year_start=1994, year_end=2014):
    """Filters and transforms survey responses for a year range.
    
    The function iterates through all the spec_files, appending the features for 
    all years before merging the features for all spec_files.
    
    Parameters:
        spec_files (list): List of names of feature specification files.
            spec_file naming convention: root_94.txt or root_00.txt - only change root
        year_start (int): 1994 <= year_start <= 2014. Defaults to 1994.
        year_end (int): year_start <= year_end <= 2014. Defaults to 2014.
    
    Returns:
        pandas dataframe with columns labelled according to 'features' specified in spec_files    
    """
    if isinstance(spec_files, list):
        pass
    else:
        spec_files = [spec_files]
    
    ff = pd.DataFrame(columns=['AnswerID','ProfileID','Unit of measurement',
                              'Survey','QuestionaireID','Year','LocName'])    
    for spec in spec_files:
        gg = pd.DataFrame()
        for year in range(year_start, year_end+1):
            try:
                gg = gg.append(generateSociosSetSingle(year, spec), sort=Trues)
            except Exception:
                ## TODO this should be logged
                print('Could not extract features for '+str(year)+' with spec '+spec)
            pass
        ff = ff.merge(gg, on=['AnswerID','ProfileID','Unit of measurement',
                              'Survey','QuestionaireID','Year','LocName'], sort=True, how='outer')
        # Clear memory
        del gg 

    # Some data wrangling to sort out data types
    for c in ff.columns:
        if ff[c].dtype == np.float64:
            # Check for columns that should be integers
            if sum(ff[c]%1) == 0.0: 
                ff[c] = ff[c].astype(int)
                #TODO also check for nan
    # Problem with duplicated profile_id 8396, answer id 2000458 - remove one            
    ff = ff[~ff.ProfileID.duplicated(keep='first')]
    cols = ff.columns.tolist()
    cols.insert(0, cols.pop(cols.index('ProfileID')))
    cols.insert(1, cols.pop(cols.index('AnswerID')))
    cols.insert(2, cols.pop(cols.index('Unit of measurement')))    
    cols.insert(3, cols.pop(cols.index('Survey')))
    cols.insert(4, cols.pop(cols.index('QuestionaireID')))
    cols.insert(5, cols.pop(cols.index('Year')))
    cols.insert(6, cols.pop(cols.index('LocName')))    

    ff = ff.reindex(columns = cols)
    ff.sort_values(by=['Year','ProfileID'], inplace=True)
    
    return ff


def genS(spec_files, year_start, year_end):
    """Saves survey responses selected and transformed as noted in spec_files.

    The function first checks if a feature file for the specified parameters
    exists. If not, it creates it with generateSociosSetMulti().
    
    Parameters:
        spec_files (list): List of names of feature specification files.
            spec_file naming convention: root_94.txt or root_00.txt - only change root
        year_start (int): 1994 <= year_start <= 2014. Defaults to 1994.
        year_end (int): year_start <= year_end <= 2014. Defaults to 2014.
    
    Returns:
        pandas dataframe with columns labelled according to 'features' specified in spec_files    
    """
    if isinstance(spec_files, list):
        pass
    else:
        spec_files = [spec_files]
        
    # Save data to disk
    root_name = '_'.join(spec_files)
    file_name =  root_name+'_'+str(year_start)+'+'+str(year_end-year_start)+'.csv'
    dir_path = os.path.join(fdata_dir, root_name)
    os.makedirs(dir_path , exist_ok=True)
    file_path = os.path.join(dir_path, file_name)
     
    try:
        features = pd.read_csv(file_path)
        print('Success! File already exists.')
    except:
        # Generate feature data
        features = generateSociosSetMulti(spec_files, year_start, year_end)
        features.to_csv(file_path, index=False)
        print('Success! Saved to data/feature_data/'+root_name+'/'+file_name)

    features.sort_index(inplace=True)
           
    return features


def recorderLocations(year):
    """Returns all survey locations and recorder abbreviations for a given year. 
    
    Parameters:
        year (int): 2009 <= year <= 2014
    
    Returns:
        pandas dataframe: recorder locations for year
    """
    if year > 2009:
        groups = loadTable('groups')
        recorderids = loadTable('recorderinstall')
        reclocs = groups.merge(recorderids, left_on='GroupID', right_on='GROUP_ID')
        reclocs['recorder_abrv'] = reclocs['RECORDER_ID'].apply(lambda x:x[:3])
        yearlocs = reclocs.loc[reclocs['Year']== year,['GroupID','LocName','recorder_abrv']].drop_duplicates()
        locations = yearlocs.sort_values('LocName').reset_index(drop=True)
        
        return locations 
    
    else:
        print('Recorder locations can only be returned for years after 2009.')