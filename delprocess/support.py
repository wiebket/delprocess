#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the delprocess package.

Updated: 4 May 2019
"""

import os
from pathlib import Path
import datetime as dt
import pandas as pd

import shapefile as shp
from shapely.geometry import Point
from shapely.geometry import shape

home_dir = str(Path.home())
usr_dir = os.path.join(home_dir, 'del_data','usr')

def getDataDir():
    """
    This function checks if a valid data directory has been specified in
    USER_HOME/del_data/usr/store_path.txt.
    """
    
    filepath = []
    #read directory paths from store_path.txt file
    with open(os.path.join(usr_dir,'store_path.txt')) as f:
        for line in f:
            try:
                filepath.append(line.strip())
            except:
                pass
    mydir = filepath[0]
    validdir = os.path.isdir(mydir)
    #check if directory paths exist (does not validate directory structure)
    if validdir is False:
        #print('Your stored {} data path is invalid.'.format(k))
        raise 
    else:
        print('Your data path is \n{}'.format(mydir))
    
    return mydir

def specifyDataDir():
    """
    This function searches for the profiles and tables data directories.

    The data in the default location must be store as follows:
    |-- your_data_dir (default: USER_HOME/del_data)
        |-- observations
            |-- profiles
            |-- tables
            
    If 'raw' data has been exported from the server or retrieved with delretrieve, 
    the default directory structure is correct.
    __________________
    PROFILES DIRECTORY
    ******************
    The data in the profiles directory is from the Profiletable in the database.
    These are metered electricity readings in A, V, kWh, kVA or Hz. The 5min readings
    must be stored in a subdirectory 'raw'. Aggregations must be stored in a subdirectory named
    by the aggregation interval (eg H = hourly, 30T = 30min). See example below:
        
    |-- profiles  
        |-- raw
            |-- unit (eg A)
                |-- group_year (eg 2013)
                    |-- year-month_unit.feather (eg 2013-5_A.feather)
        |-- H
            |-- unit (eg A)
                |-- year_unit.feather (eg 2013_A.feather)
        |-- 30T
            |-- unit (eg A)
                |-- year_unit.feather (eg 2013_A.feather)
    ________________
    TABLES DIRECTORY
    ****************
    The data in the tables directory are database tables. Do not rename them after 
    download. The directory structure must correspond to:
    |-- tables
        |-- ... (eg links.csv)       
    """

    temp_obs_dir = os.path.join(home_dir,'del_data', 'observations') #default directory for observational data
    
    try:
        mydir = getDataDir()
    
    except:
        print('Data path not set or invalid directory.')       
        while True:
            mydir = input('The default path for storing data is \n{}\n Hit enter to keep the default or paste a new path to change it.\n'.format(temp_obs_dir))
            validdir = os.path.isdir(mydir)
            
            if validdir is False:
                print('\nThe directory does not exit. Creating it now ...')
                if len(mydir) == 0:
                    mydir = temp_obs_dir
                os.makedirs(mydir, exist_ok=True)
                    
            print('The data path has been set to \n{}\n'.format(mydir))
            break
        
        #write data dir to file   
        f = open(os.path.join(usr_dir,'store_path.txt'),'w')
        f.write(mydir)
        f.close()
        
    print('You can change it in USER_HOME/del_data/usr/store_path.txt')
    
    profiles_dir = os.path.join(mydir, 'profiles')
    table_dir = os.path.join(mydir, 'tables')
    rawprofiles_dir = os.path.join(profiles_dir, 'raw')
    
    return mydir, profiles_dir, table_dir, rawprofiles_dir

#Create data structure
obs_dir, profiles_dir, table_dir, rawprofiles_dir = specifyDataDir()
fdata_dir = os.path.join(os.path.dirname(obs_dir), 'survey_features')
pdata_dir = os.path.join(os.path.dirname(obs_dir), 'resampled_profiles')

class InputError(ValueError):
    """
    Exception raised for errors in the input.

    *input*
    -------
    expression: input expression in which the error occurred
    message (str): explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
        
def validYears(*args):
    """
    This function checks if study was conducted during years specfied. 
    
    *input*
    -------
    *args (int)
    
    Valid arguments are years between 1994 and 2014.
    """
    
    for year in args:
        if year >= 1994 and year <= 2014:
            pass
        else:
            raise InputError([year], 'Year is out of range. Please select a year between 1994 and 2014')           
    return
       
def writeLog(log_line, file_name):    
    """
    This function adds a timestamp to a log line and writes it to a log file. 
    
    *input*
    -------
    log_line (dataframe)
    file_name (str): directory appended to USER_HOME/del_data/usr/ in which logs will be saved.
    """
    
    #Create log_dir and file to log path
    log_dir = os.path.join(usr_dir, 'logs')
    os.makedirs(log_dir , exist_ok=True)
    log_path = os.path.join(log_dir, file_name+'.csv')
    
    #Add timestamp
    log_line.insert(0, 'timestamp', dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    #Write log data to file
    if os.path.isfile(log_path):
        log_line.to_csv(log_path, mode='a', header=False, columns = log_line.columns, index=False)
        print('Log entries added to log/' + file_name + '.csv\n')
    else:
        log_line.to_csv(log_path, mode='w', columns = log_line.columns, index=False)
        print('Log file created and log entries added to log/' + file_name + '.csv\n')    
    return log_line

def geoMeta():
    """
    This function generates geographic metadata for groups by combining GroupID 
    Lat, Long co-ords with a municipal boundaries shapefile.
    
    *requirements*
    ------- 
    file: package_dir/delprocess/data/geometa/2016_Boundaries_Local/DLR Site coordinates.csv
    file: package_dir/delprocess/data/geometa/2016_Boundaries_Local/Local_Municipalities_2016.shp
    file: package_dir/delprocess/data/geometa/2016_Boundaries_Local/Local_Municipalities_2016.dbf
    file: package_dir/delprocess/data/geometa/2016_Boundaries_Local/Local_Municipalities_2016.shx
    
    These files are correctly installed if the package is cloned from github and set up as 
    described in the README file. 
    """
    # SHP, DBF and SHX files from http://energydata.uct.ac.za/dataset/2016-municipal-boundaries-south-africa
    this_dir = os.path.dirname(__file__)
    munic2016 = os.path.join(this_dir, 'data', 'geometa', '2016_Boundaries_Local',
                             'Local_Municipalities_2016') 
    site_ref = pd.read_csv(os.path.join(this_dir, 'data', 'geometa', 
                                        'DLR Site coordinates.csv'))
    
    sf = shp.Reader(munic2016)
    all_shapes = sf.shapes() # get all the polygons
    all_records = sf.records()
    
    g = list()
    
    for i in range(0, len(site_ref)):
        for j in range(0, len(all_shapes)):
            boundary = all_shapes[j]
            if Point(tuple([site_ref.loc[i,'Long'],site_ref.loc[i,'Lat']])).within(shape(boundary)):
                g.append([all_records[j][k] for k in (1, 5, 9)])
                
    geo_meta = pd.DataFrame(g, columns = ['Province','Municipality','District'])
    geo_meta.loc[geo_meta.Province == 'GT', 'Province'] = 'GP' #fix Gauteng province abbreviation
    
    site_geo = pd.concat([site_ref, geo_meta], axis = 1)
    site_geo = site_geo[['GPSName','Lat','Long','Province','Municipality','District']].drop_duplicates()
    site_geo.to_csv(os.path.join(this_dir,'data', 'geometa', 'site_geo.csv'), index=False)
