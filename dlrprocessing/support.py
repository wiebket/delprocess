#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

This package contains support functions for the dlrprocessing module.

"""

import os
from pathlib import Path
import datetime as dt
import pandas as pd

import shapefile as shp
from shapely.geometry import Point
from shapely.geometry import shape

home_dir = Path.home()
usr_dir = os.path.join(home_dir, 'dlr_data','usr')
os.makedirs(usr_dir, exist_ok=True)

def specifyDataDir():
    """
    This function searches for the profiles and tables data directories in
    the following order: 
        1. Default locations in /your_home_directory/dlr_data/observations/...
        2. Path variables specified previously in /your_home_directory/dlr_data/usr/store_path.txt 
        3. User input for profiles and table data dir

    The data in the default location must be store as follows:
    |-- your_data_dir (eg dlr_data)
        |-- observations
            |-- .... (ie profiles, tables)
    __________________
    PROFILES DIRECTORY
    ******************
    The data in the profiles directory is from the Profiletable in the database.
    These are recorded electricity readings in A, V, kWh, kVA or Hz. The 5min readings
    must be stored in a subdirectory 'raw'. Aggregations must be stored in a subdirectory named
    by the aggregation interval (eg H = hourly, 30T = 30min). See example below:
    |-- profiles  
        |-- raw
            |-- group_year (eg 2013)
                |-- year-month (eg 2013-5)
                    |-- year-month_unit.feather (eg 2013-5_A.feather)
        |-- H
            |-- unit (eg A)
                |-- year_unit.feather (eg 2013_A.feather)
        |-- 30T
            |-- unit (eg A)
                |-- year_unit.feather (eg 2013_A.feather)

    Note that the file hierarchy in the 'raw' directory is different to the 
    hierarchy in the aggregate directories. If you export 'raw' from the server 
    or with dlrretrieval, the directory structure is correct.
    ________________
    TABLES DIRECTORY
    ****************
    The data in the tables directory are database tables. Do not rename them after 
    download. Either a csv or a feather subdirectory is requried. The directory 
    structure must correspond to:
    |-- tables
        |-- csv   
            |-- ... (eg links.csv)
        |-- feather
            |-- ... (eg links.feather)            
    """
    
    profiles_dir = os.path.join(home_dir,'dlr_data','observations','profiles')
    table_dir = os.path.join(home_dir,'dlr_data','observations','tables')
    
    dirs = {'profiles':profiles_dir, 'tables':table_dir}

    for k, v in dirs.items():
        #check if dlr_data/.../profiles/ and dlr_data/.../tables/ exists in default locations
        if os.path.isdir(v):
            mydir = v
            print('Your stored {} path is {} .\n'.format(k, mydir))
        else:
            try:
                filepaths = {}
                #read directory paths from store_path.txt file
                with open(os.path.join(usr_dir,'store_path.txt')) as f:
                    for line in f:
                        try:
                            i, j = line.split(',')
                            filepaths[i] = j.strip()
                        except:
                            pass

                mydir = filepaths[k]
                validdir = os.path.isdir(mydir)
                #check if directory paths exist (does not validate directory structure)
                if validdir is False:
                    print('Your stored {} data path is invalid.'.format(k))
                    raise 
                else:
                    print('Your stored {} data path is {}.'.format(k, mydir))
            
            except:
                #request
                while True:
                    mydir = input('Paste the path to your {} data.\n'.format(k))
                    validdir = os.path.isdir(mydir)
                    
                    if validdir is False:
                        print('This is not a directory. Try again.')
                        continue
                    if validdir is True:
                        break
        dirs[k] = mydir
        
    #write rawprofiles dir to file   
    f = open(os.path.join(usr_dir,'store_path.txt'),'w')
    for i in dirs.items():
        f.write(', '.join(i)+'\n')
    f.close()
    
    print('\nYou can change your data paths in /your_home_directory/dlr_data/usr/store_path.txt')
    
    return dirs['profiles'], dirs['tables']

#Data structure
usr_data = specifyDataDir()
profiles_dir = usr_data[0]
table_dir = usr_data[1]
rawprofiles_dir = os.path.join(profiles_dir, 'raw')
obs_dir = os.path.dirname(profiles_dir)
data_dir = os.path.dirname(obs_dir)
fdata_dir = os.path.join(os.path.dirname(usr_dir), 'features')


class InputError(ValueError):
    """
    Exception raised for errors in the input.

    Attributes:
    expression -- input expression in which the error occurred
    message -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
        
def validYears(*args):
    """
    Checks if year range is valid. Valid years are between 1994 and 2014.
    """
    
    for year in args:
        if year >= 1994 and year <= 2014:
            pass
        else:
            raise InputError([year], 'Year is out of range. Please select a year between 1994 and 2014')           
    return
       
def writeLog(log_line, file_name):    
    """
    Adds timestamp column to dataframe, then write dataframe to csv log file. 
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
    Lat, Long co-ords with municipal boundaries dataset.
    """
    # SHP, DBF and SHX files from http://energydata.uct.ac.za/dataset/2016-municipal-boundaries-south-africa
    munic2016 = os.path.join('data', 'geo_meta', '2016-Boundaries-Local',
                             'Local_Municipalities_2016') 
    site_ref = pd.read_csv(os.path.join('data', 'geo_meta', 
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
    geo_meta.loc[geo_meta.Province == 'GT', 'Province'] = 'GP'
    
    site_geo = pd.concat([site_ref, geo_meta], axis = 1)
    site_geo = site_geo[['GPSName','Lat','Long','Province','Municipality','District']].drop_duplicates()
    site_geo.to_csv(os.path.join('data', 'geo_meta', 'site_geo.csv'), index=False)

#if __name__ ==" __main__":
    #specifyDataDir()
    #print(rawprofiles_dir, table_dir, fdata_dir)
    