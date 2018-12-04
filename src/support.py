# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the src module
"""

import os
from pathlib import Path
import shelve
import datetime as dt

def specifyDataDir(self):
    """
    This function searches for the raw profiles and table data directories. 
    If it cannot find the directories, it checks data/.store_path to see if path
    variables have been specified previously. 
    """
    
    #read line from data/store_path.txt file
    home_dir = Path.home()
    data_dir = os.path.join(home_dir, 'dlr_data')
    obs_dir = os.path.join(data_dir, 'observations')
    profiles_dir = os.path.join(obs_dir, 'profiles')
    rawprofiles_dir = os.path.join(profiles_dir, 'raw')
    
    if os.path.isdir(rawprofiles_dir):
        self.home = home_dir
        self.data = data_dir
        self.observations = obs_dir
        self.profiles = profiles_dir
        self.rawprofiles = rawprofiles_dir
    else:
        try:
            shelfFile = shelve.open('data/.store_path')
            self.rawprofiles = shelfFile['rawprofiles'] 
            
        except KeyError:
            self.rawprofiles = input('Paste the path to your 5min load profile data. \n')
            shelfFile['rawprofiles'] = self.rawprofiles
            #write rawprofiles dir to file

        self.profiles = os.path.dirname(self.rawprofiles)
        self.observations = os.path.dirname(self.profiles)
        self.data = os.path.dirname(self.observations)
        self.home = os.path.dirname(self.data)
    
    table_dir = os.path.join(obs_dir, 'tables')
    if os.path.isdir(table_dir):
        self.tables = table_dir
    else:
        try:
            
        except:
            table_dir = input('Paste the path to your table data.')
    return

#Data structure
home_dir = specifyDataDir().home
data_dir = specifyDataDir().data
obs_dir = specifyDataDir().observations
table_dir = specifyDataDir().tables
profiles_dir = specifyDataDir().profiles
rawprofiles_dir = specifyDataDir().rawprofiles
fdata_dir = os.path.join(data_dir, 'features')


class InputError(ValueError):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
        
def validYears(*args):
    for year in args:
        if year >= 1994 and year <= 2014:
            pass
        else:
            raise InputError([year], 'Year is out of range. Please select a year between 1994 and 2014')           
    return
       
def writeLog(log_line, file_name):    
    """Adds timestamp column to dataframe, then writes dataframe to csv log file. 
    """
    #Create log_dir and file to log path
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