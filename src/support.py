# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the src module
"""

import os
from pathlib import Path
import datetime as dt

def specifyDataDir():
    """
    This function searches for the raw profiles and table data directories. 
    If it cannot find the directories in their default locations, it checks 
    src/data/store_path.txt to see if path variables have been specified previously. 
    """
    
    #read line from data/store_path.txt file
    home_dir = Path.home()
    rawprofiles_dir = os.path.join(home_dir,'dlr_data','observations','profiles','raw')
    table_dir = os.path.join(home_dir,'dlr_data','observations','tables')
    
    dirs = {'rawprofiles':rawprofiles_dir, 'tables':table_dir}

    for k, v in dirs.items():
        if os.path.isdir(v):
            mydir = v
            print('Your stored {} path is {} .\n'.format(k, mydir))
        else:
            try:
                filepaths = {}
                with open('src/data/store_path.txt') as f:
                    for line in f:
                        try:
                            k, v = line.split(',')
                            filepaths[k] = v.strip()
                        except:
                            pass

                mydir = filepaths[k]
                validdir = os.path.isdir(mydir)
                if validdir is False:
                    print('Your stored {} data path is invalid.'.format(k))
                    raise 
                else:
                    print('Your stored {} data path is {}.'.format(k, mydir))
            
            except:
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
    f = open('src/data/store_path.txt','w')
    for i in dirs.items():
        f.write(','.join(i)+'\n')
    f.close()
    
    print('\nYou can change your data paths in src/data/store_path.txt')
    
    return dirs['rawprofiles'], dirs['tables']

#Data structure

data_dirs = specifyDataDir()

rawprofiles_dir = data_dirs[0]
table_dir = data_dirs[1]
profiles_dir = os.path.dirname(rawprofiles_dir)
obs_dir = os.path.dirname(profiles_dir)
data_dir = os.path.dirname(obs_dir)
home_dir = os.path.dirname(data_dir)
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


#if __name__ ==" __main__":
    #specifyDataDir()
    #print(rawprofiles_dir, table_dir, fdata_dir)
    