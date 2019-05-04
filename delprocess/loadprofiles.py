#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Monday 3 December 2018
@author: Wiebke Toussaint

This module contains functions to load and resample DEL Metering data.

Updated: 4 May 2019
"""

import pandas as pd
import numpy as np
import feather
from glob import glob
import os
import gc

from .surveys import loadID, loadTable
from .support import rawprofiles_dir, pdata_dir, InputError, validYears#, writeLog


def loadRawProfiles(year, month, unit):
    """Loads raw load profiles for a year, month and unit.
    
    """
    validYears(year)
    
    if unit in ['A','V','Hz','kVA','kW']:
        pass
    else:
        raise InputError(unit, "Invalid unit")     
    
    filename = str(year)+'-'+str(month)+'_G*'    
    filepath = glob(os.path.join(rawprofiles_dir, unit, str(year), filename))
    ts = pd.DataFrame()
    
    for p in filepath:
        try:
            data = feather.read_dataframe(p)
        except:
            data = pd.read_csv(p, parse_dates=['Datefield'], low_memory=False)         
        ts = ts.append(data)
        del data

    if len(ts)>0:
        ts.reset_index(inplace=True, drop=True)
        ts.Datefield = np.round(ts.Datefield.astype(np.int64), -9).astype('datetime64[ns]')
        ts['Valid'] = ts['Valid'].map(lambda x: x.strip()).map({'Y':1, 'N':0})
        ts['Valid'].fillna(0, inplace=True)
        ts['ProfileID'] = ts['ProfileID'].astype(int)
        
    print('{} {}: data loaded'.format(unit, year))
    
    return ts    
    

def reduceRawProfiles(year, unit, interval):
    """Resamples all raw load profiles for a specific observation 
    unit in a particular year to their mean values over an interval.  
    
    Parameters:
        year (int)  
        unit (str): one of 'A', 'V', 'Hz', 'kVA', 'kW' 
        interval (str): 'H' for hourly, '30T' for 30min
    """
    # Clear any memory garbage
    gc.collect()     
    validYears(year)
    
    if unit in ['A','V','Hz','kVA','kW']:
        pass
    else:
        raise InputError(unit, "Invalid unit")     
        
    p = os.path.join(rawprofiles_dir, unit, str(year))
    
    ts = pd.DataFrame()
    for child in os.listdir(p):
        childpath = os.path.join(p, child)
        try:
            data = feather.read_dataframe(childpath)
        except:
            data = pd.read_csv(childpath, parse_dates=['Datefield'], 
                               low_memory=False)
        if len(data)>0:
            print('Data loaded for {}'.format(child))    
            # Format data
            data.reset_index(inplace=True, drop=True)
            data.Datefield = np.round(data.Datefield.astype(np.int64), -9).astype('datetime64[ns]')
            data['Valid'] = data['Valid'].map(lambda x: x.strip()).map({'Y':1, 'N':0})
            data['Valid'].fillna(0, inplace=True)
            data['ProfileID'] = data['ProfileID'].astype(int)
            # Resample data
            data.sort_values(by=['RecorderID', 'ProfileID','Datefield'], inplace=True)
            data.reset_index(inplace=True)
            aggdata = data.groupby(['RecorderID', 'ProfileID']).resample(
                    interval, on='Datefield').mean()
            del data
            # Resampling creates lots of nan values
            aggdata.dropna(inplace=True)   
            ts = ts.append(aggdata)
            del aggdata        
        else:
            # Skip if file does not exist
            print('FAILED to load data for ' + child)   

    if ts is None:
        return print('No profiles for {} {}'.format(year, unit))
    else:      
        aggts = ts.loc[:, ['Unitsread', 'Valid']]
        aggts.reset_index(inplace=True)
        aggts.drop_duplicates(inplace=True)
        aggts.loc[(aggts.Valid!=1)&(aggts.Valid>0), 'Valid'] = 0
        # Free memory
        del ts 
           
        return aggts


def saveReducedProfiles(year, interval, filetype='feather'):
    """Iterates through profile units, reduces all profiles with 
    reduceRawProfiles() and saves the result as a feather object in a directory tree.
    
    """ 
    for unit in ['A', 'V', 'kVA', 'Hz', 'kW']:
        gc.collect() #clear any memory garbage
        
        dir_path = os.path.join(pdata_dir, interval, unit)
        os.makedirs(dir_path, exist_ok=True)
        
        try:
            ts = reduceRawProfiles(year, unit, interval)
            wpath = os.path.join(dir_path, str(year) + '_' + unit + '.'+filetype)
            #write to reduced data to file            
            try:
                if filetype=='feather':
                    feather.write_dataframe(ts, wpath)
                elif filetype=='csv':
                    ts.to_csv(wpath, index=False)
                print('Write success for', year, unit)
            except Exception as e:
                print(e)
                pass
                	#logline = [yearstart, yearend, interval]
				#log_lines = pd.DataFrame([logline], columns = ['from_year','to_year', 'resample_interval'])
				#writeLog(log_lines,'log_reduce_profiles')
            del ts #clear memory
        except FileNotFoundError as e:
            print(e)
            pass

    return


def loadReducedProfiles(year, unit, interval):
    """Loads a year's unit profiles from the dir_name in profiles 
    directory into a dataframe and returns it together with the year and unit concerned.
    """    
    file_path = None
    while file_path is None:
        try:
            # Load profiles
            file_path = glob(os.path.join(pdata_dir, interval, unit,
                                 str(year)+'_'+unit+'.*'))[-1]
        # Index error indicates file does not exist    
        except IndexError:      
            # Save profiles to disk
            saveReducedProfiles(year, interval)
   
    try:
        data = feather.read_dataframe(file_path)
    except:
        data = pd.read_csv(file_path)               

    data.drop_duplicates(inplace=True)
    
    return data
      

def getProfilePower(year, dir_name='H'):
    """Retrieves and computes kW and kVA readings for all profiles in a year.
    
    Contains important and unintuitive information about how the metered 
    electricity data is stored in the database.
    """
    # Get list of ProfileIDs in variable year
    p_id = loadID()['ProfileID']   
    # Get profile metadata (recorder ID, recording channel, recorder type, units of measurement)
    profiles = loadTable('profiles')
        
    # Get profile data for year
    iprofile = loadReducedProfiles(year, 'A', dir_name)[0]    
    vprofile = loadReducedProfiles(year, 'V', dir_name)[0]
    
    # Pre-2009 recorder type is set up so that up to 12 current profiles share one voltage profile
    if year <= 2009: 
        # Get list of ProfileIDs in variable year
        year_profiles = profiles[profiles.ProfileId.isin(p_id)] 
        # Get metadata for voltage profiles
        vchan = year_profiles.loc[year_profiles['Unit of measurement']==1, ['ProfileId','RecorderID']] 
        iprofile = iprofile.merge(vchan, on='RecorderID', suffixes=('_i','_v'))
        iprofile.rename(columns={"ProfileId": "matchcol"}, inplace=True)        
        power = iprofile.merge(vprofile, left_on=['matchcol', 'Datefield'], 
                               right_on=['ProfileID','Datefield'], suffixes=['_i', '_v'])
        power.drop(['RecorderID_i', 'matchcol'], axis=1, inplace=True)
        power.rename(columns={'RecorderID_v':'RecorderID'}, inplace=True)
    
    # Recorder type is set up so that each current profile has its own voltage profile
    elif 2009 < year <= 2014: 
        vprofile['matchcol'] = vprofile['ProfileID'] + 1
        power_temp = vprofile.merge(iprofile, left_on=['matchcol', 'Datefield'], 
                                    right_on=['ProfileID','Datefield'], suffixes=['_v', '_i'])
        power_temp.drop(['RecorderID_v','RecorderID_i', 'matchcol'], axis=1, inplace=True)
        # Get kW readings
        kwprofile = loadReducedProfiles(year, 'kW', dir_name)[0] 
        kwprofile['matchcol'] = kwprofile['ProfileID'] - 3 #UoM = 5, ChannelNo = 5, 9, 13
        # Get kVA readings
        kvaprofile = loadReducedProfiles(year, 'kVA', dir_name)[0] 
        kvaprofile['matchcol'] = kvaprofile['ProfileID'] - 2 #UoM = 4, ChannelNo = 4, 8 or 12        
        kvaprofile.drop(columns='RecorderID',inplace=True)
        power_temp2 = power_temp.merge(kwprofile, right_on=['matchcol', 'Datefield'],
                                       left_on=['ProfileID_v','Datefield'])
        power = power_temp2.merge(kvaprofile, right_on=['matchcol', 'Datefield'], 
                                  left_on=['matchcol','Datefield'], suffixes=['_kw','_kva'])
        power.drop(['matchcol'], axis=1, inplace=True)        
    else:
        return print('Year is out of range. Please select a year between 1994 and 2014')
    
    power['kw_calculated'] = power.Unitsread_v*power.Unitsread_i*0.001
    power['valid_calculated'] = power.Valid_i * power.Valid_v
    
    return power


def dailyHourlyProfiles(year, unit):
    """Creates a clean dataframe of daily hourly loadprofiles for year and unit.
    """
    data = loadReducedProfiles(year, unit, 'H')
    data.drop(labels=['RecorderID'],axis=1,inplace=True)
    # VERY NB to use != 1 and NOT ==0: 
    # Valid is a mean value of 12 5min readings averaged over an hour. 
    # A single incorrect 5min reading can cause havoc. 
    data.loc[data['Valid']!=1,'Unitsread'] = np.nan 
    data['date'] = data.Datefield.dt.date
    data['hour'] = data.Datefield.dt.hour
    df = data['Unitsread'].groupby([data.ProfileID, data.date, data.hour], sort=True).mean().unstack()
    df.columns.name = 'hour'
    
    return df


def resampleProfiles(dailyprofiles, interval, aggfunc = 'mean'):
    """
    """
    if interval is None:
        return dailyprofiles
    else:
        df = dailyprofiles.reset_index()
        df['date'] = pd.to_datetime(df.date)
        df.set_index('date', inplace=True)
        output = df.groupby('ProfileID').resample(interval).agg(aggfunc).drop(labels=['ProfileID'],axis=1)
        return output


def genX(year_range, drop_0=False, **kwargs):
    """Generates a dataframe of hourly daily profiles. The dataframe is indexed by 
    
    Variables:
        year_range -- [list]
        drop_0 -- boolean
        **kwargs -- interval (options = M, A, None; default = None)
                  aggfunc (default = mean)
                  unit (default = A)
                  filetype (default = feather)
    """
    if 'interval' in kwargs: 
        interval = kwargs['interval'] 
        intstr = interval
    else: 
        interval = None
        intstr = ''
        
    if 'aggfunc' in kwargs: aggfunc = kwargs['aggfunc']
    else: aggfunc = 'mean'
        
    if 'unit' in kwargs: unit = kwargs['unit']
    else: unit = 'A'
        
    if 'filetype' in kwargs: filetype = kwargs['filetype']
    else: filetype = 'feather'
    
    gc.collect()
   
    try:
        # Check if file exists
        xpath = glob(os.path.join(pdata_dir, 'X', str(year_range[0])+'_'+
                                  str(year_range[1])+intstr+aggfunc+unit+'.*'))[-1] 
        X = feather.read_dataframe(xpath)
        
    except IndexError:
        xpath = os.path.join(pdata_dir, 'X', str(year_range[0])+'_'+
                                  str(year_range[1])+intstr+aggfunc+unit+'.'+filetype)
        X = pd.DataFrame()
        
        for y in range(year_range[0], year_range[1]+1):
            data = resampleProfiles(dailyHourlyProfiles(y, unit), interval, aggfunc)
            # Remove missing values
            Xbatch = data.dropna() 
            Xbatch.reset_index(inplace=True)
            X = X.append(Xbatch)
        
        X.reset_index(drop=True, inplace=True)
        X['date'] = pd.to_datetime(X['date'])
        feather.write_dataframe(X, xpath)
        
        if aggfunc != 'sum':
            minx = X.iloc[:,2::].min()
            maxx = X.iloc[:,2::].max()
                
            if len(minx[minx<0]) != 0: return print('Input dataset contains \
                  outliers and invalid data. Aborting....')
            if len(maxx[maxx>1000]) != 0: return print('Input dataset may contain \
                  outliers and invalid data. Aborting....')
      
    X.set_index(['ProfileID','date'], inplace=True)
    
    # Clean and shape X by requirements
    if drop_0 == True:
        print('dropping all zero rows')
        X = X[~(X.sum(axis=1)==0)]
        
    return X
