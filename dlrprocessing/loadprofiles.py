#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Monday 3 December 2018

This module....

@author: Wiebke Toussaint
"""

import pandas as pd
import numpy as np
import feather
from pathlib import Path
from glob import glob
import os

from .surveys import loadID, loadTable
from .support import rawprofiles_dir, InputError, profiles_dir, validYears

def loadRawProfiles(year, unit):
    """
    This function uses a rolling window to reduce all raw load profiles to hourly mean values. Monthly load profiles are then concatenated into annual profiles and returned as a dictionary object.
    The data is structured as dict[unit:{year:[list_of_profile_ts]}]
    
    """
    validYears(year) #check if year input is valid
    p = os.path.join(rawprofiles_dir, str(year))
    
    #initialise empty dataframe to concatenate annual timeseries
    ts = pd.DataFrame()
    #iterate through all data files to combine 5min monthly into hourly reduced annual timeseries
    for child in next(os.walk(p))[1]:  
        try:
            childpath = glob(os.path.join(p, child, '*_' + unit + '.feather'))[0]
            data = feather.read_dataframe(childpath)
            ts = ts.append(data)
            print(child, unit)
        except:
            print('Could not add data for ' + str(child) + ' ' + unit) #skip if feather file does not exist
    ts.reset_index(inplace=True, drop=True)
    ts.Datefield = np.round(ts.Datefield.astype(np.int64), -9).astype('datetime64[ns]')
    ts['Valid'] = ts['Valid'].map(lambda x: x.strip()).map({'Y':1, 'N':0})
    ts['Valid'].fillna(0, inplace=True)
    ts['ProfileID'] = ts['ProfileID'].astype(int)
        
    return ts
    
def reduceRawProfiles(year, unit, interval):
    """
    This function uses a rolling window to reduce all raw load profiles to hourly mean values. Monthly load profiles are then concatenated into annual profiles and returned as a dictionary object.
    The data is structured as dict[unit:{year:[list_of_profile_ts]}]
    
    """
    ts = loadRawProfiles(year, unit)
    
    if unit in ['A','V','Hz','kVA','kW']:
        aggts = ts.groupby(['RecorderID', 'ProfileID']).resample(interval, on='Datefield').mean()
    else:
        print("Unit must be one of 'A', 'V', 'kVA', 'Hz', 'kW'")
    aggts = aggts.loc[:, ['Unitsread', 'Valid']]
    aggts.reset_index(inplace=True)
    aggts.drop_duplicates(inplace=True)
    aggts.loc[(aggts.Valid!=1)&(aggts.Valid>0), 'Valid'] = 0
       
    return aggts

def saveReducedProfiles(yearstart, yearend, interval, filetype='feather'):
    """
    This function iterates through profile units, reduces all profiles with 
    reduceRawProfiles() and saves the result as a feather object in a directory tree.
    
    """ 
    for year in range(yearstart, yearend + 1):   
        for unit in ['A', 'V', 'kVA', 'Hz', 'kW']:
            
            #TODO: Check if reduced profiles exist
            
            #create empty directory to save files   
            dir_path = os.path.join(profiles_dir, interval, unit)
            os.makedirs(dir_path, exist_ok=True)
            
            ts = reduceRawProfiles(year, unit, interval)
            wpath = os.path.join(dir_path, str(year) + '_' + unit + '.'+filetype)

            #write to reduced data to file            
            try:
                if filetype=='feather':
                    feather.write_dataframe(ts, wpath)
                elif filetype=='csv':
                    ts.to_csv(wpath)
                print('Write success')
            except Exception as e:
                print(e)
                pass
                #TODO: print error to log
    
				#logline = [yearstart, yearend, interval]
				#log_lines = pd.DataFrame([logline], columns = ['from_year','to_year', 'resample_interval'])
				#writeLog(log_lines,'log_reduce_profiles')

    return

def loadProfiles(year, unit, dir_name, filetype):
    """
    This function loads a year's unit profiles from the dir_name in profiles 
    directory into a dataframe and returns it together with the year and unit concerned.
    
    """
    validYears(year) #check if year input is valid
    
    file_path = os.path.join(profiles_dir, dir_name, unit,
                             str(year)+'_'+unit+'.'+filetype)
    
    #load data
    try:
        data = feather.read_dataframe()
    except:
        data = pd.read_csv(file_path)

    data.drop_duplicates(inplace=True)
    
    return data
      

#investigating one location
def aggTs(year, unit, interval, mean=True, dir_name='H'):
    """
    This function 
        1. resamples each profile over interval 
        2. gets interval mean (if True)
    aggfunc must be a list and can be any standard statistical descriptor such as mean, std, describe, etc.
    interval can be 'D' for calendar day frequency, 'M' for month end frequency or 'A' for annual frequency. See http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases for more.
    
    The aggregate function for kW and kVA is sum().
    The aggregate function for A, V and Hz is mean().
    """
    #load data
    try:
        data = loadProfiles(year, unit, dir_name)
        data['ProfileID'] = data['ProfileID'].astype('category')
        data.set_index('Datefield', inplace=True)
        data.loc[data['Valid']!=1,'Unitsread'] = np.nan #Ensure that only valid data is used in aggregation
        
    except:
        raise InputError(unit, "Invalid unit")      
        
    #specify aggregation function for different units    
    if unit in ['kW','kVA']:
        aggregated = data.groupby('ProfileID').resample(interval).agg({
                'Unitsread':'sum',
                'Valid':'sum',
                'RecorderID':'count'})
    elif unit in ['A', 'V', 'Hz']:
        aggregated = data.groupby('ProfileID').resample(interval).agg({
                'Unitsread':'mean',
                'Valid':'sum',
                'RecorderID':'count'})

    if mean is True:
        aggregated['vu'] = aggregated.Unitsread*aggregated.Valid
        tf = aggregated.groupby('ProfileID').sum()
        tf['AnnualMean_'+interval+'_'+unit] = tf.vu/tf.Valid
        tf['ValidHoursOfTotal'] = tf.Valid/tf.RecorderID
        tf = tf[['AnnualMean_'+interval+'_'+unit, 'ValidHoursOfTotal']]
    else:
        tf = aggregated
        tf.columns = ['Mean_'+interval+'_'+unit, 'ValidHours', 'TotalHours']

    tf.reset_index(inplace=True)

    ids = loadID()
    result = tf.merge(ids, on='ProfileID', how='left')    
    result = result[list(tf.columns)+['AnswerID']]
    
#    validhours = aggregated['Datefield'].apply(lambda x: (x - pd.date_range(end=x, periods=2, freq = interval)[0]) / np.timedelta64(1, 'h'))
#    aggregated['Valid'] = aggregated['Valid']/validhours
    
    return result

def getProfilePower(year, dir_name='H'):
    """
    This function retrieves and computes kW and kVA readings for all profiles in a year.
    """
    #get list of ProfileIDs in variable year
    p_id = loadID()['ProfileID']
    
    #get profile metadata (recorder ID, recording channel, recorder type, units of measurement)
    profiles = loadTable('profiles')
        
    #get profile data for year
    iprofile = loadProfiles(year, 'A', dir_name)[0]    
    vprofile = loadProfiles(year, 'V', dir_name)[0]
    
    if year <= 2009: #pre-2009 recorder type is set up so that up to 12 current profiles share one voltage profile
        #get list of ProfileIDs in variable year
        year_profiles = profiles[profiles.ProfileId.isin(p_id)]        
        vchan = year_profiles.loc[year_profiles['Unit of measurement']==1, ['ProfileId','RecorderID']] #get metadata for voltage profiles

        iprofile = iprofile.merge(vchan, on='RecorderID', suffixes=('_i','_v'))
        iprofile.rename(columns={"ProfileId": "matchcol"}, inplace=True)        
        power = iprofile.merge(vprofile, left_on=['matchcol', 'Datefield'], right_on=['ProfileID','Datefield'], suffixes=['_i', '_v'])
        power.drop(['RecorderID_i', 'matchcol'], axis=1, inplace=True)
        power.rename(columns={'RecorderID_v':'RecorderID'}, inplace=True)

    elif 2009 < year <= 2014: #recorder type is set up so that each current profile has its own voltage profile
        vprofile['matchcol'] = vprofile['ProfileID'] + 1
        power_temp = vprofile.merge(iprofile, left_on=['matchcol', 'Datefield'], right_on=['ProfileID','Datefield'], suffixes=['_v', '_i'])
        power_temp.drop(['RecorderID_v','RecorderID_i', 'matchcol'], axis=1, inplace=True)

        kwprofile = loadProfiles(year, 'kW', dir_name)[0] #get kW readings
        kwprofile['matchcol'] = kwprofile['ProfileID'] - 3 #UoM = 5, ChannelNo = 5, 9, 13

        kvaprofile = loadProfiles(year, 'kVA', dir_name)[0] #get kVA readings
        kvaprofile['matchcol'] = kvaprofile['ProfileID'] - 2 #UoM = 4, ChannelNo = 4, 8 or 12        
        kvaprofile.drop(columns='RecorderID',inplace=True)
        
        power_temp2 = power_temp.merge(kwprofile, right_on=['matchcol', 'Datefield'], left_on=['ProfileID_v','Datefield'])
        power = power_temp2.merge(kvaprofile, right_on=['matchcol', 'Datefield'], left_on=['matchcol','Datefield'], suffixes=['_kw','_kva'])
        
        power.drop(['matchcol'], axis=1, inplace=True)
        
    else:
        return print('Year is out of range. Please select a year between 1994 and 2014')
    
    power['kw_calculated'] = power.Unitsread_v*power.Unitsread_i*0.001
    power['valid_calculated'] = power.Valid_i * power.Valid_v

    return power

def aggProfilePower(profilepowerdata, interval):
    """
    This function returns the aggregated mean or total load profile for all ProfileID_i (current) for a year.
    Interval should be 'D' for calendar day frequency, 'M' for month end frequency or 'A' for annual frequency. Other interval options are described here: http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases
    
    The aggregate function for kW and kW_calculated is sum().
    The aggregate function for A, V is mean().
    """

    data = profilepowerdata.set_index('Datefield')
    
    try:
        aggprofile = data.groupby(['RecorderID','ProfileID_i']).resample(interval).agg({
                'Unitsread_i': np.mean, 
                'Unitsread_v': np.mean, 
                'Unitsread_kw': np.sum,
                'Unitsread_kva': np.mean,
                'kw_calculated': np.sum, 
                'valid_calculated': np.sum})
    
    except:
        aggprofile = data.groupby(['RecorderID','ProfileID_i']).resample(interval).agg({
                'Unitsread_i': np.mean, 
                'Unitsread_v': np.mean, 
                'kw_calculated': np.sum,  
                'valid_calculated': np.sum})
        
    aggprofile.reset_index(inplace=True)    
    
    aggprofile['interval_hours'] = aggprofile['Datefield'].apply(lambda x: (x - pd.date_range(end=x, periods=2, freq = interval)[0]) / np.timedelta64(1, 'h'))
    aggprofile['valid_obs_ratio'] = aggprofile['valid_calculated']/aggprofile['interval_hours']
    
    aggprofile['interval'] = interval

    return aggprofile

def annualIntervalDemand(aggprofilepowerdata):
    """
    This function computes the mean annual power consumption for the interval aggregated in aggprofilepowerdata.
    """
    
    interval = aggprofilepowerdata.interval[0]
    
    try:
        aggdemand = aggprofilepowerdata.groupby(['RecorderID','ProfileID_i']).agg({
                'Unitsread_kw': ['mean', 'std'], 
                'Unitsread_kva': ['mean', 'std'],
                'valid_calculated':'sum',
                'interval_hours':'sum'})
        aggdemand.columns = ['_'.join(col).strip() for col in aggdemand.columns.values]
        aggdemand.rename(columns={
                'Unitsread_kw_mean':interval+'_kw_mean', 
                'Unitsread_kw_std':interval+'_kw_std', 
                'Unitsread_kva_mean':interval+'_kva_mean', 
                'Unitsread_kva_std':interval+'_kva_std', 
                'valid_calculated_sum':'valid_hours'}, inplace=True)

    except:
        aggdemand = aggprofilepowerdata.groupby(['RecorderID','ProfileID_i']).agg({
                'kw_calculated': ['mean', 'std'], 
                'valid_calculated':'sum',
                'interval_hours':'sum'})        
        aggdemand.columns = ['_'.join(col).strip() for col in aggdemand.columns.values]
        aggdemand.rename(columns={
                'kw_calculated_mean':interval+'_kw_mean', 
                'kw_calculated_std':interval+'_kw_std', 
                'valid_calculated_sum':'valid_hours'}, inplace=True) 
        
    aggdemand['valid_obs_ratio'] = aggdemand['valid_hours']/aggdemand['interval_hours_sum']    
    aggdemand['interval'] = interval
    
    return aggdemand.reset_index()

def aggDaytypeDemand(profilepowerdata):   
    """
    This function generates an hourly load profile for each ProfileID_i.
    The model contains aggregate hourly kW readings for the parameters:
        Month
        Daytype [Weekday, Sunday, Monday]
        Hour
    """
    
    data = profilepowerdata
    data['month'] = data['Datefield'].dt.month
    data['dayix'] = data['Datefield'].dt.dayofweek
    data['hour'] = data['Datefield'].dt.hour
    cats = pd.cut(data.dayix, bins = [0, 5, 6, 7], right=False, labels= ['Weekday','Saturday','Sunday'], include_lowest=True)
    data['daytype'] = cats
    data['total_hours'] = 1    
    
    try:
        daytypedemand = data.groupby(['ProfileID_i', 'month', 'daytype', 'hour']).agg({
                'Unitsread_kw': ['mean', 'std'], 
                'Unitsread_kva': ['mean', 'std'],
                'valid_calculated':'sum', 
                'total_hours':'sum'})
        daytypedemand.columns = ['_'.join(col).strip() for col in daytypedemand.columns.values]
        daytypedemand.rename(columns={
                'Unitsread_kw_mean':'kw_mean', 
                'Unitsread_kw_std':'kw_std', 
                'Unitsread_kva_mean':'kva_mean', 
                'Unitsread_kva_std':'kva_std', 
                'valid_calculated_sum':'valid_hours'}, inplace=True)

    except: #for years < 2009 where only V and I were observed
        daytypedemand = data.groupby(['ProfileID_i', 'month', 'daytype', 'hour']).agg({
                'kw_calculated': ['mean', 'std'],
                'valid_calculated':'sum', 
                'total_hours':'sum'})
        daytypedemand.columns = ['_'.join(col).strip() for col in daytypedemand.columns.values]
        daytypedemand.rename(columns={
                'kw_calculated_mean':'kw_mean', 
                'kw_calculated_std':'kw_std', 
                'valid_calculated_sum':'valid_hours'}, inplace=True)

    daytypedemand['valid_obs_ratio'] = daytypedemand['valid_hours'] / daytypedemand['total_hours_sum']
        
    return daytypedemand.reset_index()

def generateAggProfiles(year, interval='M'):
    """
    This function generates the aggregate input data required for building the experimental model
    """
    
    #generate folder structure and file names
    feather_path= {}
    csv_path= {}
    for i in ['pp', 'aggpp_' + interval, 'a' + interval + 'd', 'adtd']: 
        ipath = os.path.join(profiles_dir, 'aggProfiles', i)
        feather_path[i] = os.path.join(ipath, 'feather', i + '_' + str(year) + '.feather')
        csv_path[i] = os.path.join(ipath, 'csv', i + '_' + str(year) + '.csv')
        os.makedirs(os.path.join(ipath, 'feather'), exist_ok=True)
        os.makedirs(os.path.join(ipath, 'csv'), exist_ok=True)

    try:        
        pp = getProfilePower(year)
        feather.write_dataframe(pp, feather_path['pp'])
        pp.to_csv(csv_path['pp'], index=False)
        print(str(year) + ': successfully saved profile power file')
        
        aggpp = aggProfilePower(pp, interval)
        feather.write_dataframe(aggpp, feather_path['aggpp_' + interval])
        aggpp.to_csv(csv_path['aggpp_' + interval], index=False)
        print(str(year) + ': successfully saved aggregate ' + interval + ' profile power file')
        
        aid = annualIntervalDemand(aggpp)
        feather.write_dataframe(aid, feather_path['a' + interval + 'd'])
        aid.to_csv(csv_path['a' + interval + 'd'], index=False)
        print(str(year) + ': successfully saved aggregate ' + interval + ' demand file')
        
        adtd = aggDaytypeDemand(pp)
        feather.write_dataframe(adtd, feather_path['adtd'])
        adtd.to_csv(csv_path['adtd'], index=False)
        print(str(year) + ': successfully saved average daytype demand file')
        
    except Exception as e:
        print(e)
        raise

def readAggProfiles(year, aggfunc = 'adtd'):
    """
    This function fetches aggregate load profile data from disk. aggfunc can be one of pp, aggpp_M, aMd, adtd
    """
    validYears(year) 
    try:       
        path = Path(os.path.join(profiles_dir, 'aggProfiles', aggfunc, 'feather'))
        for child in path.iterdir():
            n = child.name
            nu = n.split('.')[0].split('_')[-1]
            if int(nu)==year:
                df = feather.read_dataframe(str(child))
                return df
            else:
                pass        
    except FileNotFoundError:
        print('The input files did not exist or were incomplete.')        

def season(month):
    if month in [5,6,7,8]:
        season = 'high'
    else:
        season = 'low'
    return season

def generateSeasonADTD(year):

    #generate folder structure and file names    
    path = os.path.join(profiles_dir, 'aggProfiles', 'adtd_season')
    feather_path = os.path.join(path, 'feather', 'adtd_season' + '_' + str(year) + '.feather')
    csv_path = os.path.join(path, 'csv', 'adtd_season' + '_' + str(year) + '.csv')
    os.makedirs(os.path.join(path, 'feather'), exist_ok=True)
    os.makedirs(os.path.join(path, 'csv'), exist_ok=True)
    
    #read data
    df = readAggProfiles(year, 'adtd')
    df['season'] = df['month'].map(lambda x: season(x)).astype('category')
    
    seasons = df.groupby(['ProfileID_i', 'season', 'daytype', 'hour']).agg({
                'kw_mean': 'mean', 
                'kw_std': 'mean',
                'valid_hours':'sum',
                'valid_obs_ratio':'mean',
                'total_hours_sum':'sum'}).reset_index()  
    
    #write data to file
    feather.write_dataframe(seasons, feather_path)
    seasons.to_csv(csv_path, index=False)
    print(str(year) + ': successfully saved seasonal average daytype demand file')    
    
    return 

def dailyProfiles(year, unit, directory):
    
    data = loadProfiles(year, unit, directory)
    data.drop(labels=['RecorderID'],axis=1,inplace=True)
    data.loc[data['Valid']!=1,'Unitsread'] = np.nan #VERY NB to use != 1 and NOT ==0: Valid is a mean value of 12 5min readings averaged over an hour. A single incorrect 5min reading can cause havoc. 
    data['date'] = data.Datefield.dt.date
    data['hour'] = data.Datefield.dt.hour
    df = data['Unitsread'].groupby([data.ProfileID, data.date, data.hour], sort=True).mean().unstack()
    df.columns.name = 'hour'
    
    return df

def resampleProfiles(dailyprofiles, interval, aggfunc = 'mean'):
    if interval is None:
        return dailyprofiles
    else:
        df = dailyprofiles.reset_index()
        df['date'] = pd.to_datetime(df.date)
        df.set_index('date', inplace=True)
        output = df.groupby('ProfileID').resample(interval).agg(aggfunc).drop(labels=['ProfileID'],axis=1)
        return output


def genX(year_range, drop_0=False, **kwargs):

    if 'interval' in kwargs: interval = kwargs['interval']
    else: interval = None
        
    if 'aggfunc' in kwargs: aggfunc = kwargs['aggfunc']
    else: aggfunc = 'mean'
        
    if 'unit' in kwargs: unit = kwargs['unit']
    else: unit = 'A'
        
    if 'directory' in kwargs: directory = kwargs['directory']
    else: directory = 'H'
    
    #TODO: make work with .csv files
    xpath = os.path.join(profiles_dir, 'X', str(year_range[0])+'_'+str(year_range[1])+
                         aggfunc+unit+directory+'.feather')
    
    try:
        X = feather.read_dataframe(xpath)
        
    except:
        X = pd.DataFrame()
        
        for y in range(year_range[0], year_range[1]+1):
                                        
            data = resampleProfiles(dailyProfiles(y, unit, directory), interval, aggfunc)
            Xbatch = data.dropna() #remove missing values
            Xbatch.reset_index(inplace=True)
            X = X.append(Xbatch)
        
        X.reset_index(drop=True, inplace=True)
        X['date'] = pd.to_datetime(X['date'])
        feather.write_dataframe(X, xpath)
        
        if aggfunc != 'sum':
            minx = X.iloc[:,2::].min()
            maxx = X.iloc[:,2::].max()
                
            if len(minx[minx<0]) != 0: return print('Input dataset contains outliers and invalid data. Aborting....')
            if len(maxx[maxx>1000]) != 0: return print('Input dataset may contain outliers and invalid data. Aborting....')
      
    X.set_index(['ProfileID','date'], inplace=True)
    
    #Clean and shape X by requirements
    if drop_0 == True:
        print('dropping all zero rows')
        X = X[~(X.sum(axis=1)==0)]
        
    return X
