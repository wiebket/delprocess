#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 4 May 2019
@author: Wiebke Toussaint

This module contains legacy code for aggregating load profiles that may
eventually become useful again. 

Updated: 4 May 2019
"""

import pandas as pd
import numpy as np
import feather
from pathlib import Path
import os

from .surveys import loadID
from .loadprofiles import loadReducedProfiles, getProfilePower
from .support import pdata_dir, validYears, InputError#, writeLog


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
    try:
        data = loadReducedProfiles(year, unit, dir_name)
        data['ProfileID'] = data['ProfileID'].astype('category')
        data.set_index('Datefield', inplace=True)
        # Ensure that only valid data is used in aggregation
        data.loc[data['Valid']!=1,'Unitsread'] = np.nan 
    except:
        raise InputError(unit, "Invalid unit")      
        
    # Specify aggregation function for different units    
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
    
    return result


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
    aggprofile['interval_hours'] = aggprofile['Datefield'].apply(lambda x: (
            x - pd.date_range(end=x, periods=2, freq = interval)[0]) / np.timedelta64(1, 'h'))
    aggprofile['valid_obs_ratio'] = aggprofile['valid_calculated']/aggprofile['interval_hours']
    aggprofile['interval'] = interval

    return aggprofile


def annualIntervalDemand(aggprofilepowerdata):
    """Computes the mean annual power consumption for the interval aggregated in aggprofilepowerdata.
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
    cats = pd.cut(data.dayix, bins = [0, 5, 6, 7], right=False, 
                  labels= ['Weekday','Saturday','Sunday'], include_lowest=True)
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
    # For years < 2009 where only V and I were observed
    except: 
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
    """Generates the aggregate input data required for building the experimental model
    """
    # Generate folder structure and file names
    feather_path= {}
    csv_path= {}
    for i in ['pp', 'aggpp_' + interval, 'a' + interval + 'd', 'adtd']: 
        ipath = os.path.join(pdata_dir, 'aggProfiles', i)
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
        path = Path(os.path.join(pdata_dir, 'aggProfiles', aggfunc, 'feather'))
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
    path = os.path.join(pdata_dir, 'aggProfiles', 'adtd_season')
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