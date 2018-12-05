#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  1 17:55:45 2017

@author: saintlyvi
"""

import numpy as np

import plotly as py
from plotly.offline import offline
import plotly.graph_objs as go
offline.init_notebook_mode(connected=True)

from .loadprofiles import loadProfiles

def shapeProfiles(year, unit, dir_name, filetype='feather'):
    """
    This function reshapes a year's unit profiles into a dataframe indexed by date, with profile IDs as columns and units read as values.
    annualunitprofile variable should be a pandas data frame constructed with the loadProfiles() function.
    Rows with Valid=0 are removed.
    
    The function returns [shaped_profile_df, year, unit]; a tuple containing the shaped dataframe indexed by hour with aggregated unit values for all profiles, the year and unit concerned.
    
    """
    data = loadProfiles(year, unit, dir_name, filetype='feather')
    
    data.loc[(data.Unitsread.notnull())&(data.Valid != 1), 'Unitsread'] = np.nan
    data.ProfileID = data.ProfileID.astype(str)
    data.set_index(['Datefield','ProfileID'], inplace=True, drop=True)
    data = data[~data.index.duplicated(keep='first')]
    
    profile_matrix = data.unstack()['Unitsread'] #reshape dataframe
    valid_matrix = data.unstack()['Valid']
    
    return profile_matrix, year, unit, valid_matrix


def nanAnalysis(year, unit, dir_name, threshold = 0.95):
    """
    This function displays information about the missing values for all customers in a load profile unit year.
    threshold - float between 0 and 1: user defined value that specifies the percentage of observed hours that must be valid for the profile to be considered useable.
    
    The function returns:
        * two plots with summary statistics of all profiles
        * the percentage of profiles and measurement days with full observational data above the threshold value.
    """
    
    data, year, unit, valid_matrix = shapeProfiles(year, unit, dir_name)

    #prep data
    fullrows = data.count(axis=1)/data.shape[1]
    fullcols = data.count(axis=0)/data.shape[0]
    
    trace1 = go.Scatter(name='% valid profiles',
                        x=fullrows.index, 
                        y=fullrows.values)
    trace2 = go.Bar(name='% valid hours',
                    x=fullcols.index, 
                    y=fullcols.values)
#    thresh = go.Scatter(x=fullrows.index, y=threshold, mode = 'lines', name = 'threshold', line = dict(color = 'red'))
    
    fig = py.tools.make_subplots(rows=2, cols=1, subplot_titles=['Percentage of ProfileIDs with Valid Observations for each Hour','Percentage of Valid Observational Hours for each ProfileID'], print_grid=False)
    
    fig.append_trace(trace1, 1, 1)
    fig.append_trace(trace2, 2, 1)
#    fig.append_trace(thresh, 2, 1)
    fig['layout']['xaxis2'].update(title='ProfileIDs', type='category', exponentformat="none")
    fig['layout']['yaxis'].update(domain=[0.55,1])
    fig['layout']['yaxis2'].update(domain=[0, 0.375])
    fig['layout'].update(title = "Visual analysis of valid DLR load profile data for " + str(year) + " readings (units: " + unit + ")", height=850)
      
    goodhours = len(fullcols[fullcols > threshold]) / len(fullcols) * 100
    goodprofiles = len(fullrows[fullrows > threshold]) / len(fullrows) * 100
    
    print('{:.2f}% of hours have over {:.0f}% fully observed profiles.'.format(goodhours, threshold * 100))
    print('{:.2f}% of profiles have been observed over {:.0f}% of time.'.format(goodprofiles, threshold * 100))
    
    offline.iplot(fig)
    
    return 
