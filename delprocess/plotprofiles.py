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
import colorlover as cl

from .loadprofiles import loadReducedProfiles

def shapeProfiles(year, unit, dir_name, filetype='feather'):
    """
    This function reshapes a year's unit profiles into a dataframe indexed by date, with profile IDs as columns and units read as values.
    annualunitprofile variable should be a pandas data frame constructed with the loadProfiles() function.
    Rows with Valid=0 are removed.
    
    The function returns [shaped_profile_df, year, unit]; a tuple containing the shaped dataframe indexed by hour with aggregated unit values for all profiles, the year and unit concerned.
    
    """
    data = loadReducedProfiles(year, unit, dir_name, filetype='feather')
    
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

def createStaticMap(ids_df, mapbox_access_token, text_hover=True, zoom=False, zoom_province=False, annotate=True):
 
    georef = ids_df.groupby(['Province','LocName','Lat','Long']).agg(
        {'Year':['nunique','min'],'ProfileID':'nunique','AnswerID':'nunique'})
    georef.columns = ['_'.join(x) for x in georef.columns.ravel()]
    georef.rename(columns={'ProfileID_nunique':'metered','AnswerID_nunique':'surveyed',
                          'Year_nunique':'nr_years','Year_min':'start_year'}, inplace=True)
    georef.sort_values('start_year', inplace=True)
    georef.reset_index(inplace=True)
    georef['marker_size'] = round(georef['nr_years']**0.6*6)
    
    if text_hover is True:
        georef['text']=georef['LocName']+'<br>' + 'Site launch: '+georef['start_year'].astype(str)+'<br>' +'Years monitored: '+ georef['nr_years'].astype(str)+'<br>' +'Metered households: '+ georef['metered'].astype(str)+'<br>' +'Surveyed households: '+ georef['surveyed'].astype(str)
        map_hoverinfo='text'
        map_mode='markers'
    else:
        georef['text']=georef['LocName']
        map_hoverinfo='none'
        map_mode='markers+text'


    # Set marker colors to reflect start year of location monitoring
    colors = cl.scales['9']['seq']['YlOrBr'][::-1]+cl.scales['9']['seq']['YlGn'][1::]
    norm_color = np.linspace(0,1, len(georef.start_year.unique()))
    color_list = list(zip(norm_color, colors))
    
    if zoom_province in georef.Province.unique():
        zoom_level = zoom*(4.2)
        map_lat = georef.loc[georef['Province']==zoom_province,'Lat'].mean()
        map_lon = georef.loc[georef['Province']==zoom_province,'Long'].mean()
        map_title = 'NRS Load Research Programme Sites in the '+zoom_province+' Province'
    else:
        print('No valid zoom_level specified. Showing all sites.')
        zoom_level = 4.2
        map_lat = -29.1
        map_lon = 25
        map_title='NRS Load Research Programme Sites 1994-2014'
                           
    trace=go.Scattermapbox(
            name='Sites',
            lat=georef['Lat'],
            lon=georef['Long'],
            mode=map_mode,
            marker=dict(
                size=georef['marker_size'],
                color=georef['start_year'],
                colorscale=color_list,
                opacity=1,
                showscale=annotate,
                colorbar=dict(
                    lenmode='fraction', len=0.85,
                    thickness=20,
                    y=0, yanchor='bottom',
                    tickmode='linear', tick0=1994, dtick=2,
                    ticks='inside', ticklen=20,
                    title='Site launch year'
                )
            ),
            text=georef['text'],
            textposition='bottom center',
            hoverinfo=map_hoverinfo,
            showlegend=False
        )
    
    trace_border=go.Scattermapbox(
            name='marker size<br>represents<br>years on site',
            lat=georef['Lat'],
            lon=georef['Long'],
            mode='markers',
            marker=dict(
                size=georef['marker_size']+1.6,
                color='black',
                opacity=1
            ),
            hoverinfo='none',
        )

    figure=go.Figure(
        data=[trace_border, trace],
        layout = go.Layout(
                title=map_title,
                autosize=False,
                hovermode='closest',
                mapbox=dict(
                    accesstoken=mapbox_access_token,
                    bearing=0,
                    center=dict(
                        lat=map_lat,
                        lon=map_lon
                    ),
                    pitch=0,
                    zoom=zoom_level,
                    style='light'
                ),
                margin = dict(
                        l = 10,
                        r = 10,
                        t = 50,
                        b = 30
                ),
                showlegend=annotate
            )
    )
    return offline.iplot(figure)

def plotCustomerDist(ids_df, id_filter, **kwargs):
    
    year_start = ids_df['Year'].min()
    year_end = ids_df['Year'].max()
    
    ids = ids_df.groupby(['Survey','Year'])[id_filter].nunique()
    
    if 'nrslr_col' in kwargs:
        nrslr_col = kwargs['nrslr_col']
    else: nrslr_col = 'red'
    if 'eskomlr_col' in kwargs:
        eskomlr_col = kwargs['eskomlr_col']
    else: eskomlr_col = 'blue'
    
    nrslr = go.Bar(x = ids['NRS LR'].index,
                   y = ids['NRS LR'].values,
                   marker=dict(color=nrslr_col),
                   name = 'Municipalities')

    eskomlr = go.Bar(x = ids['Eskom LR'].index,
                    y = ids['Eskom LR'].values,
                    marker = dict(color=eskomlr_col),
                    name = 'Eskom')
   
    layout = go.Layout(title=kwargs['plot_title']+' from {} - {}'.format(year_start, year_end),
                    barmode = 'relative',
                    xaxis=dict(title='Year', tickangle=90, tickvals=list(range(year_start,year_end+1))),
                    yaxis=dict(title=id_filter+' Count', showline=True),
                    margin=dict(t=70),
                    height=450, width=850)
   
    fig = go.Figure(data=[nrslr, eskomlr], layout=layout)
    return offline.iplot(fig)
