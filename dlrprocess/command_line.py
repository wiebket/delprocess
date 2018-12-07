#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Monday 3 December 2018

This package contains commandline calls for the dlrprocessing module.

@author: Wiebke Toussaint
"""

import optparse

from .surveys import genS
from .loadprofiles import saveReducedProfiles
from .support import validYears

def list_callback(option, opt, value, parser):
  setattr(parser.values, option.dest, value.split(','))

def process_profiles():
    """
    """
    parser = optparse.OptionParser()
    parser.add_option('-i', '--intervalresample', dest='interval', default='30T', type=str, help='Reduce load profiles to interval')
    parser.add_option('-s', '--startyear', dest='startyear', type=int, help='Data start year')
    parser.add_option('-e', '--endyear', dest='endyear', type=int, help='Data end year')
    parser.add_option('-c', '--csv', action='store_true', dest='csv', help='Format and save output as csv files')
    parser.set_defaults(csv=False)

    (options, args) = parser.parse_args()
		
    if options.startyear is None:
        options.startyear = int(input('Enter observation start year: '))
    if options.endyear is None:
        options.endyear = int(input('Enter observation end year: '))
    if options.csv == True:
        filetype = 'csv'
    else:
        filetype = 'feather'

    validYears(options.startyear, options.endyear)   #check that year input is valid 
    
    for year in range (options.startyear, options.endyear + 1):
        saveReducedProfiles(year, options.interval, filetype)
	
    return print('>>>Load profile data processing end.<<<')


def process_surveys():
    """
    """

    parser = optparse.OptionParser()
    parser.add_option('-s', '--startyear', dest='startyear', type=int, help='Data start year')
    parser.add_option('-e', '--endyear', dest='endyear', type=int, help='Data end year')
    parser.add_option('--feather', action='store_true', dest='csv', help='Format and save output as feather files')
    parser.add_option('-f', '--specfiles', dest='specfiles', help='Feature specification file name(s)')
    parser.set_defaults(feather=False)
    
    (options, args) = parser.parse_args()
		
    if options.startyear is None:
        options.startyear = int(input('Enter survey start year: '))
    if options.endyear is None:
        options.endyear = int(input('Enter survey end year: '))
    if options.feather == False:
        filetype = 'csv'
    else:
        filetype = 'feather'

    validYears(options.startyear, options.endyear)   #check that year input is valid 
    
    S = genS(options.specfiles, options.startyear, options.endyear, filetype)    
    del S
    
    return print('>>>Survey data extraction end.<<<')
	

