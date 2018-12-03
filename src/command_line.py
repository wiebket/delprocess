#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Monday 3 December 2018

This is a shell script for processing observations from South Africa's Domestic Load Research Database

@author: Wiebke Toussaint
"""

import optparse

from features.feature_extraction import saveData
from observations.obs_processing import saveReducedProfiles, csvTables

def process_profiles():

	parser = optparse.OptionParser()

	parser.add_option('-i', '--intervalresample', dest='interval', default='H', type=str, help='Reduce load profiles to interval')
	parser.add_option('-s', '--startyear', dest='startyear', type=int, help='Start year for profile data retrieval')
	parser.add_option('-e', '--endyear', dest='endyear', type=int, help='End year for profile data retrieval')
	parser.add_option('-csv', '--csvdir', action='store_true', dest='csv', help='Format and save tables as csv files')

	parser.set_defaults(csv=False)

	(options, args) = parser.parse_args()
		
	if options.startyear is None:
		options.startyear = int(input('Enter observation start year: '))
	if options.endyear is None:
		options.endyear = int(input('Enter observation end year: '))
	saveReducedProfiles(options.startyear, options.endyear, options.interval)

	if options.csv == True:
		csvTables()
		
	return print('>>>obsProcess end<<<')


def process_surveys():

	parser = optparse.OptionParser()

	parser.add_option('-s', '--startyear', dest='startyear', type=int, help='Start year for profile data retrieval')
	parser.add_option('-e', '--endyear', dest='endyear', type=int, help='End year for profile data retrieval')
	parser.add_option('-f', '--specfile', dest='specfile', type=str, help='Feature specification file name')
	parser.add_option('-n', '--name', dest='name', type=str,  default='evidence', help='Output file naming convention')

	(options, args) = parser.parse_args()
		
	if options.startyear is None:
		options.startyear = int(input('Enter observation start year: '))
	if options.endyear is None:
		options.endyear = int(input('Enter observation end year: '))
	saveData(options.startyear, options.endyear, options.specfile, options.name)

	return print('>>>featureExtraction end<<<')
	

