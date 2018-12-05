#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 14:17:10 2018

@author: SaintlyVi
"""
from setuptools import setup, find_packages

setup(
      name='dlrprocessing',
      version=0.1,
      description='Processes local DLR data',
      long_description='This module helps researchers retrieve cleaned load profile\
      and survey data from a local directory structure of the Domestic Load Research\
      database. Processed data is store on disk.',
      keywords='domestic load research south africa data processing',
      url='https://github.com/wiebket/dlrprocessing',
      author='Wiebke Toussaint',
      author_email='wiebke.toussaint@gmail.com',
      packages=find_packages(),
      license='CC-BY-NC',
      install_requires=['pandas','numpy','pyodbc','feather-format','plotly', 
                        'pathlib','pyshp','shapely'],#, 'optparse', 'json','os','glob' ],
      include_package_data=True,
      entry_points = {
			'console_scripts': ['dlr_process_profiles=dlrprocessing.command_line:process_profiles', 'dlr_process_surveys=dlrprocessing.command_line:process_surveys'],
    		}
      )