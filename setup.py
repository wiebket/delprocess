#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 14:17:10 2018

@author: SaintlyVi
"""
from setuptools import setup, find_packages
import os
from dlrprocess.support import usr_dir

setup(
      name='dlrprocess',
      version=0.1,
      description='Processes local DLR data',
      long_description='This module helps researchers process and clean load profile\
      and survey data of the Domestic Load Research database from a local directory\
      structure. Processed data is stored on disk.',
      keywords='domestic load research south africa data processing',
      url='https://github.com/wiebket/dlrprocess',
      author='Wiebke Toussaint',
      author_email='wiebke.toussaint@gmail.com',
      license='CC-BY-NC',
      install_requires=['pandas','numpy','pyodbc','feather-format','plotly', 
                        'pathlib','pyshp','shapely'],
      packages=find_packages(),
      py_modules = ['dlrprocess.surveys', 'dlrprocess.loadprofiles', 
                    'dlrprocess.plotprofiles'],
      data_files=[(os.path.join(usr_dir,'specs'), [os.path.join(
                  'dlrprocess','data','specs', f) for f in [files for root, dirs, files 
                    in os.walk(os.path.join('dlrprocess','data','specs'))][0]])],
      include_package_data=True,
      entry_points = {
			'console_scripts': ['dlrprocess_profiles=dlrprocess.command_line:process_profiles',
                       'dlrprocess_surveys=dlrprocess.command_line:process_surveys'],
                       }
      )
