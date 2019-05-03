#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 14:17:10 2018

@author: SaintlyVi
"""
from setuptools import setup, find_packages
import os
from pathlib import Path

usr_dir = os.path.join(Path.home(), 'del_data','usr')
os.makedirs(usr_dir, exist_ok=True)

setup(
      name='delprocess',
      version=0.1,
      description='Processes DLR data in a local directory structure',
      long_description='This module helps researchers process and clean load profile\
      and survey data from the Domestic Load Research database stored in a local \
      directory structure. Processed data is stored on disk.',
      keywords='domestic load research south africa data processing',
      url='https://github.com/wiebket/delprocess',
      author='Wiebke Toussaint',
      author_email='wiebke.toussaint@gmail.com',
      license='CC-BY-NC',
      install_requires=['pandas','numpy','pyodbc','feather-format','plotly', 
                        'pathlib','pyshp','shapely'],
      include_package_data=True,
      packages=find_packages(),
      py_modules = ['delprocess.surveys', 'delprocess.loadprofiles', 
                    'delprocess.plotprofiles'],
      data_files=[(os.path.join(usr_dir,'specs'), [os.path.join(
                  'delprocess','data','specs', f) for f in [files for root, dirs, files 
                    in os.walk(os.path.join('delprocess','data','specs'))][0]])],
      entry_points = {
			'console_scripts': ['delprocess_profiles=delprocess.command_line:process_profiles',
                       'delprocess_surveys=delprocess.command_line:process_surveys'],
                       }
      )
