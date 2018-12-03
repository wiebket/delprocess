# South African Domestic Load Research Data Processing

"""
dlr\_data\_processing/
    src/
		specs/
			app_broken_00.txt
			appliance_00.txt
			appliance_94.txt	
			base_00.txt
			base_94.txt	
			behaviour_00.txt
		data/
			geometa/
				2016_Boundaries_Local/...
			store_path.txt
		command_line.py
		loadprofiles.py
		plotprofiles.py
		surveys.py
    setup.py
    README.md
    MANIFEST.in
    
usr/documents/dlr_data_store/
	observations/
		profiles/
			raw/
		tables/
			csv
			feather
	features/
"""

## About this package

This package contains tools to process primary data from the South African Domestic Load Research database. It requires local access to data from the database. Data can be obtained from [Data First's](www.datafirst.uct.ac.za) online repository or with [dlr_data_retrieval]().

## Setup instructions
0. Ensure that python 3 is installed on your computer. A simple way of getting it is to install it with [Anaconda](https://conda.io/docs/user-guide/install/index.html). 
1. Clone this repository from github.
2. Navigate to the root directory (dlr_data_processing) and run the setup.py script
3. You will be asked to confirm the data directories that contain your data. Paste the full path name when prompted. You can change this setting at a later stage by modifying the file src/data/store_path.txt .

## Data processing

### Timeseries data

#### Run from command line
1. Execute 'python process_profiles -i [interval]'. 

#### Data output
All files are saved in .csv format in /dlr_data_store/profiles/[interval].

#### Additional profile processing methods


### Survey data

searchQuestions()

searchAnswers()

extractSocios()

genS()
This function searches /src/specs/*.txt for a spec file.

#### Specs files format
TODO: describe naming convention
TODO: describe file content

#### Run from command line
If you know what survey data you want for your analysis, it is easier to extract it from the command line.
1. Create spec files *\_94.txt and *\_00.txt with your specifications
2. Execute 'python process_surveys -spec [filename.txt]'. This is equivalent to running 'genS()'.

_Additional command line options_
-q: equivalent to searchQuestions(args)
-a: equivalent to searchAnswers(args)

#### Data output
All files are saved in .csv format in /dlr_data_store/features/.

### File format
Feather has been chosen as the format for temporary data storage as it is a fast and efficient file format for storing and retrieving data frames. It is compatible with both R and python. Feather files should be stored for working purposes only as the file format is not suitable for archiving. All feather files have been built under `feather.__version__ = 0.4.0`. If your feather package is of a later version, you may have trouble reading the files and will need to reconstruct them from the raw MSSQL database. Learn more about [feather](https://github.com/wesm/feather).
