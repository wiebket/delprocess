# South African Domestic Electrical Load Data Processing

## About this package

This package contains tools to process primary data from the South African Domestic Electric Load (DEL) database. It requires access to csv or feather file hierarchy extracted from the original General_LR4 database produced during the NRS Load Research study. 

**Note on data access:** 
Data can be accessed and set up as follows:  
1. From [Data First](www.datafirst.uct.ac.za) at the University of Cape Town (UCT). On site access to the complete 5 minute data is available through their secure server room.   
2. For those with access to the original database, [delretrieve](https://github.com/wiebket/delretrieve) can be used to retrieve the data and create the file hierarchy for further processing.
3. Several datasets with aggregated views are available [online]() and can be accessed for academic purposes. If you use them, you will *not* need to install this package. 

Other useful packages are:

## Package structure

```bash
delprocess
    |-- delprocess
        |-- data
        	    |-- geometa
                |-- 2016_Boundaries_Local
                |-- ...
            |-- specs
                |-- app_broken_00.txt
                |-- appliance_00.txt
                |-- appliance_94.txt	
                |-- behaviour_00.txt
                |-- binned_base_00.txt
                |-- binned_base_94.txt
                |-- dist_base_00.txt
                |-- dist_base_94.txt	
        |-- __init.py__
        |-- command_line.py
        |-- loadprofiles.py
        |-- plotprofiles.py
        |-- support.py
        	|-- surveys.py
	|-- MANIFEST.in
	|-- README.md
	|-- setup.py
```

## Setup instructions
Ensure that python 3 is installed on your computer. A simple way of getting it is to install it with [Anaconda](https://conda.io/docs/user-guide/install/index.html). Once python has been installed, the delprocess package can be installed.
	
1. Clone this repository from github.
2. Navigate to the root directory (`delprocess`) and run `python setup.py install` (run from Anaconda Prompt or other bash with access to python if running on Windows).
3. You will be asked to confirm the data directories that contain your data. Paste the full path name when prompted. You can change this setting at a later stage by modifying the file `your_home_dir/del_data/usr/store_path.txt` .

This package only works if the data structure is _exactly_ like the directory hierarchy in _del_data_ if created with the package `delretrieve`:

```bash
your_home_dir/del_data
	|-- observations
	    |-- profiles
		    |-- raw
			    |-- unit
				    |-- GroupYear
	    |-- tables
		    |-- ...
	|-- survey_features
	|-- usr
	    |-- specs (automatically copied from delprocess/data/specs during setup)
	    |-- store_path.txt (generated during setup)
```

## Data processing
This package runs a processing pipeline from the command line or can be accessed via python directy with `import delprocess`.
		    
Modules: surveys, loadprofiles, plotprofiles

### Timeseries data
	
#### From the command line
1. Execute `delprocess_profiles -i [interval]` from the command line (equivalent to `delprocess.saveReducedProfiles()`)
2. _Options_: -s [data start year] and -e [data end year] as optional arguments: if omitted you will be prompted to add them on the command line. Must be between 1994 and 2014 inclusive
3. _Additional command line options_: `-c or [--csv]`: Format and save output as csv files (default feather)

#### In python
Run `delprocess.saveReducedProfiles()`

Additional profile processing methods:

```
loadRawProfiles(year, month, unit)
reduceRawProfiles(year, unit, interval)
loadReducedProfiles(year, unit, interval)
genX()
```

#### Data output
All files are saved in `your_home_dir/del_data/resampled_profiles/[interval]`.

#### Feather file format
Feather has been chosen as the format for temporary data storage as it is a fast and efficient file format for storing and retrieving data frames. It is compatible with both R and python. Feather files should be stored for working purposes only as the file format is not suitable for archiving. All feather files have been built under `feather.__version__ = 0.4.0`. If your feather package is of a later version, you may have trouble reading the files and will need to reconstruct them from the raw MSSQL database. Learn more about [feather](https://github.com/wesm/feather).

### Survey data

#### From the command line
If you know what survey data you want for your analysis, it is easiest to extract it from the command line.

1. Create a pair of spec files `*_94.txt` and `*_00.txt` with your specifications
2. Execute `delprocess_surveys -f [filename]` (equivalent to running `genS()`)
3. _Options_: -s [data start year] and -e [data end year] as optional arguments: if omitted you will be prompted to add them on the command line. Must be between 1994 and 2014 inclusive.
4. _Additional command line options_
`-q`: equivalent to `searchQuestions(args)`
`-a`: equivalent to `searchAnswers(args)`

#### In python
Import the package to use the following functions:

```
searchQuestions()
searchAnswers()
genS()
```

#### Data output
All files are saved in .csv format in `your_home_dir/del_data/survey_features/`.

#### Specs files format
Spec file templates are copied to `your_home_dir/del_data/usr/specs` during setup.

TODO: describe naming convention

The spec file is a dictionary of lists and dictionaries. It is loaded as a json file and _all inputs must be strings_, with key:value pairs separated by commas. The specfile must contain the following keys:

_year_range_ | list of two strings specifying start and end year, eg. ["2000","2014"]  
_features_ | list of user-defined variable names, eg. ["fridge_freezer","geyser","heater"]  
_searchlist_ | list of database question search terms, eg. ["fridgefreezerNumber" ,"geyserNumber", "heaterNumber"]  
_transform_ | dict of simple data transformations such as addition. Keys must be one of the variables in the features list, eg. {"fridge_freezer" : "x['fridgefreezerNumber'] - x['fridgefreezerBroken']"}  
_bins_ | dict . Keys must be listed as a variable in features, eg. {"floor_area" : ["0", "50", "80"]},  
_labels_ | eg. {"floor_area" : ["0-50", "50-80"]},  
_cut_ | {"monthly_income":{"right":"False", "include_lowest":"True"}},  
_replace_ | dict of dicts specifying the coding for replacing feature values, eg. {"water_access": {"1":"nearby river/dam/borehole"}}  
_geo_ | string specifying site geographic detail (can be "Municipality","District" or "Province")  

If there are no transforms, bins, labels, cuts, replace or geo, their value should be replaced with an empty dict `{}`.