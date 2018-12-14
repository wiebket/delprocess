# South African Domestic Load Research Data Processing

```bash
dlrprocess
    |-- dlrprocess
        |-- data
    	    |-- geometa
                |-- 2016\_Boundaries\_Local
		    |-- ...
		|-- specs
		    	|-- app\_broken\_00.txt
		    	|-- appliance_00.txt
		    	|-- appliance_94.txt	
		    	|-- base_00.txt
		    	|-- base_94.txt	
		    	|-- behaviour_00.txt
		    	|-- store_path.txt
	    |-- command_line.py
	    |-- loadprofiles.py
	    |-- plotprofiles.py
	    |-- support.py
	    |-- surveys.py
    |-- setup.py
    |-- README.md
    |-- MANIFEST.in
```
Directory hierarchy for *dlr_data* if create with package `dlrretrieve`:
```bash
your\_home\_dir/dlr_data
	|-- observations
	    |-- profiles
		    |-- raw
			    |-- GroupYear
				    |-- ObsYear-ObsMonth
	    |-- tables
		    |-- csv
		    |-- feather
	|-- survey_features
	|-- survey_features
	|-- usr
	    |-- specs (copied from dlrprocess/data/specs)
	    |-- store_path.txt (generated during setup)
```

## About this package

This package contains tools to process primary data from the South African Domestic Load Research database. It requires local access to data from the database. Data can be obtained from [Data First's](https://www.datafirst.uct.ac.za/dataportal/) online repository or with [dlrretrieve](https://github.com/wiebket/dlrprocess).

## Setup instructions
Ensure that python 3 is installed on your computer. A simple way of getting it is to install it with [Anaconda](https://conda.io/docs/user-guide/install/index.html). 

1. Clone this repository from github.
2. Navigate to the root directory (`dlrprocess`) and run the `python setup.py install` script (run from Anaconda Prompt or other bash wiht access to python if running on Windows).
3. You will be asked to confirm the data directories that contain your data. Paste the full path name when prompted. You can change this setting at a later stage by modifying the file `your_home_dir/dlr_data/usr/store_path.txt` .

## Data processing
This package can be accessed from the command line to run a processing pipeline, or via python directy with `import dlrprocess`.

### Timeseries data

#### Run from command line
1. Execute `dlrprocess_profiles -i [interval]`. This is equivalent to running `saveReducedProfiles()`.
2. -s [data start year] and -e [data end year] as optional arguments: if omitted you will be prompted to add them on the command line. Must be between 1994 and 2014 inclusive.

_Additional command line options_
`-c or [--csv]`: Format and save output as csv files (default feather)

#### Data output
All files are saved in .csv format in `your_home_dir/dlr_data/resampled_profiles/[interval]`.

#### Additional profile processing methods
#Those pre-loaded in __init__


### Survey data

#### Run from command line
If you know what survey data you want for your analysis, it is easier to extract it from the command line.
1. Create a pair of spec files `*_94.txt` and `d*_00.txt` with your specifications
2. Execute `dlrprocess_surveys -f [filename]`. This is equivalent to running `genS()`.
3. -s [data start year] and -e [data end year] as optional arguments: if omitted you will be prompted to add them on the command line. Must be between 1994 and 2014 inclusive.

_Additional command line options_
`-q`: equivalent to `searchQuestions(args)`
`-a`: equivalent to `searchAnswers(args)`
`--feather`: Format and save output as feather files (default csv)

#### Specs files format
Templates copied to `your_home_dir/dlr_data/usr/specs` during setup.
TODO: describe naming convention
TODO: describe file content

#### Data output
All files are saved in .csv format in `your_home_dir/dlr_data/survey_features/`.

#### Additional profile processing methods

`searchQuestions()`

`searchAnswers()`

`extractSocios()`

`genS()`


### File format
Feather has been chosen as the format for temporary data storage as it is a fast and efficient file format for storing and retrieving data frames. It is compatible with both R and python. Feather files should be stored for working purposes only as the file format is not suitable for archiving. All feather files have been built under `feather.__version__ = 0.4.0`. If your feather package is of a later version, you may have trouble reading the files and will need to reconstruct them from the raw MSSQL database. Learn more about [feather](https://github.com/wesm/feather).
