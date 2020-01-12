<a href="https://zenodo.org/badge/latestdoi/160184911"><img src="https://zenodo.org/badge/160184911.svg" alt="DOI" align="left"></a>  
<img src="/delprocess/data/images/DEL_logo.png" alt="DEL Logo" width="200" height="150" align="left"/>  

# South African <br/> Domestic Electrical Load Study <br/> Data Processing

## About this package

This package contains tools to process primary data from the South African Domestic Electric Load (DEL) database. It requires access to csv or feather file hierarchy extracted from the original General_LR4 database produced during the NRS Load Research study. 

**Notes on data access:** 

Data can be accessed and set up as follows:  
1. From [Data First](www.datafirst.uct.ac.za) at the University of Cape Town (UCT). On site access to the complete 5 minute data is available through their secure server room.   
2. For those with access to the original SQL database, [delretrieve](https://github.com/wiebket/delretrieve) can be used to retrieve the data and create the file hierarchy for further processing.
3. Several datasets with aggregated views are available [online](https://www.datafirst.uct.ac.za/dataportal/index.php/catalog/DELS/about) and can be accessed for academic purposes. If you use them, you will *not* need to install this package. 

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
		    
Modules: `surveys`, `loadprofiles`, `plotprofiles`

### Timeseries data (**DEL M**etering data)
	
#### From the command line
1. Execute `delprocess_profiles -i [interval]` from the command line (equivalent to `loadprofiles.saveReducedProfiles()`)
2. _Options_: -s [data start year] and -e [data end year] as optional arguments: if omitted you will be prompted to add them on the command line. Must be between 1994 and 2014 inclusive
3. _Additional command line options_: `-c or [--csv]`: Format and save output as csv files (default feather)

#### In python
Run `delprocess.loadprofiles.saveReducedProfiles()`

Additional profile processing methods:

```
loadRawProfiles(year, month, unit) 
reduceRawProfiles(year, unit, interval)
loadReducedProfiles(year, unit, interval)
genX(year_range, drop_0=False, **kwargs)
```
#### Data output
All files are saved in `your_home_dir/del_data/resampled_profiles/[interval]`.

#### Feather file format
Feather is the devalt format for temporary data storage of the large metering dataset as it is a fast and efficient file format for storing and retrieving data frames. It is compatible with both R and python. Feather files should be stored for working purposes only as the file format is not suitable for archiving. All feather files have been built under `feather.__version__ = 0.4.0`. If your feather package is of a later version, you may have trouble reading the files and will need to reconstruct them from the raw MSSQL database. Learn more about [feather](https://github.com/wesm/feather).

### Survey data (**DEL S**urvey data)

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
searchQuestions(searchterm)
searchAnswers(searchterm)
genS(spec_files, year_start, year_end)
```

The search is not case sensitive and has been implemented as a simple `str.contains(searchterm, case=False)`, searching all the strings of all the `Question` column entries in the `questions.csv` data file. The searchterm must be specified as a single string, but can consist of different words separated by whitespace. The search function removes the whitespace between words and joins them, so the order of words is important. For example, 'hot water' will yield results, but 'water hot' will not!

#### Data output
All files are saved in .csv format in `your_home_dir/del_data/survey_features/`.

#### Spec file format
Spec file templates are copied to `your_home_dir/del_data/usr/specs` during setup. These can be used directly to retrieve standard responses for appliance, behavioural and demographic related questions, or be adapted to create custom datasets from the household survey data.

The spec file is a dictionary of lists and dictionaries. It is loaded as a json file and **all inputs must be strings**, with `key:value` pairs separated by commas. The specfile must contain the following keys:

|key | value |
|---|--- |
|year_range | _list_ year range for which specs are valid; must be `["1994", "1999"]` or `["2000","2014"]` |
|features | _list_ of user-defined variable names, eg. `["fridge_freezer","geyser"]` |
|searchlist | _list_ of database question search terms, eg. `["fridgefreezerNumber" ,"geyserNumber"]` |
|transform | _dict_ of simple data transformations such as addition. Keys must be one of the variables in the features list, while the transformation variables must come from searchlist, eg. `{"fridge_freezer" : "x['fridgefreezerNumber'] - x['fridgefreezerBroken']"}` |
|bins | _dict of lists_ specifying bin intervals for numerical data. Keys must be one of the variables in the features list, eg. `{"floor_area" : ["0", "50", "80"]}` |  
|labels | _dict of lists_ specifying bin labels for numerical data. Keys must be one of the variables in the features list, eg. `{"floor_area" : ["0-50", "50-80"]}}` |
|cut | _dict of dicts_ specifying details of bin segments for numerical data. Keys must be one of the variables in the features list. `right` indicates whether bins includes the rightmost edge or not. `include_lowest ` indicates whether the first interval should be left-inclusive or not, eg `{"monthly_income":{"right":"False", "include_lowest":"True"}}` |
|replace | _dict of dicts_ specifying the coding for replacing feature values. Keys must be one of the variables in the features list, eg. `{"water_access": {"1":"nearby river/dam/borehole"}}` |
|geo | _string_ specifying geographic location detail (can be `"Municipality"`,`"District"` or `"Province"`)  |

If no transform, bins, labels, cuts, replace or geo is required, the value should be replaced with an empty dict `{}`.

#### Creating a custom spec file
To create a custome spec file, the following process is recommended:

1. Copy an existing spec file template and delete all values (but keep the keys and formatting!)
2. Use the `searchQuestions()` function to find all the questions that relate to a variable that you are interested in. Use this to construct your `searchlist`.
3. Use the `searchAnswers()` function to get the responses to your search.
4. Interrogate the responses to decide if any transform, bins and replacements are needed.
5. If bins are needed, decided whether labels and cut are required.
6. Decide whether high level geographic information should be added to the responses and update `geo` accordingly.
7. Save the file as `name_94.txt` or `name_00.txt`.

**NB: Surveys were changed in 2000 and questions vary between the years from 1994 - 1999 and 2000 - 2014. Survey data is thus extracted in two batches and requires two spec files with appropriate search terms matched to the questionaire.** For example, the best search term to retrieve household income for the years 1994 - 1999 is 'income', while for 2000 - 2014 it is 'earn per month'.

## Acknowledgements

### Citation
Toussaint, Wiebke. delprocess: Data Processing of the South African Domestic Electrical Load Study, version 1.01. Zenodo. https://zenodo.org/record/3605422 (2019).

### Funding
This code has been developed by the Energy Research Centre at the University of Cape Town with funding from the South African National Energy Development Initiative under the CESAR programme.


 Developed by          	|  Funded by
:----------------------:|:-------------------------:
<img src="/delprocess/data/images/erc_logo.jpg" alt="ERC Logo" width="206" height="71" align="left" hspace="20" />   |  <img src="/delprocess/data/images/sanedi_logo.jpg" alt="Sanedi Logo" width="177" height="98" align="left" hspace="20" />
