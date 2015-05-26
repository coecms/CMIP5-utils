CMIP5-utils
===========

Set of scripts to help with CMIP5 datasets stored on raijin.nci.org.au

search_CMIP5_replica.py - This script produces a file listing all the CMIP5 ensembles available on raijin.nci.org.au 
                          that satisfy the given constraints. Possible constraints are: 
                          experiment, model, frequency, variable, cmip_table. 
                          For a complete lists of arguments type:
                          python search_CMIP5_replica.py -h / --help

find_matching_variables.py - This script uses the output of search_CMIP5_replica.py and returns all the models/ensembles
                            combination that contain "all" the variables given as input.
                            For instructions on how to use it type:
                            python find_matching_variables.py -h / --help

fetch_CMIP5.py - This script given variable_mip/experiment/model list as input,
                 returns csv files detailing what has been published and if this
                 was downloaded or not on raijin. Script has google option that
                 creates as well a google spreadsheet summarising results.
                 For instructions on how to use it type:
                     python fetch_CMIP5.py -h / --help
26/05/2015
I removed this script because it should be run interactively which can occasionaly create issues on raijin. I've added a two steps procedure which split the retrieval of the wget file which can only be run interactively from the second step which check the existence of the files both online and raijin and creates output files. This can be run both interactively or in the queue system.

fetch_step1.py - performs the search for all the CMIP5 files responding to the given constraints and creates a wget_<exp>.out file for each selected experiment containing the search results.
fetch_step2.py - use the wget_<exp>.out as input and check if the files exists on raijin and if they do, if they need updating, produces three files listing which files need to be downloaded/updated, which haven't been published yet and which are alredy on raijin and where. It can also produces and optional csv table summarising the results.
