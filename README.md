CMIP5-utils
===========

Set of scripts to help with CMIP5 datasets stored on dcc.nci.org.au

search_CMIP5_replica.py - This script produces a file listing all the CMIP5 ensembles available on dcc.nci.org.au 
                          that satisfy the given constraints. Possible constraints are: 
                          experiment, model, frequency, variable, cmip_table. 
                          For a complete lists of arguments type:
                          python search_CMIP5_replica.py -h / --help

find_matching_variables.py: - This script uses the output of search_CMIP5_replica.py and returns all the models/ensembles
                            combination that contain "all" the variables given as input.
                            For instructions on how to use it type:
                            python find_matching_variables.py -h / --help
