# Paola Petrelli - paolap@utas.edu.au 4th March 2014
# Last changed on 26th of March 2014
# Updates list:
#   26/03/2014 - output files and csv table are created after 
#                 collecting data; calling process_file with multiprocessing 
#                module to speed up md5checksum
#   01/04/2014 - exclude the ACCESS and CSIRO models from check
#   03/09/2014 trying to substitute the google file with a csv table
#   01/12/2014 script has been divided into two steps, this is first step fetch_step1.py
#     that runs search on ESGF node and can be run interactively, the second step fetch_step2.py should be run in the queue
#   21/05/2015  comments updated, introduce argparse to manage inputs, added extra argument
#     "node" to choose automatically between different nodes: only pcmdi and dkrz (default) are available at the moment
#
# Retrieves a wget script (wget_<experiment>.out) listing all the CMIP5
# published files responding to the constraints passed as arguments.
# The search is run on one of the ESGF node but it searches through all the available
# nodes for the latest version. Multiple arguments can be passed to -e, -v, -m. At least one variable and experiment
# should be specified but models are optionals. The search is limited to the first 10000 matches,
# to change this you have to change the pcmdi_url variable in the code.
# The second step returns 3 files listing: the published files available on raijin (variables_replica.csv), 
# the published files that need downloading and/or updating (variables_to_download.csv), 
# the variable/model/experiment combination not yet published (variables_not_published).
# Uses md5 checksum to determine if a file already existing on raijin is exactly the same as the latest published version
# If the "table" option is selected it returns also a csv table summarising the search results. 
#
# The CMIP5 replica data is stored on raijin.nci.org.au under
# /g/data1/ua6/unofficial-ESG-replica/tmp/tree
#
# Example of how to run on raijin.nci.org.au
#
#    module load python/2.7.3  (default on raijin)
#    python fetch_step1.py  -v ua_Amon tos_Omon -m CCSM4 -e rcp45 -n pcmdi
# NB needs python version 2.7 or more recent
#
#  - the variable argument is passed as variable-name_cmip-table, this avoids confusion if looking for variables from different cmip tables
#  - multiple arguments can be passed to "-v", "-m", "-e";
#  - to pass multiple arguments, declare the option once followed by all desired values (as above);
#  - you need to pass at least one experiment and one variable, models are optional.
#  - node is optional, dkrz is default, only other option is pcmdi

import sys, urllib
import os.path as opath     # to manage files and dirs
import argparse             # to parse input arguments

# help functions
def VarCmipTable(v):
    if "_" not in v:
      raise TypeError("String '%s' does not match required format: var_cmip-table, ie tas_Amon"%(v,))
    else:
      return v

def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description='''Retrieves a wget script (wget_<experiment>.out) listing all the CMIP5 
            published files responding to the constraints passed as arguments.
            The search is run on one of the ESGF node but it searches through all the available 
            nodes for the latest version. Multiple arguments can be passed to -e, -v, -m. At least one variable and experiment 
            should be specified but models are optionals. The search is limited to the first 1000 matches,
            to change this you have to change the pcmdi_url variable in the code.''')
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=True)
    parser.add_argument('-m','--model', type=str, nargs="*", help='', required=False)
    parser.add_argument('-v','--variable', type=VarCmipTable, nargs="*", help='combination of CMIP5 variable & cmip_table Ex. tas_Amon', required=True)
    parser.add_argument('-n','--node', type=str, nargs=1, default='dkrz', help='''ESGF node to use for the search, 
                        default is dkrz, pcmdi other option ''', required=False)
    return vars(parser.parse_args())


def correct_model(model):
    ''' Correct name of models that have two, to make search work '''
# list model as dict{dir name : search name}
    models={"ACCESS1-0" : "ACCESS1.0", "ACCESS1-3" : "ACCESS1.3",
            "CESM1-BGC" : "CESM1(BGC)", "CESM1-CAM5" : "CESM1(CAM5)",
            "CESM1-CAM5-1-FV2" : "CESM1(CAM5.1,FV2)", "CESM1-WACCM" : "CESM1(WACCM)",
            "CESM1-FASTCHEM" : "CESM1(FASTCHEM)", "bcc-csm1-1" : "BCC-CSM1.1",
            "bcc-csm1-1-m" : "BCC-CSM1.1(m)", "inmcm4" : "INM-CM4"}  
# if the current model is one of the dict keys, change name
    if model in models.keys():
       return models[model]
    return model


def create_wget(exp,modlist,varmips,node):
    ''' create wget call for each query (ie each variable/exp/model combination) '''
    wgetfile = "wget_" + exp + ".out"
# if one of the wget output files exists issue a warning an exit
    if opath.isfile(wgetfile):
       print "Warning: one of the output files exists, exit to not overwrite!"
       sys.exit() 
# apply recursively correct_model to each input model
    models = ""
    if len(modlist) > 0:
      modlist = map(correct_model, [x for x in modlist])
      models = "&model=" + "&model=".join(modlist) 
# split var and mip table varmips
    varlist = list(set([i.split("_")[0] for i in varmips]))
    miplist = list(set([i.split("_")[1] for i in varmips]))
    mips = "&cmor_table=".join(miplist) 
    variables = "&variable=".join(varlist) 
# builds url to be passed depending on constraints and chosen node 
    pcmdi_url = "http://esgf-data.dkrz.de/esg-search/wget?experiment=" + exp + "&cmor_table=" + mips + "&project=CMIP5" + models + "&variable=" + variables + "&replica=false&latest=true&limit=10000"
#    pcmdi_url = "http://esgf-data.dkrz.de/esg-search/wget?experiment=" + exp + "&cmor_table=" + mips + "&project=CMIP5" + models + "&variable=" + variables + "&replica=false&latest=true&limit=10000&offset=10000"
    if node=='pcmdi': pcmdi_url = "http://pcmdi9.llnl.gov/esg-search/wget?experiment=" + exp + "&cmor_table=" + mips + "&project=CMIP5" + models + "&variable=" + variables + "&replica=false&latest=true&limit=10000"
    print pcmdi_url 
    urllib.urlretrieve(pcmdi_url,wgetfile)
    print "Finished downloading wget file from " + pcmdi_url.split("/")[2]
    return wgetfile 


def assign_constraint():
    ''' Assign default values and input to constraints '''
    global var0, exp0, mod0, node 
    var0 = []
    exp0 = []
    mod0 = []
# assign constraints from arguments list
    args = parse_input()
    var0=args["variable"] 
    if args["model"]: mod0=args["model"]
    exp0=args["experiment"] 
    node=args["node"][0] 
    return


def main():
    ''' Main program starts here '''
# read inputs and assign constraints
    assign_constraint()
# loop through experiments, 1st create a wget request for exp, then parse_file 
    for exp in exp0:
        wgetfile=create_wget(exp,mod0,var0,node)

# check python version and then call main()
if sys.version_info < ( 2, 7):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7 or newer!")
main()
