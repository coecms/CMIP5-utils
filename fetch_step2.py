# Paola Petrelli - paolap@utas.edu.au 4th March 2014
# Last changed on 26th of March 2014
# Updates list:
#   26/03/2014 - output files and table csv file are created after 
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
# If you have to parse a big number of files, you can speed up the process by using multithread module "Pool"
# if you're doing this you should run the second step in the queue, which is the reason why the script is split into 2 steps.
# To do that you can change the threads number from 1 (to run interactively) to the number of cpus you're requesting, in line 340 
#           async_results = Pool(16).map_async(process_file, result)
# The maximum number of threads depends on the number of cpus you're using, in above example 16 cpus.
#
# If the "table" option is selected it returns also a table csv file summarising the search results. 
#
# The CMIP5 replica data is stored on raijin.nci.org.au under
# /g/data1/ua6/unofficial-ESG-replica/tmp/tree
#
# Example of how to run on raijin.nci.org.au
#
#    module load python/2.7.3  (default on raijin)
#    python fetch_step2.py  -v ua_Amon tos_Omon -m CCSM4 -e rcp45 -o out -t
# NB needs python version 2.7 or more recent
#
#  - the variable argument is passed as variable-name_cmip-table, this avoids confusion if looking for variables from different cmip tables
#  - multiple arguments can be passed to "-v", "-m", "-e";
#  - to pass multiple arguments, declare the option once followed by all the desired values (as above);
#  - default output files root is "variables"
#  - you need to pass at least one experiment and one variable, models are optional.
#  - output file is optional, default is "variables"
#  - table is optional, default is False

import sys, argparse
import subprocess, re, itertools
from multiprocessing import Pool
import os.path as opath     # to manage files and dirs

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
    parser.add_argument('-t','--table', action='store_true', default='store_false', help="csv table option, default is False",
                        required=False)
    parser.add_argument('-o','--output', type=str, nargs="?", default="variables", help='''output files root, 
                       default is variables''', required=False)
    return vars(parser.parse_args())

    sys.exit()


def assign_constraint():
    ''' Assign default values and input to constraints '''
    global var0, exp0, mod0, table, outfile
    var0 = []
    exp0 = []
    mod0 = []
    outfile = 'variables'
# assign constraints from arguments list
    args = parse_input()
    var0=args["variable"]
    if args["model"]: mod0=args["model"]
    exp0=args["experiment"]
    table=args["table"]
    outfile=args["output"]
    return


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


def tree_exist(furl):
    '''  Return True if file exists in tmp/tree '''
    replica_dir = "/g/data1/ua6/unofficial-ESG-replica/tmp/tree/"
    tree_path = replica_dir + furl 
    return [opath.exists(tree_path),tree_path]

 
def write_file():
    ''' Write info on file to download or replica output '''
    global info
    files = {"R" : orep, "D" : odown}
    for item in info.values():
        outfile = files[item[-1]]
        outfile.write(",".join(item[0:-1])+"\n")


def file_details(fname):
    ''' Split the filename in variable, MIP code, model, experiment, ensemble (period is excluded) '''
    namebits = fname.replace("'","").split('_')
    if len(namebits) >= 5:
      details = namebits[0:5]
    else:
      details = []
    return details


def find_string(bits,string):
    ''' Returns matching string if found in directory structure '''
    dummy = filter(lambda el: re.findall( string, el), bits)
    if len(dummy) == 0:
        return 'no_version'
    else:
        return dummy[0]


def get_info(fname,path):
    ''' Collect the info on a file form its path return it in a list '''
    version = '[a-z]*201[0-9][0-1][0-9][0-3][0-9]'
    bits = path.split('/')
    finfo = file_details(fname)
    finfo.append(find_string(bits[:-1],version)) 
    finfo.append(path) 
    return finfo



def parse_file(wgetfile,varlist,modlist,exp):
    ''' extract file list from wget file '''
# open wget file, read content saving to a list of lines and close again
    infile = open(wgetfile,'r')
    lines = infile.readlines()
    infile.close
# if wget didn't return files print a warning and exit function 
    if lines[0] == "No files were found that matched the query":
       print lines[0] + " for ", varlist, modlist, exp
       return False 
    else:
# select only the files lines starting as var_cmortable_model_exp ...
       result=[]
# if modlist empty add to it a regex string indicating model name
       if len(modlist) > 0:
          comb_constr = itertools.product(*[varlist,modlist])
          filestrs = ["_".join(x) for x in comb_constr]
       else:
          filestrs = [var + '_[A-Za-z0-9-.()]*_' for var in varlist] 
       for line in lines:
           match = [re.search(pat,line) for pat in filestrs]
           if match.count(None) != len(match) and line.find(exp):
              [fname,furl,dummy,fmd5] = line.replace("'","").split()
              if dummy in ["MD5","md5"]:
                 result.append([fname, furl.replace("http://",""), fmd5])
              else: 
                 print "Error in parse_file() is selecting the wrong lines!"
                 print line
                 sys.exit()
       return result 
    

def check_md5sum(tree_path,fmd5):
    ''' Execute md5sum on file on tree and return True,f same as in wget file ''' 
    tree_md5 = subprocess.check_output(["md5sum", tree_path]).split()[0]
    return tree_md5 == fmd5


def process_file(result):
    ''' Check if file exist on tree and if True check md5sum '''
    info = {}
    [fname,furl,fmd5]=result
    [bool,tree_path]=tree_exist(furl)
    info[furl] = get_info(fname,tree_path)
# if file exists in tree compare md5sum with values in wgetfile, else add to update
    if "ACCESS" in fname or "CSIRO" in fname or (bool and check_md5sum(tree_path,fmd5)):
       info[furl].append("R")
    else:
       info[furl][-1] = "http://" + furl
       info[furl].append("D")
    return  info


def retrieve_info(query_item):
    ''' retrieve items of info related to input query combination '''
    global info
    # info order is: 0-var, 1-mip, 2-mod, 3-exp, 4-ens, 5-ver, 6-fname, 7-status
    var, mip = query_item[0].split("_")
    rows={}
    # add the items in info with matching var,mip,exp to rows as dictionaries 
    for item in info.values():
        if var == item[0] and mip == item[1] and query_item[-1] == item[3]:
           key = (item[2], item[4], item[5])
           try:
              rows[key].append(item[7])
           except:
              rows[key] = [item[7]]
# loop through mod_ens_vers combination counting files to download/update
    newrows=[]
    for key in rows.keys():
        ndown = rows[key].count("D") 
        status = key[2] + "  " + str(len(rows[key])) + " files, " + str(ndown) + " to update"
        newrows.append([tuple(key[0:2]), status])
    return  newrows


def result_matrix(querypub,exp0):
    ''' Build a matrix of the results to output to csv table '''
    global gmatrix
    # querypub contains only published combinations
    # initialize dictionary of exp/matrices
    gmatrix = {}
    for exp in exp0:
        # for each var_mip retrieve_info create a dict{var_mip:[[(mod1,ens1), details list][(mod1,ens2), details list],[..]]}
        # they are added to exp_dict and each key will be column header, (mod1,ens1) will indicate row and details will be cell value
        exp_dict={}
        infoexp = [x for x in querypub if x[-1] == exp]
        for item in infoexp:
                exp_dict[item[0]]=retrieve_info(item) 
        gmatrix[exp]= exp_dict
    return      


def compare_query(var0,mod0,exp0):
    ''' compare the var_mod_exp combinations found with the requested ones '''
    global info, opub
    # for each el. of info: join var_mip, transform to tuple, finally convert modified info to set
    info_set = set(map(tuple,[["_".join(x[0:2])] + x[2:-4] for x in info.values()]))
    # create set with all possible combinations of var_mip,model,exp based on constraints
    # if models not specified create a model list based on wget result
    if len(mod0) < 1: mod0 = [x[2] for x in info.values()]
    comb_query = set(itertools.product(*[var0,mod0,exp0]))
    # the difference between two sets gives combinations not published yet
    nopub_set = comb_query.difference(info_set)
    for item in nopub_set:
        opub.write(",".join(item) + "\n")
    # write a matrix to pass results to csv table in suitable format
    if table: result_matrix(comb_query.difference(nopub_set),exp0)
    return nopub_set 


def write_table(nopub):
    ''' write a csv table to summarise search '''
    global gmatrix
    for exp in exp0:
    # length of dictionary gmatrix[exp] is number of var_mip columns
    # maximum length of list in each dict inside gmatrix[exp] is number of mod/ens rows
        emat = gmatrix[exp]
        klist = emat.keys()
    # check if there are extra variables never published
        evar = list(set( [np[0] for np in nopub if np[0] not in klist if np[-1]==exp ] ))
    # calculate ncol,nrow keeping into account var never published
        ncol = len(klist) +2 + len(evar)
        nrow = max([len(emat[x]) for x in klist]) +1
    # open/create a csv file for each experiment
        try:
           csv = open(exp+".csv","w") 
        except:
           print "Can not open file " + exp + ".csv" 
        csv.write(" model_ensemble/variable," + ",".join(klist+evar) + "\n") 
      # pre-fill all values with "NP", leave 1 column and 1 row for headers 
      # write first two columns with all (mod,ens) pairs
        col1= [emat[var][i][0] for var in klist for i in range(len(emat[var])) ]
        col1 = list(set(col1))
        col1_sort=sorted(col1)
      # write first column with mod_ens combinations & save row indexes in dict where keys are (mod,ens) combination
      #  print col1_sort
        for modens in col1_sort:
            csv.write(modens[0] + "_" + modens[1]) 
            for var in klist:
                line = [item[1].replace(", " , " (")   for item in emat[var] if item[0] == modens]
                if len(line) > 0:
                   csv.write(", " +  " ".join(line) + ")")
                else:
                   csv.write(",NP")
            if len(evar) > 0:
               for var in evar:
                   csv.write(",NP")
            csv.write("\n")
        csv.close()
    print "Data written in table "
    return

def main():
    ''' Main program starts here '''
    global opub, odown, orep, info
# somefile is false starting turns to true if at elast one file found
    somefile=False
# read inputs and assign constraints
    assign_constraint()
    fdown = outfile + '_to_download.csv'
    frep = outfile + '_replica.csv'
    fpub = outfile + '_not_published.csv'
# test reading inputs
    print var0
    print exp0
    print mod0
    print fdown
    print frep
    print fpub
# if one of the output files exists issue a warning an exit
    if opath.isfile(fdown) or opath.isfile(frep) or opath.isfile(fpub):
       print "Warning: one of the output files exists, exit to not overwrite!"
       sys.exit() 
    info={}
# loop through experiments, 1st create a wget request for exp, then parse_file 
    for exp in exp0:
        wgetfile = "wget_" + exp + ".out"
        result=parse_file(wgetfile,var0,mod0,exp)
# if found any files matching constraints, process them one by one
# using multiprocessing Pool to parallelise process_file 
        if result:
           async_results = Pool(1).map_async(process_file, result)
           for dinfo in async_results.get():
               info.update(dinfo)
           somefile=True
        print "Finished md5checksum for existing files" 
# if it couldn't find any file for any experiment then exit
    if not somefile: 
     sys.exit("No files found for any of the experiments, exiting!") 
# open not published file
    opub=open(fpub, "w")
    opub.write("var_mip-table, model, experiment\n")
# build all requested combinations and compare to files found
    nopub_set = compare_query(var0,mod0,exp0)
# write replica and download output files
# open output files and write header
    odown=open(fdown, "w")
    odown.write("var, mip_table, model, experiment, ensemble, version, file url\n")
    orep=open(frep, "w")
    orep.write("var, mip_table, model, experiment, ensemble, version, filepath\n")
    write_file()
# close all the output files
    odown.close()
    orep.close()
    opub.close()
    print "Finished to write output files" 
# if table option create/open spreadsheet
# if table option write summary table in csv file
    if table: 
       write_table(nopub_set)

# check python version and then call main()
if sys.version_info < ( 2, 7):
    # python too old, kill the script
    sys.exit("This script requires Python 2.7 or newer!")
main()
