# Paola - paolap@utas.edu.au 23rd November 2013
# Last changed on 11th of February 2014
# This script reads a list of files produced by search_CMIP5_replica.py and returns a list of the CMIP5 model+ensembles that contain all the variables given as input.
# The CMIP5 replica dataset is stored under 
# /g/data1/ua6/unofficial_ESG_replica/tmp/tree
# NB: This code reads the files details from the output of search_CMIP5_replica
#     if not specified assumed the input file is CMIP5_files_in_tree.csv 
# It creates two csv file called complete_ensemble.csv that lists MIP code, model, ensemble, (version) for each ensemble that has all the requested variables
# The other file not_complete_ensemble.csv lists the remaining ensembles that have some but not all the variables
#
# Example of how to run on raijin.nci.org.au
#
#    module load python/2.7.3  (default on raijin)
#    python find_matching_variables.py  -v ua_Amon -v tos_Omon -v tas_Amon output.csv  
#
# Notes concerning the above example: 
#  - the variable argument is passed as variable-name_cmip-table, this avoids confusion if looking for variables from different cmip tables
#  - multiple arguments can be passed to "-v"; 
#  - to pass multiple arguments, declare the option multiple times (as above);
#  - you can pass a different name for the output file, just by listing as 
#    last argument (output.csv in the example); 
#
#

import os, datetime, glob, re
import sys, getopt   # these are needed to accept external arguments

## helper functions


def help():
    ''' Print out a help message and exit '''
    print '''\n           
 Takes the following arguments:\n           
   -v / --variable    combination of CMIP5 variable & cmip_table Ex. tas_Amon\n
   -h / --help        display this message and exit \n           
   output_file        this should always come last, arguments passed after this\n
                      will be ignored\n
 The script returns all the ensembles satifying the constraints\n
  [var1_cmiptable AND var2_cmiptable AND ..]  in one file \n
    '''
    sys.exit()


def file_details(file):
    ''' Split the filename in variable, MIP code, model, experiment, ensemble (period is excluded) ''' 
    bits = file.split(',')
    varcmip = '_'.join(bits[0:2]) 
    modelrun = '_'.join(bits[2:5]) 
    return (varcmip,modelrun)


# Main program starts here
#set up input file and selected variable (or group of variables) and experiment
#infile is updated every Monday and contains a list of all files replicated on raijin 
infile = 'CMIP5_files_in_tree.csv'
# assign default values to constraints
var0 = []
outfile = 'complete_ensembles.csv'
outfile2 = 'not_complete_ensembles.csv'

# assign constraints from arguments list
letters = 'v:h' # the : means an argument needs to be passed after the letter
#the = means that a value is expected after the keyword
keywords = ['variable=', 'help'] 
opts, extraparams = getopt.getopt(sys.argv[1:],letters,keywords) 
# starts at the second element of argv since the first one is the script name
# extraparams are extra arguments passed after all option/keywords are assigned
# in this case the output file
# opts is a list containing the pair "option"/"value"
for o,p in opts:
  if o in ['-v','--variable']:
     var0.append(p)
  elif o in ['-h','--help']:
     help() 
for p in extraparams:
    outfile = p 
    outfile2 = "not_" + p  
 
print "Looking for following variable/cmip_table combinations:\n", var0 
print 'Output file for model runs including all variables: ' + outfile
print 'Output file for incomplete model runs: ' + outfile2
    
### open input file and output files 
inf = open(infile, 'r')
outf = open(outfile, 'w')
outf2 = open(outfile2, 'w')
line1 = 'model,experiment,ensemble\n'
outf.write(line1)
outf2.write(line1)


# read all the file paths line by line
lines = inf.readlines()

# out_lines is a set of unique lines to add as output, 1 line for each ensemble
out_lines_true = set() 
out_lines_false = set() 
model_run = set()
file_set = set() 
# loops through all the files
for file in lines[1:]:
    file.replace('\n','')
# call file_details to retrieve var_cmip and model_run (model/exp/ensemble) for each file 
    details = file_details(file)
    file_set.add(details) 
    model_run.add(details[1])

for run in list(model_run):
    for var in var0:
       comb = ((var,run))
       if comb in file_set: 
          output = True
       else:
          output = False
          break
    if output:
       out_lines_true.add(run.replace("_",","))
    else:
       out_lines_false.add(run.replace("_",","))
       
# write to output file
for sline in out_lines_true:
    outf.write(sline+"\n")
outf.close
for sline in out_lines_false:
    outf2.write(sline+"\n")
outf2.close

# close input file
inf.close

    

