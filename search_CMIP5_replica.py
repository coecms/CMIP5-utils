# Paola - paolap@utas.edu.au 23rd August 2013
# This script produces a file listing all the CMIP5 ensembles available on dcc.nci.org.au that satisfy the given constraints.
# The CMIP5 replica dataset is stored under 
# /projects/ua6/unofficial_ESG_replica/tmp/tree
# The file list used as input is updated every Monday by Lawson Hanson (CAWCR).
# It creates a csv file called CMIP5_files_in_tree.csv that lists variable, MIP code, model, ensemble, version and path on dcc for each matching ensemble
# Example of how to run on dcc.nci.org.au
#
#    module load python/2.7.1
#    python search_CMIP5_replica.py -f "mon" -v "ua" -v "tas" -e "piControl" -e "historical" -e "rcp45" -m "bcc-csm1-1" -m "CCSM4" output.csv 
#
#  Notes concerning the above example: 
#	- multiple arguments can be passed to "-v", "-e", "-m", "-t" and -f"; 
#	- to pass multiple arguments, declare the option multiple times (as above);  
#	- you can pass a different name for the output file, just by listing as last argument (output.csv in the example); 
#	- all arguments are optional; 
#	- failing to set any constraint will result in the entire dataset being selected.  
#
#  To quickly view/browse the contents/properties of the resulting file you could do this (in bash): 
#
#    cut -d"," -f1-6 CMIP5_files_in_tree.csv | sort
#    wc CMIP5_files_in_tree.csv 
#
#  Notes regarding these bash commands:
#	- the output is "sorted" by the first column, which is by variable.  
#	- to sort by other columns, you need to rearrange the column order.  
#	eg. Sort by mip (col 2), then experiment (col 4).  
#  	cut -d"," -f2,4 CMIP5_files_in_tree.csv > cols_sorting.txt
#	cut -d"," -f1,3,5,6- CMIP5_files_in_tree.csv > cols_other.txt 
#	NB: "6-" means "column 6 and all remaining columns"
#	paste -d"," cols_sorting.txt cols_other.txt > CMIP5_files_in_tree_rearranged.csv
#
# Feel free to copy and modify this script and please report any bugs
#
#   NB: This code reads the paths of files under the unofficial replica tree from 
#       /home/548/lih548/esg-tree-LATEST-paths.txt 
#       This file is updated every Monday and kindly shared by Lawson Hanson from CAWCR
#       If you are having problems accessing it or need a more recently updated list, please let me know 

import os, datetime, glob, re
import sys, getopt   # these are needed to accept external arguments

## helper functions


def help():
    ''' Print out a help message and exit '''
    print '''Takes the following arguments:\n           
              -v / --variable    CMIP5 variable  ex tas\n           
              -m / --model       CMIP5 model     ex GFDL-CM3\n           
              -e / -- experiment CMIP5 experiment ex historical\n           
              -t / --mip_table   CMIP5 MIP table   ex Amon\n           
              -f / --frequency   valid values are: day, mon, yr, 3hr, 6hr, subhr, fx, clim\n           
              -h / --help display this message and exit \n           
              output file , this should always come last, arguments passed after this will be ignored\n
              All other arguments  can be repeated, for example to select two variables  -v tas -v tasmin. All arguments are optional, failing to input any argument will return the entire dataset.\n
              The script return all the ensembles satifying the constraints\n
              [var1 OR var2 OR ..] AND [model1 OR model2 OR ..] AND [exp1 OR exp2 OR ...] \n
              AND [mip1 OR mip2 OR ...] \n
              Frequency add all the correspondent mip_tables to the mip_table list\n
              If a constraint isn't specified for one of the fields automatically all values for that field will be selected.
    '''
    sys.exit()


def file_details(fname):
    ''' Split the filename in variable, MIP code, model, experiment, ensemble (period is excluded) ''' 
    namebits = fname.split('_')
    if len(namebits) >= 5:
      details = namebits[0:5] 
    else:
      details = []
    return details


def match_constraints(details,cons):
    ''' If the variable and experiment match the constraints add file to list '''
    con_num = len(cons)
    i = 0
    details_set = set(details)
    add_file = True
    while i <= con_num -1:
       if len(cons[i]) > 0:
         if list(details_set.intersection(cons[i])) == []:
            add_file = False
            break 
       i +=1
    return add_file
    

def find_string(bits,string):
    ''' Returns matching string if found in directory structure '''
    dummy = filter(lambda el: re.findall( string, el), bits)
    if len(dummy) == 0:
        return 'not_specified'
    else:
        return dummy[0]


def assign_frequency(frq):
    ''' Append the cmip5 mip tables corresponding to the input frequency to the listmip0 ''' 
    global mip0
    if frq == 'day':
       mip0 = mip0 + ['day', 'cfDay', 'dayExtras']
    elif frq == 'mon':
       mip0 = mip0 + ['Omon', 'OmonExtras', 'Amon', 'AmonExtras', 'Lmon', 
                 'LmonExtras', 'OImon', 'LImon', 'cfMon', 'aero', 'cfOff']
    elif frq == '3hr':
       mip0 = mip0 + ['3hr', '3hrLev', 'cf3hr', 'cfSites']
    elif frq == '6hr':
       mip0 = mip0 + ['6hr', '6hrPlev', '6hrLev'] 
    elif frq == 'monClim':
       mip0 = mip0 + ['Oclim', 'Lclim', 'Aclim', 'LIclim'] 
    elif frq == 'yr':
       mip0 = mip0 + ['Oyr', 'OyrExtras'] 
    elif frq == 'fx':
       mip0 = mip0 + ['fx'] 
    elif frq == 'subhr':
       mip0 = mip0 + ['cfSites']

# Main program starts here
#set up input file and selected variable (or group of variables) and experiment
#infile is updated every Monday and contains a list of all files replicated on dcc 
infile = '/home/548/lih548/esg-tree-LATEST-paths.txt'
# assign default values to constraints
var0 = []
exp0 = [] 
mod0 = []
mip0 = []
outfile = 'CMIP5_files_in_tree.csv'

# assign constraints from arguments list
letters = 'v:m:e:t:f:h' # the : means an argument needs to be passed after the letter
#the = means that a value is expected after the keyword
keywords = ['variable=', 'model=', 'experiment=', 'mip_table=', 'frequency=', 'help='] 
opts, extraparams = getopt.getopt(sys.argv[1:],letters,keywords) 
# starts at the second element of argv since the first one is the script name
# extraparams are extra arguments passed after all option/keywords are assigned
# in this case the output file
# opts is a list containing the pair "option"/"value"
for o,p in opts:
  if o in ['-v','--variable']:
     var0.append(p)
  elif o in ['-m','--model']:
     mod0.append(p)
  elif o in ['-e','--experiment']:
     exp0.append(p)
  elif o in ['-t','--mip']:
     mip0.append(p)
  elif o in ['-f','--frequency']:
     frq = p
     assign_frequency(frq) 
  elif o in ['-h','--help']:
     help() 
for p in extraparams:
    outfile = p 
 
# join constraints in a list
constraints = [var0, mod0, exp0, mip0]
for i in range(len(constraints)):
    print keywords[i] + ":  " + str(constraints[i])
print 'Output file: ' + outfile
    
### needs to include something to take care of all decadals
inf = open(infile, 'r')
outf = open(outfile, 'w')
line1 = 'variable,mip_table,model,experiment,ensemble,version,path\n'
outf.write(line1)


# read all the file paths line by line
lines = inf.readlines()

# define a valid pattern for version
version = '[a-z]*201[0-9][0-1][0-9][0-3][0-9]'
# out_lines is a set of unique lines to add as output, 1 line for each ensemble
out_lines = set()
# loops through all the files
for filepath in lines[:]:
    filepath.replace('\n','')
    bits = filepath.split('/')
    fname = (bits[-1])[:-1]
# call file_details to retrieve experiment, variable, model etc. from filename 
    details = file_details(fname)
# make sure details list isn't empty 
    if len(details) > 0:
# if file details satisfies the constraint file added to output 
      if match_constraints(details,constraints):
         vers = find_string(bits[:-1], version)
         newpath = "/projects/ua6/unofficial-ESG-replica/tmp/tree/" + '/'.join(bits[:-1]) 
         slist = details + [vers, newpath]
         sline = ','.join(slist)
         out_lines.add(sline)
    
# write to output file
for sline in out_lines:
    outf.write(sline+"\n")

# close input and output file
inf.close
outf.close

    

