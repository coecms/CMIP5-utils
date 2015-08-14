# Paola - paolap@utas.edu.au 20th November 2013
# 18 June 2015 modified to produce a sqlite database instead of a csv file
# This script produces a database listing all the CMIP5 ensembles available on raijin.nci.org.au that satisfy the given constraints.
# The CMIP5 replica dataset is stored under 
# /g/data/ua6/unofficial_ESG_replica/tmp/tree
# The file list used as input is updated every Monday or after we downloaded more cmip5 data.
# It creates a sqlite database called CMIP5_database.db, that contains a "cmip5" table 
# with the following fields id (this is actually the ensemble path on raijin and acts as unique index), variable, mip, model, experiment, ensemble, version, for each matching ensemble.
# Example of how to run on raijin.nci.org.au
#
#    module load python
#    python CMIP5_replica_db.py -f mon -v ua tas -e piControl -e historical rcp45 -m bcc-csm1-1 CCSM4 -o output 
#
# Notes concerning the above example: 
#  - multiple arguments can be passed to "-v", "-e", "-m", "-t" and -f"; 
#  - to pass multiple arguments, declare the option once followed by all desired values (as above);
#  - you can pass a different name for the output file, using -o/--output option (output.db in the example); 
#  - all arguments are optional; 
#  - failing to set any constraint will result in the entire dataset being 
#    selected.  
#
# Feel free to copy and modify this script and please report any bugs
#
# NB: This code reads the paths of files under the unofficial replica tree from 
#       /g/data/ua6/unofficial-ESG-replica/tmp/tree/esg-tree-LATEST-paths.txt 
#  This file is updated every Monday 
#  If you are having problems accessing it or need a more recently updated list,
#  please let us know 

import os, datetime, glob, re
import sys, getopt   # these are needed to accept external arguments
import sqlite3, argparse
import itertools as it

## helper functions


def parse_input():
    ''' Parse input arguments '''
    parser = argparse.ArgumentParser(description='''Lists all the CMIP5 ensembles available on raijin and 
             responding to the constraints passed as arguments.
            All arguments, except the output file name,  can be repeated, for example to select two variables:
            -v tas tasmin 
            All arguments are optional, failing to input any argument will return the entire dataset.
            The script returns all the ensembles satifying the constraints
            [var1 OR var2 OR ..] AND [model1 OR model2 OR ..] AND [exp1 OR exp2 OR ...] 
            AND [mip1 OR mip2 OR ...] 
            Frequency adds all the correspondent mip_tables to the mip_table list
            If a constraint isn't specified for one of the fields automatically all values
            for that field will be selected.''')
    parser.add_argument('-e','--experiment', type=str, nargs="*", help='CMIP5 experiment', required=False)
    parser.add_argument('-m','--model', type=str, nargs="*", help='CMIP5 model', required=False)
    parser.add_argument('-v','--variable', type=str, nargs="*", help='CMIP5 variable', required=False)
    parser.add_argument('-t','--mip_table', type=str, nargs="*", help='CMIP5 MIP table', required=False)
    parser.add_argument('-f','--frequency', type=str, nargs="*", help='CMIP5 frequency', required=False)
    parser.add_argument('-o','--output', type=str, nargs=1, help='database output file name', required=False)
    return vars(parser.parse_args())


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


def add_row(tup_details):
    global conn,c
    ''' If found file check if it's already in database, otherwise add to it '''
# Insert a row with file details and commit changes
    details=list(tup_details)
    fpath,var,mip,mod,exp,ens,ver = details
    c.execute("INSERT INTO cmip5(id, variable, mip, model, experiment, ensemble, version) VALUES(?,?,?,?,?,?,?)",
              (fpath,var,mip,mod,exp,ens,ver))
    conn.commit()
    return


def assign_frequency(frq):
    ''' Append the cmip5 mip tables corresponding to the input frequency to the list mip0 ''' 
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


def assign_constraint():
    ''' Assign default values and input to constraints '''
    global var0, exp0, mod0, mip0, dbfile 
# assign constraints from arguments list
    args = parse_input()
    var0=args["variable"]
    if not var0: var0=[]
    mod0=args["model"]
    if not mod0: mod0=[]
    exp0=args["experiment"]
    if not exp0: exp0=[]
    mip0=args["mip_table"]
    if not mip0: mip0=[]
    dbfile=args["output"][0]+ ".db"
    if not dbfile: dbfile = 'CMIP5_database.db' 
    frq0=args["frequency"]
    if frq0: 
       for frq in frq0:
           assign_frequency(frq)
    return


def select_match(constraints):
    ''' pre-select rows from database that match constraints to avoid adding them again to database '''
# still working on this!
    fields=""
    values=[]
    print constraints
    print constraints
    #constraints = filter(bool, constraints)
    #comb_constraints = set(it.product(*constraints))
    #print comb_constraints
# build key, values comb using constraints!
    for key in comb:
        fields+= key+"==? AND "
        values.append(comb[key])
    print fields
    print tuple(values)
    cursor = conn.execute("SELECT * FROM cmip5 where " + fields[:-4], tuple(values))  
    rows=cursor.fetchall()
    print rows
    return rows


def open_db(dbfile):
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    # Create table cmip5 if doesn't exists
    c.execute('''CREATE TABLE IF NOT EXISTS cmip5
             (id text, variable text, mip text, model text, experiment text, ensemble text, version text)''')
    # Save (commit) the changes
    conn.commit()
    c.close()
    conn.close()
    print "Opened database successfully";
    return sqlite3.connect(dbfile)


# Main program starts here
#set up input file and selected variable (or group of variables) and experiment
#infile is updated every Monday and contains a list of all files replicated on dcc 
infile = '/g/data/ua6/unofficial-ESG-replica/tmp/tree/esg-tree-LATEST-paths.txt'
# assign default values to constraints
assign_constraint()
 
# join constraints in a list
constraints = [var0, mod0, exp0, mip0]
print 'Output database: ' + dbfile 
    
### needs to include something to take care of all decadals
# open input file and database
inf = open(infile, 'r')
conn = open_db(dbfile)
c = conn.cursor()

# read all the file paths line by line
lines = inf.readlines()

# define a valid pattern for version
version = '[a-z]*201[0-9][0-1][0-9][0-3][0-9]'
# db_set is a set of unique rows to add to the database, 1 row for each ensemble
db_set = set()
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
         newpath = '/'.join(bits[:-1])
         slist = [newpath] + details + [vers]
         db_set.add(tuple(slist))

# load from database rows that match constraints
# still working on this!!! is commented for the moment
#rows_set = select_match(constraints) 
# the difference between two sets gives rows not yet in database
#notindb_set = rows_set.difference(db_set)

# write to output database 
for row in db_set:
#for row in notindb_set:
    add_row(row)

# close input and output file
inf.close
c.close()
conn.close()

    

