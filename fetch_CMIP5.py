# Paola Petrelli - paolap@utas.edu.au 4th March 2014
# Last changed on 26th of March 2014
# Updates list:
#   26/03/2014 - output files and google spreadsheet are created after 
#                 collecting data; calling process_file with multiprocessing 
#                module to speed up md5checksum
#   01/04/2014 - exclude the ACCESS and CSIRO models from check
#
# This script searches on pcmdi9.llnl.gov for all CMIP5 files responding to the constraints given as input.
# It returns 3 files listing: the published files available on raijin (variables_replica.csv), the published files that need downloading and/or updating (variables_to_download.csv), the variable/model/experiment combination not yet published (variables_not_published).
# Uses md5 checksum to determine if a file already existing on raijin is exactly the same as the latest published version
# If the "google" option is selected it returns also a google spreadsheet summarising the search results. 
# If using the google option, you will need a google drive account and to install the python packages gdata and gspread
# To download and install on raijin:
#    git clone https://github.com/burnash/gspread.git
#    cd gspread
#    module unload intel-fc intel-cc
#    module load python
#    python setup.py install --user
#    cd 
#    wget https://gdata-python-client.googlecode.com/files/gdata-2.0.18.tar.gz
#    tar -xvzf gdata-2.0.18.tar.gz
#    cd gdata-2.0.18
#    python setup.py install --user
#
# The CMIP5 replica data is stored on raijin.nci.org.au under
# /g/data1/ua6/unofficial-ESG-replica/tmp/tree
#
# Example of how to run on raijin.nci.org.au
#
#    module load python/2.7.3  (default on raijin)
#    python fetch_CMIP5.py  -v ua_Amon -v tos_Omon -m CCSM4 -e rcp45 -g out
# NB needs python version 2.7 or more recent
#
#  - the variable argument is passed as variable-name_cmip-table, this avoids confusion if looking for variables from different cmip tables
#  - multiple arguments can be passed to "-v", "-m", "-e";
#  - to pass multiple arguments, declare the option multiple times (as above);
#  - default output root is variables_
#  - you can pass a different name for the root of the output files, just by 
#    listing it as last argument (out in the example);
#  - you need to pass at least one experiment and one variable, models are optional.

import sys, getopt, urllib, time
import subprocess, re, itertools
from multiprocessing import Pool
import os.path as opath     # to manage files and dirs

# help functions
def help():
    ''' Print out a help message and exit '''
    print '''\n
   Takes the following arguments:\n
   -v / --variable    combination of CMIP5 variable & cmip_table Ex. tas_Amon\n
   -e / --experiment  CMIP5 experiment  Ex. historical\n
   -m / --model       CMIP5 model     ex GFDL-CM3\n
   -g / --google      google table option\n
   -h / --help        display this message and exit \n
   output_file        this should always come last, arguments passed after this
                      will be ignored\n
   - multiple arguments can be passed to "-v", "-m", "-e";\n
   - to pass multiple arguments, declare the option multiple times;\n 
   - you need to pass at least one experiment and one variable,
     models are optional.\n
    '''
    sys.exit()


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


def create_wget(exp,modlist,varmips):
    ''' create wget call for each query (ie each variable/exp/model combination) '''
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
    pcmdi_url = "https://pcmdi9.llnl.gov/esg-search/wget?experiment=" + exp + "&cmor_table=" + mips + "&project=CMIP5" + models + "&variable=" + variables + "&replica=false&latest=true"
    print pcmdi_url 
    wgetfile = "wgetfetch2.out"
    urllib.urlretrieve(pcmdi_url,wgetfile)
    print "Finished downloading wget file from pcmdi9.llnl.gov"
    return wgetfile 

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
    ''' Build a matrix of the results to output to google table '''
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
    # write a matrix to pass results to google table in suitable format
    if google: result_matrix(comb_query.difference(nopub_set),exp0)
    return nopub_set 


def write_google(sh,gc,gname,nopub):
    ''' write a google spreadsheet table to summarise search '''
    import gspread
    global gmatrix
    tic = time.time()
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
    # open/create a worksheet for each experiment
        try:
           work = sh.worksheet(exp) 
        except:
           work = sh.add_worksheet(title=exp,rows=str(nrow),cols=str(ncol))
      # pre-fill all values with "NP", leave 1 column and 1 row for headers 
        print "%f s up to  create a worksheet" % (time.time() - tic)
        [work.update_cell(i,j,"NP") for j in range(3,ncol+1) for i in range(2,nrow+1)]
        print "%f s up to fill NP " % (time.time() - tic)
      # write first two columns with all (mod,ens) pairs
        col1= [emat[var][i][0] for var in klist for i in range(len(emat[var])) ]
        col1 = list(set(col1))
        col1_sort=sorted(col1)
        row={}
        ind=1
      # write first column with mod_ens combinations & save row indexes in dict where keys are (mod,ens) combination
        for modens in col1_sort:
            ind+=1
            work.update_cell(ind,1,modens[0])  
            work.update_cell(ind,2,modens[1])  
            row[modens]=ind  
      # start writing var_mip column one by one
        for var in klist:
            x = klist.index(var)
            work.update_cell(1,x+3,var)
            for item in emat[var]:
                if item[0] in row.keys(): 
                   work.update_cell(row[item[0]],x+3,item[1])
      # write header for variables never published
        if len(evar) > 0:
          [work.update_cell(1,ncol-evar.index(var),var) for var in evar]
    # delete the automatically created "Sheet 1" if exists
    print "%f s up to written table " % (time.time() - tic)
    sh2 = gc.open(gname)
    try: 
         ws = sh2.worksheet("Sheet 1")
         sh2.del_worksheet(ws)
    except gspread.exceptions.WorksheetNotFound:
         print "No Sheet 1 to delete"
    return


def create_google():
    ''' create a google spreadsheet if doesn't exists 
        copied from http://diorsman.me/2013/11/05/create-new-google-spreadsheet-in-python/ '''
    import gspread
    import gdata.docs.client
    import getpass 
    # if using in batch these 3 needs to be given not interactively 
    email = raw_input("Google drive account: ")
    password = getpass.getpass("Google drive password: ")
    gname = raw_input("Spreadsheet name: ")
    try:
      gc = gspread.login(email, password)
    except BadAuthentication:
      print "Username or password are wrong"
      sys.exit()
    except NotVerified:
      print "Account not verified, user need to acces Google account directly first"
      sys.exit()
    except ServiceUnavailable:
      print "Service unavailable, try again later"
      sys.exit()
     
#Open Spreadsheet, if doesn't exist then create a new one
    try:
        spr = gc.open(gname)
        print "Spreadsheet exists, it will be overwritten: ", gname 
    except:
        source = 'CMIP5 Spreadsheet'
        gd_client = gdata.docs.client.DocsClient()
        gd_client.ClientLogin(email, password, source)
        document = gdata.docs.data.Resource(type='spreadsheet', title=gname)
        document = gd_client.CreateResource(document)
        print 'Created Spreadsheet: '+ gname
        spr = gc.open(gname)
    return spr,gc,gname 


def assign_constraint():
    ''' Assign default values and input to constraints '''
    global var0, exp0, mod0, google, outfile
    var0 = []
    exp0 = []
    mod0 = []
    outfile = 'variables'
#  google option is False by default, activate to create a summary google table
    google = False
# assign constraints from arguments list
    letters = 'v:m:e:gh' # the : means an argument needs to be passed after the letter
#the = means that a value is expected after the keyword
    keywords = ['variable=', 'model=', 'experiment=', 'google', 'help']
    opts, extraparams = getopt.getopt(sys.argv[1:],letters,keywords)
# starts at the second element of argv since the first one is the script name
# extraparams are extra arguments passed after all option/keywords are assigned
# in this case the first part of the output file
# opts is a list containing the pair "option"/"value"
# if no variable or experiment are defined issue a warning
    for o,p in opts:
       if o in ['-v','--variable']:
         if "_" in p:
            var0.append(p)
         else:
            print "Warning: -v/--variable takes var_miptable as input" 
            sys.exit()
       elif o in ['-m','--model']:
         mod0.append(p)
       elif o in ['-e','--experiment']:
         exp0.append(p)
       elif o in ['-g','--google']:
         google = True 
       elif o in ['-h','--help']:
         help()
    for p in extraparams:
       outfile = p
    if len(var0) == 0 or len(exp0) == 0 : 
       print "Error: at least 1 variable and 1 experiment need to be defined"
       sys.exit()
    return


def main():
    ''' Main program starts here '''
    global opub, odown, orep, info
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
        wgetfile=create_wget(exp,mod0,var0)
        result=parse_file(wgetfile,var0,mod0,exp)
# if found any files matching constraints, process them one by one
# using multiprocessing Pool to parallelise process_file 
        tic = time.time()
        if result:
           async_results = Pool().map_async(process_file, result)
           for dinfo in async_results.get():
               info.update(dinfo)
        print "%f s for parallel computation." % (time.time() - tic)
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
    print "%f s up to written files" % (time.time() - tic)
# if google option create/open spreadsheet
# if google option write summary table on google spreadsheet
    if google: 
       (gsheet,gc,gname) = create_google()
       print "%f s up to icreate google" % (time.time() - tic)
       write_google(gsheet,gc,gname,nopub_set)

# call main()
main()
