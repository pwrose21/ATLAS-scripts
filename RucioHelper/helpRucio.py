#!/usr/bin/env python

"""
NAME
    helpRucio.py - given a text file containing a separate dataset on each line,
                   help rucio perform bulk actions

SYNOPSIS
    helpRucio.py [OPTIONS]

DESCRIPTION

OPTIONS
    -h, --help
        Prints this message and exits

    -f FILE, --file=FILE

    -s, --size
        Return the total size of all datasets

    -d, --download
        Download the list of datasets

    -v, --verbose
        Use verbose output

    -p, --path
        Download datasets to this path
        
    -n, --ndownloader
        Number of concurrent file downloads for a given Rucio process

    -N, --nJobs
        Number of concurrent Rucio processes

AUTHOR
    Peyton Rose <prose@ucsc.edu>

"""

## /////// ##
## imports ##
## /////// ##

import os, sys, commands
import getopt, subprocess

# -------------------------------------------------------------------------------------

## /////// ##
## globals ##
## /////// ##

# input file ------------------
dataset_file = ""

# retrive size of datasets? ---
getSize = False

# download datasets? ----------
doDownload = False
path = ""
ndownloader = 0

# list of datasets ------------
datasets = []

# lots of text? ---------------
verbose = False

# number of rucio jobs --------
nJobs = 1

# convert byte to GB ----------
GB = 1e9


def main(argv):

    ParseCommandLineOptions(argv)
    print "\nINFO : This is how I am configured:\n"
    print "       Input file            :", dataset_file
    print "       Get size?             :", boolToText(getSize)
    print "       Download?             :", boolToText(doDownload)
    print "       Download path         :", path
    print "       Concurrent downloads  :", ndownloader
    print "       Concurrent rucio jobs :", nJobs
    print "       Verbose?              :", boolToText(verbose)
    print "\n"

    if dataset_file:
        FillListOfDatasets()
    else:
        sys.exit("ERROR : No input file present.  Please use \"-f FILE\" option. Exiting\n")

    if not datasets:
        sys.exit("ERROR : No datasets found.  Please use a valid file.\n")

    size = 0
    if getSize:
        print "INFO : Getting the total size of all", len(datasets), "datasets" 
        print "       This will take ~", len(datasets) *470./329. , "seconds\n"
        size = GetTotalSizeOfDatasets()
        print "\nINFO : The total size of all datasets in this list is:", float(size), "GB"
        
    if doDownload:
        DownloadListOfDatasets()

    sys.exit("")

def boolToText(bool):
    if bool:
        return "yes"
    return "no"

def FillListOfDatasets():
    global datasets

    f = open(dataset_file, 'r')
    for line in f:
        line = line.strip()
        datasets.append(line)

def GetTotalSizeOfDatasets():
    total_size = 0
    """
    for iDS in datasets:
        cmdListFiles = 'rucio list-files ' + iDS
        if verbose:
            status, output = printAndGetStatusOutput(cmdListFiles)
        else:
            status, output = commands.getstatusoutput(cmdListFiles)
        size = int(output.split("Total size : ")[1].strip()) / GB
        if verbose:
            print "The size of this dataset is:", size, "GB"
        total_size = total_size + size
    """
    myProcs = []
    for iDS in datasets:
        while len(myProcs) >= nJobs:
            if verbose:
                print "Number of active jobs:", len(myProcs)
            #os.system("sleep 0.1")
            pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
            for iProc in myProcs:
                if iProc.pid in pids:
                    continue
                output, err = iProc.communicate()
                size = int(output.split("Total size : ")[1].strip()) / GB
                if verbose:
                    print "The size of this dataset is:", size, "GB"
                total_size = total_size + size
                myProcs.remove(iProc)
                break
        cmdListFiles = 'rucio list-files ' + iDS
        cmdList = cmdListFiles.split(" ")
        myProcs.append(subprocess.Popen(cmdList,stdout=subprocess.PIPE))
    # finish tallying remaining processes
    while len(myProcs) > 0:
        if verbose:
            print "Number of active jobs:", len(myProcs)
        #os.system("sleep 2")
        pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
        for iProc in myProcs:
            if iProc.pid in pids:
                continue
            output, err = iProc.communicate()
            size = int(output.split("Total size : ")[1].strip()) / GB
            if verbose:
                print "The size of this dataset is:", size, "GB"
            total_size = total_size + size
            myProcs.remove(iProc)
            break
    """    
    # implementation using Popen for concurrent size checks
    myProcs = []
    DEVNULL = open(os.devnull, 'wb')
    for iDS in datasets:
        while len(myProcs)>=nJobs:
            if verbose:
                print "Number of active jobs:", len(myProcs)
            os.system("sleep 10")
            pids = [pid for pid in os.listdir('/proc') if pid.isdigit()]
            for iProc in myProcs:
                if not iProc.pid in pids:
                    #output, error = iProc.communicate()
                    #print output
                    print iProc.stdout
                    #total_size = total_size + int(output.split("Total size : ")[1].strip()) / GB
                    myProcs.remove(iProc)
        cmdListFiles = 'rucio list-files ' + iDS
        cmdList = cmdListFiles.split(" ")
        myProcs.append(subprocess.Popen(cmdList,stdin=None, stdout=subprocess.PIPE, stderr=None))
    """
    return total_size


def DownloadListOfDatasets():
    myPIDs = set([])
    for iDS in datasets:
        while len(myPIDs)>=nJobs:
            if verbose:
                print "Number of active jobs:", len(myPIDs)
            os.system("sleep 10")
            pids = set([pid for pid in os.listdir('/proc') if pid.isdigit()])
            myPIDs = myPIDs & pids
        cmdDownload = 'rucio download '
        if path:
            cmdDownload = cmdDownload + '--dir ' + path + ' '
        if ndownloader:
            cmdDownload = cmdDownload + '--ndownloader ' + str(ndownloader) + ' '
        cmdDownload = cmdDownload + iDS
        cmdList = cmdDownload.split(" ")

        if verbose:
            print cmdDownload
            myPIDs.add(subprocess.Popen(cmdList).pid)
        else:
            myPIDs.add(subprocess.Popen(cmdList).pid)
    return

def ParseCommandLineOptions(argv):
    global dataset_file
    global getSize
    global doDownload
    global verbose
    global path
    global ndownloader
    global nJobs

    _short_options = 'hf:sdvp:n:N:'
    _long_options  = ['help', 'file=', 'size', 'download', 'verbose', 'path=', 'ndownloader=', 'nJobs=']
    try:
        opts, args = getopt.gnu_getopt(argv, _short_options, _long_options)
    except getopt.GetoptError:
        print 'getopt.GetoptError\n'
        print __doc__
        sys.exit(2)

    for opt, val in opts:
        if opt in ('-h', '--help'):
            print __doc__
            sys.exit()
        if opt in ('-f', '--file'):
            dataset_file = val
        if opt in ('-s', '--size'):
            getSize = True
        if opt in ('-d', '--download'):
            doDownload = True
        if opt in ('-v', '--verbose'):
            verbose = True
        if opt in ('-p', '--path'):
            path = val
            if not path.startswith('/'):
                path = os.getcwd() + '/' + path
        if opt in ('-n', '--ndownloader'):
            ndownloader = val
        if opt in ('-N', '--nJobs'):
            nJobs = int(val)
            if nJobs > 10:
                print "DON'T BE GREEDY! Limiting number of jobs to 10\n"
                nJobs = 10
    return
    
def printAndRun(cmd):
    print "\n",cmd
    os.system(cmd)

def printAndGetStatusOutput(cmd):
    print "\n",cmd
    return commands.getstatusoutput(cmd)

if __name__ == '__main__':
    main(sys.argv[1:])
