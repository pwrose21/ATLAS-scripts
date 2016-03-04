#!/usr/bin/env python

"""
NAME
    pbookParser.py - given a job set identifier, parses the output of pbook 
                     into a useful format

SYNOPSIS
    pbookParser.py [OPTIONS]

DESCRIPTION
    

OPTIONS
    -h, --help
        Prints this message and exits

    -i JOBSETIDENTIFIER, --identifier=JOBSETIDENTIFIER
        A unique identifier for a set of jobs that is present in the outDS name

    -n, --newSite
        Use the 'newSite=True' option when writing the retry(jediTaskID) commands

    -o OPT1KEY:OPT1VALUE:[OPT2KEY:OPT2VALUE], --newOpts=OPT1KEY:OPT1VALUE:[OPT2KEY:OPT2VALUE]
        Use the 'newOpts={"OPT1KEY" : OPT1VALUE, "OPT2KEY" : OPT2VALUE}' option
        when writing the retry(jediTaskID) commands

    -e PATTERN1[,PATTERN2,...,], --exclude PATTERN1[,PATTERN2,...,]
        Exclude datasets with this pattern when writing download list, e.g. files containing ".log"

AUTHOR
    Peyton Rose <prose@ucsc.edu>

"""
## /////// ##
## imports ##
## /////// ##

import os, sys, commands 
import getopt
import datetime


# ----------------------------------------------------------------------------------


## /////// ##
## globals ##
## /////// ##

# jobset identifier --------
idFlag = "HtX4Tops_00-00-01"     

# retry options -------------
newSite = False
newOpts = {}#{'memory': 2000}

# download options -------
skipLogFiles = True
logFileIdentifier = ['.log']

# sorting options --------------------------------------------------------------
status_done = ['done'] # jobs to be downloaded
status_active = ['registered', 'defined', 'pending', 'ready',
                 'assigning', 'scouting', 'scouted', 
                 'throttled','running', 'prepared'] # jobs to be left alone
status_retry = ['failed', 'finished', 'exhausted'] # jobs to retry
status_broken = ['tobroken', 'broken', 'aborting', 'aborted'] # jobs to resubmit


# ----------------------------------------------------------------------------------


def main(argv):

    ParseCommandLineOptions(argv)

     ## check configuration
    print "\nINFO : This is how I am configured : "
    print "       Jobset identifier             :", idFlag
    print "       Use new site on retry?        :", boolToYN(newSite)
    print "       Excluded download identifiers :", logFileIdentifier 
    print "       New opts for retry            :", newOpts


    # check that pbook is setup
    if not CheckForPBook():
        sys.exit("\n    ERROR::pbook not available.  Please execute $ lsetup panda\n")
    
    # create the pbook log
    f = GetPBookLog()
    
    # get the list of jobs
    myJobs = GetJobsFromPBookLog(f)

    # sort and write output
    SortJobsandWriteOutput(myJobs)

    sys.exit("")

def boolToYN(bool):
    if bool:
        return "yes"
    return "no"

def ParseCommandLineOptions(argv):

    global idFlag
    global newSite
    global newOpts
    global skipLogFiles
    global logFileIdentifier

    ## parse options
    _short_options = 'hni:o:e:'
    _long_options = ['help', 'newSite', 'identifier=', 'newOpts=','exclude=']
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
        if opt in ('-n', '--newSite'):
            newSite=True
        if opt in ('-i', '--identifier'):
            idFlag=val
        if opt in ('-e', '--exclude'):
            skipLogFiles = True
            logFileIdentifier = val.split(',')
        if opt in ('-o', '--newOpts'):
            val = val.split(':')
            if len(val)%2 == 1:
                print "ERROR : New opt must have a key AND value. Expected even"
                print "        number of opts, but got odd. Ignoring new opts"
            else:
                for i, v in enumerate(val):
                    if i%2 == 1:
                        continue
                    try:
                        val[i+1] = int(val[i+1])
                    except:
                        pass
                    newOpts[v] = val[i+1]


def MakeRetryCommand(iJob):
    cmdRetry = 'pbook -c "retry(' + iJob['jediTaskID']
    if newSite:
        cmdRetry = cmdRetry + ',newSite=True'
    if len(newOpts)>0:
        cmdRetry = cmdRetry + ',newOpts={'
        for i,key in enumerate(newOpts):
            if i==0:
                cmdRetry = cmdRetry + '\'' + key + '\':' + str(newOpts[key])
            else:
                cmdRetry = cmdRetry + ',\'' + key + '\':' + str(newOpts[key])
        cmdRetry = cmdRetry + '}'
    cmdRetry = cmdRetry + ')"'
    return cmdRetry


def SortJobsandWriteOutput(myJobs):
    print "\nINFO : Sorting jobs according to 'taskStatus'"
    print "       Jobs with status in", status_done, "are considered successful"
    print "       Jobs with status in", status_active, "are considered still active"
    print "       Jobs with status in", status_retry, "are considered failed, but can be retried"
    print "       Jobs with status in", status_broken, "are considered failed, and need to be resubmitted"

    doneJobsIdx = []
    activeJobsIdx = []
    retryJobsIdx = []
    brokenJobsIdx = []
    statusList = set([])
    for i,iJob in enumerate(myJobs):
        status = iJob['taskStatus']
        statusList.add(status)
        if status in status_done:
            doneJobsIdx.append(i)
        if status in status_active:
            activeJobsIdx.append(i)
        if status in status_retry:
            retryJobsIdx.append(i)
        if status in status_broken:
            brokenJobsIdx.append(i)
        if not status in (status_done + status_active + status_retry + status_broken):
            print "ERROR : Unrecognized status --", status

    if len(myJobs) != len(doneJobsIdx + activeJobsIdx + retryJobsIdx + brokenJobsIdx):
        print "WARNING : Inital number of jobs:", len(myJobs)
        print "          Sorted number of jobs:", len(doneJobsIdx + activeJobsIdx + retryJobsIdx + brokenJobsIdx)

    print "\nINFO : The following statuses were found in this jobset:"
    print "       ", statusList

    print "\nINFO : Total number of jobs         : " + str(len(myJobs)) 
    print "         Number of done jobs        : " + str(len(doneJobsIdx)) 
    print "         Number of active jobs      : " + str(len(activeJobsIdx)) 
    print "         Number of jobs to retry    : " + str(len(retryJobsIdx)) 
    print "         Number of jobs to resubmit : " + str(len(brokenJobsIdx))
    print "\n"

    # list of datasets to download
    download_list = open("datasets_to_download.txt", 'w')    
    for i in doneJobsIdx:
        iJob = myJobs[i]
        outDS = iJob['outDS']
        outDS = outDS.split(',')
        for iDS in outDS:
            if skipLogFiles and any(a in iDS for a in logFileIdentifier):
                continue
            download_list.write(iDS + '\n')
    download_list.close()

    retry_cmd     = open("retry_commands.sh", 'w')
    for i in retryJobsIdx:
        iJob = myJobs[i]
        cmdRetry = MakeRetryCommand(iJob)
        retry_cmd.write("echo \"" + cmdRetry.replace('"', "'") + "\"\n")
        retry_cmd.write(cmdRetry+'\n')
    retry_cmd.close()

    params        = open("broken_params.sh", 'w')
    for i in brokenJobsIdx:
        iJob = myJobs[i]
        jobParams = iJob['params']
        jobParams = jobParams.replace('outTarBall', 'inTarBall')
        params.write("echo \"" + jobParams.replace('"', "'") + "\"\n")
        params.write(jobParams)
    params.close()

    print "\nINFO : The datasets that should be downloaded have been written to: datasets_to_download.txt"
    print "INFO : The jobs that should be retried can be retried with $ source retry_commands.sh"
    print "INFO : The jobs that should be resubmitted are written to broken_params.sh"
    print "       WARNING : For resubmission, you will need to setup your original   "
    print "         environment, move to your original submit directory, and change"
    print "         the outDS container name!"

    return True

def printAndRun(cmd):
    print '\n'+cmd
    os.system(cmd)

def CheckForPBook():
    status, output = commands.getstatusoutput("which pbook")
    return (status==0)

def GetPBookLog(f = 'pbook_show_out.txt'):
    # sync pbook jobs
    cmdSync = "pbook -c \"sync()\""
    printAndRun(cmdSync)

    #show pbook jobs
    cmdShow = "pbook -c \"show()\" > " + f
    printAndRun(cmdShow)
    return f

def ConvertTimeToDateTime(timeString):
    year = int(timeString.split('-')[0])
    month = int(timeString.split('-')[1])
    day = int(timeString.split('-')[2].split(' ')[0])
    hour = int(timeString.split(' ')[1].split(':')[0])
    minute = int(timeString.split(':')[1])
    second = int(timeString.split(':')[2])
    return datetime.datetime(year, month, day, hour, minute, second)
    

def RemoveDuplicateJobs(thisJob, myJobs, duplKey, timeKey):
    for i,iJob in enumerate(myJobs):
        if not iJob[duplKey] == thisJob[duplKey]:
            continue
        if iJob[timeKey] < thisJob[timeKey]:
            myJobs[i] = thisJob
        return False
    return True

def GetJobsFromPBookLog(f):
    myJobs = []
    m_file = open(f)
    f_iter = iter(m_file)
    for line in f_iter:
        # each new task starts with this line
        if("======================================") in line:
            thisJob = {}
            line = next(f_iter)
            # task ends with a blank line
            # all lines before this have " : " in them
            while " : " in line:
                linesplit = line.split(" : ")
                thisJob[linesplit[0].strip()] = linesplit[1].strip()
                line = next(f_iter)

            # convert times into a useful format
            thisJob['lastUpdate'] = ConvertTimeToDateTime(thisJob['lastUpdate'])
            thisJob['creationTime'] = ConvertTimeToDateTime(thisJob['creationTime'])

            # veto jobs without the idFlat
            if not idFlag in thisJob['outDS']:
                continue
            
            # check for overlaps -- needToAppend flag keeps track
            needToAppend = True
            needToAppend = needToAppend and RemoveDuplicateJobs(thisJob, myJobs, 'jediTaskID', 'lastUpdate')
            needToAppend = needToAppend and RemoveDuplicateJobs(thisJob, myJobs, 'inDS', 'creationTime')

            if needToAppend:
                myJobs.append(thisJob)

    m_file.close()
    return myJobs


if __name__ == '__main__':
    main(sys.argv[1:])
