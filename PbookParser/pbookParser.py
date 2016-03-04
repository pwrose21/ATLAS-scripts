
## /////// ##
## imports ##
## /////// ##

import os, sys, commands
import datetime


# ---------------------------------------------------------------------------


## /////// ##
## globals ##
## /////// ##

idFlag = "HtX4Tops_00-00-01"     
autoRetry = True
newSite = False
newOpts = {}#{'memory': 2000}
skipLogFiles = True
logFileIdentifier = '.log'

status_done = ['done']
status_active = ['registered', 'defined', 'pending', 'ready',
                 'assigning', 'scouting', 'scouted', 
                 'throttled','running', 'prepared']
status_retry = ['failed', 'finished', 'exhausted']
status_broken = ['tobroken', 'broken', 'aborting', 'aborted']


# ----------------------------------------------------------------------------


def main():

    # check that pbook is setup
    if not CheckForPBook():
        sys.exit("\n    ERROR::pbook not available.  Please execute $ lsetup panda\n")
    
    # create the pbook log
    f = GetPBookLog()
    
    # get the list of jobs
    myJobs = GetJobsFromPBookLog(f)

    # useful output?
    print "\nINFO : Number of jobs: ", len(myJobs), ".  How many did you expect?\n\n"
    print "INFO : Present statuses:"
    statusList = set([])
    for iJob in myJobs:
        status = iJob['taskStatus']
        if not status in statusList:
            print "         " + status
            statusList.add(status)
    print "\n"

    # sort and write output
    SortJobsandWriteOutput(myJobs)

    sys.exit("")

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
    print "INFO : Sorting jobs according to 'taskStatus'"
    print "       Jobs with status in", status_done, "are considered successful"
    print "       Jobs with status in", status_active, "are considered still active"
    print "       Jobs with status in", status_retry, "are considered failed, but can be retried"
    print "       Jobs with status in", status_broken, "are considered failed, and need to be resubmitted"

    doneJobs = []
    activeJobs = []
    retryJobs = []
    brokenJobs = []
    for iJob in myJobs:
        status = iJob['taskStatus']
        if status in status_done:
            doneJobs.append(iJob)
        if status in status_active:
            activeJobs.append(iJob)
        if status in status_retry:
            retryJobs.append(iJob)
        if status in status_broken:
            brokenJobs.append(iJob)
        if not status in (status_done + status_active + status_retry + status_broken):
            print "ERROR : Unrecognized status --", status

    if len(myJobs) != len(doneJobs + activeJobs + retryJobs + brokenJobs):
        print "WARNING : Inital number of jobs:", len(myJobs)
        print "          Sorted number of jobs:", len(doneJobs + activeJobs+ retryJobs + brokenJobs)


    print "\nINFO : Total number of jobs         : " + str(len(myJobs)) 
    print "         Number of done jobs        : " + str(len(doneJobs)) 
    print "         Number of active jobs      : " + str(len(activeJobs)) 
    print "         Number of jobs to retry    : " + str(len(retryJobs)) 
    print "         Number of jobs to resubmit : " + str(len(brokenJobs))
    print "\n"

    # list of datasets to download
    download_list = open("datasets_to_download", 'w')    
    for iJob in doneJobs:
        outDS = iJob['outDS']
        outDS = outDS.split(',')
        for iDS in outDS:
            if skipLogFiles and logFileIdentifier in iDS:
                continue
            download_list.write(iDS + '\n')
    download_list.close()

    retry_cmd     = open("retry_commands.sh", 'w')
    for iJob in retryJobs:
        cmdRetry = MakeRetryCommand(iJob)
        retry_cmd.write("echo \"" + cmdRetry.replace('"', "'") + "\"\n")
        retry_cmd.write(cmdRetry+'\n')
    retry_cmd.close()

    params        = open("broken_params.sh", 'w')
    for iJob in brokenJobs:
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
    main()
