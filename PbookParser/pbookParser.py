
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


# ----------------------------------------------------------------------------

def main():

    # check that pbook is setup
    if not CheckForPBook():
        sys.exit("\n    ERROR::pbook not available.  Please execute $ lsetup panda\n")
    
    # create the pbook log
    f = OpenPBookLog()
    
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

    # 


    sys.exit("Debugging")


    
def printAndRun(cmd):
    print '\n'+cmd
    os.system(cmd)

def CheckForPBook():
    status, output = commands.getstatusoutput("which pbook")
    return (status==0)

def OpenPBookLog(f = 'pbook_show_out.txt'):
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
    


#print "rm pbook_show_out.txt"
#os.system("rm pbook_show_out.txt")

#exhaustive list of status :
# registered, defined, assigning, ready, pending,
# scouting, scouted, running, prepared, done, failed,
# finished, aborting, aborted, finishing, topreprocess,
# preprocessing, tobroken, broken, toretry, toincexec,
# rerefine, paused, throttled, exhausted, passed

# jobs finished fine, ready to download
# status in ['done']
doneDSs = []

# jobs fid not finish properly, can be retried
#   intervention automatic in script
# status in ['failed', 'finished', 'exhausted']
retryIDs = []
retryDSs = []

# jobs currently running
# status in ['registered', 'defined', 'pending', 'ready', 'assigning', 'scouting', 'scouted', 'throttled', 'running', 'prepared']
activeIDs = []
activeDSs = []

# jobs broken. need manual intervention
# status in ['tobroken', 'broken', 'aborting', 'aborted']
brokenDSs = []
brokenParams = []

otherDSs = []
otherIDs = []


## sort jobs based on status
## extract the useful information
for iJob in myJobs:
    if not idFlag in iJob['outDS']:
        print 'ERROR::FLAG NOT FOUND IN OUTDS!!!'
        print 'Problem in job filtering for job:' , iJob
        continue

    status = iJob['taskStatus']
    taskID = iJob['jediTaskID']
    inDS   = iJob['inDS']
    outDS  = iJob['outDS']
    params = iJob['params']

    if status in ['done']:
        doneDSs.append(outDS)
    elif status in ['failed', 'finished', 'exhausted']:
        retryIDs.append(taskID)
        retryDSs.append(inDS)
    elif status in ['tobroken', 'broken', 'aborting', 'aborted']:
        brokenDSs.append(inDS)
        brokenParams.append(params)
    elif status in ['registered', 'defined', 'pending', 'ready', 
                    'assigning', 'scouting', 'scouted', 'throttled', 
                    'running', 'prepared']:
        activeIDs.append(taskID)
        activeDSs.append(inDS)
    else:
        print "Status: " , status, "not recognized!!!"

## can automatically retry jobs
for i in retryIDs:
    cmdRetry = 'pbook -c "retry(' + str(i)
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
    print cmdRetry
    if autoRetry:
        os.system(cmdRetry)
    else:
        print "autoRetry OFF!!! Command not executed!\n\n"
        
## make output file summaries
f = open("retry_params.txt", 'w')
for iparam in brokenParams:
    f.write(iparam.split("--inDS=")[1].split(" --outDS=")[0] + '\n')
    f.write(iparam + '\n\n')
f.close()

f = open("job_summary.txt", 'w')
f.write("These jobs have finished and can be downloaded!\n")
f.write("-----------------------------------------------\n")
for iDS in doneDSs:
    f.write(iDS + '\n')

f.write("\nThese jobs are actively running:\n")
f.write(  "--------------------------------\n")
for iD in activeIDs:
    f.write(iD + '\n')

if autoRetry:
    f.write("\nThese jobs were retried:\n")
    f.write(  "------------------------\n")
else:
    f.write("\nThese jobs should be retried:\n")
    f.write(  "-----------------------------\n")
for iD in retryIDs:
    f.write(iD + '\n')

f.write("\nThese jobs are broken and should be resubmitted:\n")
f.write(  "------------------------------------------------\n")
for iDS in brokenDSs:
    f.write(iDS + '\n')
f.close()

sys.exit("Debugging")




#activeIDs = activeIDs - doneIDs
#retryIDs = retryIDs - (doneIDs | activeIDs)
#retry(15,newSite=True,newOpts={'nGBperJob':10})


print "active IDs"
print activeIDs


print "retry IDs"
print retryIDs

# some datasets may have been submitted multipls times
# if initial jobs were broken
doneDSs = set(doneDSs)
activeDSs = set(activeDSs)
retryDSs = set(retryDSs)
brokenDSs = set(brokenDSs)

activeDSs = activeDSs - doneDSs
retryDSs = retryDSs - (doneDSs | activeDSs)
brokenDSs = brokenDSs - (doneDSs | activeDSs | retryDSs)

f = open('job_summary.txt', 'w')
f.write("The following datasets have been analyzed successfully:\n")
f.write("-------------------------------------------------------\n")
for i in doneDSs:
    f.write(i + '\n')

f.write("\nThe following datasets are actively running:\n")
f.write(  "--------------------------------------------\n")
for i in activeDSs:
    f.write(i + '\n')

f.write("\nThe following datasets were retried:\n")
f.write(  "------------------------------------\n")
for i in retryDSs:
    f.write(i + '\n')

f.write("\nThe following datasets are broken and NEED MANUAL INTERVENTION:\n")
f.write(  "---------------------------------------------------------------\n")
for i in brokenDSs:
    f.write(i + '\n')

f.close()
