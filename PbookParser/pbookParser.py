import sys, commands, os, datetime


"test"

### This script can be used to parse the output of pbook
### It will create three output files :
###    a list of "done" jobs -- all succeeded
###    a list of "broken" jobs -- these need to 
###      be re-sumitted using the original submit env.
###    a list of "retry" jobs -- these can be
###      retry using retry(jobSetID) within pbook

### takes a pbook job list file as input
###   this can be generated by doing the following
###   $ pbook -c "sync()" ; $ pbook -c "show()" > pbook_log.txt
###   if not present, this will be automatically generated

### change the idFlag to a string contained in the outDS to 
###   select a subset of your panda jobs

### run with : $ python parsePbookOut.py [pbookLogFile]

idFlag = "HtX4Tops_00-00-01"     
autoRetry = False
newSite = True
newOpts = {'memory': 2000}


## ///////////////////////////////////////// ##
## Use existing pbook file from command line ##
##   or create one a new one                 ##
## ///////////////////////////////////////// ##
try:
    f = open(sys.argv[1], 'r')
except:
    print "pbook -c \"sync()\"" 
    os.system("pbook -c \"sync()\"")
    print "pbook -c \"show()\" > pbook_show_out.txt"
    os.system("pbook -c \"show()\" > pbook_show_out.txt")
    f = open("pbook_show_out.txt", 'r')


## ////////////////////////////////// ##
## Extract information about each job ##
##   from the pbook file.  In case of ## 
##   duplicate jediTasks, keep the    ##
##   one most recently updated        ##
## ////////////////////////////////// ##

myJobs = []
f_iter = iter(f)
for line in f_iter:
    # each new task starts with this line
    if("======================================") in line:
        print "\nStarting new job!"
        thisJob = {}
        line = next(f_iter)
        # task ends with a blank line
        # all lines before this have " : " in them
        while " : " in line:
            linesplit = line.split(" : ")
            thisJob[linesplit[0].strip()] = linesplit[1].strip()
            line = next(f_iter)
        
        # convert the lastUpdate time into a datetime entry
        lastUpdate = thisJob['lastUpdate']
        year = int(lastUpdate.split('-')[0])
        month = int(lastUpdate.split('-')[1])
        day = int(lastUpdate.split('-')[2].split(' ')[0])
        hour = int(lastUpdate.split(' ')[1].split(':')[0])
        minute = int(lastUpdate.split(':')[1])
        second = int(lastUpdate.split(':')[2])
        thisJob['lastUpdate'] = datetime.datetime(year, month, day, hour, minute, second)

        creationTime = thisJob['creationTime']
        year = int(creationTime.split('-')[0])
        month = int(creationTime.split('-')[1])
        day = int(creationTime.split('-')[2].split(' ')[0])
        hour = int(creationTime.split(' ')[1].split(':')[0])
        minute = int(creationTime.split(':')[1])
        second = int(creationTime.split(':')[2])
        thisJob['creationTime'] = datetime.datetime(year, month, day, hour, minute, second)

        if not idFlag in thisJob['outDS']:
            print "Skipping job because did not match idFlag:", idFlag
            print "jediTaskID", thisJob['jediTaskID']
            continue
        needToAppend = True
        # check for duplicates of jediTaskID
        #  and inDS
        for i,iJob in enumerate(myJobs):
            # if duplicate jediTaskID, pick the most
            #  recently updated
            if iJob['jediTaskID'] == thisJob['jediTaskID']:
                print 'Found duplicate jobs for task:', thisJob['jediTaskID']
                if iJob['lastUpdate'] < thisJob['lastUpdate']:
                    print "Replacing original job updated at", iJob['lastUpdate'], 'with later job updated at', thisJob['lastUpdate']
                    myJobs[i] = thisJob
                    print myJobs[i]['lastUpdate']
                needToAppend = False
                break
            # if duplicate inDS, pick the most 
            #  recenty created
            if iJob['inDS'] == thisJob['inDS']:
                print 'Found duplicate jobs for inDS:', thisJob['inDS']
                if iJob['creationTime'] < thisJob['creationTime']:
                    print "Replacing original job created at", iJob['creationTime'], 'with later job created at', thisJob['creationTime']
                    myJobs[i] = thisJob
                    print myJobs[i]['creationTime']
                needToAppend = False
                break

        if needToAppend:
            print "Appending new job!"
            myJobs.append(thisJob)

f.close()

print "Number of jobs: ", len(myJobs)
print "How many did you expect?"

statusList = set([])
for iJob in myJobs:
    statusList.add(iJob['taskStatus'])
print "status list", statusList



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
