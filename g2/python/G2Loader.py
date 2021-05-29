#--python imports
import argparse
try: import configparser
except: import ConfigParser as configparser
import sys
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding('utf-8')
import signal
import os
import json
import csv
import time
import tempfile

hasPsutil = True
try: import psutil
except:
    hasPsutil = False

from datetime import timedelta
from glob import glob
from multiprocessing import Queue,Value,Process
try: from queue import Full,Empty
except: from Queue import Full,Empty
import threading
import pprint

#--project classes
import G2Exception
from G2Database import G2Database
from G2ConfigTables import G2ConfigTables
from G2Project import G2Project
from G2Engine import G2Engine
from G2Product import G2Product
from G2Exception import G2ModuleException, G2ModuleResolveMissingResEnt, G2ModuleLicenseException
from CompressedFile import openPossiblyCompressedFile, isCompressedFile
import DumpStack
 
#--optional libraries
try: import pyodbc
except: pass
try: import sqlite3
except: pass

#---------------------------------------------------------------------
#--G2Loader
#---------------------------------------------------------------------

#---------------------------------------
def processRedo(q, processEverything=False):
    ''' process any records in the redo queue '''
    exitCode = 0

    g2Dbo = G2Database(g2dbUri)
    if not g2Dbo.success:
        return

    deleteStmt = 'delete from SYS_EVAL_QUEUE where LENS_CODE = ? and ETYPE_CODE = ? and DSRC_CODE = ? and ENT_SRC_KEY = ?'
    execManyList = []

    passNum = 0
    cntRows = 0
    while True:
        if processEverything is False and passNum > 10:
          print('Too many resolution review cycles, taking a break from reprocessing')
          break

        passStartTime = time.time()
        try:
            if g2Dbo.dbType == 'DB2':
                myCursor = g2Dbo.sqlExec('select LENS_CODE, ETYPE_CODE, DSRC_CODE, ENT_SRC_KEY, varchar(MSG) from SYS_EVAL_QUEUE')
            else:
                myCursor = g2Dbo.sqlExec('select LENS_CODE, ETYPE_CODE, DSRC_CODE, ENT_SRC_KEY, MSG from SYS_EVAL_QUEUE')
            allRows = g2Dbo.fetchAllRows(myCursor)
            if not allRows:
              break
        except G2Exception.G2DBException as err:
            print(err)
            print('ERROR: Selecting redo queue entries')
            exitCode = 1
            break

        if allRows:
            passNum += 1
            if redoMode:
                print
                print ('Reviewing approximately %s resolutions in pass %s ...' % (len(allRows), passNum))
        batchStartTime = time.time()
        for row in allRows:
            cntRows += 1
            while True:
                try: q.put(row[4],True,1)
                except Full:
                    continue
                break
            execManyList.append([row[0],row[1],row[2],row[3]])
            if cntRows % sqlCommitSize == 0:
                try: g2Dbo.execMany(deleteStmt, execManyList)
                except G2Exception.G2DBException as err:
                    print(err)
                    print('ERROR: could not delete from SYS_EVAL_QUEUE table')
                    exitCode = 1
                    break
                else:
                    execManyList = []
                if redoMode:
                    print('  %s rows processed, %s records per second' % (cntRows, int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1))))) 
                    batchStartTime = time.time()

        if exitCode == 0 and passNum > 0:
            if execManyList:
                try: g2Dbo.execMany(deleteStmt, execManyList)
                except G2Exception.G2DBException as err:
                    print(err)
                    print('ERROR: could not delete from SYS_EVAL_QUEUE table')
                    exitCode = 1
                    break
                else:
                    execManyList = []

    if cntRows > 0:
        resultStr = '%s reevaluations completed in %s passes' % (cntRows, passNum)
    else:
        resultStr = None

    g2Dbo.close()

    return exitCode, resultStr

#---------------------------------------
def checkMiminumMemory():
    if not hasPsutil:
        print('WARNING: psutil not installed.  Unable to check available memory.')
        print('WARNING: Try typing "sudo pip install psutil" or installing the OS python psutil package')
        print('WARNING: Continuing without checking available memory.')
        time.sleep(3)
        return

    memoryRequirementsSatisfied = True

    # Check for 8GB of physial memory
    totalPhysicalMemory = psutil.virtual_memory().total
    if totalPhysicalMemory < (8000000000):
        memoryRequirementsSatisfied = False;

    # Check for 6GB of available memory
    totalPhysicalMemoryAvailable = psutil.virtual_memory().available
    if totalPhysicalMemoryAvailable < (6000000000):
        memoryRequirementsSatisfied = False;

    if memoryRequirementsSatisfied != True:
        # Report a memory issue, and pause for a few seconds
        print('WARNING:  This machine has less than the minimum recommended memory available.')
        time.sleep(3)

#---------------------------------------
def runSetupProcess(doPurge):
    #-- This may look weird but ctypes/ffi have problems with the native code and fork. Also, don't allow purge first or we lose redo queue
    setupProcess = Process(target=startSetup, args=(doPurge , True, g2iniPath, debugTrace))
    setupProcess.start()
    setupProcess.join()
    if setupProcess.exitcode != 0:
        exitCode = 1
        return

#---------------------------------------
def startSetup(doPurge, doLicense, g2iniPath, debugTrace):
    #--check for minimum system memory
    checkMiminumMemory()

    #--check the product version and license
    try:
        g2_product = G2Product()
        g2_product.init('pyG2LicenseVersion', g2iniPath, debugTrace)
    except G2ModuleException as ex:
        print('ERROR: could not start the G2 product module at ' + g2iniPath)
        print(ex)
        exit(1)

    if (doLicense):
        licInfo = json.loads(g2_product.license())
        verInfo = json.loads(g2_product.version())
        print("****LICENSE****")
        if 'VERSION' in verInfo: print("     Version: " + verInfo['VERSION'] + " (" + verInfo['BUILD_DATE'] + ")")
        if 'customer' in licInfo: print("    Customer: " + licInfo['customer'])
        if 'licenseType' in licInfo: print("        Type: " + licInfo['licenseType'])
        if 'expireDate' in licInfo: print("  Expiration: " + licInfo['expireDate'])
        if 'recordLimit' in licInfo: print("     Records: " + str(licInfo['recordLimit']))
        if 'contract' in licInfo: print("    Contract: " + licInfo['contract'])
        print("***************")

    g2_product.destroy()
    del g2_product

    #--purge the repository
    try:
        g2_engine = G2Engine()
        g2_engine.init('pyG2Purge', g2iniPath, debugTrace)
    except G2ModuleException as ex:
        print('ERROR: could not start the G2 engine at ' + g2iniPath)
        print(ex)
        exit(1)

    if (doPurge):
        print('Purging G2 database ...')
        g2_engine.purgeRepository(False)

    g2_engine.destroy()
    del g2_engine


#---------------------------------------
def loadProject():
    exitCode = 0
    DumpStack.listen()
    procStartTime = time.time()

    if testMode:
        actionStr = 'Testing'
    elif dsrcAction == 'D':
        actionStr = 'Deleting'
    else:
        actionStr = 'Loading'

    print('%s %s' % (actionStr, projectFileName if projectFileName else projectFileSpec))

    #--prepare G2 database
    if not prepareG2db():
        exitCode = 1
        return

    #--purge log files created by g2 from prior runs
    for filename in glob('pyG2*') :
        os.remove(filename)

    runSetupProcess(purgeFirst and not testMode)

    #--start loading!
    for sourceDict in g2Project.sourceList:        
        dataSource = sourceDict['DATA_SOURCE'] 
        entityType = sourceDict['ENTITY_TYPE'] 
        fileFormat = sourceDict['FILE_FORMAT']
        masterFileName = sourceDict['FILE_NAME']
        masterDict = sourceDict['FILE_LIST'][0]
        filePath = masterDict['FILE_PATH']
        fileMappings = masterDict['MAP']
        fileSource = sourceDict['FILE_SOURCE']

        cntRows = 0
        cntBadParse = 0
        cntBadUmf = 0
        cntGoodUmf = 0
        g2Project.clearStatPack()

        print('')
        print('-' * 50)
        print('Data source: %s, Format: %s' % (dataSource, fileFormat)) 
        if not testMode:
            print(' Loading %s ...' % filePath)
        else:
            print(' Testing %s ... (press control-c at any time to end test)' % filePath)

        if fileFormat == 'JSON' or fileFormat == 'UMF':
            fileReader = openPossiblyCompressedFile(filePath, 'r')
        else:
            csvFile = openPossiblyCompressedFile(filePath, 'r')
            csv.register_dialect('CSV', delimiter = ',', quotechar = '"')
            csv.register_dialect('TAB', delimiter = '\t', quotechar = '"')
            csv.register_dialect('PIPE', delimiter = '|', quotechar = '"')
            csvHeaders = masterDict['CSV_HEADERS']
            fileReader = csv.reader(csvFile, fileFormat)
            next(fileReader) #--use previously stored header row, so get rid of this one
                
        #--drop to a single thread for files under 500k
        if os.path.getsize(filePath) < (100000 if isCompressedFile(filePath) else 500000):
            print(' dropping to single thread due to small file size')
            transportThreadCount = 1
        else:
            transportThreadCount = defaultThreadCount

        threadList, workQueue = startLoaderProcessAndThreads(transportThreadCount)
        if threadStop.value != 0:
            return exitCode

        #--start processing rows
        fileStartTime = time.time()
        batchStartTime = time.time()
        cntRows = 0
        for row in fileReader:

            #--always increment rowcount so agrees with a line count and references to bad rows are correct!
            cntRows += 1 

            #--skip blank rows
            #--note a row could be a umf string, a json string or a csv list
            if type(row) == list: #--csv
                isBlank = len(''.join(map(str, row)).strip()) == 0
            else:
                row = row.strip()
                isBlank = len(row) == 0
            if isBlank:
                cntBadParse += 1
                continue

            #-- don't do any transformation if this is raw UMF
            okToContinue = True
            if fileFormat != 'UMF':
                try: rowDict = json.loads(row) if fileFormat == 'JSON' else dict(list(zip(csvHeaders, row)))
                except: 
                    print('  WARNING: could not parse row %s' % cntRows) 
                    cntBadParse += 1
                    okToContinue = False
                else:
                    #--update with file defaults
                    if 'DATA_SOURCE' not in rowDict:
                        rowDict['DATA_SOURCE'] = dataSource
                    if 'LOAD_ID' not in rowDict:
                        rowDict['LOAD_ID'] = masterFileName
                    if 'ENTITY_TYPE' not in rowDict:
                        rowDict['ENTITY_TYPE'] = entityType
                    if 'DSRC_ACTION' not in rowDict:
                        rowDict['DSRC_ACTION'] = dsrcAction
                    row = json.dumps(rowDict, sort_keys=True)

                    if testMode:
                        mappingResponse = g2Project.mapJsonRecord(rowDict)
                        if mappingResponse[0]:
                            cntBadUmf += 1
                            okToContinue = False
                            for mappingError in mappingResponse[0]:
                                print('  WARNING: mapping error in row %s (%s)' % (cntRows, mappingError))


            #--put the record on the queue
            if okToContinue:
                cntGoodUmf += 1
                if not testMode:
                    while True:
                        try: workQueue.put(row, True, 1)
                        except Full:
                            #check to see if any of our threads have died
                            for thread in threadList:
                                if thread.is_alive() == False:
                                    print('ERROR: Thread shutdown!')
                                    return
                            continue
                        break

            if cntRows % sqlCommitSize == 0:

                redoStatDisplay = ''
                if cntRows % (100*sqlCommitSize) == 0:
                    #--process redo
                    if not testMode and processRedoQueue:
                        redoError, redoMsg = processRedo(workQueue)
                        if redoMsg:
                            redoStatDisplay = ', ' + redoMsg

                #--display current stats
                print('  %s rows processed, %s records per second%s' % (cntRows, int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1))), redoStatDisplay)) 
                batchStartTime = time.time()

            #--check to see if any threads threw errors or control-c pressed and shut down
            if threadStop.value != 0:
                exitCode = threadStop.value
                break
        
        if threadStop.value == 0 and not testMode and processRedoQueue:
          redoError, redoMsg = processRedo(workQueue, True)

        #--close input files
        if fileFormat in ('JSON', 'UMF'):
            fileReader.close()
        else:
            csvFile.close()

        stopLoaderProcessAndThreads(threadList, workQueue)

        if fileSource == 'S3':
            print(" Removing temporary file created by S3 download " +  filePath)
            os.remove(filePath)
        
        #--print the stats if not error or they pressed control-c
        if exitCode in (0, 9):
            elapsedSecs = time.time() - fileStartTime
            elapsedMins = round(elapsedSecs / 60, 1)
            if elapsedSecs > 0:
                fileTps = int(float(cntGoodUmf + cntBadParse + cntBadUmf) / float(elapsedSecs))
            else:
                fileTps = 0
            print('')
            print(' Statistics:')
            print('  good records .......... ' + str(cntGoodUmf))
            print('  bad records ........... ' + str(cntBadParse))
            print('  incomplete records .... ' + str(cntBadUmf))
            print('  elapsed time .......... %s minutes' % elapsedMins)
            print('  records per second .... ' + str(fileTps))

            statPack = g2Project.getStatPack()
            if statPack['FEATURES']:
                print(' Features:')
                for stat in statPack['FEATURES']:
                    print(('  ' + stat['FEATURE'] + ' ' + '.' * 25)[0:25] + ' ' + (str(stat['COUNT']) + ' ' * 12)[0:12] + ' (' + str(stat['PERCENT']) + '%)')
            if statPack['UNMAPPED']:
                print(' Unmapped:')
                for stat in statPack['UNMAPPED']:
                    print(('  ' + stat['ATTRIBUTE'] + ' ' + '.' * 25)[0:25] + ' ' + (str(stat['COUNT']) + ' ' * 12)[0:12] + ' (' + str(stat['PERCENT']) + '%)')

        #--don't go on to the next source if errors hit
        if exitCode:
            break

    print('')
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if exitCode:
        print('Process aborted after %s minutes' % elapsedMins)
    else:
        print('Process completed successfully in %s minutes' % elapsedMins)

    return exitCode

#---------------------------------------
def loadRedoQueueAndProcess():
    exitCode = 0
    DumpStack.listen()
    procStartTime = time.time()

    threadList, workQueue = startLoaderProcessAndThreads(defaultThreadCount)
    if threadStop.value != 0:
        return exitCode

    #--start processing queue
    fileStartTime = time.time()
    if threadStop.value == 0 and not testMode and processRedoQueue:
      redoError, redoMsg = processRedo(workQueue, True)
    
    stopLoaderProcessAndThreads(threadList, workQueue)

    print('')
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if exitCode:
        print('Redo processing cycle aborted after %s minutes' % elapsedMins)
    else:
        print('Redo processing cycle completed successfully in %s minutes' % elapsedMins)

    return exitCode

#---------------------------------------
def prepareG2db():

    g2ConfigTables = G2ConfigTables(configTableFile,g2iniPath)	
    if not g2ConfigTables.success:
        return
	
    #--make sure g2 has the correct config
    if (g2ConfigTables.verifyEntityTypeExists("GENERIC") == False):
        print('ERROR: The G2 generic configuration must be updated before loading')
        return False

    #--add any missing source codes and entity types to the g2 config
    g2RestartRequired = False
    for sourceDict in g2Project.sourceList:
        try: 
            if g2ConfigTables.addDataSource(sourceDict['DATA_SOURCE']) == 1: #--inserted
                g2RestartRequired = True
            if g2ConfigTables.addEntityType(sourceDict['ENTITY_TYPE']) == 1: #--inserted
                g2RestartRequired = True
        except G2Exception.G2DBException as err:
            print(err)
            print('ERROR: could not prepare G2 database')
            return False

    return True

#---------------------------------------
def startLoaderProcessAndThreads(transportThreadCount):
    threadList = []
    workQueue = None
    
    #--bypass threads if in test mode
    if not testMode:
        #--start the transport threads
        threadStop.value = 0

        workQueue = Queue(transportThreadCount * 10)
        numThreadsLeft=transportThreadCount;
        threadId=0
        while (numThreadsLeft>0):
            threadId+=1
            threadList.append(Process(target=sendToG2, args=(threadId, workQueue, min(maxThreadsPerProcess, numThreadsLeft), g2iniPath, debugTrace, threadStop, workloadStats, dsrcAction)))
            numThreadsLeft-=maxThreadsPerProcess;
        for thread in threadList:
            thread.start()
            
    return threadList, workQueue

#---------------------------------------
def stopLoaderProcessAndThreads(threadList, workQueue):
    #--stop the threads
    if not testMode:
        #--display if items still on the queue
        if workQueue.qsize() and threadStop.value == 0:
            print(' Finishing up ...') 

        #--stop the threads
        with threadStop.get_lock():
           if threadStop.value == 0:
               threadStop.value = 1

        for thread in threadList:
          thread.join()

#---------------------------------------
def sendToG2(threadId_, workQueue_, numThreads_, g2iniPath, debugTrace, threadStop, workloadStats, dsrcAction):
  global numProcessed
  numProcessed = 0

  try:
      g2_engine = G2Engine()
      g2_engine.init('pyG2Engine' + str(threadId_), g2iniPath, debugTrace)
  except G2ModuleException as ex:
      print('ERROR: could not start the G2 engine at ' + g2iniPath)
      print(ex)
      with threadStop.get_lock():
          threadStop.value = 1

  try:

    if (numThreads_ > 1):
      threadList = []
      for myid in range(numThreads_):
        threadList.append(threading.Thread(target=g2Thread, args=(str(threadId_)+"-"+str(myid), workQueue_, g2_engine, threadStop, workloadStats, dsrcAction)))
      for thread in threadList:
        thread.start()
      for thread in threadList:
        thread.join()
    else:
      g2Thread(str(threadId_), workQueue_, g2_engine, threadStop, workloadStats, dsrcAction)

  except: pass

  if workloadStats > 0:
    pprint.pprint(g2_engine.stats())
  
  try: g2_engine.destroy()
  except: pass

  return

#---------------------------------------
def g2Thread(threadId_, workQueue_, g2Engine_, threadStop, workloadStats, dsrcAction):
    ''' g2 thread function ''' 
    global numProcessed

    #--for each queue entry
    while threadStop.value == 0 or workQueue_.empty() == False:
        try:
            row = workQueue_.get(True, 1)
        except Empty as e:
            row = None
            continue

        #--call g2engine
        numProcessed += 1
        if (workloadStats > 0 and (numProcessed%(maxThreadsPerProcess*sqlCommitSize)) == 0):
          print(g2Engine_.stats())

        try: 
            g2Engine_.process(row)
        except G2ModuleLicenseException as err:
            print(err)
            print('ERROR: G2Engine licensing error!')
            with threadStop.get_lock():
                threadStop.value = 1
            return
        except G2ModuleException as err:
            print(row)
            print(err)

    return

#---------------------------------------------------------------------
#-- utilities
#---------------------------------------------------------------------

#---------------------------------------
def safe_csv_reader(csv_reader, cntBadParse): 
    try: 
      next(csv_reader) 
    except csv.Error: 
      cntBadParse += 1
    except StopIteration:
      return

#---------------------------------------
def getFromDict(dict_, key_, max_ = 0):
    value_ = None
    try: value_ = dict_[key_]
    except:
        value_ = None
    else:
        if value_ and max_ != 0: 
            try: value_ = value_[0,max_] 
            except: pass
    return value_

#----------------------------------------
def pause(question = 'PRESS ENTER TO CONTINUE ...'):
    response = input(question)
    return response

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Stopping threads ... (please wait)')
    with threadStop.get_lock():
        threadStop.value = 9
    return

#----------------------------------------
if __name__ == '__main__':
    exitCode = 0
    threadStop = Value('i', 0)
    signal.signal(signal.SIGINT, signal_handler)
    DumpStack.listen()

    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))
    iniFileName = appPath + os.path.sep + 'G2Project.ini'
    if not os.path.exists(iniFileName):
        print('ERROR: The G2Project.ini file is missing from the application path!')
        sys.exit(1)

    #--get parameters from ini file
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)
    try: g2dbUri = iniParser.get('g2', 'G2Connection')
    except: g2dbUri = None
    ####try: odsDbUri = iniParser.get('g2', 'ODSConnection')
    ####except: odsDbUri = None
    try: configTableFile = iniParser.get('g2', 'G2ConfigFile')
    except: configTableFile = None
    try: g2iniPath = os.path.expanduser(iniParser.get('g2', 'iniPath'))
    except: g2iniPath = None
    try: evalQueueProcessing = int(iniParser.get('g2', 'evalQueueProcessing'))
    except: evalQueueProcessing = 1
    try: projectFileName = iniParser.get('project', 'projectFileName')
    except: projectFileName = None
    try: projectFileSpec = iniParser.get('project', 'projectFileSpec')
    except: projectFileSpec = None
    try: tempFolderPath = iniParser.get('project', 'tempFolderPath')
    except: tempFolderPath = os.path.join(tempfile.gettempdir(), 'senzing', 'g2')
    try: defaultThreadCount = int(iniParser.get('transport', 'numThreads'))
    except: defaultThreadCount = 1
    try: sqlCommitSize = int(iniParser.get('report', 'sqlCommitSize'))
    except: sqlCommitSize = 1000

    #--capture the command line arguments
    dsrcAction = 'A'
    purgeFirst = False
    testMode = False
    debugTrace = 0
    workloadStats = 0
    processRedoQueue = True
    redoMode = False
    redoModeInterval = 60
    if len(sys.argv) > 1:
        argParser = argparse.ArgumentParser()
        argParser.add_argument('-p', '--projectFile', dest='projectFileName', default='', help='the name of a g2 project csv or json file')
        argParser.add_argument('-f', '--fileSpec', dest='projectFileSpec', default='', help='the name of a file to load such as /data/*.json/?data_source=?,file_format=?')
        argParser.add_argument('-P', '--purgeFirst', dest='purgeFirst', action='store_true', default=False, help='purge the g2 repository first')
        argParser.add_argument('-T', '--testMode', dest='testMode', action='store_true', default=False, help='run in test mode to get stats without loading, ctrl-c anytime')
        argParser.add_argument('-D', '--delete', dest='deleteMode', action='store_true', default=False, help='run in delete mode')
        argParser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
        argParser.add_argument('-w', '--workloadStats', dest='workloadStats', action='store_true', default=False, help='output workload statistics information')
        argParser.add_argument('-n', '--noRedo', dest='noRedo', action='store_false', default=True, help='disable redo processing')
        argParser.add_argument('-R', '--redoMode', dest='redoMode', action='store_true', default=False, help='run in redo mode that only processes the redo queue')
        argParser.add_argument('-i', '--redoModeInterval', dest='redoModeInterval', type=int, default=60, help='time to wait between redo processing runs, in seconds. Only used in redo mode')
        args = argParser.parse_args()
        if args.projectFileName:
            projectFileName = args.projectFileName
        if args.projectFileSpec:
            projectFileSpec = args.projectFileSpec
        if args.purgeFirst:
            purgeFirst = args.purgeFirst
        if args.testMode:
            testMode = args.testMode
        if args.deleteMode:
            dsrcAction = 'D'
        if args.debugTrace:
            debugTrace = 1
        if args.workloadStats:
            workloadStats = 1
        processRedoQueue = args.noRedo
        if args.redoMode:
            redoMode = args.redoMode
        redoModeInterval = args.redoModeInterval

    #--validations
    if not g2dbUri:
        print('ERROR: A G2 database connection is not specified!')
        sys.exit(1)
    ####if not odsDbUri:
    ####    print('ERROR: A ODS database connection is not specified!')
        sys.exit(1)
    if not configTableFile:
        print('ERROR: A G2 setup configuration file is not specified')
        sys.exit(1)
    if (not projectFileName) and (not projectFileSpec):
        print('ERROR: A project file name or file specification must be specified!')
        sys.exit(1)
    else:
        if projectFileSpec: #--file spec takes precendence over name
            projectFileName = None

    #--set globals for the g2 engine
    maxThreadsPerProcess=4

    if redoMode:
        runSetupProcess(False) # no purge because we would purge the redo queue
        while threadStop.value != 9 and exitCode == 0:
            print("\nProcessing redo queue...")
            exitCode = loadRedoQueueAndProcess()
            if threadStop.value == 9 or exitCode != 0:
                    break
            print("Wating " + str(redoModeInterval) + " seconds for next cycle.")
            # sleep in 1 second increments to respond to user input
            for x in range (1, redoModeInterval):
                if threadStop.value == 9:
                    break
                time.sleep(1.0)
    else :
        #--attempt to open the g2 database
        g2Dbo = G2Database(g2dbUri)
        if not g2Dbo.success:
            sys.exit(1)
            
        #-- Load the G2 configuration file
        g2ConfigTables = G2ConfigTables(configTableFile,g2iniPath)

        #--open the project
        cfg_attr = g2ConfigTables.loadConfig('CFG_ATTR')
        g2Project = G2Project(cfg_attr, projectFileName, projectFileSpec, tempFolderPath)
        if not g2Project.success:
            sys.exit(1)

        g2Dbo.close()
    
        #--all good, lets start loading!
        exitCode = loadProject()

    sys.exit(exitCode)
