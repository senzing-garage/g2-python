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
import math

from time import sleep
from datetime import datetime, timedelta
from glob import glob
from multiprocessing import Queue,Value,Process
try: from queue import Full,Empty
except: from Queue import Full,Empty
import threading
import pprint

#--project classes
import G2Exception
from G2ConfigTables import G2ConfigTables
from G2Project import G2Project
from G2Engine import G2Engine
from G2Diagnostic import G2Diagnostic
from G2Product import G2Product
from G2Exception import G2ModuleException, G2ModuleResolveMissingResEnt, G2ModuleLicenseException
from CompressedFile import openPossiblyCompressedFile, isCompressedFile, fileRowParser
import DumpStack
 
#--set globals for the g2 engine
maxThreadsPerProcess=16
defaultThreadCount=4

#---------------------------------------------------------------------
#--G2Loader
#---------------------------------------------------------------------

#---------------------------------------
def processRedo(q, processEverything=False):
    #-- This may look weird but ctypes/ffi have problems with the native code and fork.
    setupProcess = Process(target=redoFeed, args=(q, processEverything, g2iniPath, debugTrace))
    setupProcess.start()
    setupProcess.join()
    if setupProcess.exitcode != 0:
        exitCode = 1
        return




def redoFeed(q, processEverything, g2iniPath, debugTrace):
    ''' process any records in the redo queue '''
    #--purge the repository
    try:
        g2_engine = G2Engine()
        g2_engine.init('pyG2Redo', g2iniPath, debugTrace)
    except G2ModuleException as ex:
        print('ERROR: could not start the G2 engine at ' + g2iniPath)
        print(ex)
        return -1

    exitCode = 0

    passNum = 0
    cntRows = 0
    passStartTime = time.time()
    batchStartTime = time.time()

    while True:
        if threadStop.value != 0:
           break
        recBytes = bytearray()
        ret = g2_engine.getRedoRecord(recBytes)
        rec = recBytes.decode()
        if not rec:
          passNum += 1
          if (passNum > 10):
            break
          sleep(0.05)
          continue

        cntRows += 1
        while True:
            try: q.put(rec,True,1)
            except Full:
                continue
            break
        if cntRows % sqlCommitSize == 0:
            recordsLeft = g2_engine.countRedoRecords()
            print('  %s redo records processed at %s, %s records per second, approx %d remaining' % (cntRows, datetime.now().strftime('%I:%M%p').lower(), int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1))), recordsLeft)) 
            batchStartTime = time.time()

    if cntRows > 0:
        print('%s reevaluations completed' % (cntRows))

    g2_engine.destroy()
    del g2_engine
    return exitCode, ""

#---------------------------------------
def checkResources():

    diag = G2Diagnostic()
    diag.init('pyG2Diagnostic', g2iniPath, debugTrace)

    physicalCores = diag.getPhysicalCores()
    logicalCores = diag.getLogicalCores()
    availableMem = diag.getAvailableMemory()/1024/1024/1024.0

    print('System Resources:')
    print('   Physical cores  : %d' % physicalCores)
    if physicalCores != logicalCores:
      print('   Logical cores   : %d' % logicalCores)
    print('   Total Memory    : %.1fGB' % (diag.getTotalSystemMemory()/1024/1024/1024.0))
    print('   Available Memory: %.1fGB' % availableMem)
    print('')

    minRecommendedCores = math.ceil(defaultThreadCount/4+1)
    # 2.5GB per process
    # .5GB per thread
    numProcesses = math.ceil(defaultThreadCount/maxThreadsPerProcess);
    minRecommendedMemory = (numProcesses*2.5+defaultThreadCount*.5)
    
    print('Resource requested:')
    print('   maxThreadsPerProcess      : %d' % maxThreadsPerProcess)
    print('   numThreads                : %d' % defaultThreadCount)
    print('   Minimum recommended cores : %d' % minRecommendedCores)
    print('   Minimum recommended memory: %.1fGB' % minRecommendedMemory)
    print('')

    print('Performing database performance tests...')
    perfInfo = json.loads(diag.checkDBPerf(3))

    timePerInsert=999
    maxTimePerInsert=4

    if 'numRecordsInserted' in perfInfo:
      numRecordsInserted = perfInfo['numRecordsInserted']
      insertTime = perfInfo['insertTime']
      if numRecordsInserted > 0:
        timePerInsert = 1.0*insertTime/numRecordsInserted

      print('  %d records inserted in %dms with an avg of %.1fms per insert' % (numRecordsInserted,insertTime,timePerInsert));
    else:
      print(' ERROR: Database performance tests failed')
    print('')

    criticalError = False
    warningIssued = False

    if timePerInsert > maxTimePerInsert:
      warningIssued = True
      print('WARNING: Database performance of %.1fms per insert is slower than the recommended minimum performance of %.1fms per insert' % (timePerInsert,maxTimePerInsert))
      print('WARNING: For tuning your Database please refer to: https://senzing.zendesk.com/hc/en-us/sections/360000386433-Technical-Database')

    if physicalCores < minRecommendedCores:
      warningIssued = True
      print('WARNING: System has fewer (%d) than the minimum recommended cores (%d) for this configuration' % (physicalCores,minRecommendedCores))

    if minRecommendedMemory > availableMem:
      criticalError = True
      print('!!!!!CRITICAL WARNING: SYSTEM HAS LESS MEMORY AVAILABLE (%.fGB) THAN THE MINIMUM RECOMMENDED (%.fGB) !!!!!' % (availableMem,minRecommendedMemory))

    if criticalError:
      time.sleep(10)
      print('')
    elif warningIssued:
      time.sleep(3)
      print('')



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
    checkResources()

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
    elif dsrcAction == 'X':
        actionStr = 'Reprocessing'
    else:
        actionStr = 'Loading'

    #print('\n%s %s\n' % (actionStr, projectFileName if projectFileName else projectFileSpec))

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
        filePath = sourceDict['FILE_PATH']

        cntRows = 0
        cntBadParse = 0
        cntBadUmf = 0
        cntGoodUmf = 0
        g2Project.clearStatPack()

        print('')
        print('-' * 50)
        if not testMode:
            if dsrcAction == 'D':
                print('*** Deleting *** %s ...' % filePath)
            elif dsrcAction == 'X':
                print('*** Reprocessing *** %s ...' % filePath)
            else:
                print('Loading %s ...' % filePath)
        else:
            if not createJsonOnly:
                print('Testing %s ... ' % filePath)
                print(' press control-c at any time to end test')
            else:
                print('Creating %s ...' % (filePath + '.json',))
                try: outputFile = open(filePath + '.json', 'w', encoding='utf-8', newline='')
                except IOError as err:
                    print('ERROR: could not create output file %s' % filePath + '.json')
                    exitCode = 1
                    return

        #--drop to a single thread for files under 500k
        if os.path.getsize(filePath) < (100000 if isCompressedFile(filePath) else 500000):
            print(' dropping to single thread due to small file size')
            transportThreadCount = 1
        else:
            transportThreadCount = defaultThreadCount

        #--shuffle unless directed not to or in test mode or single threaded
        if (not noShuffle) and (not testMode) and transportThreadCount > 1:
            print (' shuffling file ...')
            #if not os.path.exists(tempFolderPath):
            #    try: os.makedirs(tempFolderPath)
            #    except: 
            #        print('ERROR: could not create temp directory %s' % tempFolderPath)
            #        exitCode = 1
            #        return
            #shufFilePath = tempFolderPath + os.sep + sourceDict['FILE_NAME']
            #--abandoned above ... shuffling to temp folder is more likley to run out of space!
            shufFilePath = filePath + '.shuf'
            if sourceDict['FILE_FORMAT'] in ('JSON', 'UMF'):
                cmd = 'shuf %s > %s' % (filePath, shufFilePath)
            else:
                #cmd = 'cat ' + filePath + ' | (read -r; printf "%s\n" "$REPLY"; shuf) > ' + shufFilePath
                cmd = 'head -n1 ' + filePath + ' > ' + shufFilePath + ' && tail -n+2 ' + filePath + ' | shuf >> ' + shufFilePath
            try: os.system(cmd)
            except: 
                print('ERROR: shuffle command failed: %s' % cmd)
                exitCode = 1
                return
            else:
                filePath = shufFilePath

        fileReader = openPossiblyCompressedFile(filePath, 'r')
        #--fileReader = safe_csv_reader(csv.reader(csvFile, fileFormat), cntBadParse)

        if sourceDict['FILE_FORMAT'] not in ('JSON', 'UMF'):
            next(fileReader) #--use previously stored header row, so get rid of this one
                
        threadList, workQueue = startLoaderProcessAndThreads(transportThreadCount)
        if threadStop.value != 0:
            return exitCode

        #--start processing rows
        fileStartTime = time.time()
        batchStartTime = time.time()
        cntRows = 0
        while True:
            try: row = next(fileReader)
            except StopIteration: 
                break
            except: 
                cntRows += 1
                cntBadParse += 1
                print('could not read row %s, %s' % (cntRows, sys.exc_info()[0]))
                continue
            #--always increment rowcount so agrees with a line count and references to bad rows are correct!
            cntRows += 1 

            #--skip blank rows
            rowData = fileRowParser(row, sourceDict, cntRows)
            if not rowData:
                cntBadParse += 1
                continue

            #-- don't do any transformation if this is raw UMF
            okToContinue = True
            if sourceDict['FILE_FORMAT'] != 'UMF':

                #--update with file defaults
                if 'DATA_SOURCE' not in rowData and 'DATA_SOURCE' in sourceDict:
                    rowData['DATA_SOURCE'] = sourceDict['DATA_SOURCE']
                if 'ENTITY_TYPE' not in rowData and 'ENTITY_TYPE' in sourceDict:
                    rowData['ENTITY_TYPE'] = sourceDict['ENTITY_TYPE']
                if 'LOAD_ID' not in rowData:
                    rowData['LOAD_ID'] = sourceDict['FILE_NAME']
                #if 'DSRC_ACTION' not in rowData:
                #    rowData['DSRC_ACTION'] = dsrcAction

                #if 'DATA_SOURCE' not in rowData or 'ENTITY_TYPE' not in rowData:
                #    print('  WARNING: data source or entity type missing on row %s' % cntRows) 
                #    cntBadParse += 1
                #    okToContinue = False
                #    continue
                    #--update with file defaults
                if sourceDict['MAPPING_FILE']:
                    rowData['_MAPPING_FILE'] = sourceDict['MAPPING_FILE']
                    
                if testMode:
                    if '_MAPPING_FILE' not in rowData:
                        recordList = [rowData]
                    else:
                        recordList, errorCount = g2Project.csvMapper(rowData)
                        if errorCount:
                            cntBadParse += 1
                            recordList = []

                    for rowData in recordList:
                        mappingResponse = g2Project.mapJsonRecord(rowData)
                        if mappingResponse[0]:
                            cntBadUmf += 1
                            okToContinue = False
                            for mappingError in mappingResponse[0]:
                                print('  WARNING: mapping error in row %s (%s)' % (cntRows, mappingError))
                        elif createJsonOnly:
                            outputFile.write(json.dumps(rowData, sort_keys = True) + '\n')

            #--put the record on the queue
            if okToContinue:
                cntGoodUmf += 1
                if not testMode:
                    while True:
                        try: workQueue.put(rowData, True, 1)
                        except Full:
                            #check to see if any of our threads have died
                            for thread in threadList:
                                if thread.is_alive() == False:
                                    print('ERROR: Thread shutdown!')
                                    return
                            continue
                        break

            if cntRows % sqlCommitSize == 0:
                batchSpeed = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))

                #--display current stats
                print('  %s rows processed at %s, %s records per second' % (cntRows, datetime.now().strftime('%I:%M%p').lower(), batchSpeed))

                if cntRows % (100*sqlCommitSize) == 0:
                    #--process redo
                    if not testMode and processRedoQueue:
                        redoError = processRedo(workQueue)

                batchStartTime = time.time()


            #--check to see if any threads threw errors or control-c pressed and shut down
            if threadStop.value != 0:
                exitCode = threadStop.value
                break
        
        if threadStop.value == 0 and not testMode and processRedoQueue:
          redoError = processRedo(workQueue, True)

        #--close input files
        fileReader.close()

        stopLoaderProcessAndThreads(threadList, workQueue)

        if sourceDict['FILE_SOURCE'] == 'S3':
            print(" Removing temporary file created by S3 download " +  filePath)
            os.remove(filePath)
        
        #--close outut file if just created json
        if createJsonOnly:
            outputFile.close()

        #--remove shuffled file
        if (not noShuffle) and (not testMode) and transportThreadCount > 1:
            os.remove(shufFilePath)

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
        print('Process aborted at %s after %s minutes' % (datetime.now().strftime('%I:%M%p').lower(), elapsedMins))
    else:
        print('Process completed successfully at %s in %s minutes' % (datetime.now().strftime('%I:%M%p').lower(), elapsedMins))

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
      redoError = processRedo(workQueue, True)
    
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

    g2ConfigTables = G2ConfigTables(configTableFile,g2iniPath, configuredDatasourcesOnly)	
    if not g2ConfigTables.success:
        return
	
    #--make sure g2 has the correct config
    if (g2ConfigTables.verifyEntityTypeExists("GENERIC") == False):
        print('ERROR: The G2 generic configuration must be updated before loading')
        return False

    #--add any missing source codes and entity types to the g2 config
    g2RestartRequired = False
    for sourceDict in g2Project.sourceList:
        if 'DATA_SOURCE' in sourceDict: 
            try: 
                if g2ConfigTables.addDataSource(sourceDict['DATA_SOURCE']) == 1: #--inserted
                    g2RestartRequired = True
            except G2Exception.G2DBException as err:
                print(err)
                print('ERROR: could not prepare G2 database')
                return False
            except G2Exception.UnconfiguredDataSourceException as err:
                print(err)
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

  if workloadStats > 0 and numProcessed > 0:
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


        #--perform mapping if necessary
        if type(row) == dict:
            if '_MAPPING_FILE' not in row:
                rowList = [row]
            else:
                rowList, errorCount = g2Project.csvMapper(row)
                if errorCount:
                    recordList = []
        else:
            rowList = [row]

        #--call g2engine
        for row in rowList:

            #--only umf is not a dict (csv and json are)
            if type(row) == dict:
                if dsrcAction in ('D', 'X'): #--strip deletes and reprocess (x) messages down to key only
                    newRow = {}
                    newRow['DATA_SOURCE'] = row['DATA_SOURCE']
                    newRow['DSRC_ACTION'] = dsrcAction
                    if 'ENTITY_TYPE' in row:
                        newRow['ENTITY_TYPE'] = row['ENTITY_TYPE'] 
                    if 'ENTITY_KEY' in row:
                        newRow['ENTITY_KEY'] = row['ENTITY_KEY']
                    if 'RECORD_ID' in row:
                        newRow['RECORD_ID'] = row['RECORD_ID']
                    newRow['DSRC_ACTION'] = dsrcAction
                    #print ('-'* 25)
                    #print(json.dumps(newRow, indent=4))
                    row = newRow
                row = json.dumps(row, sort_keys=True)

            numProcessed += 1
            if (workloadStats > 0 and (numProcessed%(maxThreadsPerProcess*sqlCommitSize)) == 0):
              print(g2Engine_.stats())

            try: 
                returnCode = g2Engine_.process(row)
            except G2ModuleLicenseException as err:
                print(err)
                print('ERROR: G2Engine licensing error!')
                with threadStop.get_lock():
                    threadStop.value = 1
                return
            except G2ModuleException as err:
                print(row)
                print('exception: %s' % err)

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
    try: 
        if sys.version[0] == '2':
            response = raw_input(question)
        else:
            response = input(question)
    except: response = ''
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
    noShuffle = 0
    workloadStats = 0
    processRedoQueue = True
    redoMode = False
    redoModeInterval = 60
    configuredDatasourcesOnly = False
    createJsonOnly = False

    argParser = argparse.ArgumentParser()
    argParser.add_argument('-p', '--projectFile', dest='projectFileName', default='', help='the name of a g2 project csv or json file', nargs='?')
    argParser.add_argument('-f', '--fileSpec', dest='projectFileSpec', default='', help='the name of a file to load such as /data/*.json/?data_source=?,file_format=?')
    argParser.add_argument('-P', '--purgeFirst', dest='purgeFirst', action='store_true', default=False, help='purge the g2 repository first')
    argParser.add_argument('-T', '--testMode', dest='testMode', action='store_true', default=False, help='run in test mode to get stats without loading, ctrl-c anytime')
    argParser.add_argument('-D', '--delete', dest='deleteMode', action='store_true', default=False, help='force deletion of a previously loaded file')
    argParser.add_argument('-X', '--reprocess', dest='reprocessMode', action='store_true', default=False, help='force reprocessing of previously loaded file')
    argParser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
    argParser.add_argument('-ns', '--noShuffle', dest='noShuffle', action='store_true', default=False, help='shuffle the file to improve performance')
    argParser.add_argument('-w', '--workloadStats', dest='workloadStats', action='store_true', default=False, help='output workload statistics information')
    argParser.add_argument('-n', '--noRedo', dest='noRedo', action='store_false', default=True, help='disable redo processing')
    argParser.add_argument('-R', '--redoMode', dest='redoMode', action='store_true', default=False, help='run in redo mode that only processes the redo queue')
    argParser.add_argument('-i', '--redoModeInterval', dest='redoModeInterval', type=int, default=60, help='time to wait between redo processing runs, in seconds. Only used in redo mode')
    argParser.add_argument('-k', '--knownDatasourcesOnly', dest='configuredDatasourcesOnly', action='store_true', default=False, help='only accepts configured(known) data sources')
    argParser.add_argument('-j', '--createJsonOnly', dest='createJsonOnly', action='store_true', default=False, help='only create json files from mapped csv files')
    argParser.add_argument('-nt', '--defaultThreadCount', dest='defaultThreadCount', type=int, help='total number of threads to start, default=' + str(defaultThreadCount))
    argParser.add_argument('-mtp', '--maxThreadsPerProcess', dest='maxThreadsPerProcess', type=int, help='maximum threads per process, default=' + str(maxThreadsPerProcess))
    args = argParser.parse_args()

    if len(sys.argv) < 2:
        print('')
        argParser.print_help()
        sys.exit(0)
    else:
        #If -p and a value is present use it, otherwise G2Project.ini project file will be used, allows no arguments to display help and still have default project
        if args.projectFileName and len(args.projectFileName) > 0:
            projectFileName = args.projectFileName
        if args.projectFileSpec:
            projectFileSpec = args.projectFileSpec
        if args.purgeFirst:
            purgeFirst = args.purgeFirst
        if args.testMode:
            testMode = args.testMode
        if args.deleteMode:
            dsrcAction = 'D'
        if args.reprocessMode:
            dsrcAction = 'X'
        if args.debugTrace:
            debugTrace = 1
        if args.noShuffle:
            noShuffle = 1
        if args.workloadStats:
            workloadStats = 1
        if args.defaultThreadCount:
            defaultThreadCount = args.defaultThreadCount
        if args.maxThreadsPerProcess:
            maxThreadsPerProcess = args.maxThreadsPerProcess
        processRedoQueue = args.noRedo        
        redoMode = args.redoMode
        redoModeInterval = args.redoModeInterval
        configuredDatasourcesOnly = args.configuredDatasourcesOnly
        if args.createJsonOnly:
            testMode = True
            createJsonOnly = True

    #--validations
    if not configTableFile:
        print('ERROR: A G2 setup configuration file is not specified')
        sys.exit(1)

    if redoMode:
        defaultThreadCount = min(defaultThreadCount, maxThreadsPerProcess)
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
        # More validation specific to this mode.        
        if (not projectFileName) and (not projectFileSpec):
            print('ERROR: A project file name or file specification must be specified!')
            sys.exit(1)
        else:
            if projectFileSpec: #--file spec takes precendence over name
                projectFileName = None
            
        #-- Load the G2 configuration file
        g2ConfigTables = G2ConfigTables(configTableFile,g2iniPath, configuredDatasourcesOnly)

        #--open the project
        g2Project = G2Project(g2ConfigTables, projectFileName, projectFileSpec, tempFolderPath)
        if not g2Project.success:
            sys.exit(1)

        #--all good, lets start loading!
        exitCode = loadProject()

    sys.exit(exitCode)
