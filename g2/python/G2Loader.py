#! /usr/bin/env python3

import argparse
import csv
import importlib
import json
import math
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from datetime import datetime
from glob import glob
from multiprocessing import Process, Queue, Value
from queue import Empty, Full

import DumpStack
import G2Exception
import G2Paths
from CompressedFile import (fileRowParser, isCompressedFile,
                            openPossiblyCompressedFile)
from G2Config import G2Config
from G2ConfigMgr import G2ConfigMgr
from G2ConfigTables import G2ConfigTables
from G2Diagnostic import G2Diagnostic
from G2Engine import G2Engine
from G2Exception import (G2ModuleException, G2ModuleLicenseException,
                         G2ModuleNotInitialized, G2ModuleResolveMissingResEnt)
from G2Health import G2Health
from G2IniParams import G2IniParams
from G2Product import G2Product
from G2Project import G2Project

# -----------------------------------------------------------------------------
# Class: Governor
#
# Dummy class for when no governor is imported
# -----------------------------------------------------------------------------

class Governor:

    def __init__(self, *args, **kwargs):

        return


    def govern(self, *args, **kwargs):
        ''' Main function to trigger action(s) '''

        return


    def govern_cleanup(self, *args, **kwargs):
        ''' Tasks to perform when shutting down, e.g., close DB connections '''

        return


#---------------------------------------------------------------------
# G2Loader
#---------------------------------------------------------------------

def init_engine(name, config_parms, debug_trace, add_start_time=False):
    '''  Initialize an engine. add_start_time is for redo engines only '''

    if add_start_time:
        engine_start_time = time.perf_counter()

    try:
        engine = G2Engine()
        engine.initV2(name, config_parms, debug_trace)
        engine.primeEngine()
    except G2ModuleException:
        raise

    if add_start_time:
        time_starting_engines.value += (time.perf_counter() - engine_start_time)

    return engine


def processRedo(q, empty_q_wait=False, empty_q_msg='', empty_q_wait_time=None):
    ''' Called in normal and redo only mode (-R) to process redo records that need re-evaluation '''

    # Drain the processing queue of ingest records before starting to process redo
    if empty_q_wait:
        sleep_interval = 1
        drain_msg = True 
        while not q.empty():
            if drain_msg: 
                print(f'\n  {empty_q_msg}')
                drain_msg = False
            time.sleep(sleep_interval)
            empty_q_wait_time.value += sleep_interval        

    # This may look weird but ctypes/ffi have problems with the native code and fork.
    setupProcess = Process(target=redoFeed, args=(q, args.debugTrace, args.redoMode, args.redoModeInterval))

    redo_start_time = time.perf_counter()

    setupProcess.start()
    setupProcess.join()

    time_redo.value += (time.perf_counter() - redo_start_time)

    return setupProcess.exitcode

#---------------------------------------
def redoFeed(q, debugTrace, redoMode, redoModeInterval):
    ''' Process records in the redo queue '''

    try:
        redo_engine = init_engine('pyG2Redo', g2module_params, debugTrace, True)
        redo_count = redo_engine.countRedoRecords()
    except G2ModuleException as ex:
        print(f'ERROR: Could not start the G2 engine for redoFeed()')
        print(f'       {ex}')
        exit(1)

    if redo_count == 0 and not args.redoMode:
        print('\n  No redo to perform, resuming loading...\n')
        return

    # Init redo governor in this process, trying to use DB connection in forked process created outside will cause errors when using
    # PostgreSQL governor such as: psycopg2.OperationalError: SSL error: decryption failed or bad record mac
    if import_governor:
        redo_governor = governor.Governor(type=redo_args['type'], g2module_params=redo_args['g2module_params'], frequency=redo_args['frequency'], pre_post_msgs=False)
    else:
        redo_governor = Governor()

    exitCode = passNum = cntRows = 0
    passStartTime = time.time()
    batchStartTime = time.time()
    recBytes = bytearray()
    batch_time_governing = 0

    if not args.redoMode:
        print('\n  Pausing loading to process redo records...')

    while True:

        if threadStop.value != 0:
           break

        try:
            redo_engine.getRedoRecord(recBytes)
            rec = recBytes.decode()
        except G2ModuleException as ex:
            print(f'ERROR: Could not get redo record for redoFeed()')
            print(f'       {ex}')
            exit(1)
        
        if not rec:
            passNum += 1
            if (passNum > 10):
                if args.redoMode:
                    print(f'  Redo queue empty, {cntRows} total records processed. Waiting {args.redoModeInterval} seconds for next cycle at {time_now(True)} (CTRL-C to quit at anytime)...')
                    # Sleep in 1 second increments to respond to user input
                    for x in range (1, args.redoModeInterval):
                        if threadStop.value == 9:
                            break
                        time.sleep(1.0)
                else:
                    break
            time.sleep(0.05)
            continue
        else:
            passNum = 0

        cntRows += 1
        while True:
            try: 
                q.put(rec, True, 1)
            except Full:
                continue
            break

        if cntRows % args.loadOutputFrequency == 0:
            redoSpeed = int(args.loadOutputFrequency / (time.time() - batchStartTime - batch_time_governing)) if time.time() - batchStartTime != 0 else 1
            try:
                remain_redo_count = redo_engine.countRedoRecords()
            except G2ModuleException as ex:
                shutdown(f'\nERROR: Could not fetch remaining redo record count: {ex}')

            print(f'  {cntRows} redo records processed at {time_now()}, {redoSpeed} records per second, {remain_redo_count} remaining.')

            batchStartTime = time.time()
            batch_time_governing = 0

        # Governor called for each redo record
        try: 
            redo_gov_start = time.perf_counter()
            redo_governor.govern()
            redo_gov_stop = time.perf_counter() 
            time_governing.value += (redo_gov_stop - redo_gov_start)
            batch_time_governing += (redo_gov_stop - redo_gov_start)
        except Exception as ex: 
            shutdown(f'\nERROR: Calling per redo governor: {ex}')


    if cntRows > 0:
        print(f'\t{cntRows} reevaluations completed\n')

    redo_engine.destroy()
    del redo_engine

    redo_governor.govern_cleanup()

    if not args.redoMode and redo_count > 0:
        print('  Redo processing complete resuming loading...\n')

    return


def check_resources(thread_count):
    ''' Check system resources and calculate a safe number of threads when argument not specified on command line '''

    try:
        diag = G2Diagnostic()
        diag.initV2('pyG2Diagnostic', g2module_params, args.debugTrace)
    except G2ModuleException as ex:
        print(f'\nERROR: Could not start the G2 engine for check_resources()')
        print(f'       {ex}')
        sys.exit(1)

    physical_cores = diag.getPhysicalCores()
    logical_cores = diag.getLogicalCores()
    available_mem = diag.getAvailableMemory()/1024/1024/1024.0
    total_mem = diag.getTotalSystemMemory()/1024/1024/1024.0

    pause_msg = 'WARNING: Pausing for warning message(s)...'
    db_tune_article = 'https://senzing.zendesk.com/hc/en-us/articles/360016288254-Tuning-Your-Database'
    critical_error = warning_issued = False
    max_time_per_insert = 0.5
    
    # Limit the number of threads for sqlite, doesn't benefit from more and slowes down (8 is approx)
    max_sqlite_threads = 8
    sqlite_limit_msg = ''
    sqlite_warned = False
    
    # Obtain the default value to use for the max amount of memory to use
    calc_max_avail_mem = vars(g2load_parser)["_option_string_actions"]["-ntm"].const
    
    # Allow for higher factor when logical cores are available 
    calc_cores_factor = 2 if physical_cores != logical_cores else 1.2

    # Add command line arguments to output to capture when redirecting output
    print(f'\nArguments: {" ".join(sys.argv[1:])}')

    print(textwrap.dedent(f'''\n\
        System Resources
        ----------------

            Physical Cores:         {physical_cores}
            Logical Cores:          {logical_cores if physical_cores != logical_cores else ''}
            Total Memory (GB):      {total_mem:.1f}
            Available Memory (GB):  {available_mem:.1f}
        '''))

    if not args.thread_count:

        # Allow for 1 GB / thread
        thread_calc_from_mem = math.ceil(available_mem / 100 * calc_max_avail_mem)
        possible_num_threads = math.ceil(physical_cores * calc_cores_factor)

        # Are the number of safe calculated threads <= 80% of available mem
        if possible_num_threads <= thread_calc_from_mem:
            thread_count = possible_num_threads
            calc_thread_msg = '- Using maximum calculated number of threads. This can likely be increased if there is no database running locally.'
        # Else if the thread_calc_from_mem (num of threads of 80% mem) is greater than the number of physical cores use that many threads
        elif thread_calc_from_mem >= physical_cores:
            thread_count = thread_calc_from_mem
            calc_thread_msg = '- Additional processing capability is available, but not enough memory to safely support a higher thread count.'
        # Low available memory compared to physical cores x factor, set to use half safe calculated memory value 
        else:
            thread_count = math.ceil(thread_calc_from_mem/2)
            calc_thread_msg = f'- WARNING: System has less than 1 GB {(thread_calc_from_mem / physical_cores):.2f} GB available per physical core.\n \
                            Number of threads will be significantly reduced, you may see further warnings and should check your resources.'

        # If a value was specified for -ntm override the thread count with the min of calc value or possible_num_threads
        thread_count = min(math.ceil(available_mem / 100 * args.threadCountMem), possible_num_threads) if args.threadCountMem else thread_count
        mem_percent = calc_max_avail_mem if not args.threadCountMem else args.threadCountMem
        mem_msg = 'available' if not args.threadCountMem else 'requested (-ntm)'
        calc_thread_msg = calc_thread_msg if not args.threadCountMem else ''


        print(textwrap.dedent(f'''\n\
            Number of Threads
            -----------------

                - Number of threads arg (-nt) not specified. Calculating number of threads using {mem_percent}% of {mem_msg} memory.

                - Monitor system resources. Use command line argument -nt to increase (or decrease) the number of threads. 
        '''))

        # Add extra message if args.threadCountMem wasn't specified
        if calc_thread_msg:
            print(f'    {calc_thread_msg}\n')

    # Limit number of threads when sqlite, unless -nt arg specified
    if db_type == 'SQLITE3':
        if args.thread_count:
            thread_count = args.thread_count
            if thread_count > max_sqlite_threads:
                sqlite_limit_msg = f' - WARNING greater than {max_sqlite_threads} likely slower when the database is sqlite'
                sqlite_warned = True
        else:
            thread_count = min(thread_count, max_sqlite_threads)
            sqlite_limit_msg = f' - Limited to {max_sqlite_threads} when the database is sqlite'

    # 2.5GB per process - .5GB per thread
    min_recommend_cores = math.ceil(thread_count / 4 + 1)
    num_processes = math.ceil(float(thread_count) / args.max_threads_per_process);
    min_recommend_mem = (num_processes * 2.5 + thread_count * .5)

    print(textwrap.dedent(f'''\n\
        Resources Requested
        -------------------

            Number of Threads:           {thread_count} {sqlite_limit_msg}
            Threads calculated:          {'Yes' if not args.thread_count else 'No, -nt argument was specified'}
            Threads per Process:         {args.max_threads_per_process}
            Number of Processes:         {num_processes}
            Min Recommeded Cores:        {min_recommend_cores}
            Min Recommeded Memory (GB):  {min_recommend_mem:.1f}
        '''))

    if sqlite_warned:
        print(pause_msg, flush=True)
        time.sleep(10)

    print(textwrap.dedent(f'''\n\
        Database Performance
        --------------------
        '''))    

    db_perf_response = bytearray()
    diag.checkDBPerf(3, db_perf_response)
    perf_info = json.loads(db_perf_response.decode())

    num_recs_inserted = perf_info.get('numRecordsInserted', None)
    if num_recs_inserted:
        insert_time = perf_info['insertTime']
        time_per_insert = (1.0 * insert_time / num_recs_inserted) if num_recs_inserted > 0 else 999
        print(textwrap.indent(textwrap.dedent(f'''\
            Database type:       {db_type}
            Records Inserted:    {num_recs_inserted}
            Period for Inserts:  {insert_time} ms
            Average per Insert:  {time_per_insert:.1f} ms
        '''),'    '))
    else:
      print('\nERROR: Database performance tests failed!\n')
    
    if time_per_insert > max_time_per_insert:
        warning_issued = True
        print(textwrap.dedent(f'''\
            WARNING: Database performance of {time_per_insert:.1f} ms per insert is slower than the recommended minimum of {max_time_per_insert:.1f} ms per insert
                     For database tuning please refer to: {db_tune_article}
        ''')) 

    if physical_cores < min_recommend_cores:
      warning_issued = True
      print(f'WARNING: System has fewer ({physical_cores}) than the minimum recommended cores ({min_recommend_cores}) for this configuration\n')

    if min_recommend_mem > available_mem:
      critical_error = True
      print(f'!!!!! CRITICAL WARNING: SYSTEM HAS LESS MEMORY AVAILABLE ({available_mem:.1f} GB) THAN THE MINIMUM RECOMMENDED ({min_recommend_mem:.1f} GB) !!!!!\n')

    if critical_error:
        print(pause_msg, flush=True)
        time.sleep(10)
    elif warning_issued:
        print(pause_msg, flush=True)
        time.sleep(3)

    diag.destroy()

    return thread_count


#---------------------------------------
def runSetupProcess(doPurge):

    # This may look weird but ctypes/ffi have problems with the native code and fork. Also, don't allow purge first or we lose redo queue
    setupProcess = Process(target=startSetup, args=(doPurge , True, args.debugTrace))
    setupProcess.start()
    setupProcess.join()

    return setupProcess.exitcode

#---------------------------------------
def startSetup(doPurge, doLicense, debugTrace):
    '''  Display configuration and license details and purge the repository if requested '''

    # Get configuration details
    try:
        g2_engine = init_engine('pyG2StartSetup', g2module_params, args.debugTrace)
    except G2ModuleException as ex:
        print(f'ERROR: Could not start the G2 engine for startSetup() -> config details')
        print(f'       {ex}')
        exit(1)

    try:
        g2_configmgr = G2ConfigMgr()
        g2_configmgr.initV2('pyG2ConfigMgr', g2module_params, args.debugTrace)
    except G2ModuleException as ex:
        print(f'ERROR: Could not start the G2 config manager for startSetup() -> config details')
        print(f'       {ex}')
        exit(1)


    # Get license details
    if doLicense:
        try:
            g2_product = G2Product()
            g2_product.initV2('pyG2LicenseVersion', g2module_params, args.debugTrace)
        except G2ModuleException as ex:
            print(f'ERROR: Could not start the G2 engine for startSetup() -> doLicense')
            print(f'       {ex}\n')
            raise
        
        licInfo = json.loads(g2_product.license())
        verInfo = json.loads(g2_product.version())


    # Get the configuration list
    try:
        response = bytearray() 
        g2_configmgr.getConfigList(response)
        config_list = json.loads(response.decode())
    except G2Exception.G2Exception as ex:
       print(f'ERROR: Could not get config list in startSetup() -> config details')
       print(f'       {ex}')
       exit(1)
        
    # Get the active config ID
    try: 
        response = bytearray() 
        g2_engine.getActiveConfigID(response)
        active_cfg_id = int(response.decode())
    except G2Exception.G2Exception as ex:
       print(f'ERROR: Could not get the active config in startSetup() -> config details')
       print(f'       {ex}')
       exit(1)
        
    # Get details for the currently active ID 
    active_cfg_details = [details for details in config_list['CONFIGS'] if details['CONFIG_ID'] == active_cfg_id]
    config_comments = active_cfg_details[0]['CONFIG_COMMENTS']
    config_created = active_cfg_details[0]['SYS_CREATE_DT']

    print(textwrap.dedent(f'''\n\
        Version & Config Details
        ------------------------

            Senzing Version:            {verInfo["VERSION"] + " (" + verInfo["BUILD_DATE"] + ")"  if "VERSION" in verInfo else ""}
            Configuration Parameters:   {iniFileName}
            Instance Config ID:         {active_cfg_id}
            Instance Config Comments:   {config_comments}
            Instance Config Created:    {config_created}
    '''))

    print(textwrap.dedent(f'''\n\
        License Details
        ---------------

            Customer:    {licInfo["customer"]}
            Type:        {licInfo["licenseType"]}
            Records:     {licInfo["recordLimit"]}
            Expiration:  {licInfo["expireDate"]}
            Contract:    {licInfo["contract"]}
     '''))

    # Purge repository 
    if doPurge:
        print('\nPurging Senzing database...')
        g2_engine.purgeRepository(False)

    # Clean up
    g2_engine.destroy()
    del g2_engine

    g2_configmgr.destroy()
    del g2_configmgr

    if doLicense:
        g2_product.destroy()
        del g2_product


#---------------------------------------
def perform_load():

    exitCode = 0
    DumpStack.listen()
    procStartTime = time.time()

    exitCode = runSetupProcess((args.purgeFirst or args.forcePurge) and not args.testMode)
    if exitCode:
        return exitCode
   
    # Prepare the G2 configuration
    g2ConfigJson = bytearray()
    if not getInitialG2Config(g2module_params, g2ConfigJson):
        return 1

    g2ConfigTables = G2ConfigTables(g2ConfigJson)

    g2Project = G2Project(g2ConfigTables, args.projectFileName, args.projectFileSpec, args.tmpPath)
    if not g2Project.success:
        return 1

    # Enhance the G2 configuration, by adding data sources and entity types
    if not enhanceG2Config(g2Project, g2module_params, g2ConfigJson, args.configuredDatasourcesOnly):
        return 1

    # Purge log files created by g2 from prior runs
    for filename in glob('pyG2*') :
        os.remove(filename)

    # Start loading
    for sourceDict in g2Project.sourceList:

        filePath = sourceDict['FILE_PATH']
        orig_filePath = filePath

        cntRows = cntBadParse = cntBadUmf = cntGoodUmf = 0
        g2Project.clearStatPack()

        if not args.testMode:
            if dsrcAction == 'D':
                print(f'\n{"-"*30}  Deleting  {"-"*30}\n')
            elif dsrcAction == 'X':
                print(f'\n{"-"*30}  Reevaluating  {"-"*30}\n')
            else:
                print(f'\n{"-"*30}  Loading  {"-"*30}\n')
        else:
            if not args.createJsonOnly:
                print(f'\nTesting {filePath}, CTRL-C to end test at any time...\n')
            else:
                file_path_json = f'{filePath}.json'
                print(f'\nCreating {file_path_json}...\n')
                try: 
                    outputFile = open(file_path_json, 'w', encoding='utf-8', newline='')
                except IOError as ex:
                    print(f'\nERROR: Could not create output file {file_path_json}: {ex}')
                    exitCode = 1
                    return

        # Drop to a single thread for files under 500k
        if os.path.getsize(filePath) < (100000 if isCompressedFile(filePath) else 500000):
            print('  Dropping to single thread due to small file size')
            transportThreadCount = 1
        else:
            transportThreadCount = defaultThreadCount

        # Shuffle unless directed not to or in test mode or single threaded
        if (not args.noShuffle) and (not args.testMode) and transportThreadCount > 1:

            shufFilePath = filePath + '.shuf'
            print(f'\nShuffling file into {shufFilePath}...\n')
           
            cmd = f'shuf {filePath} > {shufFilePath}'
            if sourceDict['FILE_FORMAT'] not in ('JSON', 'UMF'):
                cmd = f'head -n1 {filePath} > {shufFilePath} && tail -n+2 {filePath} | shuf >> {shufFilePath}'
            
            try: 
                process = subprocess.run(cmd, shell=True, check=True)
            except subprocess.CalledProcessError as err: 
                print(f'\nERROR: Shuffle command failed: {err}')
                exitCode = 1
                return

            filePath = shufFilePath
            
        fileReader = openPossiblyCompressedFile(filePath, 'r')
        #--fileReader = safe_csv_reader(csv.reader(csvFile, fileFormat), cntBadParse)

        # Use previously stored header row, so get rid of this one
        if sourceDict['FILE_FORMAT'] not in ('JSON', 'UMF'):
            next(fileReader) 
                
        # Start processes and threads for this file 
        threadList, workQueue = startLoaderProcessAndThreads(transportThreadCount)

        if threadStop.value != 0:
            return exitCode

        # Start processing rows from source file
        fileStartTime = time.time()
        batchStartTime = time.perf_counter()
        cntRows = 0
        batch_time_governing = 0
        drain_time_ingest.value = 0
        
        while True:
            try: 
                row = next(fileReader)
            except StopIteration: 
                break
            except: 
                cntRows += 1
                cntBadParse += 1
                print(f'WARNING: Could not read row {cntRows}, {sys.exc_info()[0]}')
                continue
            
            # Increment row count to agree with line count and references to bad rows are correct
            cntRows += 1 

            # Skip blank rows
            rowData = fileRowParser(row, sourceDict, cntRows)
            if not rowData:
                cntBadParse += 1
                continue

            # Don't do any transformation if this is raw UMF
            okToContinue = True
            if sourceDict['FILE_FORMAT'] != 'UMF':

                # Update with file defaults
                if 'DATA_SOURCE' not in rowData and 'DATA_SOURCE' in sourceDict:
                    rowData['DATA_SOURCE'] = sourceDict['DATA_SOURCE']
                if 'ENTITY_TYPE' not in rowData and 'ENTITY_TYPE' in sourceDict:
                    rowData['ENTITY_TYPE'] = sourceDict['ENTITY_TYPE']
                if 'LOAD_ID' not in rowData:
                    rowData['LOAD_ID'] = sourceDict['FILE_NAME']

                # Update with file defaults
                if sourceDict['MAPPING_FILE']:
                    rowData['_MAPPING_FILE'] = sourceDict['MAPPING_FILE']
                    
                if args.testMode:
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
                        elif args.createJsonOnly:
                            outputFile.write(json.dumps(rowData, sort_keys = True) + '\n')

            # Put the record on the queue
            if okToContinue:
                cntGoodUmf += 1
                if not args.testMode:
                    while True:
                        try: 
                            workQueue.put(rowData, True, 1)
                        except Full:
                            # Check to see if any threads have died
                            for thread in threadList:
                                if thread.is_alive() == False:
                                    print(textwrap.dedent(f'''\n\
                                        ERROR: Thread(s) have shutdown unexpectedly!
                                               
                                               - This typically happens when memory resources are exhausted and the system randomly kills processes.
                                               
                                               - Please review: https://senzing.zendesk.com/hc/en-us/articles/115000856453

                                               - Check output from the following command for out of memory messages.

                                                    - dmesg -e 
                                    '''))
                                    return
                            continue
                        break

            if cntRows % args.loadOutputFrequency == 0:
                batchSpeed = int(args.loadOutputFrequency / (time.perf_counter() - (batchStartTime - batch_time_governing))) if time.perf_counter() - batchStartTime != 0 else 1
                print(f'  {cntRows} rows processed at {time_now()}, {batchSpeed} records per second')

                batchStartTime = time.perf_counter()
                batch_time_governing = 0

            # Process redo during ingestion
            if cntRows % args.redoInterruptFrequency == 0 and not args.testMode and not args.noRedo:
                if processRedo(workQueue, True, 'Waiting for processing queue to empty to start redo...', drain_time_ingest):
                        print(f'\nERROR: Could not process redo record!\n')

            # Check to see if any threads threw errors or control-c pressed and shut down
            if threadStop.value != 0:
                exitCode = threadStop.value
                break

            # Check if any of the threads died without throwing errors
            areAlive = True
            for thread in threadList:
                if thread.is_alive() == False:
                    print('\nERROR: Thread failure!')
                    areAlive = False;
                    break
            if areAlive == False:
                break

            # Governor called for each record
            # Called here instead of when reading from queue to allow queue to act as a small buffer
            try: 
                rec_gov_start = time.perf_counter() 
                record_governor.govern()
                rec_gov_stop = time.perf_counter()
                time_governing.value += (rec_gov_stop - rec_gov_start)
                batch_time_governing += rec_gov_stop - rec_gov_start
            except Exception as err: 
                shutdown(f'\nERROR: Calling per record governor: {err}')

            # Break this file if stop on record value
            if not args.redoMode and args.stopOnRecord and cntRows == args.stopOnRecord:
                print(f'\nINFO: Stopping at record {cntRows}, --stopOnRecord (-sr) argument was set')
                break

        # Process redo at end of processing a source. Wait for queue to empty of ingest records first
        if threadStop.value == 0 and not args.testMode and not args.noRedo:
            drain_time_source.value = 0 
            if processRedo(workQueue, True, 'Source file processed, waiting for processing queue to empty to start redo...', drain_time_source):
                print(f'\nERROR: Could not process redo record!\n')

        end_time = time_now(True)

        # Close input file
        fileReader.close()

        if sourceDict['FILE_SOURCE'] == 'S3':
            print(" Removing temporary file created by S3 download " +  filePath)
            os.remove(filePath)
        
        # Close output file if created json
        if args.createJsonOnly:
            outputFile.close()

        # Remove shuffled file
        if not args.noShuffleDelete:
            try:
                os.remove(shufFilePath)
            except:
                pass

        # Stop processes and threads
        stopLoaderProcessAndThreads(threadList, workQueue)

        # Print load stats if not error or no ctrl-c
        if exitCode in (0, 9):
            elapsedSecs = time.time() - fileStartTime
            elapsedMins = round(elapsedSecs / 60, 1)
            fileTps = int((cntGoodUmf + cntBadParse + cntBadUmf) / (elapsedSecs - time_governing.value - time_starting_engines.value - drain_time_ingest.value - drain_time_source.value)) if elapsedSecs > 0 else 0
            # Use good records count istead of 0 on small fast files
            fileTps = fileTps if fileTps > 0 else cntGoodUmf

            print(textwrap.dedent(f'''\n\
                Statistics
                ----------

                    Start time:                     {datetime.fromtimestamp(fileStartTime).strftime('%I:%M:%S%p').lower()}
                    End time:                       {end_time}
                    File loaded:                    {orig_filePath}
                    Shuffled into:                  {shufFilePath if args.noShuffleDelete and 'shufFilePath' in locals() else 'Shuffled file deleted (-nsd to keep after load)'}
                    Good records:                   {cntGoodUmf}
                    Bad records:                    {cntBadParse}
                    Incomplete records:             {cntBadUmf}
                    Total elapsed time:             {elapsedMins} mins
                    Time processing redo:           {str(round((time_redo.value - time_starting_engines.value) / 60, 1)) + ' mins' if not args.testMode and not args.noRedo else 'Redo disabled (-n)'}
                    Time paused in governor(s):     {round(time_governing.value / 60, 1)} mins
                    Records per second:             {fileTps}
            '''))

            statPack = g2Project.getStatPack()
            if statPack['FEATURES']:
                print(' Features:')
                for stat in statPack['FEATURES']:
                    print(('  ' + stat['FEATURE'] + ' ' + '.' * 25)[0:25] + ' ' + (str(stat['COUNT']) + ' ' * 12)[0:12] + ' (' + str(stat['PERCENT']) + '%)')
            if statPack['UNMAPPED']:
                print(' Unmapped:')
                for stat in statPack['UNMAPPED']:
                    print(('  ' + stat['ATTRIBUTE'] + ' ' + '.' * 25)[0:25] + ' ' + (str(stat['COUNT']) + ' ' * 12)[0:12] + ' (' + str(stat['PERCENT']) + '%)')

            # Governor called for each source
            try: 
                source_governor.govern()
            except Exception as err: 
                shutdown(f'\nERROR: Calling per source governor: {err}')

        # Don't process next source file if errors
        if exitCode:
            break

    elapsed_mins = round((time.time() - procStartTime) / 60, 1)
    if exitCode:
        print(f'\nProcess aborted at {time_now()} after {elapsed_mins} minutes')
    else:
        print(f'\nProcess completed successfully at {time_now()} in {elapsed_mins} minutes')

    return exitCode

#---------------------------------------
def loadRedoQueueAndProcess():

    exitCode = 0
    DumpStack.listen()
    procStartTime = time.time()

    threadList, workQueue = startLoaderProcessAndThreads(defaultThreadCount)
    if threadStop.value != 0:
        return exitCode

    # Start processing queue
    fileStartTime = time.time()

    if threadStop.value == 0 and not args.testMode and not args.noRedo:
        exitCode = processRedo(workQueue)
    
    stopLoaderProcessAndThreads(threadList, workQueue)

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if exitCode:
        print(f'\nRedo processing cycle aborted after {elapsedMins} minutes')
    else:
        print(f'\nRedo processing cycle completed successfully in {elapsedMins} minutes')

    return exitCode


#----------------------------------------
def verifyEntityTypeExists(configJson, entityType):

    cfgDataRoot = json.loads(configJson)
    for rowNode in cfgDataRoot['G2_CONFIG']['CFG_ETYPE']:
        if rowNode['ETYPE_CODE'] == entityType:
            return True

    return False


#----------------------------------------
def addDataSource(g2ConfigModule, configDoc, dataSource, configuredDatasourcesOnly):
    ''' adds a data source if does not exist '''

    returnCode = 0  #--1=inserted, 2=updated

    configHandle = g2ConfigModule.load(configDoc)
    dsrcExists = False
    dsrcListDocString = bytearray()
    g2ConfigModule.listDataSources(configHandle,dsrcListDocString)
    dsrcListDoc = json.loads(dsrcListDocString.decode())
    dsrcListNode = dsrcListDoc['DSRC_CODE']

    for dsrcNode in dsrcListNode:
        if dsrcNode.upper() == dataSource:
            dsrcExists = True

    if dsrcExists == False :
        if configuredDatasourcesOnly == False:
            addDataSourceJson = '{\"DSRC_CODE\":\"%s\"}' % dataSource
            addDataSourceResultBuf = bytearray()
            g2ConfigModule.addDataSourceV2(configHandle,addDataSourceJson,addDataSourceResultBuf)
            newConfig = bytearray()
            g2ConfigModule.save(configHandle,newConfig)
            configDoc[::]=b''
            configDoc += newConfig
            returnCode = 1
        else:
            raise G2Exception.UnconfiguredDataSourceException(dataSource)

    g2ConfigModule.close(configHandle)

    return returnCode

#----------------------------------------
def addEntityType(g2ConfigModule,configDoc,entityType, configuredDatasourcesOnly):
    ''' adds an entity type if does not exist '''

    returnCode = 0  #--1=inserted, 2=updated

    configHandle = g2ConfigModule.load(configDoc)
    etypeExists = False
    entityTypeDocString = bytearray()
    g2ConfigModule.listEntityTypesV2(configHandle,entityTypeDocString)
    etypeListDoc = json.loads(entityTypeDocString.decode())
    etypeListNode = etypeListDoc['ENTITY_TYPES']

    for etypeNode in etypeListNode:
        if etypeNode['ETYPE_CODE'].upper() == entityType:
            etypeExists = True

    if not etypeExists:
        if configuredDatasourcesOnly == False:
            addEntityTypeJson = '{\"ETYPE_CODE\":\"%s\",\"ECLASS_CODE\":\"ACTOR\"}' % entityType
            addEntityTypeResultBuf = bytearray()
            g2ConfigModule.addEntityTypeV2(configHandle,addEntityTypeJson,addEntityTypeResultBuf)
            newConfig = bytearray()
            g2ConfigModule.save(configHandle,newConfig)
            configDoc[::]=b''
            configDoc += newConfig
            returnCode = 1
        else:
            raise G2Exception.UnconfiguredDataSourceException(entityType)

    g2ConfigModule.close(configHandle)

    return returnCode


#---------------------------------------
def getInitialG2Config(g2module_params, g2ConfigJson):


    # Get the configuration from the ini parms, this is deprecated and G2Health reports this
    if has_g2configfile:
        g2ConfigJson[::]=b''
        try: 
            g2ConfigJson += json.dumps(json.load(open(has_g2configfile), encoding="utf-8")).encode()
        except ValueError as err:
            print(f'ERROR: {has_g2configfile} appears broken!')
            print(f'        {err}')
            return False
    else:
        # Get the current configuration from the database
        g2ConfigMgr = G2ConfigMgr()
        g2ConfigMgr.initV2('g2ConfigMgr', g2module_params, False)
        defaultConfigID = bytearray() 
        g2ConfigMgr.getDefaultConfigID(defaultConfigID)
        
        if len(defaultConfigID) == 0:
            print('ERROR: No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
            return False
        defaultConfigDoc = bytearray() 
        g2ConfigMgr.getConfig(defaultConfigID, defaultConfigDoc)
        
        if len(defaultConfigDoc) == 0:
            print('ERROR: No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
            return False
        g2ConfigJson[::]=b''
        g2ConfigJson += defaultConfigDoc
        g2ConfigMgr.destroy()

    return True


def enhanceG2Config(g2Project, g2module_params, g2ConfigJson, configuredDatasourcesOnly):

    # verify that we have the needed entity type
    if (verifyEntityTypeExists(g2ConfigJson,"GENERIC") == False):
        print('\nERROR: Entity type GENERIC must exist in the configuration, please add with G2ConfigTool')
        return False

    # Define variables for where the config is stored.
    g2ConfigFileUsed = False
    g2configFile = ''

    g2Config = G2Config()
    g2Config.initV2("g2Config", g2module_params, False)

    # Add any missing source codes and entity types to the g2 config
    g2NewConfigRequired = False
    for sourceDict in g2Project.sourceList:
        if 'DATA_SOURCE' in sourceDict: 
            try: 
                if addDataSource(g2Config,g2ConfigJson,sourceDict['DATA_SOURCE'],configuredDatasourcesOnly) == 1: #--inserted
                    g2NewConfigRequired = True
            except G2Exception.UnconfiguredDataSourceException as err:
                print(err)
                return False
        if 'ENTITY_TYPE' in sourceDict: 
            try: 
                if addEntityType(g2Config,g2ConfigJson,sourceDict['ENTITY_TYPE'],configuredDatasourcesOnly) == 1: #--inserted
                    g2NewConfigRequired = True
            except G2Exception.UnconfiguredDataSourceException as err:
                print(err)
                return False

    # Add a new config, if we made changes
    if g2NewConfigRequired == True:
        if has_g2configfile:
            with open(has_g2configfile, 'w') as fp:
                json.dump(json.loads(g2ConfigJson), fp, indent = 4, sort_keys = True)
        else:
            g2ConfigMgr = G2ConfigMgr()
            g2ConfigMgr.initV2("g2ConfigMgr", g2module_params, False)
            new_config_id = bytearray()
            try: 
                g2ConfigMgr.addConfig(g2ConfigJson.decode(),'Updated From G2Loader', new_config_id)
            except G2Exception.G2Exception as err:
                print ("Error:  Failed to add new config to the datastore")
                return False
            try: 
                g2ConfigMgr.setDefaultConfigID(new_config_id)
            except G2Exception.G2Exception as err:
                print ("Error:  Failed to set new config as default")
                return False
            g2ConfigMgr.destroy()

    g2Config.destroy()

    return True

#---------------------------------------
def startLoaderProcessAndThreads(transportThreadCount):
    
    threadList = []
    workQueue = None

    # Start transport threads, bypass if in test mode
    if not args.testMode:
        threadStop.value = 0
        workQueue = Queue(transportThreadCount * 10)
        numThreadsLeft = transportThreadCount
        threadId = 0

        while (numThreadsLeft > 0):
            threadId += 1
            threadList.append(Process(target=sendToG2, args=(threadId, workQueue, min(args.max_threads_per_process, numThreadsLeft), args.debugTrace, threadStop, workloadStats, dsrcAction)))
            numThreadsLeft -= args.max_threads_per_process
        
        for thread in threadList:
            thread.start()

    return threadList, workQueue


#---------------------------------------
def sendToG2(threadId_, workQueue_, numThreads_, debugTrace, threadStop, workloadStats, dsrcAction):

    global numProcessed
    numProcessed = 0

    try:
        g2_engine = init_engine('pyG2Engine' + str(threadId_), g2module_params, debugTrace)
    except G2ModuleException as ex:
        print(f'ERROR: Could not start the G2 engine for sendToG2()')
        print(f'       {ex}')

        with threadStop.get_lock():
            threadStop.value = 1
        return

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

    except: 
        pass

    if workloadStats and numProcessed > 0:
        dump_workload_stats(g2_engine)

    try:
        g2_engine.destroy()
    except: 
        pass

    return

#---------------------------------------
def g2Thread(threadId_, workQueue_, g2Engine_, threadStop, workloadStats, dsrcAction):
    ''' g2 thread function ''' 

    global numProcessed

    # For each queue entry
    while threadStop.value == 0 or workQueue_.empty() == False:

        try:
            row = workQueue_.get(True, 1)
        except Empty as ex:
            row = None
            continue

        # Perform mapping if necessary
        if type(row) == dict:
            if '_MAPPING_FILE' not in row:
                rowList = [row]
            else:
                rowList, errorCount = g2Project.csvMapper(row)
                if errorCount:
                    recordList = []
        else:
            rowList = [row]

        # Call g2engine
        for row in rowList:

            dataSource = recordID = ''
            # Only umf is not a dict (csv and json are)
            if type(row) == dict:
                if dsrcAction in ('D', 'X'): #--strip deletes and reprocess (x) messages down to key only
                    newRow = {}
                    dataSource = newRow['DATA_SOURCE'] = row['DATA_SOURCE']
                    newRow['DSRC_ACTION'] = dsrcAction
                    if 'ENTITY_TYPE' in row:
                        newRow['ENTITY_TYPE'] = row['ENTITY_TYPE'] 
                    if 'ENTITY_KEY' in row:
                        newRow['ENTITY_KEY'] = row['ENTITY_KEY']
                    if 'RECORD_ID' in row:
                        recordID = newRow['RECORD_ID'] = row['RECORD_ID']
                    newRow['DSRC_ACTION'] = dsrcAction
                    #print ('-'* 25)
                    #print(json.dumps(newRow, indent=4))
                    row = newRow
                row = json.dumps(row, sort_keys=True)

            numProcessed += 1
            if (workloadStats and (numProcessed % (args.max_threads_per_process * args.loadOutputFrequency)) == 0):
                dump_workload_stats(g2Engine_)

            try: 
                # Temp fix for distinguising between X messages that are either JSON or UMF.
                row_is_json = type(row) == dict
                if row_is_json and dsrcAction == 'X':
                    g2Engine_.reevaluateRecord(dataSource, recordID, 0)
                else:
                    g2Engine_.process(row)
            except G2ModuleLicenseException as ex:
                print('ERROR: G2Engine licensing error!')
                print(f'     {ex}')
                with threadStop.get_lock():
                    threadStop.value = 1
                return
            except G2ModuleException as ex:
                print(f'ERROR: {ex}')
                print(f'       {row}')
            except Exception as ex:
                print(f'ERROR: {ex}')
                print(f'       {row}')
    return


#---------------------------------------
def stopLoaderProcessAndThreads(threadList, workQueue):
    
    if not args.testMode:

        # Stop the threads
        with threadStop.get_lock():
           if threadStop.value == 0:
               threadStop.value = 1

        for thread in threadList:
          thread.join()

        workQueue.close()


def dump_workload_stats(engine):
    ''' Print JSON workload stats '''

    response = bytearray()
    engine.stats(response)
    
    print(f'\n{json.dumps(json.loads(response.decode()))}\n')


def time_now(add_secs=False):
    ''' Date time now for processing messages '''

    fmt_string = '%I:%M%p'
    if add_secs:
        fmt_string = '%I:%M:%S%p'
    
    return datetime.now().strftime(fmt_string).lower()


def signal_int(signal, frame):
    ''' Signal interupt handler '''

    shutdown(f'USER INTERUPT ({signal}) - Stopping threads, please be patient this can take a number of minutes...')


def shutdown(msg=None):
    ''' Shutdown threads and exit cleanly from Governor if in use and clean up Governor '''

    if msg:
        print(f'{msg}')

    print('Shutting down...\n')

    with threadStop.get_lock():
        threadStop.value = 9

    global governor_cleaned
    governor_cleaned = True
    governor_cleanup()

    return


def governor_setup():
    ''' Import and create governors '''

    global record_governor
    global source_governor
    global redo_governor
    global governor_cleaned
    global governor
    global import_governor
    global redo_args

    import_governor = True
    record_governor = source_governor = redo_governor = governor_cleaned = False

    redo_args = {
        'type': 'Redo per redo record', 
        'g2module_params': g2module_params,
        'frequency': 'record'
    }

    # When Postgres always import the Postgres governor - unless requested off (e.g., getting started and no native driver) 
    if not args.governor and db_type == 'POSTGRESQL' and not args.governorDisable:
        print(f'\nUsing {db_type}, loading default governor: {default_postgres_governor}\n')
        import_governor = default_postgres_governor[:-3] if default_postgres_governor.endswith('.py') else default_postgres_governor
    # If a governor was specified import it
    elif args.governor:
        import_governor = args.governor[0][:-3] if args.governor[0].endswith('.py') else args.governor[0]
        print(f'\nLoading governor {import_governor}\n')
    # Otherwise use dummy class 
    else:
        import_governor = False

    if import_governor:
        try:
            governor = importlib.import_module(import_governor)
        except ImportError as ex:
            print(f'\nERROR: Unable to import governor {import_governor}') 
            print(f'       {ex}')
            sys.exit(1)
        else:

            # If not in redo mode create all governors we may call upon
            if not args.redoMode:
            
                # Init governors for each record, each source and redo. Governor creation sets defaults, can override. See sample governor-postgresXID.py
                # Minimum keyword args: g2module_params, frequency
                record_governor = governor.Governor(type='Ingest per source record', g2module_params=g2module_params, frequency='record')
                # Example of overriding governor init parms
                # record_governor = governor.Governor(type='Ingest per source record', g2module_params=g2module_params, frequency='record', wait_time=20, resume_age=5000, xid_age=1500000)
                source_governor = governor.Governor(type='Ingest per source', g2module_params=g2module_params, frequency='source')
            
                # Redo governor created and destroyed here to produce startup output, redo governor is created in redo process when used
                if not args.noRedo:
                    redo_governor = governor.Governor(type=redo_args['type'], g2module_params=redo_args['g2module_params'], frequency=redo_args['frequency'])
                    redo_governor.govern_cleanup()
            
            # If in redo mode only create a redo governor
            else:
                # Redo governor created and destroyed here to produce startup output, redo governor is created in redo process when used
                redo_governor = governor.Governor(type=redo_args['type'], g2module_params=redo_args['g2module_params'], frequency=redo_args['frequency'])
                redo_governor.govern_cleanup()
    # For dummy governor class            
    else:
        record_governor = Governor()
        source_governor = Governor()


def governor_cleanup():
    ''' Perform any actions defined in governor cleanup function '''

    if not args.redoMode:
        record_governor.govern_cleanup()
        source_governor.govern_cleanup()


#----------------------------------------

if __name__ == '__main__':

    exitCode = 0
    threadStop = Value('i', 0)
    time_starting_engines = Value('d', 0)
    time_governing = Value('d', 0)
    time_redo = Value('d', 0)
    drain_time_ingest = Value('i', 0)
    drain_time_source = Value('i', 0)

    signal.signal(signal.SIGINT, signal_int)
    DumpStack.listen()

    tmp_path = os.path.join(tempfile.gettempdir(), 'senzing', 'g2')
    default_postgres_governor = 'governor_postgres_xid.py'

    g2load_parser = argparse.ArgumentParser()
    g2load_parser.add_argument('-c', '--iniFile', dest='iniFile', default=None, help='the name of a G2Module.ini file to use', nargs=1)
    g2load_parser.add_argument('-T', '--testMode', dest='testMode', action='store_true', default=False, help='run in test mode to collect stats without loading, CTRL-C anytime')
    g2load_parser.add_argument('-X', '--reprocess', dest='reprocessMode', action='store_true', default=False, help='force reprocessing of previously loaded file')
    g2load_parser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
    g2load_parser.add_argument('-w', '--workloadStats', dest='workloadStats', action='store_false', default=False, help='DEPRECATED workload statistics on by default, -nw to disable')
    g2load_parser.add_argument('-nw', '--noWorkloadStats', dest='noWorkloadStats', action='store_false', default=True, help='disable workload statistics information')
    g2load_parser.add_argument('-n', '--noRedo', dest='noRedo', action='store_true', default=False, help='disable redo processing')
    g2load_parser.add_argument('-i', '--redoModeInterval', dest='redoModeInterval', type=int, default=60, help='time in secs to wait between redo processing checks, only used in redo mode')
    g2load_parser.add_argument('-k', '--knownDatasourcesOnly', dest='configuredDatasourcesOnly', action='store_true', default=False, help='only accepts configured and known data sources')
    g2load_parser.add_argument('-j', '--createJsonOnly', dest='createJsonOnly', action='store_true', default=False, help='only create JSON files from mapped CSV files')
    g2load_parser.add_argument('-mtp', '--maxThreadsPerProcess', dest='max_threads_per_process', default=16, type=int, help='maximum threads per process, default=%(default)s')
    g2load_parser.add_argument('-g', '--governor', dest='governor', default=None, help='user supplied governor to load and call during processing', nargs=1)
    g2load_parser.add_argument('-gpd', '--governorDisable', dest='governorDisable', action='store_true', default=False, help='disable default Postgres governor, when repository is Postgres')
    g2load_parser.add_argument('-tmp', '--tmpPath', default=tmp_path, help=f'use this path instead of {tmp_path} (For S3 files)', nargs='?')

    # Both -nt and -ntm shouldn't be used together
    num_threads_group = g2load_parser.add_mutually_exclusive_group()
    num_threads_group.add_argument('-nt', '--threadCount', dest='thread_count', type=int, default=0, help='total number of threads to start, default is calculated')
    num_threads_group.add_argument('-ntm', '--threadCountMem', dest='threadCountMem', default=None, const=80, nargs='?', type=int, choices=range(10,81), metavar='10-80', help='maximum memory %% to use when calculating threads (when -nt not specified), default=%(const)s')
    # Both -p and -f shouldn't be used together
    file_project_group = g2load_parser.add_mutually_exclusive_group()
    file_project_group.add_argument('-p', '--projectFile', dest='projectFileName', default=None, help='the name of a project CSV or JSON file')
    file_project_group.add_argument('-f', '--fileSpec', dest='projectFileSpec', default=None, help='the name of a file and parameters to load such as /data/mydata.json/?data_source=?,file_format=?', nargs='+')
    # Both -ns and -nsd shouldn't be used together
    no_shuf_no_shuf_del = g2load_parser.add_mutually_exclusive_group()
    no_shuf_no_shuf_del.add_argument('-ns', '--noShuffle', dest='noShuffle', action='store_true', default=False, help='don\'t shuffle input file(s), shuffling improves performance')
    no_shuf_no_shuf_del.add_argument('-nsd', '--noShuffleDelete', dest='noShuffleDelete', action='store_true', default=False, help='don\'t delete shuffled file(s), default is to delete')
    # Both -R and -sr shouldn't be used together
    stop_row_redo_node= g2load_parser.add_mutually_exclusive_group()
    stop_row_redo_node.add_argument('-R', '--redoMode', dest='redoMode', action='store_true', default=False, help='run in redo mode only processesing the redo queue')
    stop_row_redo_node.add_argument('-sr', '--stopOnRecord', dest='stopOnRecord', default=0, type=int, help='stop processing after n recods (for testing large files)')
    # Both -P and -D shouldn't be used together
    purge_dsrc_delete = g2load_parser.add_mutually_exclusive_group()
    purge_dsrc_delete.add_argument('-D', '--delete', dest='deleteMode', action='store_true', default=False, help='force deletion of a previously loaded file')
    purge_dsrc_delete.add_argument('-P', '--purgeFirst', dest='purgeFirst', action='store_true', default=False, help='purge the Senzing repository before loading, confirmation prompt before purging')
    g2load_parser.add_argument('-FORCEPURGE', '--FORCEPURGE', dest='forcePurge', action='store_true', default=False, help='purge the Senzing repository before loading, no confirmation prompt before purging')
    # Options hidden from help, mostly used for testing
    # Frequency to ouput load and redo rate
    g2load_parser.add_argument('-lof', '--loadOutputFrequency', dest='loadOutputFrequency', default=1000, type=int, help=argparse.SUPPRESS)
    # Frequency to pause loading and perform redo
    g2load_parser.add_argument('-rif', '--redoInterruptFrequency', dest='redoInterruptFrequency', default=100000, type=int, help=argparse.SUPPRESS)

    args = g2load_parser.parse_args()
    
    if len(sys.argv) < 2:
        print(f'\n{g2load_parser.format_help()}')
        sys.exit(0)

    # Check G2Project.ini isn't being used, now deprecated. 
    # Don't try and auto find G2Module.ini, safer to ask to be specified during this change!
    if args.iniFile and 'G2PROJECT.INI' in args.iniFile[0].upper():
        print('\nINFO: G2Loader no longer uses G2Project.ini, it is deprecated and uses G2Module.ini instead.')
        print('      G2Loader attempts to locate a default G2Module.ini (no -c) or use -c to specify path/name to your G2Module.ini')
        sys.exit(0)

    # If ini file isn't specified try and locate it with G2Paths
    iniFileName = pathlib.Path(G2Paths.get_G2Module_ini_path()) if not args.iniFile else pathlib.Path(args.iniFile[0]).resolve()
    G2Paths.check_file_exists_and_readable(iniFileName)

    # Warn if using out dated parms
    g2health = G2Health()
    g2health.checkIniParams(iniFileName)

    # Get the INI paramaters to use
    iniParamCreator = G2IniParams()
    g2module_params = iniParamCreator.getJsonINIParams(iniFileName)

    # Deprecated but still supported at this time, is G2CONFIGFILE being used? 
    has_g2configfile = json.loads(g2module_params)['SQL'].get('G2CONFIGFILE', None)
    
    # Check what DB Type - new API requested for engine to return instead of parsing here when added to engine
    conn_str = json.loads(g2module_params)['SQL'].get('CONNECTION', None)
    try:
        db_type = conn_str.split('://')[0].upper()
    except:
        print(f'\nERROR: Unable to determine DB type, is CONNECTION correct in INI file?')
        print(f'       {iniFileName}')
        sys.exit(1)

    # Load sample data if neither -p or -f and not in redo mode
    if not args.projectFileName and not args.projectFileSpec and not args.redoMode :
        print(f'\nNo source file or project file was specified, loading sample data')
        # Convert the path to a string, G2Project needs updating to accomodate pathlib objects
        args.projectFileName = str(pathlib.Path(os.environ.get('SENZING_ROOT', '/opt/senzing/g2/')).joinpath('python', 'demo', 'sample', 'project.csv'))

    # Are you really sure you want to purge!
    if args.purgeFirst and not args.forcePurge:
        if not input(textwrap.dedent('''
            WARNING: Purging (-P) will delete all loaded data from the Senzing repository!
                     Bypass this confirmation using the --FORCEPURGE command line argument

            Type YESPURGE to continue and purge or enter to quit:
        ''')) == "YESPURGE":
           sys.exit(0)

    # Check resources and acquire num threads
    defaultThreadCount = check_resources(args.thread_count)

    # -w has been deprecated and the default is now output workload stats, noWorkloadStats is checked in place of
    # workloadStats. -w will be removed in future updates.
    workloadStats = args.noWorkloadStats
    
    if args.createJsonOnly:
        args.testMode = True

    # Setup the governor(s)
    governor_setup()

    # Set DSRC mode
    dsrcAction = 'A'
    if args.deleteMode:
        dsrcAction = 'D'
    if args.reprocessMode:
        dsrcAction = 'X'

    # Running in redo only mode? Don't purge in redo only mode, would purge the queue!
    if args.redoMode:
        exitCode = runSetupProcess(False) 
        while threadStop.value != 9 and exitCode == 0:
            print('\nStarting in redo only mode, processing redo queue (CTRL-C to quit)\n')
            exitCode = loadRedoQueueAndProcess()
            if threadStop.value == 9 or exitCode != 0:
                    break
    else:
        exitCode = perform_load()

    # Perform governor clean up
    if not governor_cleaned:
        governor_cleanup()

    sys.exit(exitCode)
