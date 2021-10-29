#! /usr/bin/env python3

import argparse
import importlib
import json
import math
import os
import pathlib
import select
import signal
import subprocess
import sys
import tempfile
import textwrap
import threading
import time
from contextlib import suppress
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
from G2Exception import G2ModuleException, G2ModuleLicenseException
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


def check_resources_and_startup(returnQueue, thread_count, doPurge, doLicense=True):
    ''' Check system resources, calculate a safe number of threads when argument not specified on command line '''

    try:
        diag = G2Diagnostic()
        diag.init('pyG2Diagnostic', g2module_params, args.debugTrace)
    except G2ModuleException as ex:
        print('\nERROR: Could not start G2Diagnostic for check_resources_and_startup()')
        print(f'       {ex}')
        returnQueue.put(-1)
        return

    try:
        g2_engine = init_engine('pyG2StartSetup', g2module_params, args.debugTrace, prime_engine=False)
    except G2ModuleException as ex:
        print('ERROR: Could not start the G2 engine for check_resources_and_startup()')
        print(f'       {ex}')
        returnQueue.put(-1)
        return

    try:
        g2_configmgr = G2ConfigMgr()
        g2_configmgr.init('pyG2ConfigMgr', g2module_params, args.debugTrace)
    except G2ModuleException as ex:
        print('ERROR: Could not start G2ConfigMgr for check_resources_and_startup()')
        print(f'       {ex}')
        returnQueue.put(-1)
        return

    try:
        g2_product = G2Product()
        g2_product.init('pyG2LicenseVersion', g2module_params, args.debugTrace)
    except G2ModuleException as ex:
        print('ERROR: Could not start G2Product for check_resources_and_startup()')
        print(f'       {ex}\n')
        returnQueue.put(-1)
        return

    licInfo = json.loads(g2_product.license())
    verInfo = json.loads(g2_product.version())

    # Get the configuration list
    try:
        response = bytearray()
        g2_configmgr.getConfigList(response)
        config_list = json.loads(response.decode())
    except G2Exception.G2Exception as ex:
        print('ERROR: Could not get config list in check_resources_and_startup()')
        print(f'       {ex}')
        returnQueue.put(-1)
        return

    # Get the active config ID
    try:
        response = bytearray()
        g2_engine.getActiveConfigID(response)
        active_cfg_id = int(response.decode())
    except G2Exception.G2Exception as ex:
        print('ERROR: Could not get the active config in check_resources_and_startup()')
        print(f'       {ex}')
        returnQueue.put(-1)
        return

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

    physical_cores = diag.getPhysicalCores()
    logical_cores = diag.getLogicalCores()
    available_mem = diag.getAvailableMemory() / 1024 / 1024 / 1024.0
    total_mem = diag.getTotalSystemMemory() / 1024 / 1024 / 1024.0

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

    print(textwrap.dedent(f'''\n\
        System Resources
        ----------------

            Physical cores:         {physical_cores}
            Logical cores:          {logical_cores if physical_cores != logical_cores else ''}
            Total memory (GB):      {total_mem:.1f}
            Available memory (GB):  {available_mem:.1f}
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
            thread_count = math.ceil(thread_calc_from_mem / 2)
            calc_thread_msg = f'- WARNING: System has less than 1 GB {(thread_calc_from_mem / physical_cores):.2f} GB available per physical core.\n \
                            Number of threads will be significantly reduced, you may see further warnings and should check your resources.'

        # If a value was specified for -ntm override the thread count with the min of calc value or possible_num_threads
        thread_count = min(math.ceil(available_mem / 100 * args.threadCountMem), possible_num_threads) if args.threadCountMem else thread_count
        mem_percent = calc_max_avail_mem if not args.threadCountMem else args.threadCountMem
        mem_msg = 'available' if not args.threadCountMem else 'requested (-ntm)'
        calc_thread_msg = calc_thread_msg if not args.threadCountMem else ''

        # Don't reformat and move end ''')), it's neater to do this than try and move cursor for next optional calc_thread_msg
        print(textwrap.dedent(f'''
            Number of threads
            -----------------

                - Number of threads arg (-nt) not specified. Calculating number of threads using {mem_percent}% of {mem_msg} memory.
                - Monitor system resources. Use command line argument -nt to increase (or decrease) the number of threads on subsequent use.'''))

        # Add extra message if args.threadCountMem wasn't specified
        if calc_thread_msg:
            print(f'    {calc_thread_msg}')
        print()

    # Limit number of threads when sqlite, unless -nt arg specified
    if db_type == 'SQLITE3':
        if args.thread_count:
            thread_count = args.thread_count
            if thread_count > max_sqlite_threads:
                sqlite_limit_msg = f' - WARNING: Greater than {max_sqlite_threads} likely slower when the database is sqlite'
                sqlite_warned = True
        else:
            thread_count = min(thread_count, max_sqlite_threads)
            sqlite_limit_msg = f' - Limited to {max_sqlite_threads} when the database is sqlite'

    # 2.5GB per process - .5GB per thread
    min_recommend_cores = math.ceil(thread_count / 4 + 1)
    num_processes = math.ceil(float(thread_count) / args.max_threads_per_process)
    min_recommend_mem = (num_processes * 2.5 + thread_count * .5)

    print(textwrap.dedent(f'''\n\
        Resources Requested
        -------------------

            Number of threads:           {thread_count} {sqlite_limit_msg}
            Threads calculated:          {'Yes' if not args.thread_count else 'No, -nt argument was specified'}
            Threads per process:         {args.max_threads_per_process}
            Number of processes:         {num_processes}
            Min recommeded cores:        {min_recommend_cores}
            Min recommeded memory (GB):  {min_recommend_mem:.1f}
        '''))

    if sqlite_warned:
        print(pause_msg, flush=True)
        time.sleep(10)

    # Skip perf check if specified on CLI args or container env var
    if not args.skipDBPerf and not env_var_skip_dbperf:
        print(textwrap.dedent('''\n\
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
                Records inserted:    {num_recs_inserted}
                Period for inserts:  {insert_time} ms
                Average per insert:  {time_per_insert:.1f} ms
            '''), '    '))
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

    if critical_error or warning_issued:
        print(pause_msg, flush=True)
        time.sleep(10 if critical_error else 3)

    # Purge repository
    if doPurge:
        print('\nPurging Senzing database...')
        g2_engine.purgeRepository(False)

    # Clean up (in reverse order of initialization)
    g2_product.destroy()
    del g2_product

    g2_configmgr.destroy()
    del g2_configmgr

    g2_engine.destroy()
    del g2_engine

    diag.destroy()
    del diag

    # Return values are put in a queue
    returnQueue.put(thread_count)


def perform_load():
    ''' Main processing when not in redo only mode '''

    exitCode = 0
    DumpStack.listen()
    procStartTime = time.time()

    # Prepare the G2 configuration
    g2ConfigJson = bytearray()
    tempQueue = Queue()
    getInitialG2ConfigProcess = Process(target=getInitialG2Config_processWrapper, args=(tempQueue, g2module_params, g2ConfigJson))
    getInitialG2ConfigProcess.start()
    g2ConfigJson = tempQueue.get(block=True)
    resultOfGetInitialG2Config = tempQueue.get(block=True)
    getInitialG2ConfigProcess.join()

    if not resultOfGetInitialG2Config:
        return (1, 0)

    g2ConfigTables = G2ConfigTables(g2ConfigJson)

    g2Project = G2Project(g2ConfigTables, dsrcAction, args.projectFileName, args.projectFileSpec, args.tmpPath)
    if not g2Project.success:
        return (1, 0)

    # Enhance the G2 configuration, by adding data sources and entity types
    if not args.testMode:
        tempQueue = Queue()
        enhanceG2ConfigProcess = Process(target=enhanceG2Config_processWrapper, args=(tempQueue, g2Project, g2module_params, g2ConfigJson, args.configuredDatasourcesOnly))
        enhanceG2ConfigProcess.start()
        g2ConfigJson = tempQueue.get(block=True)
        resultOfEnhanceG2Config = tempQueue.get(block=True)
        enhanceG2ConfigProcess.join()
        if not resultOfEnhanceG2Config:
            return (1, 0)

    # Start loading
    for sourceDict in g2Project.sourceList:

        filePath = sourceDict['FILE_PATH']
        orig_filePath = filePath
        shuf_detected = False

        cntRows = cntBadParse = cntBadUmf = cntGoodUmf = api_errors.value = 0
        dsrc_action_add_count.value = dsrc_action_del_count.value = dsrc_action_reeval_count.value = 0

        g2Project.clearStatPack()

        if args.testMode:
            print(f'\nTesting {filePath}, CTRL-C to end test at any time...\n')
        else:
            if dsrcAction == 'D':
                print(f'\n{"-"*30}  Deleting  {"-"*30}\n')
            elif dsrcAction == 'X':
                print(f'\n{"-"*30}  Reevaluating  {"-"*30}\n')
            else:
                print(f'\n{"-"*30}  Loading  {"-"*30}\n')

        # Drop to a single thread for files under 500k
        if os.path.getsize(filePath) < (100000 if isCompressedFile(filePath) else 500000):
            print('  Dropping to single thread due to small file size')
            transportThreadCount = 1
        else:
            transportThreadCount = defaultThreadCount

        # Shuffle the source file for performance, unless directed not to or in test mode or single threaded
        if not args.noShuffle and not args.testMode and transportThreadCount > 1:

            if isCompressedFile(filePath):
                print('INFO: Not shuffling compressed file. Please ensure the data was shuffled before compressing!\n')

            # If it looks like source file was previously shuffled by G2Loader don't do it again
            elif SHUF_NO_DEL_TAG in filePath or SHUF_TAG in filePath:

                shuf_detected = True
                print(f'INFO: Not shuffling source file, previously shuffled. {SHUF_TAG} or {SHUF_NO_DEL_TAG} in file name\n')
                if SHUF_NO_DEL_TAG in filePath and args.shuffleNoDelete:
                    print(f'INFO: Source files with {SHUF_NO_DEL_TAG} in the name are not deleted by G2Loader. Argument -snd (--shuffleNoDelete) used\n')
                time.sleep(10)

            else:

                # Add timestamp to no delete shuffled files
                shuf_file_suffix = SHUF_NO_DEL_TAG + datetime.now().strftime("%Y%m%d_%H-%M-%S") if args.shuffleNoDelete else SHUF_TAG
                plib_file_path = pathlib.Path(filePath).resolve()
                shuf_file_path = pathlib.Path(str(plib_file_path) + shuf_file_suffix)

                # Look for previously shuffled files in orginal path...
                if not args.shuffFilesIgnore:

                    prior_shuf_files = [str(pathlib.Path(p).resolve()) for p in glob(filePath + SHUF_TAG_GLOB)]

                    # ...and  shuffle redirect path if specified
                    if args.shuffFileRedirect:
                        redirect_glob = str(shuf_path_redirect.joinpath(plib_file_path.name)) + SHUF_TAG_GLOB
                        prior_shuf_files.extend([str(pathlib.Path(p).resolve()) for p in glob(redirect_glob)])

                    if prior_shuf_files:
                        print(f'\nFound previously shuffled files matching {plib_file_path.name}...\n')

                        prior_shuf_files.sort()
                        for psf in prior_shuf_files:
                            print(f'  {psf}')

                        print(textwrap.dedent(f'''
                            Pausing for {SHUF_RESPONSE_TIMEOUT} seconds... (This check can be skipped with the command line argument --shuffFilesIgnore (-sfi) )

                            The above listed files may not contain the same data as the input file.
                            If you wish to use one of the above, please check and compare the files to ensure the previously shuffled file is what you expect.

                            To quit and use a previously shuffled file hit <Enter>. To continue, wait {SHUF_RESPONSE_TIMEOUT} seconds or type c and <Enter>
                        '''))

                        # Wait 30 seconds to allow user to use a prior shuffle, timeout and continue if automated
                        while True:
                            r, _, _ = select.select([sys.stdin], [], [], SHUF_RESPONSE_TIMEOUT)
                            if r:
                                # Read wihtout hitting enter?
                                read_input = sys.stdin.readline()
                                if read_input == '\n':
                                    sys.exit(0)
                                elif read_input.lower() == 'c\n':
                                    break
                                else:
                                    print('<Enter> to quit or type c and <Enter> to continue: ')
                            else:
                                break

                else:
                    print(f'INFO: Skipping check for previously shuffled files for {plib_file_path.name}')

                # If redirecting the shuffled file modify to redirect path
                if args.shuffFileRedirect:
                    shuf_file_path = shuf_path_redirect.joinpath(shuf_file_path.name)

                print(f'\nShuffling file to: {shuf_file_path}\n')

                cmd = f'shuf {filePath} > {shuf_file_path}'
                if sourceDict['FILE_FORMAT'] not in ('JSON', 'UMF'):
                    cmd = f'head -n1 {filePath} > {shuf_file_path} && tail -n+2 {filePath} | shuf >> {shuf_file_path}'

                try:
                    process = subprocess.run(cmd, shell=True, check=True)
                except subprocess.CalledProcessError as ex:
                    print(f'\nERROR: Shuffle command failed: {ex}')
                    return (1, 0)

                filePath = str(shuf_file_path)

        fileReader = openPossiblyCompressedFile(filePath, 'r')
        #--fileReader = safe_csv_reader(csv.reader(csvFile, fileFormat), cntBadParse)

        # Use previously stored header row, so get rid of this one
        if sourceDict['FILE_FORMAT'] not in ('JSON', 'UMF'):
            next(fileReader)

        # Start processes and threads for this file
        threadList, workQueue = startLoaderProcessAndThreads(transportThreadCount)

        if threadStop.value != 0:
            return (exitCode, 0)

        # Start processing rows from source file
        fileStartTime = time.time()
        batchStartTime = time.perf_counter()

        cntRows = batch_time_governing = time_redo.value = time_governing.value = dsrc_action_diff.value = 0
        global del_errors_file

        while True:

            try:
                row = next(fileReader)
            except StopIteration:
                break
            except Exception as ex:
                cntRows += 1
                cntBadParse += 1
                print(f'WARNING: Could not read row {cntRows}, {ex}')
                continue

            # Increment row count to agree with line count and references to bad rows are correct
            cntRows += 1

            # Skip records
            if not args.redoMode and args.skipRecords and cntRows < args.skipRecords + 1:
                if cntRows == 1 or cntRows % args.loadOutputFrequency == 0:
                    print(f'INFO: Skipping the first {args.skipRecords} records{"..." if cntRows == 1 else ","} {"skipped " + str(cntRows) + "..." if cntRows > 1 else ""}')
                continue

            # Skip blank or records that error, errors written to errors file if not disabled
            rowData = fileRowParser(row, sourceDict, cntRows, errors_file=errors_file, errors_short=args.errorsShort, errors_disable=args.errorsDisable)
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

                if args.testMode:
                    mappingResponse = g2Project.testJsonRecord(rowData, cntRows, sourceDict)
                    if mappingResponse[0]:
                        cntBadUmf += 1
                        okToContinue = False

                #--only add force a load_id if not in test mode (why do we do this??)
                if 'LOAD_ID' not in rowData:
                    rowData['LOAD_ID'] = sourceDict['FILE_NAME']

            # Put the record on the queue
            if okToContinue:
                cntGoodUmf += 1
                if not args.testMode:
                    while True:
                        try:
                            # Assist in indicating what type of record this is for processing thread
                            # Detect and set here if dsrc action was set as reeval on args
                            workQueue.put((rowData, True if dsrcAction == 'X' else False), True, 1)
                        except Full:
                            # Check to see if any threads have died
                            for thread in threadList:
                                if thread.is_alive() is False:
                                    print(textwrap.dedent('''\n\
                                        ERROR: Thread(s) have shutdown unexpectedly!

                                               - This typically happens when memory resources are exhausted and the system randomly kills processes.

                                               - Please review: https://senzing.zendesk.com/hc/en-us/articles/115000856453

                                               - Check output from the following command for out of memory messages.

                                                    - dmesg -e
                                    '''))
                                    return (1, cntBadParse)
                            continue
                        break

            if cntRows % args.loadOutputFrequency == 0:
                batchSpeed = int(args.loadOutputFrequency / (time.perf_counter() - (batchStartTime - batch_time_governing))) if time.perf_counter() - batchStartTime != 0 else 1
                print(f'  {cntRows} rows processed at {time_now()}, {batchSpeed} records per second{", " +  str(api_errors.value) + " API errors" if api_errors.value > 0 else ""}')

                batchStartTime = time.perf_counter()
                batch_time_governing = 0

            # Process redo during ingestion
            if cntRows % args.redoInterruptFrequency == 0 and not args.testMode and not args.noRedo:
                if processRedo(workQueue, True, 'Waiting for processing queue to empty to start redo...'):
                    print('\nERROR: Could not process redo record!\n')

            # Check to see if any threads threw errors or control-c pressed and shut down
            if threadStop.value != 0:
                exitCode = threadStop.value
                break

            # Check if any of the threads died without throwing errors
            areAlive = True
            for thread in threadList:
                if thread.is_alive() is False:
                    print('\nERROR: Thread failure!')
                    areAlive = False
                    break
            if areAlive is False:
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
            if not args.redoMode and args.stopOnRecord and cntRows >= args.stopOnRecord:
                print(f'\nINFO: Stopping at record {cntRows}, --stopOnRecord (-sr) argument was set')
                break

        # Process redo at end of processing a source. Wait for queue to empty of ingest records first
        if threadStop.value == 0 and not args.testMode and not args.noRedo:
            if processRedo(workQueue, True, 'Source file processed, waiting for processing queue to empty to start redo...'):
                print('\nERROR: Could not process redo record!\n')

        end_time = time_now(True)

        # Close input file
        fileReader.close()

        if sourceDict['FILE_SOURCE'] == 'S3':
            print(" Removing temporary file created by S3 download " + filePath)
            os.remove(filePath)

        # Remove shuffled file unless run with -snd or prior shuffle detected and not small file/low thread count
        if not args.shuffleNoDelete \
           and not shuf_detected \
           and not args.noShuffle \
           and not args.testMode \
           and transportThreadCount > 1:
            with suppress(Exception):
                print(f'\nDeleting shuffled file: {shuf_file_path}')
                os.remove(shuf_file_path)

        # Stop processes and threads
        stopLoaderProcessAndThreads(threadList, workQueue)

        if cntBadParse > 0 or api_errors.value > 0:
            del_errors_file = False

        # Print load stats if not error or ctrl-c
        if exitCode in (0, 9):
            elapsedSecs = time.time() - fileStartTime
            elapsedMins = round(elapsedSecs / 60, 1)

            # Calculate approximate transactions/sec, remove timings that aren't part of ingest
            fileTps = int((cntGoodUmf + cntBadParse + cntBadUmf) / (elapsedSecs - time_governing.value - time_starting_engines.value - time_redo.value)) if elapsedSecs > 0 else 0

            # Use good records count instead of 0 on small fast files
            fileTps = fileTps if fileTps > 0 else cntGoodUmf

            if shuf_detected:
                shuf_msg = 'Shuffling skipped, file was previously shuffled by G2Loader'
            elif transportThreadCount > 1:
                if args.noShuffle:
                    shuf_msg = 'Not shuffled (-ns was specified)'
                else:
                    shuf_msg = shuf_file_path if args.shuffleNoDelete and 'shuf_file_path' in locals() else 'Shuffled file deleted (-snd to keep after load)'
            else:
                shuf_msg = 'File wasn\'t shuffled, small size or number of threads was 1'

            # Format with seperator if specified
            skip_records = f'{args.skipRecords:,}' if args.skipRecords and args.skipRecords != 0 else ''
            stop_on_record = f'{args.stopOnRecord:,}' if args.stopOnRecord and args.stopOnRecord != 0 else ''

            # Set error log file to blank or disabled msg if no errors or arg disabled it
            errors_log_file = errors_file.name if errors_file else ''

            if del_errors_file and not cntBadParse:
                errors_log_file = 'No errors'

            if args.errorsDisable:
                errors_log_file = 'Disabled with -ed'

            rec_dsrc_action = dsrc_action_add_count.value + dsrc_action_del_count.value + dsrc_action_reeval_count.value if dsrc_action_diff.value else 0
            if dsrcAction != 'A':
                rec_dsrc_action = f'Not considered, explicit mode specified ({dsrc_action_names[dsrcAction]})'

            print(textwrap.dedent(f'''\n\
                    Processing Information
                    ----------------------

                        Arguments:                      {" ".join(sys.argv[1:])}
                        Action                          {dsrc_action_names[dsrcAction]}
                        Repository purged:              {'Yes' if (args.purgeFirst or args.forcePurge) else 'No'}
                        File loaded:                    {pathlib.Path(orig_filePath).resolve()}
                        Shuffled into:                  {shuf_msg}
                        Total records:                  {cntGoodUmf + cntBadParse + cntBadUmf}
                        \tGood records:               {cntGoodUmf:,}
                        \tBad records:                {cntBadParse:,}
                        \tIncomplete records:         {cntBadUmf:,}
                        Records specifying action:      {rec_dsrc_action}
                        \tAdds:                       {dsrc_action_add_count.value}
                        \tDeletes:                    {dsrc_action_del_count.value}
                        \tReeval:                     {dsrc_action_reeval_count.value}
                        Errors:
                        \tErrors log file:            {errors_log_file}
                        \tFailed API calls:           {api_errors.value:,}
                        Skipped records:                {skip_records + ' (-skr was specified)' if args.skipRecords else 'Not requested'}
                        Stop on record:                 {stop_on_record + ' (-sr was specified)' if args.stopOnRecord else 'Not requested'}
                        Total elapsed time:             {elapsedMins} mins
                        \tStart time:                 {datetime.fromtimestamp(fileStartTime).strftime('%I:%M:%S%p').lower()}
                        \tEnd time:                   {end_time}
                        \tTime processing redo:       {str(round((time_redo.value) / 60, 1)) + ' mins' if not args.testMode and not args.noRedo else 'Redo disabled (-n)'}
                        \tTime paused in governor(s): {round(time_governing.value / 60, 1)} mins
                        Records per second:             {fileTps:,}
                '''))

        # Don't process next source file if errors
        if exitCode:
            break

    elapsed_mins = round((time.time() - procStartTime) / 60, 1)

    if exitCode:
        print(f'\nProcess aborted at {time_now()} after {elapsed_mins} minutes')
    else:
        print(f'\nProcess completed successfully at {time_now()} in {elapsed_mins} minutes')
        if args.noRedo:
            print(textwrap.dedent(f'''\n

                    Process Redo Records
                    --------------------

                    Loading is complete but the processing of redo records was disabled with --noRedo (-n).

                    All source records have been entity resolved, there may be minor clean up of some entities
                    required. Please review: https://senzing.zendesk.com/hc/en-us/articles/360007475133

                    If redo records are not being processed separately by another G2Loader instance in redo only
                    mode - or another process you've built to manage redo records - run G2Loader again in redo
                    only mode:

                        ./G2Loader.py --redoMode {'--iniFile ' + str(iniFileName) if args.iniFile else ''}

                    or:

                        ./G2Loader.py -R {'-c ' + str(iniFileName) if args.iniFile else ''}

                    '''))

    if args.testMode:

        report = g2Project.getTestResults('F')
        report += '\nPress (s)ave to file or (q)uit when reviewing is complete\n'

        less = subprocess.Popen(["less", "-FMXSR"], stdin=subprocess.PIPE)

        # Less returns BrokenPipe if hitting q to exit earlier than end of report
        try:
            less.stdin.write(report.encode('utf-8'))
        except IOError:
            pass

        less.stdin.close()
        less.wait()

    # Governor called for each source
    try:
        source_governor.govern()
    except Exception as err:
        shutdown(f'\nERROR: Calling per source governor: {err}')

    return (exitCode, cntBadParse)


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


def sendToG2(threadId_, workQueue_, numThreads_, debugTrace, threadStop, workloadStats, dsrcAction):

    global num_processed
    num_processed = 0

    try:
        g2_engine = init_engine('pyG2Engine' + str(threadId_), g2module_params, debugTrace, prime_engine=True, add_start_time=True)
    except G2ModuleException as ex:
        print('ERROR: Could not start the G2 engine for sendToG2()')
        print(f'       {ex}')

        with threadStop.get_lock():
            threadStop.value = 1
        return

    try:

        if (numThreads_ > 1):
            threadList = []

            for myid in range(numThreads_):
                threadList.append(threading.Thread(target=g2Thread, args=(str(threadId_) + "-" + str(myid), workQueue_, g2_engine, threadStop, workloadStats, dsrcAction)))

            for thread in threadList:
                thread.start()

            for thread in threadList:
                thread.join()
        else:
            g2Thread(str(threadId_), workQueue_, g2_engine, threadStop, workloadStats, dsrcAction)

    except Exception:
        pass

    if workloadStats and num_processed > 0:
        dump_workload_stats(g2_engine)

    with suppress(Exception):
        g2_engine.destroy()
        del g2_engine

    return


def g2Thread(threadId_, workQueue_, g2Engine_, threadStop, workloadStats, dsrcActionArgs):
    ''' g2 thread function '''

    global num_processed

    def parse_json(row):
        ''' Parse record id and data source from a JSON record'''

        data_source = row.get('DATA_SOURCE', None)
        record_id = row.get('RECORD_ID', None)

        return (data_source, record_id)

    def parse_umf(row):
        ''' Parse record id and data source from a UMF record'''

        data_source = record_id = None

        # Find delimiters for dsrc_code and record_id
        dsrc_start = row.find(DSRC_START_STR)
        dsrc_end = row.find(DSRC_END_STR)
        rid_start = row.find(RID_START_STR)
        rid_end = row.find(RID_END_STR)

        # Only modify dataSource & recordID if strings found, catch exceptions in API call if missing
        if dsrc_start != -1 and dsrc_end != -1 and rid_start != -1 and rid_end != -1:
            data_source = row[dsrc_start + len(DSRC_START_STR):dsrc_end]
            record_id = row[rid_start + len(RID_START_STR):rid_end]

        return (data_source, record_id)

    def g2thread_error(msg, dsrc_action_str):
        ''' Write out errors during processing '''

        global del_errors_file
        del_errors_file = False

        call_error = textwrap.dedent(f'''
            {str(datetime.now())} ERROR: {dsrc_action_str} - {msg}
                Data source: {dataSource} - Record ID: {recordID}
            ''')

        # If logging to error file is disabled print instead
        if args.errorsDisable:
            print(call_error, flush=True)

        if not args.errorsDisable:
            try:
                row_out = f'\n\t{row}\n' if not args.errorsShort else '\n'
                errors_file.write(call_error + f'\tRecord Type: {"Redo" if is_redo_record else "Ingest"}{row_out}')
                errors_file.flush()
            except Exception as ex:
                print(f'\nWARNING: Unable to write API error to {errors_file.name}')
                print(f'        {ex}')

        # Increment value to report at end of processing each source
        api_errors.value += 1

    # For each work queue item
    while threadStop.value == 0 or workQueue_.empty() is False:

        try:
            row = workQueue_.get(True, 1)
        except Empty:
            row = None
            continue

        # Unpack tuple from the work queue into the data and indicator for being a redo record
        row, is_redo_record = row

        dataSource = recordID = dsrc_action_str = None
        # Start with dsrdAction set to what was used as the CLI arg or default of add
        dsrcAction = dsrcActionArgs

        # Record is JSON
        if isinstance(row, dict):
            dataSource, recordID = parse_json(row)
        # Record is UMF
        else:
            dataSource, recordID = parse_umf(row)

        # Is the record from the work queue specifically a redo record to be processed during redo time/mode?
        if is_redo_record:
            dsrcAction = 'X'
        # If not, it's a normal ingestion record from file or project
        else:
            # If -D and -X were not specified, check each record for dsrc_action and use it instead of default add mode
            # Consideration of dsrc_action is only valid in default add mode
            if not args.deleteMode and not args.reprocessMode:

                # Use the DSRC_ACTION from inbound row?
                # Check if the inbound row specifies dsrc_action, use it and override CLI args (X, D, default is A) if present
                # This provides functionality of sending in input file with multiple dsrc actions
                row_dsrc_action = row.get('DSRC_ACTION', None)
                dsrcAction = row_dsrc_action if row_dsrc_action else dsrcAction
                dsrcAction = dsrcAction.upper() if isinstance(dsrcAction, str) else dsrcAction

                # If the row dsrc_action differs from the CLI ARGs dsrc_action mode, log the fact to print info at end of data source
                if dsrcAction != dsrcActionArgs:
                    # Not enabled, could quickly fill up redirected logging files
                    if dsrc_action_diff.value != 1:
                        with dsrc_action_diff.get_lock():
                            dsrc_action_diff.value = 1

                    if dsrcAction == 'A':
                        with dsrc_action_add_count.get_lock():
                            dsrc_action_add_count.value += 1

                    if dsrcAction == 'D':
                        with dsrc_action_del_count.get_lock():
                            dsrc_action_del_count.value += 1

                    if dsrcAction == 'X':
                        with dsrc_action_reeval_count.get_lock():
                            dsrc_action_reeval_count.value += 1

        try:

            # Catch invalid dsrc_actions and push to error log and log as an API error
            if dsrcAction not in ("A", "D", "X"):
                g2thread_error('Unknown dsrc_action', dsrcAction)
                continue

            if dsrcAction == 'A':
                dsrc_action_str = 'addRecord()'
                g2Engine_.addRecord(dataSource, str(recordID), json.dumps(row, sort_keys=True))

            if dsrcAction == 'D':
                dsrc_action_str = 'deleteRecord()'
                g2Engine_.deleteRecord(dataSource, str(recordID))

            if dsrcAction == 'X':
                dsrc_action_str = 'reevaluateRecord()'
                g2Engine_.reevaluateRecord(dataSource, str(recordID), 0)

        except G2ModuleLicenseException as ex:
            print('ERROR: G2Engine licensing error!')
            print(f'     {ex}')
            with threadStop.get_lock():
                threadStop.value = 1
            return

        except Exception as ex:
            g2thread_error(ex, dsrc_action_str)


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


def init_engine(name, config_parms, debug_trace, prime_engine=True, add_start_time=False):
    '''  Initialize an engine. add_start_time is for redo engines only '''

    if add_start_time:
        engine_start_time = time.perf_counter()

    try:
        engine = G2Engine()
        engine.init(name, config_parms, debug_trace)
        if prime_engine:
            engine.primeEngine()
    except G2ModuleException:
        raise

    if add_start_time:
        time_starting_engines.value += (time.perf_counter() - engine_start_time)

    return engine


def processRedo(q, empty_q_wait=False, empty_q_msg='', sleep_interval=1):
    ''' Called in normal and redo only mode (-R) to process redo records that need re-evaluation '''

    # Drain the processing queue of ingest records before starting to process redo
    if empty_q_wait:
        print(f'\n{empty_q_msg}') if empty_q_msg else print('', end='')
        while not q.empty():
            time.sleep(sleep_interval)

    # This may look weird but ctypes/ffi have problems with the native code and fork.
    setupProcess = Process(target=redoFeed, args=(q, args.debugTrace, args.redoMode, args.redoModeInterval))

    redo_start_time = time.perf_counter()

    setupProcess.start()
    setupProcess.join()

    time_redo.value += (time.perf_counter() - redo_start_time)

    return setupProcess.exitcode


def redoFeed(q, debugTrace, redoMode, redoModeInterval):
    ''' Process records in the redo queue '''

    passNum = cntRows = batch_time_governing = 0
    batchStartTime = time.time()
    recBytes = bytearray()
    rec = None
    test_get_redo = False

    try:
        redo_engine = init_engine('pyG2Redo', g2module_params, debugTrace, prime_engine=True, add_start_time=False)

        # Only do initial count if in redo mode, counting large redo can be expensive
        if args.redoMode:
            redo_count = redo_engine.countRedoRecords()
            print(f'Redo records: {redo_count}')

        # Test if there is anything on redo queue
        else:
            try:
                redo_engine.getRedoRecord(recBytes)
                rec = recBytes.decode()
                test_get_redo = True
            except G2ModuleException as ex:
                print('ERROR: Could not get redo record for redoFeed()')
                print(f'       {ex}')
                exit(1)

    except G2ModuleException as ex:
        print('ERROR: Could not start the G2 engine for redoFeed()')
        print(f'       {ex}')
        exit(1)

    # If test didn't return a redo record, exit early
    if not rec and not args.redoMode:
        print('\n  No redo to perform, resuming loading...\n')
        return

    # Init redo governor in this process, trying to use DB connection in forked process created outside will cause errors when using
    # PostgreSQL governor such as: psycopg2.OperationalError: SSL error: decryption failed or bad record mac
    if import_governor:
        redo_governor = governor.Governor(type=redo_args['type'], g2module_params=redo_args['g2module_params'], frequency=redo_args['frequency'], pre_post_msgs=False)
    else:
        redo_governor = Governor()

    if not args.redoMode:
        print('\n  Pausing loading to process redo records...')

    while True:

        if threadStop.value != 0:
            break

        # Don't get another redo record if fetched one during test of redo queue
        if not test_get_redo:
            try:
                redo_engine.getRedoRecord(recBytes)
                rec = recBytes.decode()
            except G2ModuleException as ex:
                print('ERROR: Could not get redo record for redoFeed()')
                print(f'       {ex}')
                exit(1)

        test_get_redo = False

        if not rec:
            passNum += 1
            if (passNum > 10):
                if args.redoMode:
                    print(f'  Redo queue empty, {cntRows} total records processed. Waiting {args.redoModeInterval} seconds for next cycle at {time_now(True)} (CTRL-C to quit at anytime)...')
                    # Sleep in 1 second increments to respond to user input
                    for x in range(1, args.redoModeInterval):
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
                # Tuple to indicate if a record on workqueue is redo - (rec, True == this is redo)
                q.put((rec, True), True, 1)
            except Full:
                continue
            break

        if cntRows % args.loadOutputFrequency == 0:
            redoSpeed = int(args.loadOutputFrequency / (time.time() - batchStartTime - batch_time_governing)) if time.time() - batchStartTime != 0 else 1
            print(f'  {cntRows} redo records processed at {time_now()}, {redoSpeed} records per second')
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

    if not args.redoMode:
        print('  Redo processing complete resuming loading...\n')

    return


def loadRedoQueueAndProcess():

    exitCode = 0
    DumpStack.listen()
    procStartTime = time.time()

    threadList, workQueue = startLoaderProcessAndThreads(defaultThreadCount)
    if threadStop.value != 0:
        return exitCode

    if threadStop.value == 0 and not args.testMode and not args.noRedo:
        exitCode = processRedo(workQueue)

    stopLoaderProcessAndThreads(threadList, workQueue)

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if exitCode:
        print(f'\nRedo processing cycle aborted after {elapsedMins} minutes')
    else:
        print(f'\nRedo processing cycle completed successfully in {elapsedMins} minutes')

    return exitCode


def verifyEntityTypeExists(configJson, entityType):

    cfgDataRoot = json.loads(configJson)
    for rowNode in cfgDataRoot['G2_CONFIG']['CFG_ETYPE']:
        if rowNode['ETYPE_CODE'] == entityType:
            return True

    return False


def addDataSource(g2ConfigModule, configDoc, dataSource, configuredDatasourcesOnly):
    ''' Adds a data source if does not exist '''

    returnCode = 0  # 1=inserted, 2=updated

    configHandle = g2ConfigModule.load(configDoc)
    dsrcExists = False
    dsrcListDocString = bytearray()
    g2ConfigModule.listDataSources(configHandle, dsrcListDocString)
    dsrcListDoc = json.loads(dsrcListDocString.decode())
    dsrcListNode = dsrcListDoc['DATA_SOURCES']

    for dsrcNode in dsrcListNode:
        if dsrcNode['DSRC_CODE'].upper() == dataSource:
            dsrcExists = True

    if dsrcExists is False:
        if configuredDatasourcesOnly is False:
            addDataSourceJson = '{\"DSRC_CODE\":\"%s\"}' % dataSource
            addDataSourceResultBuf = bytearray()
            g2ConfigModule.addDataSource(configHandle, addDataSourceJson, addDataSourceResultBuf)
            newConfig = bytearray()
            g2ConfigModule.save(configHandle, newConfig)
            configDoc[::] = b''
            configDoc += newConfig
            returnCode = 1
        else:
            raise G2Exception.UnconfiguredDataSourceException(dataSource)

    g2ConfigModule.close(configHandle)

    return returnCode


def getInitialG2Config_processWrapper(returnQueue, g2module_params, g2ConfigJson):
    result = getInitialG2Config(g2module_params, g2ConfigJson)
    # Return values are put in a queue
    returnQueue.put(g2ConfigJson)
    returnQueue.put(result)


def getInitialG2Config(g2module_params, g2ConfigJson):

    # Get the configuration from the ini parms, this is deprecated and G2Health reports this
    if has_g2configfile:
        g2ConfigJson[::] = b''
        try:
            g2ConfigJson += json.dumps(json.load(open(has_g2configfile), encoding="utf-8")).encode()
        except ValueError as err:
            print(f'ERROR: {has_g2configfile} appears broken!')
            print(f'        {err}')
            return False
    else:
        # Get the current configuration from the database
        g2ConfigMgr = G2ConfigMgr()
        g2ConfigMgr.init('g2ConfigMgr', g2module_params, False)
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
        g2ConfigJson[::] = b''
        g2ConfigJson += defaultConfigDoc
        g2ConfigMgr.destroy()
        del g2ConfigMgr

    return True


def enhanceG2Config_processWrapper(returnQueue, g2Project, g2module_params, g2ConfigJson, configuredDatasourcesOnly):
    result = enhanceG2Config(g2Project, g2module_params, g2ConfigJson, configuredDatasourcesOnly)
    # Return values are put in a queue
    returnQueue.put(g2ConfigJson)
    returnQueue.put(result)


def enhanceG2Config(g2Project, g2module_params, g2ConfigJson, configuredDatasourcesOnly):

    # verify that we have the needed entity type
    if (verifyEntityTypeExists(g2ConfigJson, "GENERIC") is False):
        print('\nERROR: Entity type GENERIC must exist in the configuration, please add with G2ConfigTool')
        return False

    # Define variables for where the config is stored.

    g2Config = G2Config()
    g2Config.init("g2Config", g2module_params, False)

    # Add any missing source codes and entity types to the g2 config
    g2NewConfigRequired = False
    for sourceDict in g2Project.sourceList:
        if 'DATA_SOURCE' in sourceDict:
            try:
                if addDataSource(g2Config, g2ConfigJson, sourceDict['DATA_SOURCE'], configuredDatasourcesOnly) == 1:  # inserted
                    g2NewConfigRequired = True
            except G2Exception.UnconfiguredDataSourceException as err:
                print(err)
                return False

    # Add a new config, if we made changes
    if g2NewConfigRequired is True:
        if has_g2configfile:
            with open(has_g2configfile, 'w') as fp:
                json.dump(json.loads(g2ConfigJson), fp, indent=4, sort_keys=True)
        else:
            g2ConfigMgr = G2ConfigMgr()
            g2ConfigMgr.init("g2ConfigMgr", g2module_params, False)
            new_config_id = bytearray()
            try:
                g2ConfigMgr.addConfig(g2ConfigJson.decode(), 'Updated From G2Loader', new_config_id)
            except G2Exception.G2Exception as err:
                print("Error:  Failed to add new config to the datastore")
                return False
            try:
                g2ConfigMgr.setDefaultConfigID(new_config_id)
            except G2Exception.G2Exception as err:
                print("Error:  Failed to set new config as default")
                return False
            g2ConfigMgr.destroy()
            del g2ConfigMgr

    g2Config.destroy()
    del g2Config

    return True


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
        print(f'\nUsing {db_type}, loading default governor: {DEFAULT_POSTGRES_GOVERNOR}\n')
        import_governor = DEFAULT_POSTGRES_GOVERNOR[:-3] if DEFAULT_POSTGRES_GOVERNOR.endswith('.py') else DEFAULT_POSTGRES_GOVERNOR
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


if __name__ == '__main__':

    DEFAULT_POSTGRES_GOVERNOR = 'governor_postgres_xid.py'
    SHUF_NO_DEL_TAG = '_-_SzShufNoDel_-_'
    SHUF_TAG = '_-_SzShuf_-_'
    SHUF_TAG_GLOB = '_-_SzShuf*'
    SHUF_RESPONSE_TIMEOUT = 30
    DSRC_START_STR = '<DSRC_CODE>'
    DSRC_END_STR = '</DSRC_CODE>'
    RID_START_STR = '<RECORD_ID>'
    RID_END_STR = '</RECORD_ID>'

    exitCode = 0
    del_errors_file = True

    threadStop = Value('i', 0)
    time_starting_engines = Value('d', 0)
    time_governing = Value('d', 0)
    time_redo = Value('d', 0)
    api_errors = Value('i', 0)
    dsrc_action_diff = Value('i', 0)

    # Human friendly names
    dsrc_action_names = {"A": "Add",
                         "D": "Delete",
                         "X": "Reevaluate"
                         }

    # Counts for records using different dsrc action
    dsrc_action_add_count = Value('i', 0)
    dsrc_action_del_count = Value('i', 0)
    dsrc_action_reeval_count = Value('i', 0)

    signal.signal(signal.SIGINT, signal_int)
    signal.signal(signal.SIGTERM, signal_int)
    DumpStack.listen()

    tmp_path = os.path.join(tempfile.gettempdir(), 'senzing', 'g2')

    # Set default path for errors file to current path of script to start, modify if can find a suitable var path
    errors_file_name = f'g2loader_errors.{str(datetime.now().strftime("%Y%m%d_%H%M%S"))}'
    errors_file_default = pathlib.Path(__file__).parent.resolve().joinpath(errors_file_name)

    senz_root = os.environ.get('SENZING_ROOT', None)
    sys_senz_var = pathlib.Path('/var/opt/senzing')

    # SENZING_ROOT is available on bare metal
    if senz_root:
        errors_file_default = pathlib.Path(senz_root) / 'var' / errors_file_name

    # In containers /var/opt/senzing should be available
    if sys_senz_var.exists() and sys_senz_var.is_dir():
        errors_file_default = pathlib.Path(sys_senz_var) / errors_file_name

    # Don't allow argparse to create abbreviations of options
    g2load_parser = argparse.ArgumentParser(allow_abbrev=False)
    g2load_parser.add_argument('-c', '--iniFile', default=None, help='the name of a G2Module.ini file to use', nargs=1)
    g2load_parser.add_argument('-T', '--testMode', action='store_true', default=False, help='run in test mode to collect stats without loading, CTRL-C anytime')
    g2load_parser.add_argument('-X', '--reprocess', dest='reprocessMode', action='store_true', default=False, help='force reprocessing of previously loaded file')
    g2load_parser.add_argument('-t', '--debugTrace', action='store_true', default=False, help='output debug trace information')
    g2load_parser.add_argument('-w', '--workloadStats', action='store_false', default=False, help='DEPRECATED workload statistics on by default, -nw to disable')
    g2load_parser.add_argument('-nw', '--noWorkloadStats', action='store_false', default=True, help='disable workload statistics information')
    g2load_parser.add_argument('-n', '--noRedo', action='store_true', default=False, help='disable redo processing')
    g2load_parser.add_argument('-i', '--redoModeInterval', type=int, default=60, help='time in secs to wait between redo processing checks, only used in redo mode')
    g2load_parser.add_argument('-k', '--knownDatasourcesOnly', dest='configuredDatasourcesOnly', action='store_true', default=False, help='only accepts configured and known data sources')
    g2load_parser.add_argument('-mtp', '--maxThreadsPerProcess', dest='max_threads_per_process', default=16, type=int, help='maximum threads per process, default=%(default)s')
    g2load_parser.add_argument('-g', '--governor', default=None, help='user supplied governor to load and call during processing', nargs=1)
    g2load_parser.add_argument('-gpd', '--governorDisable', action='store_true', default=False, help='disable default Postgres governor, when repository is Postgres')
    g2load_parser.add_argument('-tmp', '--tmpPath', default=tmp_path, help=f'use this path instead of {tmp_path} (For S3 files)', nargs='?')
    g2load_parser.add_argument('-skr', '--skipRecords', default=0, type=int, help='skip the first n records in a file')
    g2load_parser.add_argument('-sfi', '--shuffFilesIgnore', action='store_true', default=False, help='skip checking for previously shuffled files and pausing')
    g2load_parser.add_argument('-sfr', '--shuffFileRedirect', default=None, nargs='+', help='alternative path to output shuffled file to, useful for performance and device space')

    # Both -ef and -ed shouldn't be used together
    g2load_parser.add_argument('-es', '--errorsShort', action='store_true', default=False, help='reduce errors file size by not including the record')
    error_file_group = g2load_parser.add_mutually_exclusive_group()
    error_file_group.add_argument('-ef', '--errorsFile', default=errors_file_default, help='path/file to write errors to, default=%(default)s', nargs='?')
    error_file_group.add_argument('-ed', '--errorsDisable', action='store_true', default=False, help='turn off writing errors to file, written to terminal instead')

    # Both -nt and -ntm shouldn't be used together
    num_threads_group = g2load_parser.add_mutually_exclusive_group()
    num_threads_group.add_argument('-nt', '--threadCount', dest='thread_count', type=int, default=0, help='total number of threads to start, default is calculated')
    num_threads_group.add_argument('-ntm', '--threadCountMem', default=None, const=80, nargs='?', type=int, choices=range(10, 81), metavar='10-80', help='maximum memory %% to use when calculating threads (when -nt not specified), default=%(const)s')

    # Both -p and -f shouldn't be used together
    file_project_group = g2load_parser.add_mutually_exclusive_group()
    file_project_group.add_argument('-p', '--projectFile', dest='projectFileName', default=None, help='the name of a project CSV or JSON file')
    file_project_group.add_argument('-f', '--fileSpec', dest='projectFileSpec', default=[], nargs='+', help='the name of a file and parameters to load such as /data/mydata.json/?data_source=?,file_format=?')    # Both -ns and -nsd shouldn't be used together

    # Both -ns and -snd shouldn't be used together
    no_shuf_shuf_no_del = g2load_parser.add_mutually_exclusive_group()
    no_shuf_shuf_no_del.add_argument('-ns', '--noShuffle', action='store_true', default=False, help='don\'t shuffle input file(s), shuffling improves performance')
    no_shuf_shuf_no_del.add_argument('-snd', '--shuffleNoDelete', action='store_true', default=False, help=f'don\'t delete shuffled source file(s) after G2Loader shuffles them. Adds {SHUF_NO_DEL_TAG} and timestamp to the shuffled file')
    no_shuf_shuf_no_del.add_argument('-nsd', '--noShuffleDelete', action='store_true', default=False, help='DEPRECATED please use --shuffleNoDelete (-snd)')

    # Both -R and -sr shouldn't be used together
    stop_row_redo_node = g2load_parser.add_mutually_exclusive_group()
    stop_row_redo_node.add_argument('-R', '--redoMode', action='store_true', default=False, help='run in redo mode only processesing the redo queue')
    stop_row_redo_node.add_argument('-sr', '--stopOnRecord', default=0, type=int, help='stop processing after n records (for testing large files)')

    # Both -P and -D shouldn't be used together
    purge_dsrc_delete = g2load_parser.add_mutually_exclusive_group()
    purge_dsrc_delete.add_argument('-D', '--delete', dest='deleteMode', action='store_true', default=False, help='force deletion of a previously loaded file')
    purge_dsrc_delete.add_argument('-P', '--purgeFirst', action='store_true', default=False, help='purge the Senzing repository before loading, confirmation prompt before purging')
    purge_dsrc_delete.add_argument('--FORCEPURGE', dest='forcePurge', action='store_true', default=False, help='purge the Senzing repository before loading, no confirmation prompt before purging')

    # Options hidden from help, used for testing
    # Frequency to ouput load and redo rate
    g2load_parser.add_argument('-lof', '--loadOutputFrequency', default=1000, type=int, help=argparse.SUPPRESS)

    # Frequency to pause loading and perform redo
    g2load_parser.add_argument('-rif', '--redoInterruptFrequency', default=100000, type=int, help=argparse.SUPPRESS)

    # Disable DB Perf - automate this for autoscaling cloud
    g2load_parser.add_argument('-sdbp', '--skipDBPerf', action='store_true', default=False, help=argparse.SUPPRESS)

    args = g2load_parser.parse_args()

    if len(sys.argv) < 2:
        print(f'\n{g2load_parser.format_help()}')
        sys.exit(0)

    # Check env vars that may be needed to react to, e.g. set in infrastructure such as Senzing containers
    check_skip_dbperf = os.environ.get("SENZING_SKIP_DATABASE_PERFORMANCE_TEST", "UNSET")
    env_var_skip_dbperf = True if check_skip_dbperf != "UNSET" and check_skip_dbperf == 'true' else False

    sshd_warning = os.environ.get("SENZING_SSHD_SHOW_PERFORMANCE_WARNING", "UNSET")
    if sshd_warning != "UNSET" and sshd_warning == 'true':
        print(textwrap.dedent('''
            ********** WARNING **********

            It looks like you are trying to run G2Loader in a resource limited container.

            G2Loader in this environment will not perform or scale, it's acceptable to use for small tests.

            For appropriate ingestion infrastructure please check:

                https://github.com/Senzing/stream-producer
                https://github.com/Senzing/stream-loader

            *****************************
            '''))
        time.sleep(5)

    # Check for additional mutually exclusuve arg combinations not easy to cover in argparse and sensible values
    if (args.purgeFirst or args.forcePurge) and args.redoMode:
        print('\nWARNING: Purge cannot be used with redo only mode. This would purge the repository before processing redo!')
        sys.exit(1)

    if args.skipRecords and args.stopOnRecord and args.skipRecords > args.stopOnRecord:
        print('\nWARNING: The number of records to skip is greater than the record number to stop on. No work would be done!')
        sys.exit(1)

    # Cheack early we can read -f/-p - G2Project can handle but early out before running dbperf etc
    # Note args.projectFileSpec is a list, G2Project accepts file globbing, split to get only filename not URI
    if args.projectFileSpec or args.projectFileName:
        file_list = []

        # Spec such as -f /tmp/json/*.json - arg parser returns list with all json files
        #   ['/tmp/json/sample_company.json', '/tmp/json/sample_person.json']
        # Slurp up all the files that meet the wildcard
        if len(args.projectFileSpec) > 1:
            file_list = args.projectFileSpec

        # Spec such as -f /tmp/json/*.json/?data_source=test or -f /tmp/json/sample_person.json  - arg parser returns list with only the arg value
        #   ['/tmp/json/*.json/?data_source=test']
        # Split on /? if exists and glob the path/files
        elif len(args.projectFileSpec) == 1:
            file_list = glob(args.projectFileSpec[0].split('/?')[0])
        # Not a filespec it's a project file
        else:
            tokens = args.projectFileName.split('/?')
            if len(tokens) > 1:
                print(f'\nERROR: Project files do not take parameters: {tokens[1]}')
                sys.exit(1)

            file_list.append(args.projectFileName)

        for f in file_list:
            try:
                with open(f, 'r') as fh:
                    pass
            except IOError as ex:
                print('\nERROR: Unable to read file or project')
                print(f'       {ex}')
                sys.exit(1)

    # Check early if shuffFileRedirect is accessible and is a path
    if args.shuffFileRedirect:
        shuf_path_redirect = pathlib.Path(args.shuffFileRedirect[0]).resolve()
        if shuf_path_redirect.is_file() or not shuf_path_redirect.is_dir():
            print('\nERROR: The path to redirect the shuffled source file to does not exist or is a file.')
            sys.exit(1)

        # Writeable?
        try:
            test_touch = shuf_path_redirect.joinpath('senzTestTouch')
            test_touch.touch()
            test_touch.unlink()
        except IOError as ex:
            print('\nERROR: Unable to write to the path for the shuffled source file redirection.')
            print(f'       {ex}')
            sys.exit(1)

    # Test early if can create an errors file, as long as not disabled
    if not args.errorsDisable:
        try:
            errors_file = open(args.errorsFile, 'w')
            errors_file.write(str(datetime.now()) + '\n')
            errors_file.write(f'Arguments: {" ".join(sys.argv[1:])}\n')
            errors_file.flush()
        except IOError as ex:
            print('\nERROR: Unable to write to bad records file')
            print(f'       {ex}')
            sys.exit(1)
    else:
        errors_file = ''

    # Check G2Project.ini isn't being used, now deprecated.
    # Don't try and auto find G2Module.ini, safer to ask to be specified during this change!
    if args.iniFile and 'G2PROJECT.INI' in args.iniFile[0].upper():
        print('\nINFO: G2Loader no longer uses G2Project.ini, it is deprecated and uses G2Module.ini instead.')
        print('      G2Loader attempts to locate a default G2Module.ini (no -c) or use -c to specify path/name to your G2Module.ini')
        sys.exit(1)

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
    except Exception:
        print('\nERROR: Unable to determine DB type, is CONNECTION correct in INI file?')
        print(f'       {iniFileName}')
        sys.exit(1)

    # Are you really sure you want to purge!
    if args.purgeFirst and not args.forcePurge:
        if not input(textwrap.dedent('''
            WARNING: Purging (-P) will delete all loaded data from the Senzing repository!
                     Bypass this confirmation using the --FORCEPURGE command line argument

            Type YESPURGE to continue and purge or enter to quit:
        ''')) == "YESPURGE":
            sys.exit(0)

    # test mode settings
    if args.testMode:
        defaultThreadCount = 1
        args.loadOutputFrequency = 10000 if args.loadOutputFrequency == 1000 else args.loadOutputFrequency

    # Check resources and acquire num threads
    else:
        tempQueue = Queue()
        checkResourcesProcess = Process(target=check_resources_and_startup,
                                        args=(tempQueue,
                                              args.thread_count,
                                              (args.purgeFirst or args.forcePurge) and not (args.testMode or args.redoMode),
                                              True))
        checkResourcesProcess.start()
        checkResourcesProcess.join()
        defaultThreadCount = tempQueue.get()

        # Exit if checkResourcesProcess failed to start an engine
        if defaultThreadCount == -1:
            sys.exit(1)

    # -w has been deprecated and the default is now output workload stats, noWorkloadStats is checked in place of
    # workloadStats. -w will be removed in future updates.
    workloadStats = args.noWorkloadStats

    # -nsd is deprecated, warn and convert until removed
    if args.noShuffleDelete:
        print(textwrap.dedent('''
            WARNING: --noShuffleDelete (-nsd) has been replaced with --shuffleNoDelete (-snd) and will be removed in the future.
                     Converting --noShuffleDelete to --shuffleNoDelete for this run, please modify scripts etc to use --shuffleNoDelete.
        '''))
        args.shuffleNoDelete = True
        time.sleep(5)

    # Setup the governor(s)
    governor_setup()

    # Set DSRC mode, can be overridden by dsrc_action on a record, see G2Thread()
    dsrcAction = 'A'
    if args.deleteMode:
        dsrcAction = 'D'
    if args.reprocessMode:
        dsrcAction = 'X'

    # Load truthset data if neither -p and -f and not in redo mode but pruge was requested
    if not args.projectFileName and not args.projectFileSpec and not args.redoMode and (args.purgeFirst or args.forcePurge):
        print('\nINFO: No source file or project file was specified, loading the sample truth set data...')
        # Convert the path to a string, G2Project needs updating to accomodate pathlib objects
        args.projectFileName = str(pathlib.Path(os.environ.get('SENZING_ROOT', '/opt/senzing/g2/')).joinpath('python', 'demo', 'truth', 'project.csv'))

    # Running in redo only mode? Don't purge in redo only mode, would purge the queue!
    if args.redoMode:
        while threadStop.value != 9:
            print('\nStarting in redo only mode, processing redo queue (CTRL-C to quit)\n')
            exitCode = loadRedoQueueAndProcess()
            # Delete errors file if no errors occurred
            del_errors_file = False if api_errors.value > 0 else del_errors_file
            if threadStop.value == 9 or exitCode != 0:
                break
    else:
        # Didn't load truthset data and nothing to do!
        if not args.projectFileName and not args.projectFileSpec:
            print('\nERROR: No file or project was specified to load!')
            sys.exit(1)

        exitCode, bad_recs = perform_load()

    # Perform governor clean up
    if not governor_cleaned:
        governor_cleanup()

    with suppress(Exception):
        errors_file.close()

    if not args.errorsDisable and del_errors_file:
        pathlib.Path(args.errorsFile).unlink()
    else:
        if not args.errorsDisable:
            print('\nWARNING: Errors occurred during load, please check the error log file.')
        else:
            print('\nWARNING: Errors occurred during load but error file logging disabled. Logged to terminal only.')

    sys.exit(exitCode)
