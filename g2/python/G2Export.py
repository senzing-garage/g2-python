#! /usr/bin/env python3

####
# - Removed olf V2 check
# - Switched to better arg parsing module = optparse -> argparse
# - Use default in arg parsing and don't then try and calc
# - Improved help messaging
# - Cleaned up messages
# - Removed unused functions
# - Removed use of G2Project.ini, reporting frequency is now hard coded
# - Removed use of configparser
# - Added adjustments to csv field size to not abbend on larger fields with csv formats
# - Added new output
# - Cleaned up imported modules
# - Fixed bug counting number of written records for CSV format
# - Improved help output

import argparse
import csv
import json
import os
import pathlib
import sys
import textwrap
import time
from datetime import datetime

import G2Paths
from G2Engine import G2Engine
from G2Exception import G2ModuleException
from G2Health import G2Health
from G2IniParams import G2IniParams


def exportEntityResume():

    # Initialize Senzing 
    print(f'\nStarting Senzing...')

    try:
        g2_engine = G2Engine()
        g2_engine.initV2('pyG2Export', g2module_params, False)
    except G2ModuleException as ex:
        print(f'\nERROR: Could not start the G2 engine at {iniFileName}')
        print(f'       {ex}')
        return 1

    # Create output file to write to
    outputFileHandle = fileCreate(outputFileName)
    if not outputFileHandle:
        return 1

    # Determine the output flags
    exportFlags = 0

    if args.outputFilter == 1:
        exportFlags = g2_engine.G2_EXPORT_INCLUDE_RESOLVED
    elif args.outputFilter == 2:
        exportFlags = g2_engine.G2_EXPORT_INCLUDE_RESOLVED | g2_engine.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_engine.G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS
    elif args.outputFilter == 3:
        exportFlags = g2_engine.G2_EXPORT_INCLUDE_RESOLVED | g2_engine.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_engine.G2_EXPORT_INCLUDE_POSSIBLY_RELATED | g2_engine.G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS | g2_engine.G2_ENTITY_INCLUDE_POSSIBLY_RELATED_RELATIONS
    elif args.outputFilter == 4:
        exportFlags = g2_engine.G2_EXPORT_INCLUDE_RESOLVED | g2_engine.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_engine.G2_EXPORT_INCLUDE_POSSIBLY_RELATED | g2_engine.G2_EXPORT_INCLUDE_NAME_ONLY | g2_engine.G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS | g2_engine.G2_ENTITY_INCLUDE_POSSIBLY_RELATED_RELATIONS | g2_engine.G2_ENTITY_INCLUDE_NAME_ONLY_RELATIONS
    elif args.outputFilter >= 5:
        exportFlags = g2_engine.G2_EXPORT_INCLUDE_RESOLVED | g2_engine.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_engine.G2_EXPORT_INCLUDE_POSSIBLY_RELATED | g2_engine.G2_EXPORT_INCLUDE_NAME_ONLY | g2_engine.G2_EXPORT_INCLUDE_DISCLOSED | g2_engine.G2_ENTITY_INCLUDE_ALL_RELATIONS
    else:
        exportFlags = g2_engine.G2_EXPORT_INCLUDE_ALL_ENTITIES | g2_engine.G2_ENTITY_INCLUDE_ALL_RELATIONS

    #if not extended:
    #  exportFlags |= g2_engine.G2_ENTITY_MINIMAL_FORMAT

    #--but use these headers as this export includes computed column: resolved name
    csvFields = []
    csvFields.append('RESOLVED_ENTITY_ID')
    if args.extended: csvFields.append('RESOLVED_ENTITY_NAME')
    csvFields.append('RELATED_ENTITY_ID')
    csvFields.append('MATCH_LEVEL')
    csvFields.append('MATCH_KEY')
    csvFields.append('DATA_SOURCE')
    csvFields.append('RECORD_ID')
    if args.extended: csvFields.append('JSON_DATA')
    csvFields.append('LENS_CODE')
    if extendedFormat: #--hard coded to false for now
        csvFields.append('REF_SCORE')
        csvFields.append('ENTITY_TYPE')
        csvFields.append('ERRULE_CODE')

    # Initialize the export
    print(f'\nExecuting export...')

    try:
        if args.outputFormat == 'CSV':
            if csvFields and isinstance(csvFields, list):
                csvFields = ','.join(csvFields)
            exportHandle = g2_engine.exportCSVEntityReportV2(csvFields, exportFlags)
        else:
            exportHandle = g2_engine.exportJSONEntityReport(exportFlags)

        if not exportHandle:
            print(f'\nERROR: Could not initialize export')
            print(f'         {g2_engine.getLastException()}')
            return 1

    except G2ModuleException as ex:
        print(f'\nERROR: Could not initialize export')
        print(f'         {ex}')
        return 1

    if args.outputFormat == 'CSV':
        export_error = csvExport(g2_engine, exportHandle, outputFileHandle)
    else:
        export_error = jsonExport(g2_engine, exportHandle, outputFileHandle)

    outputFileHandle.close()
    g2_engine.destroy()

    return export_error


def jsonExport(g2_engine, exportHandle, outputFileHandle):

    appError = rowCount = 0
    batchStartTime = time.time()
    
    rowResponse = bytearray()
    rowData = g2_engine.fetchNext(exportHandle,rowResponse)
    row = rowResponse.decode()
    
    while row:

        try: 
            outputFileHandle.write(row)
        except IOError as err:
            print(f'\nERROR: could not write to JSON file')
            print(f'         {err}')
            appError = 1
            break

        rowCount += 1
        if rowCount % export_output_frequency == 0:
            print(f'  {rowCount} entities processed at {datetime.now().strftime("%I:%M%p").lower()} ({int(float(export_output_frequency) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))} per second)')
            batchStartTime = time.time()

        rowData = g2_engine.fetchNext(exportHandle,rowResponse)
        row = rowResponse.decode()

    if not appError:
        print(f' {rowCount} records written.')
    else:
        print(' Aborted due to errors!')

    return appError


def csvExport(g2_engine, exportHandle, outputFileHandle):

    # First row is header for CSV
    rowResponse = bytearray()
    rowData = g2_engine.fetchNext(exportHandle, rowResponse)
    exportColumnHeaders = rowResponse.decode()
    
    if exportColumnHeaders[-1] == '\n':
        exportColumnHeaders = exportColumnHeaders[0:-1]
    exportColumnHeaders = exportColumnHeaders.split(',')

    # Write rows with csv module for proper quoting
    try: 
        outputFileWriter = csv.DictWriter(outputFileHandle, exportColumnHeaders, dialect=csv.excel, quoting=csv.QUOTE_ALL)
    except csv.Error as err:
        print(f'\nERROR: Could not open {outputFileName} for writing')
        print(f'         {err}')
        return 1

    try: 
        outputFileWriter.writeheader()
    except csv.Error as err:
        print(f'\nERROR: Writing to CSV file')
        print(f'         {err}')
        return 1

    # Read the rows from the export handle
    lineCount = badCnt1 = badCnt2 = appError = rowCount = entityCnt = totEntityCnt = 0

    rowDataVal = g2_engine.fetchNext(exportHandle, rowResponse)
    rowData = rowResponse.decode()

    if rowData:
        csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
    else:
        csvRecord = None

    lineCount += 1
    batchStartTime = time.time()

    while rowData:

        # Bypass bad rows
        if 'LENS_CODE' not in csvRecord:
            print(f'\nERROR on line {lineCount}')           
            print(rowData)
            print(csvRecord)
            badCnt1 += 1
            rowDataVal = g2_engine.fetchNext(exportHandle,rowResponse)
            rowData = rowResponse.decode()
            
            if rowData:
                csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
            else:
                csvRecord = None
            lineCount += 1
            
            continue

        # Determine entity name if csv
        rowList = []
        resolvedRecordID = None
        resolvedName = None
        resolvedID = csvRecord['RESOLVED_ENTITY_ID']
        lensID = csvRecord['LENS_CODE']

        while csvRecord and csvRecord['RESOLVED_ENTITY_ID'] == resolvedID and csvRecord['LENS_CODE'] == lensID:

            # Bypass bad rows
            if 'RECORD_ID' not in csvRecord:
                print(f'\nERROR on line {lineCount}')           
                print(rowData)
                print(csvRecord)
                badCnt2 += 1
                rowDataVal = g2_engine.fetchNext(exportHandle,rowResponse)
                rowData = rowResponse.decode()
                
                if rowData:
                    csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
                else:
                    csvRecord = None
                lineCount += 1
                
                continue

            if not resolvedRecordID or (csvRecord['RECORD_ID'] < resolvedRecordID and csvRecord['MATCH_LEVEL'] == 0):
                if args.extended: 
                    resolvedName = csvRecord['RESOLVED_ENTITY_NAME']
                resolvedRecordID = csvRecord['RECORD_ID']

            rowList.append(csvRecord)
            rowDataVal = g2_engine.fetchNext(exportHandle,rowResponse)
            rowData = rowResponse.decode()
            
            # Check the row data doesn't exceed the csv field limit, increase if it does
            if len(rowData) > csv.field_size_limit():
                csv.field_size_limit(len(rowData * 1.5))

            if rowData:
                csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
            else:
                csvRecord = None
            lineCount += 1

        entityCnt += 1
        totEntityCnt += 1
        
        # Write the rows for the entity
        for row in rowList:
            try: 
                outputFileWriter.writerow(row)
            except csv.Error as err:
                print(f'\nERROR: Writing to CSV file')
                print(f'         {err}')
                appError = 1
                break

            rowCount += 1

        if totEntityCnt % export_output_frequency == 0:
                print(f'  {totEntityCnt} entities processed at {datetime.now().strftime("%I:%M%p").lower()} ({int(float(export_output_frequency) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))} per second), {int(float(rowCount) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))} rows per second')
                batchStartTime = time.time()

        # Shut down if error
        if appError:
            break

    if badCnt1 + badCnt2 > 0:
        print(f' {badCnt1 - badCnt2} bad records skipped')

    if not appError:
        print(f'\n {rowCount} records written.')
    else:
        print(f'\n Aborted due to errors!')

    return appError

#---------------------------------------------------------------------
#-- utilities
#---------------------------------------------------------------------

def fileCreate(fileName):
    ''' Open file for writing '''
    
    try: 
        fileHandle = open(fileName, 'w')
    except IOError as ex:
        print(f'ERROR: Could not open file {fileName}')
        print(f'       {ex}')
        return None

    return fileHandle


def fileWrite(fileHandle, fileRow):
    ''' Write a line to the file '''

    try: 
        fileHandle.write(fileRow + '\n')
    except:
        success = False
    else:
        success = True

    return success


if __name__ == '__main__':

    exit_code = 0
    export_output_frequency = 1000    
    
    # Some CSV exports can be large especially with extended data, increase the csv field limit
    csv.field_size_limit(300000)

    #--extended format (with lens_id, errule code, etc) is hard coded to false for now
    extendedFormat = False

    g2export_parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    g2export_parser.add_argument('-c', '--iniFile', default=None, help='name of a G2Module.ini file to use', nargs=1)
    g2export_parser.add_argument('-o', '--outputFile', default='g2export.csv', help='name of a file to write the output to')
    g2export_parser.add_argument('-f', '--outputFilter', default=0, type=int, choices=range(0, 6), help=textwrap.dedent('''\
                                                                                                            0 = All
                                                                                                            1 = Resolved Entities only
                                                                                                            2 = Add possible matches
                                                                                                            3 = Add relationships
                                                                                                            4 = Add name-only (internal)
                                                                                                            5 = Add disclosed relationships

                                                                                                            Default: %(default)s

                                                                                                            '''))
    g2export_parser.add_argument('-F', '--outputFormat', default='CSV', type=str.upper, choices=('CSV', 'JSON'), help='JSON or CSV')
    g2export_parser.add_argument('-x', '--extended', default=False, action="store_true", help='Return extended details - RESOLVED_ENTITY_NAME & JSON_DATA')

    args = g2export_parser.parse_args()

    # Check G2Project.ini isn't being used, now deprecated. 
    # Don't try and auto find G2Module.ini, safer to ask to be specified during this change!
    if args.iniFile and 'G2PROJECT.INI' in args.iniFile[0].upper():
        print('\nINFO: G2Export no longer uses G2Project.ini, it is deprecated and uses G2Module.ini instead.')
        print('      G2Export attempts to locate a default G2Module.ini (no -c) or use -c to specify path/name to your G2Module.ini')
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

    # If using default file name but specified JSON format convert file name. Get absolute path of file name
    outputFileName = 'g2export.json' if args.outputFile == 'g2export.csv' and args.outputFormat == 'JSON' else args.outputFile
    outputFileName = pathlib.Path(outputFileName).resolve()

    # Perform the export
    exit_code = exportEntityResume()

    # Argumnent parser limits values to these here
    output_display = {
        0: 'All',
        1: 'Resolved entities only',
        2: 'Including possible matches',
        3: 'Including relationships',
        4: 'Including relationships',
        5: 'Including relationships'
    }

    # Display information at end for reference
    print(textwrap.dedent(f'''
                            Configuration parameters:    {iniFileName}
                            Export output file:          {outputFileName}
                            Export output format:        {args.outputFormat}
                            Export filter level:         {args.outputFilter} - {output_display[args.outputFilter]}
                           '''))
    if not exit_code:
        print(f'Export completed successfully.')
    else:
        print(f'ERROR: Export did NOT complete!')

    sys.exit(exit_code)
