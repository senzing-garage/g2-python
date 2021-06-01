#! /usr/bin/env python3

#--python imports
import optparse
try: import configparser
except: import ConfigParser as configparser
import sys
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding('utf-8')
import os
import json
import csv
import time
import G2Paths
from datetime import datetime, timedelta

#--extended format (with lens_id, errule code, etc) is hard coded to false for now
extendedFormat = False


#--project classes
from G2Engine import G2Engine
from G2Exception import G2ModuleException
from G2IniParams import G2IniParams

#---------------------------------------------------------------------
#-- g2 export
#---------------------------------------------------------------------

#---------------------------------------
def exportEntityResume(appError):

    if outputFilter == 0:
        outputFilterDisplay = 'All'
    elif outputFilter == 1:
        outputFilterDisplay = 'Resolved entities only'
    elif outputFilter == 2:
        outputFilterDisplay = 'Including possible matches'
    elif outputFilter == 3:
        outputFilterDisplay = 'Including relationships'
    elif outputFilter == 4:
        outputFilterDisplay = 'Including relationships'
    elif outputFilter >= 5:
        outputFilterDisplay = 'Including relationships'
    print('')    
    print('Writing to %s ... (%s)' % (outputFileName, outputFilterDisplay))
    print('')

    #--creates an output file
    outputFileHandle = fileCreate(outputFileName)
    if not outputFileHandle:
        appError = 1
        return

    # initialize G2
    try:
        iniParamCreator = G2IniParams()
        iniParams = iniParamCreator.getJsonINIParams(g2iniPath)

        g2_module = G2Engine()
        g2_module.initV2('pyG2Export', iniParams, False)
    except G2ModuleException as ex:
        print('ERROR: could not start the G2 module at ' + g2iniPath)
        print(ex)
        return 1

    #--determine the output flags
    exportFlags = 0
    if outputFilter == 1:
        exportFlags = g2_module.G2_EXPORT_INCLUDE_RESOLVED | g2_module.G2_ENTITY_INCLUDE_NO_RELATIONS
    elif outputFilter == 2:
        exportFlags = g2_module.G2_EXPORT_INCLUDE_RESOLVED | g2_module.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_module.G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS
    elif outputFilter == 3:
        exportFlags = g2_module.G2_EXPORT_INCLUDE_RESOLVED | g2_module.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_module.G2_EXPORT_INCLUDE_POSSIBLY_RELATED | g2_module.G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS | g2_module.G2_ENTITY_INCLUDE_POSSIBLY_RELATED_RELATIONS
    elif outputFilter == 4:
        exportFlags = g2_module.G2_EXPORT_INCLUDE_RESOLVED | g2_module.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_module.G2_EXPORT_INCLUDE_POSSIBLY_RELATED | g2_module.G2_EXPORT_INCLUDE_NAME_ONLY | g2_module.G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS | g2_module.G2_ENTITY_INCLUDE_POSSIBLY_RELATED_RELATIONS | g2_module.G2_ENTITY_INCLUDE_NAME_ONLY_RELATIONS
    elif outputFilter >= 5:
        exportFlags = g2_module.G2_EXPORT_INCLUDE_RESOLVED | g2_module.G2_EXPORT_INCLUDE_POSSIBLY_SAME | g2_module.G2_EXPORT_INCLUDE_POSSIBLY_RELATED | g2_module.G2_EXPORT_INCLUDE_NAME_ONLY | g2_module.G2_EXPORT_INCLUDE_DISCLOSED | g2_module.G2_ENTITY_INCLUDE_ALL_RELATIONS
    else:
        exportFlags = g2_module.G2_EXPORT_INCLUDE_ALL_ENTITIES

    if not extended:
      exportFlags |= g2_module.G2_ENTITY_MINIMAL_FORMAT

    #--but use these headers as this export includes computed column: resolved name
    csvFields = []
    csvFields.append('RESOLVED_ENTITY_ID')
    if extended: csvFields.append('RESOLVED_ENTITY_NAME')
    csvFields.append('RELATED_ENTITY_ID')
    csvFields.append('MATCH_LEVEL')
    csvFields.append('MATCH_KEY')
    csvFields.append('DATA_SOURCE')
    csvFields.append('RECORD_ID')
    if extended: csvFields.append('JSON_DATA')
    csvFields.append('LENS_CODE')
    if extendedFormat: #--hard coded to false for now
        csvFields.append('REF_SCORE')
        csvFields.append('ENTITY_TYPE')
        csvFields.append('ERRULE_CODE')

    #--initialize the g2module export
    print('Executing query ...')
    try:
        if outputFormat == "CSV":
            if csvFields and isinstance(csvFields, list):
                csvFields = ",".join(csvFields)
            exportHandle = g2_module.exportCSVEntityReportV2(csvFields,exportFlags)
        else:
            exportHandle = g2_module.exportJSONEntityReport(exportFlags)
        if not exportHandle:
            print('ERROR: could not initialize export')
            print(g2_module.getLastException())
            return 1

    except G2ModuleException as ex:
        print('ERROR: could not initialize export')
        print(ex)
        return 1

    if outputFormat == "CSV":
        appError = csvExport(g2_module, exportHandle, outputFileHandle)
    else:
        appError = jsonExport(g2_module, exportHandle, outputFileHandle)

    #--close out
    outputFileHandle.close()
    g2_module.destroy()

    return appError

#---------------------------------------
def jsonExport(g2_module, exportHandle, outputFileHandle):

    appError = 0
    rowCount = 0
    batchStartTime = time.time()
    rowResponse = bytearray()
    rowData = g2_module.fetchNext(exportHandle,rowResponse)
    row = rowResponse.decode()
    while row:

        try: outputFileHandle.write(row)
        except IOError as err:
            print('ERROR: could not write to json file')
            print(err)
            appError = 1
            break

        rowCount += 1
        if rowCount % sqlCommitSize == 0:
            print('  %s entities processed at %s (%s per second)' % (rowCount, datetime.now().strftime('%I:%M%p').lower(), int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))))
            batchStartTime = time.time()

        rowData = g2_module.fetchNext(exportHandle,rowResponse)
        row = rowResponse.decode()

    if not appError:
        print(' %s records written, complete!' % rowCount)
    else:
        print(' Aborted due to errors!')

    return appError


#---------------------------------------
def csvExport(g2_module, exportHandle, outputFileHandle):

    #--first row is header for CSV
    rowResponse = bytearray()
    rowData = g2_module.fetchNext(exportHandle,rowResponse)
    exportColumnHeaders = rowResponse.decode()
    if exportColumnHeaders[-1] == '\n':
        exportColumnHeaders = exportColumnHeaders[0:-1]
    exportColumnHeaders = exportColumnHeaders.split(',')

    #--write rows with csv module for proper quoting
    try: outputFileWriter = csv.DictWriter(outputFileHandle, exportColumnHeaders, dialect=csv.excel, quoting=csv.QUOTE_ALL)
    except csv.Error as err:
        print('ERROR: Could not open %s for writing' % outputFilePath)
        print(err)
        return 1
    
    try: outputFileWriter.writeheader()
    except csv.Error as err:
        print('ERROR: writing to CSV file')
        print(err)
        return 1

    #--read the rows from the export handle
    lineCount = 0
    badCnt1 = 0
    badCnt2 = 0
    appError = 0
    rowCount = 0
    entityCnt = 0
    totEntityCnt = 0
    rowDataVal = g2_module.fetchNext(exportHandle,rowResponse)
    rowData = rowResponse.decode()
    if rowData:
        csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
    else:
        csvRecord = None
    lineCount += 1
    batchStartTime = time.time()
    while rowData:

        #--bypass bad rows
        if 'LENS_CODE' not in csvRecord:
            print('ERROR on line %s' % lineCount)           
            print(rowData)
            print(csvRecord)
            badCnt1 += 1
            rowDataVal = g2_module.fetchNext(exportHandle,rowResponse)
            rowData = rowResponse.decode()
            if rowData:
                csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
            else:
                csvRecord = None
            lineCount += 1
            continue

        #--determine entity name if csv
        rowList = []
        resolvedRecordID = None
        resolvedName = None
        resolvedID = csvRecord['RESOLVED_ENTITY_ID']
        lensID = csvRecord['LENS_CODE']
        while csvRecord and csvRecord['RESOLVED_ENTITY_ID'] == resolvedID and csvRecord['LENS_CODE'] == lensID:

            #--bypass bad rows
            if 'RECORD_ID' not in csvRecord:
                print('ERROR on line %s' % lineCount)           
                print(rowData)
                print(csvRecord)
                badCnt2 += 1
                rowDataVal = g2_module.fetchNext(exportHandle,rowResponse)
                rowData = rowResponse.decode()
                if rowData:
                    csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
                else:
                    csvRecord = None
                lineCount += 1
                continue

            if not resolvedRecordID or (csvRecord['RECORD_ID'] < resolvedRecordID and csvRecord['MATCH_LEVEL'] == 0):
                if extended: resolvedName = csvRecord['RESOLVED_ENTITY_NAME']
                resolvedRecordID = csvRecord['RECORD_ID']

            rowList.append(csvRecord)
            rowDataVal = g2_module.fetchNext(exportHandle,rowResponse)
            rowData = rowResponse.decode()
            if rowData:
                csvRecord = next(csv.DictReader([rowData], fieldnames=exportColumnHeaders))
            else:
                csvRecord = None
            lineCount += 1

        entityCnt += 1
        totEntityCnt += 1
        #--write the rows for the entity
        for row in rowList:
            #--write to output file
            try: outputFileWriter.writerow(row)
            except csv.Error as err:
                print('ERROR: writing to CSV file')
                print(err)
                appError = 1
                break

            rowCount += 1

        if totEntityCnt % sqlCommitSize == 0:
                print('  %s entities processed at %s (%s per second), %s rows per second' % (totEntityCnt, datetime.now().strftime('%I:%M%p').lower(), int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1))),int(float(rowCount) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))))
                batchStartTime = time.time()
                entityCnt = 0
                rowCount = 0

        #--shut down if errors hit
        if appError:
            break

    if badCnt1 + badCnt2 > 0:
        print(' %s-%s bad records skipped' % (badCnt1, badCnt2))

    if not appError:
        print(' %s records written, complete!' % rowCount)
    else:
        print(' Aborted due to errors!')

    return appError

#---------------------------------------------------------------------
#-- utilities
#---------------------------------------------------------------------

#---------------------------------------
def parseUri(uriString):
    ''' parse a database or file uri string '''
    uriData = {}

    #--pull off any parameters if supplied
    if '/?' in uriString:
        parmString = uriString.split('/?')[1]
        uriString = uriString.split('/?')[0]
        parmList = parmString.split('&')
        for parm in parmList:
            if '=' in parm:
                parmType = parm.split('=')[0].strip().upper()
                parmValue = parm.split('=')[1].strip().replace('"', '').replace("'", '')
                uriData[parmType] = parmValue

    #--get uri type
    if '://' not in uriString:
        uriData = None
    else:
        uriType = uriString.split('://')[0].upper()
        uriString = uriString.split('://')[1]

        #--put together the dict
        uriData['TYPE'] = uriType
        if uriType == 'FILE':
            uriData['FILE_PATH'] = uriString
        else:
            uriData['TYPE'] = 'DATABASE'
            if '@' in uriString:
                justUidPwd = uriString.split('@')[0]
                justDsnSch = uriString.split('@')[1]
            else: #--just dsn with trusted connection?
                justUidPwd = ':'
                justDsnSch = uriString

            #--separate uid and password
            if ':' in justUidPwd:
                uid = justUidPwd.split(':')[0]
                pwd = justUidPwd.split(':')[1]
            else: #--just uid with no password?
                uid = justUidPwd
                pwd = ''

            #--separate dsn and schema
            if ':' in justDsnSch:
                dsn = justDsnSch.split(':')[0]
                sch = justDsnSch.split(':')[1]
            else: #--just dsn with no schema?
                dsn = justDsnSch
                sch = ''

            uriData['DSN'] = dsn
            uriData['USER_ID'] = uid
            uriData['PASSWORD'] = pwd
            uriData['SCHEMA'] = sch

    return uriData

#----------------------------------------
def fileCreate(fileName):
    ''' open json for writing '''
    
    #--open file for writing, truncating if it exists
    try: fileHandle = open(fileName, 'w')
    except:
        print('ERROR: could not open %s' % (fileName))
        return None

    return fileHandle

#----------------------------------------
def fileWrite(fileHandle, fileRow):
    ''' write a line to the file '''
    try: fileHandle.write(fileRow + '\n')
    except:
        success = False
    else:
        success = True

    return success


#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    response = input(question)
    return response

#----------------------------------------
if __name__ == '__main__':

    #--defaults
    appError = 0
    outputFilter = 0
    outputFormat = 'csv'
    extended = False
    outputFileName = 'g2export.csv'
    iniFileName = ''

    #--capture the command line arguments
    if len(sys.argv) > 1:
        optParser = optparse.OptionParser()
        optParser.add_option('-c', '--iniFile', dest='iniFile', default=iniFileName, help='the name of a G2Project.ini file to use')
        optParser.add_option('-o', '--output-file', dest='outputFileName', default=outputFileName, help='the name of a file to write the output to')
        optParser.add_option('-f', '--outputFilter', dest='outputFilter', type='int', default=0, help='0=All; 1=Resolved Entities only; 2=add possible matches; 3=add relationships; 4=add name-only (internal); 5=add disclosed relationships')
        optParser.add_option('-F', '--outputFormat', dest='outputFormat', default=outputFormat, help='json or csv style')
        optParser.add_option('-x', '--extended', dest='extended', action="store_true", help='Returns extended details - RESOLVED_ENTITY_NAME, JSON_DATA')
        (options, args) = optParser.parse_args()
        if options.outputFileName:
            outputFileName = options.outputFileName
        if options.outputFilter:
            outputFilter = options.outputFilter
        if options.outputFormat:
            outputFormat = options.outputFormat
        if options.extended:
            extended = options.extended
        if options.iniFile and len(options.iniFile) > 0:
            iniFileName = os.path.abspath(options.iniFile)

    #--get parameters from ini file
    if not iniFileName:
        iniFileName = G2Paths.get_G2Project_ini_path()
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)
    try: sqlCommitSize = int(iniParser.get('report', 'sqlCommitSize'))
    except: sqlCommitSize = 1000
    try: g2iniPath = os.path.expanduser(iniParser.get('g2', 'iniPath'))
    except: g2iniPath = None

    #--validations
    if not g2iniPath:
        print('ERROR: G2 ini file not specified!')
        sys.exit(1)
    if not outputFileName:
        print('ERROR: Output file name must be specified')
        sys.exit(1)
    if outputFilter not in (0,1,2,3,4,5):
        print('ERROR: Resume filter must be 1, 2, 3, 4 or 5')
        sys.exit(1)

    #--adjust from default of csv to json if they did not change it themselves
    outputFormat = outputFormat.upper()
    if outputFormat not in ('CSV', 'JSON'):
        print('ERROR: Output format must be either CSV or JSON')
        sys.exit(1)
    else:
        if outputFileName == 'g2export.csv' and outputFormat == 'JSON':
            outputFileName = 'g2export.json'

    #--build resolved entities
    appError = exportEntityResume(appError)
    
    print('')
    if not appError:
        print('SUCCESS: Process completed successfully!')
    else:
        print('ERROR: Process did NOT complete!')
    print('')

    sys.exit(appError)

