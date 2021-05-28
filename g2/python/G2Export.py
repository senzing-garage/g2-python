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

#--project classes
from G2Module import G2Module
from G2Exception import G2ModuleException

#---------------------------------------------------------------------
#-- g2 export
#---------------------------------------------------------------------

#---------------------------------------
def exportEntityResume(appError):

    if outputFilter == 1:
        outputFilterDisplay = 'Resolved entities only'
    elif outputFilter == 2:
        outputFilterDisplay = 'Including possible matches'
    elif outputFilter >= 3:
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
        g2_module = G2Module('pyG2Export', g2iniPath, False)
        g2_module.init()
    except G2ModuleException as ex:
        print('ERROR: could not start the G2 module at ' + g2iniPath)
        print(ex)
        return 1

    #--initialize the g2module export
    print('Executing query ...')
    try: exportHandle = g2_module.getExportHandle(outputFormat, outputFilter)
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
    row = g2_module.fetchExportRecord(exportHandle)
    while row:

        try: outputFileHandle.write(row + '\n')
        except IOError as err:
            print('ERROR: could not write to json file')
            print(err)
            appError = 1
            break

        rowCount += 1
        if rowCount % sqlCommitSize == 0:
            print(' %s records written' % rowCount)

        row = g2_module.fetchExportRecord(exportHandle)

    if not appError:
        print(' %s records written, complete!' % rowCount)
    else:
        print(' Aborted due to errors!')

    return appError


#---------------------------------------
def csvExport(g2_module, exportHandle, outputFileHandle):

    #--extended format (with lens_id, errule code, etc) is hard coded to false for now
    extendedFormat = False

    #--wite rows with csv module for proper quoting
    try: outputFileWriter = csv.writer(outputFileHandle, dialect=csv.excel, quoting=csv.QUOTE_ALL)
    except csv.Error as err:
        print('ERROR: Could not open %s for writing' % outputFilePath)
        print(err)
        return 1
    
    #--first row is header for CSV
    exportColumnHeaders = g2_module.fetchExportRecord(exportHandle).split(',')

    #--but use these headers as this export includes computed column: resolved name
    csvFields = []
    csvFields.append('RESOLVED_ENTITY_ID')
    csvFields.append('RESOLVED_NAME')
    csvFields.append('RELATED_ENTITY_ID')
    csvFields.append('MATCH_LEVEL')
    csvFields.append('MATCH_KEY')
    csvFields.append('DATA_SOURCE')
    csvFields.append('RECORD_ID')
    csvFields.append('JSON_DATA')
    if extendedFormat: #--hard coded to false for now
        csvFields.append('LENS_ID')
        csvFields.append('REF_SCORE')
        csvFields.append('ENTITY_TYPE')
        csvFields.append('ERRULE_CODE')
    try: outputFileWriter.writerow(csvFields)
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
    rowData = g2_module.fetchCsvExportRecord(exportHandle, exportColumnHeaders)
    lineCount += 1
    while rowData:

        #--bypass bad rows
        if 'LENS_ID' not in rowData:
            print('ERROR on line %s' % lineCount)           
            print(rowData)
            badCnt1 += 1
            rowData = g2_module.fetchCsvExportRecord(exportHandle, exportColumnHeaders)
            lineCount += 1
            continue

        #--determine entity name if csv
        rowList = []
        resolvedRecordID = None
        resolvedName = None
        resolvedID = rowData['RESOLVED_ENTITY_ID']
        lensID = rowData['LENS_ID']
        while rowData and rowData['RESOLVED_ENTITY_ID'] == resolvedID and rowData['LENS_ID'] == lensID:

            #--bypass bad rows
            if 'RECORD_ID' not in rowData:
                print('ERROR on line %s' % lineCount)           
                print(rowData)
                badCnt2 += 1
                rowData = g2_module.fetchCsvExportRecord(exportHandle, exportColumnHeaders)
                lineCount += 1
                continue

            if not resolvedRecordID or (rowData['RECORD_ID'] < resolvedRecordID and rowData['MATCH_LEVEL'] == 0):
                resolvedName = rowData['ENTITY_NAME']
                resolvedRecordID = rowData['RECORD_ID']

            rowList.append(rowData)
            rowData = g2_module.fetchCsvExportRecord(exportHandle, exportColumnHeaders)
            lineCount += 1

        #--write the rows for the entity
        for row in rowList:

            csvFields = []
            csvFields.append(row['RESOLVED_ENTITY_ID'])
            csvFields.append(resolvedName)
            csvFields.append(row['RELATED_ENTITY_ID'])
            csvFields.append(row['MATCH_LEVEL'])
            csvFields.append(row['MATCH_KEY'])
            csvFields.append(row['DATA_SOURCE'])
            csvFields.append(row['RECORD_ID'])
            csvFields.append(row['JSON_DATA'])
            if extendedFormat:
                csvFields.append(row['LENS_ID'])
                csvFields.append(row['ENTITY_TYPE'])
                csvFields.append(row['REF_SCORE'])
                csvFields.append(row['ERRULE_CODE'])

            #--write to output file
            try: outputFileWriter.writerow(csvFields)
            except csv.Error as err:
                print('ERROR: writing to CSV file')
                print(err)
                appError = 1
                break

            rowCount += 1
            if rowCount % sqlCommitSize == 0:
                print(' %s records written' % rowCount)

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
    #--remove file if exists
    if os.path.exists(fileName):
        try: os.remove(fileName)
        except:
            print('ERROR: could not remove %s' % (fileName))
            return None
    
    #--open file for append
    try: fileHandle = open(fileName, 'a')
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

    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))
    iniFileName = appPath + os.path.sep + 'G2Project.ini'
    if not os.path.exists(iniFileName):
        print('ERROR: The G2Project.ini file is missing from the application path!')
        sys.exit(1)

    #--get parameters from ini file
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)
    try: sqlCommitSize = int(iniParser.get('report', 'sqlCommitSize'))
    except: sqlCommitSize = 1000
    try: g2iniPath = os.path.expanduser(iniParser.get('g2', 'iniPath'))
    except: g2iniPath = None

    #--defaults
    appError = 0
    outputFilter = 3
    outputFormat = 'csv'
    outputFileName = 'g2export.csv'

    #--capture the command line arguments
    if len(sys.argv) > 1:
        optParser = optparse.OptionParser()
        optParser.add_option('-o', '--output-file', dest='outputFileName', default=outputFileName, help='the name of a file to write the output to')
        optParser.add_option('-f', '--outputFilter', dest='outputFilter', type='int', default=3, help='1=Resolved Entities only; 2=add possible matches; 3=add relationships')
        optParser.add_option('-F', '--outputFormat', dest='outputFormat', default=outputFormat, help='json or csv style')
        (options, args) = optParser.parse_args()
        if options.outputFileName:
            outputFileName = options.outputFileName
        if options.outputFilter:
            outputFilter = options.outputFilter
        if options.outputFormat:
            outputFormat = options.outputFormat

    #--validations
    if not g2iniPath:
        print('ERROR: G2 ini file not specified!')
        sys.exit(1)
    if not outputFileName:
        print('ERROR: Output file name must be specified')
        sys.exit(1)
    if not outputFilter:
        print('ERROR: Resume filter value must be specified')
        sys.exit(1)
    if outputFilter not in (1,2,3,4,5):
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


