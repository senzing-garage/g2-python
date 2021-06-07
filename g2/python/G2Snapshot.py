#! /usr/bin/env python3

import argparse
import csv
import json
import os
import random
import signal
import sys
import time
from datetime import datetime, timedelta
import configparser

#--senzing python classes
try: 
    import G2Paths
    from G2Product import G2Product
    from G2Engine import G2Engine
    from G2IniParams import G2IniParams
    from G2ConfigMgr import G2ConfigMgr
    from G2Exception import G2Exception
except:
    print('')
    print('Please export PYTHONPATH=<path to senzing python directory>')
    print('')
    sys.exit(1)

#---------------------------------------
def nextExportRecord(exportHandle, exportHeaders = None):
    rowString = bytearray()
    rowData = g2Engine.fetchNext(exportHandle, rowString)
    if not(rowData):
        return None

    if exportHeaders == 'JSON':
        return json.loads(rowData)

    try: rowData = next(csv.reader([rowString.decode()[0:-1]]))
    except: 
        print(' err: ' + rowString.decode())        
        return None

    if exportHeaders: 
        return dict(zip(exportHeaders, rowData))

    return rowData

#---------------------------------------
def initDataSourceStats(dataSource1, dataSource2 = None):
    if not dataSource2:
        statPack['DATA_SOURCES'][dataSource1] = {}
        statPack['DATA_SOURCES'][dataSource1]['ENTITY_COUNT'] = 0
        statPack['DATA_SOURCES'][dataSource1]['RECORD_COUNT'] = 0
        statPack['DATA_SOURCES'][dataSource1]['SINGLE_COUNT'] = 0
        statPack['DATA_SOURCES'][dataSource1]['SINGLE_SAMPLE'] = []
        for statType in ['DUPLICATE', 'AMBIGUOUS_MATCH', 'POSSIBLE_MATCH', 'POSSIBLY_RELATED', 'DISCLOSED_RELATION']:
            statPack['DATA_SOURCES'][dataSource1][statType + '_ENTITY_COUNT'] = 0
            statPack['DATA_SOURCES'][dataSource1][statType + '_RECORD_COUNT'] = 0
            statPack['DATA_SOURCES'][dataSource1][statType + '_SAMPLE'] = []
        statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'] = {}
    else:
        statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2] = {}
        for statType in ['MATCH', 'AMBIGUOUS_MATCH', 'POSSIBLE_MATCH', 'POSSIBLY_RELATED', 'DISCLOSED_RELATION']:
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statType + '_ENTITY_COUNT'] = 0
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statType + '_RECORD_COUNT'] = 0
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statType + '_SAMPLE'] = []

#---------------------------------------
def randomSampleIndex():
    targetIndex = random.randint(1, sampleSize)
    if targetIndex % round(sampleSize/10) == 0:
        return 0
    return targetIndex

#---------------------------------------
def updateStatpack(dataSource1, dataSource2, statPrefix, entityCount, recordCount, sampleValue):
    randomIndex = randomSampleIndex()

    if dataSource1 not in statPack['DATA_SOURCES']:
        initDataSourceStats(dataSource1)
    if dataSource2 and dataSource2 not in statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES']:
        initDataSourceStats(dataSource1, dataSource2)

    #--special case for entity/record count at the data source level with no sample value
    if not statPrefix:
        statPack['DATA_SOURCES'][dataSource1]['ENTITY_COUNT'] += entityCount
        statPack['DATA_SOURCES'][dataSource1]['RECORD_COUNT'] += recordCount
        return

    #--within data source
    if not dataSource2:
        #--count
        if recordCount == 0:
            statPack['DATA_SOURCES'][dataSource1][statPrefix + '_COUNT'] += entityCount
        else:
            statPack['DATA_SOURCES'][dataSource1][statPrefix + '_ENTITY_COUNT'] += entityCount
            statPack['DATA_SOURCES'][dataSource1][statPrefix + '_RECORD_COUNT'] += recordCount
        #--sample
        if len(statPack['DATA_SOURCES'][dataSource1][statPrefix + '_SAMPLE']) < sampleSize:
            statPack['DATA_SOURCES'][dataSource1][statPrefix + '_SAMPLE'].append(sampleValue)
        elif randomIndex != 0:
            statPack['DATA_SOURCES'][dataSource1][statPrefix + '_SAMPLE'][randomIndex] = sampleValue

    #--across data sources
    else:
        #--count
        if recordCount == 0:
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statPrefix + '_COUNT'] += entityCount
        else:
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statPrefix + '_ENTITY_COUNT'] += entityCount
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statPrefix + '_RECORD_COUNT'] += recordCount
        #--sample
        if len(statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statPrefix + '_SAMPLE']) < sampleSize:
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statPrefix + '_SAMPLE'].append(sampleValue)
        elif randomIndex != 0:
            statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES'][dataSource2][statPrefix + '_SAMPLE'][randomIndex] = sampleValue

#---------------------------------------
def writeCsvRecord(csvData):
    global shutDown
    columnValues = []
    columnValues.append(str(csvData['RESOLVED_ENTITY_ID']))
    columnValues.append(str(csvData['RELATED_ENTITY_ID']))
    columnValues.append(str(csvData['MATCH_LEVEL']))
    columnValues.append(csvData['MATCH_KEY'][1:] if csvData['MATCH_KEY'] else '')
    columnValues.append(csvData['DATA_SOURCE'])
    columnValues.append(csvData['RECORD_ID'])
    try: exportFileHandle.write(','.join(columnValues) + '\n')        
    except IOError as err: 
        print('')
        print('ERROR: cannot write to %s \n%s' % (exportFilePath, err))
        print('')
        shutDown = True

#---------------------------------------
def processEntities():
    global shutDown

    statPack['TOTAL_RECORD_COUNT'] = 0
    statPack['TOTAL_ENTITY_COUNT'] = 0
    statPack['TOTAL_AMBIGUOUS_MATCHES'] = 0
    statPack['TOTAL_POSSIBLE_MATCHES'] = 0
    statPack['TOTAL_POSSIBLY_RELATEDS'] = 0
    statPack['TOTAL_DISCLOSED_RELATIONS'] = 0
    statPack['DATA_SOURCES'] = {}
    statPack['ENTITY_SIZE_BREAKDOWN'] = {}
    entitySizeReviewCount = 0

    categoryTotalStat = {}
    categoryTotalStat['AMBIGUOUS_MATCH'] = 'TOTAL_AMBIGUOUS_MATCHES'
    categoryTotalStat['POSSIBLE_MATCH'] = 'TOTAL_POSSIBLE_MATCHES'
    categoryTotalStat['POSSIBLY_RELATED'] = 'TOTAL_POSSIBLY_RELATEDS'
    categoryTotalStat['DISCLOSED_RELATION'] = 'TOTAL_DISCLOSED_RELATIONS'

    #--determine the output flags
    exportFlags = 0
    exportFlags = g2Engine.G2_EXPORT_INCLUDE_ALL_ENTITIES
    if relationshipFilter == 1:
        pass #--don't include any relationships
    elif relationshipFilter == 2:
        exportFlags = exportFlags | g2Engine.G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS
    else:
        exportFlags = exportFlags | g2Engine.G2_ENTITY_INCLUDE_ALL_RELATIONS

    if exportType == 'JSON':  #--specify what sections for json
        print('\nExport type is JSON')
        exportFlags = exportFlags | g2Engine.G2_ENTITY_INCLUDE_RECORD_DATA
        exportFlags = exportFlags | g2Engine.G2_ENTITY_INCLUDE_RECORD_MATCHING_INFO 
        exportFlags = exportFlags | g2Engine.G2_ENTITY_INCLUDE_RELATED_MATCHING_INFO
        exportFlags = exportFlags | g2Engine.G2_ENTITY_INCLUDE_RELATED_RECORD_SUMMARY

    else:  #--specify what fields for csv
        exportFields = []
        exportFields.append('RESOLVED_ENTITY_ID')
        exportFields.append('RELATED_ENTITY_ID')
        exportFields.append('MATCH_LEVEL')
        exportFields.append('MATCH_KEY')
        exportFields.append('IS_DISCLOSED')
        exportFields.append('IS_AMBIGUOUS')
        exportFields.append('ERRULE_CODE')
        exportFields.append('DATA_SOURCE')
        exportFields.append('RECORD_ID')

    #--initialize the export
    print('\nQuerying entities ...')
    try: 
        if exportType == 'JSON':
            exportHandle = g2Engine.exportJSONEntityReport(exportFlags)
        else:
            exportHandle = g2Engine.exportCSVEntityReportV2(",".join(exportFields), exportFlags)
            exportHeaders = nextExportRecord(exportHandle)

    except G2Exception as err:
        print('\n%s\n' % str(err))
        return 1

    batchStartTime = time.time()
    batchEntitySizeSum = 0
    batchRelationCountSum = 0
    maxEntitySize = 0
    maxRelationCount = 0
    if exportType == 'JSON':
        exportRecord = nextExportRecord(exportHandle, exportType)
    else:
        exportRecord = nextExportRecord(exportHandle, exportHeaders)
    while exportRecord:

        entitySize = 0
        recordList = []
        resumeData = {}

        #--gather the records in a common structure
        rowList = []
        if exportType == 'JSON':
            resolvedEntityID = str(exportRecord['RESOLVED_ENTITY']['ENTITY_ID'])
            for dsrcRecord in exportRecord['RESOLVED_ENTITY']['RECORDS']:
                rowData = {}
                rowData['RESOLVED_ENTITY_ID'] = resolvedEntityID
                rowData['RELATED_ENTITY_ID'] = '0'
                rowData['MATCH_LEVEL'] = str(dsrcRecord['MATCH_LEVEL'])
                rowData['MATCH_KEY'] = dsrcRecord['MATCH_KEY']
                rowData['IS_DISCLOSED'] = '0'
                rowData['IS_AMBIGUOUS'] = '0'
                rowData['DATA_SOURCE'] = dsrcRecord['DATA_SOURCE']
                rowData['RECORD_ID'] = dsrcRecord['RECORD_ID']
                rowList.append(rowData)
            for relatedEntity in exportRecord['RELATED_ENTITIES'] if 'RELATED_ENTITIES' in exportRecord else []:
                for dsrcSummary in relatedEntity['RECORD_SUMMARY']:
                    rowData = {}
                    rowData['RESOLVED_ENTITY_ID'] = resolvedEntityID
                    rowData['RELATED_ENTITY_ID'] = str(relatedEntity['ENTITY_ID'])
                    rowData['MATCH_LEVEL'] = str(relatedEntity['MATCH_LEVEL'])
                    rowData['MATCH_KEY'] = relatedEntity['MATCH_KEY']
                    rowData['IS_DISCLOSED'] = str(relatedEntity['IS_DISCLOSED'])
                    rowData['IS_AMBIGUOUS'] = str(relatedEntity['IS_AMBIGUOUS'])
                    rowData['DATA_SOURCE'] = dsrcSummary['DATA_SOURCE']
                    rowData['RECORD_ID'] = None
                    rowList.append(rowData)
            exportRecord = nextExportRecord(exportHandle, exportType)
        else: #--csv
            entityID = exportRecord['RESOLVED_ENTITY_ID']
            while exportRecord and exportRecord['RESOLVED_ENTITY_ID'] == entityID:
                rowList.append(exportRecord)
                exportRecord = nextExportRecord(exportHandle, exportHeaders)

        #--summarize entity resume 
        entityID = rowList[0]['RESOLVED_ENTITY_ID']
        for rowData in rowList:
            relatedID = rowData['RELATED_ENTITY_ID']
            dataSource = rowData['DATA_SOURCE']
            recordID = rowData['RECORD_ID']

            if rowData['RELATED_ENTITY_ID'] == '0':
                matchCategory = 'RESOLVED'
                entitySize += 1
                recordList.append(dataSource + ':' + recordID)
            elif rowData['IS_DISCLOSED'] != '0':
                matchCategory = 'DISCLOSED_RELATION'
            elif rowData['IS_AMBIGUOUS'] != '0':
                matchCategory = 'AMBIGUOUS_MATCH'
            elif rowData['MATCH_LEVEL'] == '2':
                matchCategory = 'POSSIBLE_MATCH'
            else:          
                matchCategory = 'POSSIBLY_RELATED'

            if relatedID not in resumeData:
                resumeData[relatedID] = {}
                resumeData[relatedID]['matchCategory'] = matchCategory
                resumeData[relatedID]['dataSources'] = {}

            if dataSource not in resumeData[relatedID]['dataSources']:
                resumeData[relatedID]['dataSources'][dataSource] = 1
            else:
                resumeData[relatedID]['dataSources'][dataSource] += 1

            if exportFilePath and recordID: #--filters json related entities which currently only provide a summart, not a list of records
                writeCsvRecord(rowData)

            if shutDown:
                break

        #--update entity size breakdown
        randomIndex = randomSampleIndex()
        strEntitySize = str(entitySize)
        if strEntitySize not in statPack['ENTITY_SIZE_BREAKDOWN']:
            statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize] = {}
            statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['COUNT'] = 0
            statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE'] = []
        statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['COUNT'] += 1
        if len(statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE']) < sampleSize:
            statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE'].append(entityID)
            entitySizeReviewCount += 1
        elif randomIndex != 0:
            statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE'][randomIndex] = entityID

        #--resolved entity stats
        statPack['TOTAL_ENTITY_COUNT'] += 1
        statPack['TOTAL_RECORD_COUNT'] += entitySize
        for dataSource1 in resumeData['0']['dataSources']:
            recordCount = resumeData['0']['dataSources'][dataSource1]

            #--this just updates entity and record count for the data source
            updateStatpack(dataSource1, None, None, 1, recordCount, None) 

            if recordCount == 1:
                updateStatpack(dataSource1, None, 'SINGLE', 1, 0, entityID)
            else:
                updateStatpack(dataSource1, None, 'DUPLICATE', 1, recordCount, entityID)

            #--cross matches
            for dataSource2 in resumeData['0']['dataSources']:
                if dataSource2 == dataSource1:
                    continue
                updateStatpack(dataSource1, dataSource2, 'MATCH', 1, recordCount, entityID)

            #--related entity stats
            for relatedID in resumeData:
                if relatedID == '0':
                    continue
                matchCategory = resumeData[relatedID]['matchCategory']
                statPack[categoryTotalStat[matchCategory]] += 1
                for dataSource2 in resumeData[relatedID]['dataSources']:
                    recordCount = resumeData[relatedID]['dataSources'][dataSource2]
                    if dataSource2 == dataSource1:
                        dataSource2 = None

                    #--avoid double counting within data source (can't be avoided across data sources)
                    if entityID < relatedID or dataSource2:
                        updateStatpack(dataSource1, dataSource2, matchCategory, 1, recordCount, entityID + ' ' + relatedID)

        #--status display
        batchEntitySizeSum += entitySize
        if entitySize > maxEntitySize:
            maxEntitySize = entitySize 
        relationCount = len(resumeData) - 1
        batchRelationCountSum += relationCount
        if relationCount > maxRelationCount:
            maxRelationCount = relationCount

        if statPack['TOTAL_ENTITY_COUNT'] % progressInterval == 0 or not exportRecord:
            entityCount = statPack['TOTAL_ENTITY_COUNT']
            now = datetime.now().strftime('%I:%M%p').lower()
            elapsedMins = round((time.time() - procStartTime) / 60, 1)
            eps = int(float(progressInterval) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
            avgEntitySize = round(batchEntitySizeSum / progressInterval)
            avgRelationCount = round(batchRelationCountSum / progressInterval)

            if exportRecord:
                print(' %s entities processed at %s, %s per second | avg/max entity size = %s/%s | avg/max relationship count %s/%s' % (entityCount, now, eps, avgEntitySize, maxEntitySize, avgRelationCount, maxRelationCount))
            else:
                eps = int(float(entityCount) / (float(time.time() - procStartTime if time.time() - procStartTime != 0 else 1)))
                print(' %s entities completed at %s after %s minutes, at %s per second' % (entityCount, now, elapsedMins, eps))

            batchStartTime = time.time()
            batchEntitySizeSum = 0
            batchRelationCountSum = 0
            maxEntitySize = 0
            maxRelationCount = 0

        #--get out if errors hit or out of records
        if shutDown or not rowData:
            break
 
    #--get out if errors hit
    if shutDown:
        return 1

    #--calculate some percentages
    statPack['TOTAL_COMPRESSION'] = str(round(100.00-((float(statPack['TOTAL_ENTITY_COUNT']) / float(statPack['TOTAL_RECORD_COUNT'])) * 100.00), 2)) + '%'
    for dataSource in statPack['DATA_SOURCES']:
        statPack['DATA_SOURCES'][dataSource]['COMPRESSION'] = str(round(100.00-((float(statPack['DATA_SOURCES'][dataSource]['ENTITY_COUNT']) / float(statPack['DATA_SOURCES'][dataSource]['RECORD_COUNT'])) * 100.00), 2)) + '%'

    #--add feature stats to the entity size break down
    print('\nReviewing %s entities ...' % entitySizeReviewCount)
    reviewStartTime = time.time()
    batchStartTime = time.time()
    reviewCount = 0
    getFlags = g2Engine.G2_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES
    entitySizeBreakdown = {}
    for strEntitySize in statPack['ENTITY_SIZE_BREAKDOWN']:
        entitySize = int(strEntitySize)
        if entitySize < 10:
            entitySizeLevel = entitySize
        elif entitySize < 100:
            entitySizeLevel = int(entitySize/10) * 10
        else:
            entitySizeLevel = int(entitySize/100) * 100

        if entitySizeLevel not in entitySizeBreakdown:
            entitySizeBreakdown[entitySizeLevel] = {}
            entitySizeBreakdown[entitySizeLevel]['ENTITY_COUNT'] = 0
            entitySizeBreakdown[entitySizeLevel]['SAMPLE_ENTITIES'] = []
            entitySizeBreakdown[entitySizeLevel]['REVIEW_COUNT'] = 0
            entitySizeBreakdown[entitySizeLevel]['REVIEW_FEATURES'] = []
        entitySizeBreakdown[entitySizeLevel]['ENTITY_COUNT'] += statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['COUNT']

        for entityID in statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE']:

            #--gather feature statistics
            entityInfo = {}
            if entitySize > 1:
                try: 
                    response = bytearray()
                    retcode = g2Engine.getEntityByEntityIDV2(int(entityID), getFlags, response)
                    response = response.decode() if response else ''
                except G2Exception as err:
                    print(str(err))
                    #shutDown = True
                    #return 1
                    continue
                try: 
                    jsonData = json.loads(response)
                except:
                    response = 'None' if not response else response
                    print('warning: entity %s response %s' % (entityID, response))
                    continue

                for ftypeCode in jsonData['RESOLVED_ENTITY']['FEATURES']:

                    #-count how many with special cases 
                    distinctFeatureCount = 0
                    for distinctFeature in jsonData['RESOLVED_ENTITY']['FEATURES'][ftypeCode]:
                        if ftypeCode == 'GENDER' and distinctFeature['FEAT_DESC'] not in ('M', 'F'): #--don't count invalid genders
                            continue
                        distinctFeatureCount += 1

                    if ftypeCode not in entityInfo:
                        entityInfo[ftypeCode] = 0
                    entityInfo[ftypeCode] += distinctFeatureCount

                if entitySize <= 3: #--super small
                    maxExclusiveCnt = 1
                    maxNameCnt = 2
                    maxAddrCnt = 2
                    #maxF1Cnt = 3
                    #maxFFCnt = 5
                elif entitySize <= 10: #--small
                    maxExclusiveCnt = 1
                    maxNameCnt = 3
                    maxAddrCnt = 3
                    #maxF1Cnt = 3
                    #maxFFCnt = 5
                elif entitySize <= 50: #--medium
                    maxExclusiveCnt = 1
                    maxNameCnt = 10
                    maxAddrCnt = 10
                    #maxF1Cnt = 10
                    #maxFFCnt = 10
                else: #--large
                    maxExclusiveCnt = 1 #--large
                    maxNameCnt = 25
                    maxAddrCnt = 25
                    #maxF1Cnt = 25
                    #maxFFCnt = 25

                reviewFeatures = []
                for ftypeCode in entityInfo:
                    distinctFeatureCount = entityInfo[ftypeCode]

                    #--watch lists have more multiple features per record like 5 dobs and 10 names!
                    if distinctFeatureCount > entitySize:
                        continue

                    frequency = ftypeCodeLookup[ftypeCode]['FTYPE_FREQ']
                    exclusive = str(ftypeCodeLookup[ftypeCode]['FTYPE_EXCL']).upper() in ('1', 'Y', 'YES')

                    needsReview = False
                    if exclusive and distinctFeatureCount > maxExclusiveCnt:
                        needsReview = True
                    elif ftypeCode == 'NAME' and distinctFeatureCount > maxNameCnt:
                        needsReview = True
                    elif ftypeCode == 'ADDRESS' and distinctFeatureCount > maxAddrCnt:
                        needsReview = True

                    if needsReview: 
                        reviewFeatures.append(ftypeCode)

                if reviewFeatures:
                    entityInfo['REVIEW_FEATURES'] = reviewFeatures
                    entitySizeBreakdown[entitySizeLevel]['REVIEW_COUNT'] += 1
                    for ftypeCode in reviewFeatures:
                        if ftypeCode not in entitySizeBreakdown[entitySizeLevel]['REVIEW_FEATURES']:
                            entitySizeBreakdown[entitySizeLevel]['REVIEW_FEATURES'].append(ftypeCode)

            entityInfo['ENTITY_ID'] = entityID
            entityInfo['ENTITY_SIZE'] = entitySize
            entitySizeBreakdown[entitySizeLevel]['SAMPLE_ENTITIES'].append(entityInfo)

            #--status display
            reviewCount += 1
            if reviewCount % progressInterval == 0 or reviewCount == entitySizeReviewCount:
                entityCount = reviewCount
                now = datetime.now().strftime('%I:%M%p').lower()
                elapsedMins = round((time.time() - procStartTime) / 60, 1)
                eps = int(float(progressInterval) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
                batchStartTime = time.time()
                if rowData:
                    print(' %s entities reviewed at %s, %s per second' % (entityCount, now, eps))
                else:
                    eps = int(float(entitySizeReviewCount) / (float(time.time() - reviewStartTime if time.time() - reviewStartTime != 0 else 1)))
                    print(' %s entities reviewed at %s after %s minutes' % (entityCount, now, elapsedMins))

    statPack['ENTITY_SIZE_BREAKDOWN'] = []
    for entitySize in sorted(entitySizeBreakdown.keys()):
        entitySizeRecord = entitySizeBreakdown[entitySize]
        entitySizeRecord['ENTITY_SIZE'] = int(entitySize)
        entitySizeRecord['ENTITY_SIZE_GROUP'] = str(entitySize) + ('+' if int(entitySize) >= 10 else '')
        statPack['ENTITY_SIZE_BREAKDOWN'].append(entitySizeRecord)

    return 0

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shutDown
    shutDown = True
    return

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    try: response = input(question)
    except: response = None
    return response

#----------------------------------------
if __name__ == '__main__':
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()
    progressInterval = 10000

    #--defaults
    try: iniFileName = G2Paths.get_G2Module_ini_path()
    except: iniFileName = '' 
    outputFileRoot = os.getenv('SENZING_OUTPUT_FILE_ROOT') if os.getenv('SENZING_OUTPUT_FILE_ROOT', None) else None
    sampleSize = int(os.getenv('SENZING_SAMPLE_SIZE')) if os.getenv('SENZING_SAMPLE_SIZE', None) and os.getenv('SENZING_SAMPLE_SIZE').isdigit() else 1000
    relationshipFilter = int(os.getenv('SENZING_RELATIONSHIP_FILTER')) if os.getenv('SENZING_RELATIONSHIP_FILTER', None) and os.getenv('SENZING_RELATIONSHIP_FILTER').isdigit() else 3

    #--capture the command line arguments
    argParser = argparse.ArgumentParser()
    argParser.add_argument('-c', '--config_file_name', dest='ini_file_name', default=iniFileName, help='name of the g2.ini file, defaults to %s' % iniFileName)
    argParser.add_argument('-o', '--output_file_root', dest='output_file_root', default=outputFileRoot, help='root name for files created such as "/project/snapshots/snapshot1"')
    argParser.add_argument('-s', '--sample_size', dest='sample_size', type=int, default=sampleSize, help='defaults to %s' % sampleSize)
    argParser.add_argument('-f', '--relationship_filter', dest='relationship_filter', type=int, default=relationshipFilter, help='filter options 1=No Relationships, 2=Include possible matches, 3=Include possibly related and disclosed. Defaults to %s' % relationshipFilter)
    argParser.add_argument('-x', '--export_csv', dest='export_csv', action='store_true', default=False, help='also export the full csv')
    argParser.add_argument('-D', '--debug', dest='debug', action='store_true', default=False, help='print debug statements')

    args = argParser.parse_args()
    iniFileName = args.ini_file_name
    outputFileRoot = args.output_file_root
    sampleSize = args.sample_size
    relationshipFilter = args.relationship_filter
    noCsvExport = not args.export_csv
    debugOn = args.debug
    exportType = 'CSV'

    #--try to initialize the g2engine
    try:
        g2Engine = G2Engine()
        iniParamCreator = G2IniParams()
        iniParams = iniParamCreator.getJsonINIParams(iniFileName)
        g2Engine.initV2('G2Snapshot', iniParams, False)
    except G2Exception as err:
        print('\n%s\n' % str(err))
        sys.exit(1)

    #--get the version information
    try: 
        g2Product = G2Product()
        apiVersion = json.loads(g2Product.version())
    except G2Exception.G2Exception as err:
        print(err)
        sys.exit(1)
    g2Product.destroy()

    #--get needed config data
    try: 
        g2ConfigMgr = G2ConfigMgr()
        g2ConfigMgr.initV2('pyG2ConfigMgr', iniParams, False)
        defaultConfigID = bytearray() 
        g2ConfigMgr.getDefaultConfigID(defaultConfigID)
        defaultConfigDoc = bytearray() 
        g2ConfigMgr.getConfig(defaultConfigID, defaultConfigDoc)
        cfgData = json.loads(defaultConfigDoc.decode())
        g2ConfigMgr.destroy()
        ftypeCodeLookup = {}
        for cfgRecord in cfgData['G2_CONFIG']['CFG_FTYPE']:
            ftypeCodeLookup[cfgRecord['FTYPE_CODE']] = cfgRecord 
    except Exception as err:
        print('\n%s\n' % str(err))
        sys.exit(1)
    g2ConfigMgr.destroy()

    #--check the output file
    if not outputFileRoot:
        print('\nPlease use -o to select and output path and root file name such as /project/audit/run1\n')
        sys.exit(1)
    if os.path.splitext(outputFileRoot)[1]:
        print("\nPlease don't use a file extension as both a .json and a .csv file will be created\n")
        sys.exit(1)

    #--create output file paths
    statsFilePath = outputFileRoot + '.json'
    if noCsvExport:
        exportFilePath = None
    else:
        exportFilePath = outputFileRoot + '.csv'

    #--open the export file if set
    if exportFilePath:
        columnHeaders = []
        columnHeaders.append('RESOLVED_ENTITY_ID')
        columnHeaders.append('RELATED_ENTITY_ID')
        columnHeaders.append('MATCH_LEVEL')
        columnHeaders.append('MATCH_KEY')
        columnHeaders.append('DATA_SOURCE')
        columnHeaders.append('RECORD_ID')
        try: 
            exportFileHandle = open(exportFilePath, 'w')
            exportFileHandle.write(','.join(columnHeaders) + '\n')        
        except IOError as err: 
            print('\nERROR: cannot write to %s \n%s\n' % (exportFilePath, err))
            sys.exit(1)
            
    #--get entities and relationships
    statPack = {}
    statPack['SOURCE'] = 'G2Snapshot'
    statPack['API_VERSION'] = apiVersion['BUILD_VERSION']
    statPack['RUN_DATE'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    returnCode = processEntities()

    #--wrap ups
    if exportFilePath:
        exportFileHandle.close()
    g2Engine.destroy()

    #--dump the stats to screen and file
    print('')
    for stat in statPack:
        if type(statPack[stat]) not in (list, dict):
            print ('%s = %s' % (stat, statPack[stat]))
    with open(statsFilePath, 'w') as outfile:
        json.dump(statPack, outfile)    
    print('')

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if returnCode == 0:
        print('Process completed successfully in %s minutes' % elapsedMins)
    else:
        print('Process aborted after %s minutes!' % elapsedMins)
    print('')

    sys.exit(returnCode)
