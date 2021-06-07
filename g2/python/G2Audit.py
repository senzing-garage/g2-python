#! /usr/bin/env python3
import os
import sys
import argparse
try: import configparser
except: import ConfigParser as configparser
import signal
import csv
import json
from datetime import datetime, timedelta
import time
import random

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    """ pause for debug purposes """
    try: input(question)
    except KeyboardInterrupt:
        global shutDown
        shutDown = True
    except: pass

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shutDown
    shutDown = True
    return

#----------------------------------------
def splitCost(a, b):
    return (a*b)

#----------------------------------------
def mergeCost(a, b):
    return (a*b)

#----------------------------------------
def makeKeytable(fileName, tableName):

    print('loading %s ...' % fileName)

    try: 
        with open(fileName,'r') as f:
            headerLine = f.readline()
    except IOError as err:
        print(err)
        return None
    csvDialect = csv.Sniffer().sniff(headerLine)
    columnNames = next(csv.reader([headerLine], dialect = csvDialect))
    columnNames = [x.upper() for x in columnNames]

    fileMap = {}
    fileMap['algorithmName'] = '<name of the algorthm that produced the entity map>'
    fileMap['clusterField'] = '<csvFieldName> for unique ID'
    fileMap['recordField'] = '<csvFieldName> for the record ID'
    fileMap['sourceField'] = '<csvFieldName> for the data source (only required if multiple)'
    fileMap['sourceValue'] = 'hard coded value that matches Senzing data source source'
    fileMap['scoreField'] = '<csvFieldName> for the matching score (optional)'

    if 'RESOLVED_ENTITY_ID' in columnNames and 'DATA_SOURCE' in columnNames and 'RECORD_ID' in columnNames:
        fileMap['algorithmName'] = 'Senzing'
        fileMap['clusterField'] = 'RESOLVED_ENTITY_ID'
        fileMap['recordField'] = 'RECORD_ID'
        fileMap['sourceField'] = 'DATA_SOURCE'
        fileMap['scoreField'] = 'MATCH_KEY'
    elif 'CLUSTER_ID' in columnNames and 'RECORD_ID' in columnNames:
        fileMap['algorithmName'] = 'Other'
        fileMap['clusterField'] = 'CLUSTER_ID'
        fileMap['recordField'] = 'RECORD_ID'
        if 'DATA_SOURCE' in columnNames:
            fileMap['sourceField'] = 'DATA_SOURCE'
        else: 
            del fileMap['sourceField']
            print()
            fileMap['sourceValue'] = input('What did you name the data_source? ')
            print() 
            if not fileMap['sourceValue']:
                print('Unfortunately a data source name is required. process aborted.')
                print()
                return None
        if 'SCORE' in columnNames:
            fileMap['scoreField'] = 'SCORE'
        else:
            del fileMap['scoreField']
    else:
        if not os.path.exists(fileName + '.map'):
            print('')
            print('please describe the fields for ' + fileName + ' as follows in a file named ' + fileName + '.map')
            print(json.dumps(fileMap, indent=4))
            print('')
            return None
        else:
            try: fileMap = json.load(open(fileName + '.map'))
            except ValueError as err:
                print('error opening %s' % (fileName + '.map'))
                print(err)
                return None
            if 'clusterField' not in fileMap:
                print('clusterField missing from file map')
                return None
            if 'recordField' not in fileMap:
                print('recordField missing from file map')
                return None
            if 'sourceField' not in fileMap and 'sourceValue' not in fileMap:
                print('either a sourceField or sourceValue must be specified in the file map')
                return None

    fileMap['fileName'] = fileName
    fileMap['tableName'] = tableName
    fileMap['columnHeaders'] = columnNames
    if fileMap['clusterField'] not in fileMap['columnHeaders']:
        print('column %s not in %s' % (fileMap['clusterField'], fileMap['fileName']))
        return 1
    if fileMap['recordField'] not in fileMap['columnHeaders']:
        print('column %s not in %s' % (fileMap['recordField'], fileMap['fileName']))
        return 1
    #if  fileMap['sourceField'] not in fileMap['columnHeaders']:
    #    print('column %s not in %s' % (fileMap['sourceField'], fileMap['fileName']))
    #    return 1

    fileMap['clusters'] = {}
    fileMap['records'] = {}
    fileMap['relationships'] = {}
    nextMissingCluster_id = 0

    with open(fileMap['fileName'],'r') as csv_file:
        csv_reader = csv.reader(csv_file, dialect = csvDialect)
        next(csv_reader) #--remove header
        for row in csv_reader:
            rowData = dict(zip(columnNames, row))
            if fileMap['algorithmName'] == 'Senzing' and 'RELATED_ENTITY_ID' in rowData and rowData['RELATED_ENTITY_ID'] != '0':
                ent1str = str(rowData['RESOLVED_ENTITY_ID'])
                ent2str = str(rowData['RELATED_ENTITY_ID'])
                relKey = ent1str + '-' + ent2str if ent1str < ent2str else ent2str + '-' + ent1str
                if relKey not in fileMap['relationships']:
                    fileMap['relationships'][relKey] = rowData['MATCH_KEY']
                continue
            if 'sourceField' in fileMap:
                sourceValue = rowData[fileMap['sourceField']]
            else:
                sourceValue = fileMap['sourceValue']        
            if 'scoreField' in fileMap:
                scoreValue = rowData[fileMap['scoreField']]
            else:
                scoreValue = None        

            rowData[fileMap['recordField']] = str(rowData[fileMap['recordField']]) + '|DS=' + str(sourceValue)
            if not rowData[fileMap['clusterField']]:
                nextMissingCluster_id += 1
                rowData[fileMap['clusterField']] = '(sic) ' + str(nextMissingCluster_id)
            else:             
                rowData[fileMap['clusterField']] = str(rowData[fileMap['clusterField']])
            fileMap['records'][rowData[fileMap['recordField']]] = rowData[fileMap['clusterField']]
            if rowData[fileMap['clusterField']] not in fileMap['clusters']:
                fileMap['clusters'][rowData[fileMap['clusterField']]] = {}
            fileMap['clusters'][rowData[fileMap['clusterField']]][rowData[fileMap['recordField']]] = scoreValue

    return fileMap

def erCompare(fileName1, fileName2, outputRoot):

    #--load the second file into a database table (this is the prior run or prior ground truth)
    fileMap2 = makeKeytable(fileName2, 'prior')
    if not fileMap2:
        return 1

    #--load the first file into a database table (this is the newer run or candidate for adoption)
    fileMap1 = makeKeytable(fileName1, 'newer')
    if not fileMap1:
        return 1

    #--set output files and columns
    outputCsvFile = outputRoot + '.csv'
    outputJsonFile = outputRoot + '.json'
    try: csvHandle = open(outputCsvFile, 'w')
    except IOError as err:
        print(err)
        print('could not open output file %s' % outputCsvFile)
        return 1

    csvHeaders = []
    csvHeaders.append('audit_id')
    csvHeaders.append('audit_category')
    csvHeaders.append('audit_result')
    csvHeaders.append('data_source')
    csvHeaders.append('record_id')
    csvHeaders.append('prior_id')
    csvHeaders.append('prior_score')
    csvHeaders.append('newer_id')
    csvHeaders.append('newer_score')
    try: csvHandle.write(','.join(csvHeaders) + '\n')
    except IOError as err:
        print(err)
        print('could not write to output file %s' % outputCsvFile)
        return 
    nextAuditID = 0

    #--initialize stats
    statpack = {}
    statpack['SOURCE'] = 'G2Audit'
 
    statpack['ENTITY'] = {}
    statpack['ENTITY']['STANDARD_COUNT'] = 0
    statpack['ENTITY']['RESULT_COUNT'] = 0
    statpack['ENTITY']['COMMON_COUNT'] = 0

    statpack['CLUSTERS'] = {}
    statpack['CLUSTERS']['STANDARD_COUNT'] = 0
    statpack['CLUSTERS']['RESULT_COUNT'] = 0
    statpack['CLUSTERS']['COMMON_COUNT'] = 0

    statpack['ACCURACY'] = {}
    statpack['ACCURACY']['PRIOR_POSITIVE'] = 0
    statpack['ACCURACY']['NEW_POSITIVE'] = 0
    statpack['ACCURACY']['NEW_NEGATIVE'] = 0

    statpack['PAIRS'] = {}
    statpack['PAIRS']['RESULT_COUNT'] = 0
    statpack['PAIRS']['STANDARD_COUNT'] = 0
    statpack['PAIRS']['COMMON_COUNT'] = 0

    statpack['SLICE'] = {}
    statpack['SLICE']['COST'] = 0

    statpack['AUDIT'] = {}
    statpack['MISSING_RECORD_COUNT'] = 0

    #--to track the largest matching clusters with new positives
    newPositiveClusters = {}

    #--go through each cluster in the second file
    #print('processing %s ...' % fileMap2['fileName'])
    batchStartTime = time.time()
    entityCnt = 0
    for side2clusterID in fileMap2['clusters']:

        #--progress display
        entityCnt += 1
        if entityCnt % 10000 == 0:
            now = datetime.now().strftime('%I:%M%p').lower()
            elapsedMins = round((time.time() - batchStartTime) / 60, 1)
            eps = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
            batchStartTime = time.time()
            print(' %s entities processed at %s, %s per second' % (entityCnt, now, eps))

        #--store the side2 cluster 
        statpack['ENTITY']['STANDARD_COUNT'] += 1
        side2recordIDs = fileMap2['clusters'][side2clusterID]
        side2recordCnt = len(side2recordIDs)
        if debugOn:
            print('-' * 50)
            print('prior cluster [%s] has %s records (%s)' % (side2clusterID, side2recordCnt, ','.join(sorted(side2recordIDs)[:10])))

        #--lookup those records in side1 and see how many clusters they created (ideally one)
        auditRows = []
        missingCnt = 0
        side1recordCnt = 0
        side1clusterIDs = {}
        for recordID in side2recordIDs:
            auditData = {}
            auditData['_side2clusterID_'] = side2clusterID
            auditData['_recordID_'] = recordID
            auditData['_side2score_'] = fileMap2['clusters'][side2clusterID][recordID]
            try: side1clusterID = fileMap1['records'][recordID]
            except:             
                missingCnt += 1
                auditData['_auditStatus_'] = 'missing'
                auditData['_side1clusterID_'] = 'unknown'
                auditData['_side1score_'] = ''
                if debugOn: 
                    print('newer run missing record [%s]' % recordID)
            else:
                side1recordCnt += 1
                auditData['_auditStatus_'] = 'same' #--default, may get updated later
                auditData['_side1clusterID_'] = fileMap1['records'][recordID]
                auditData['_side1score_'] = fileMap1['clusters'][auditData['_side1clusterID_']][recordID]

                if fileMap1['records'][recordID] in side1clusterIDs:
                    side1clusterIDs[fileMap1['records'][recordID]] += 1
                else:
                    side1clusterIDs[fileMap1['records'][recordID]] = 1
            auditRows.append(auditData)
        side1clusterCnt = len(side1clusterIDs) 
        statpack['MISSING_RECORD_COUNT'] += missingCnt

        if debugOn:
            print('newer run has those %s records in %s clusters [%s]' % (side1recordCnt, side1clusterCnt, ','.join(map(str, side1clusterIDs.keys()))))

        #--count as prior positive and see if any new negatives
        newNegativeCnt = 0
        if side2recordCnt > 1:
            statpack['CLUSTERS']['STANDARD_COUNT'] += 1
            statpack['PAIRS']['STANDARD_COUNT'] += ((side2recordCnt * (side2recordCnt - 1)) / 2)
            statpack['ACCURACY']['PRIOR_POSITIVE'] += side2recordCnt
            if len(side1clusterIDs) > 1: #--gonna be some new negatives here

                #--give credit for largest side1cluster 
                largestSide1clusterID = None
                for clusterID in side1clusterIDs:
                    if (not largestSide1clusterID) or side1clusterIDs[clusterID] > side1clusterIDs[largestSide1clusterID]:
                        largestSide1clusterID = clusterID
                statpack['PAIRS']['COMMON_COUNT'] += ((side1clusterIDs[largestSide1clusterID] * (side1clusterIDs[largestSide1clusterID] - 1)) / 2)

                #--mark the smaller clusters as new negatives
                for i in range(len(auditRows)):
                    if auditRows[i]['_side1clusterID_'] != largestSide1clusterID:
                        newNegativeCnt += 1
                        auditRows[i]['_auditStatus_'] = 'new negative'
            else:
                statpack['PAIRS']['COMMON_COUNT'] += ((side2recordCnt * (side2recordCnt - 1)) / 2)

        #--now check for new positives
        newPositiveCnt = 0
        for side1clusterID in side1clusterIDs:
            clusterNewPositiveCnt = 0
            for recordID in fileMap1['clusters'][side1clusterID]:
                if recordID not in side2recordIDs:
                    newPositiveCnt += 1
                    clusterNewPositiveCnt += 1
                    side1recordCnt += 1
                    auditData = {}
                    auditData['_recordID_'] = recordID
                    auditData['_side1clusterID_'] = side1clusterID
                    auditData['_side1score_'] = fileMap1['clusters'][auditData['_side1clusterID_']][recordID]

                    #--must lookup the side2 clusterID
                    try: side2clusterID2 = fileMap2['records'][recordID]
                    except:             
                        missingCnt += 1
                        auditData['_auditStatus_'] = 'missing'
                        auditData['_side2clusterID_'] = 'unknown'
                        if debugOn: 
                            print('side 2 missing record [%s]' % recordID)
                    else:
                        auditData['_auditStatus_'] = 'new positive'
                        auditData['_side2clusterID_'] = side2clusterID2
                        auditData['_side2score_'] = fileMap2['clusters'][auditData['_side2clusterID_']][recordID]
                    auditRows.append(auditData)

            if clusterNewPositiveCnt > 0:
                if debugOn:
                    print('newer cluster %s has %s more records!' % (side1clusterID, clusterNewPositiveCnt))

        #--if exactly same, note and goto top
        if side1clusterCnt == 1 and side1recordCnt == side2recordCnt: 
            if debugOn:
                print('RESULT IS SAME!')
            statpack['ENTITY']['COMMON_COUNT'] += 1
            if side1recordCnt > 1:
                statpack['CLUSTERS']['COMMON_COUNT'] += 1
            continue

        #--log it to the proper categories 
        auditCategory = ''
        if missingCnt:
            auditCategory += '+MISSING'
        if side1clusterCnt > 1:
            auditCategory += '+SPLIT'
        if side1recordCnt > side2recordCnt:
            auditCategory += '+MERGE'
        if not auditCategory:
            auditCategory = '+UNKNOWN'
        auditCategory = auditCategory[1:] if auditCategory else auditCategory

        #--only count if current side2 cluster is largest merged
        largerClusterID = None
        lowerClusterID = None
        if 'MERGE' in auditCategory:
            side2clusterCounts = {}
            for auditData in auditRows:
                if auditData['_side2clusterID_'] not in side2clusterCounts:
                    side2clusterCounts[auditData['_side2clusterID_']] = 1
                else:
                    side2clusterCounts[auditData['_side2clusterID_']] += 1

            for clusterID in side2clusterCounts:
                if side2clusterCounts[clusterID] > side2clusterCounts[side2clusterID]:
                    largerClusterID = clusterID
                    break
                elif side2clusterCounts[clusterID] == side2clusterCounts[side2clusterID] and clusterID < side2clusterID:
                    lowerClusterID = clusterID

            if debugOn:
                if largerClusterID:
                    print('largerClusterID found! %s' % largerClusterID)
                elif lowerClusterID:
                    print('lowerClusterID if equal size found! %s' % lowerClusterID)

        #--if the largest audit status is not same, wait for the largest to show up
        if largerClusterID or lowerClusterID:
            if debugOn:
                print('AUDIT RESULT BYPASSED!')
                pause()
            continue
        else:
            if debugOn:
                print('AUDIT RESULT WILL BE COUNTED!')

        #--compute the slice algorithm's cost
        if newNegativeCnt > 0:
            statpack['SLICE']['COST'] += splitCost(side1recordCnt, newNegativeCnt)

        if newPositiveCnt > 0:
            statpack['SLICE']['COST'] += splitCost(side1recordCnt, newPositiveCnt)

        #--initialize audit category
        if auditCategory not in statpack['AUDIT']:
            statpack['AUDIT'][auditCategory] = {}
            statpack['AUDIT'][auditCategory]['COUNT'] = 0
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'] = {}

        #--adjust the side1Score (match key for senzing)
        clarifyScores = True
        if clarifyScores:

            #--get the same entity details
            same_side1clusterID = 0
            same_side1matchKeys = [] #--could be more than one
            for i in range(len(auditRows)):
                if auditRows[i]['_auditStatus_'] == 'same':
                    same_side1clusterID = auditRows[i]['_side1clusterID_']
                    if auditRows[i]['_side1score_'] and auditRows[i]['_side1score_'] not in same_side1matchKeys:
                        same_side1matchKeys.append(auditRows[i]['_side1score_'])

            #--adjust the new positives/negatives
            for i in range(len(auditRows)):
                #--clear the scores on the records that are the same
                if auditRows[i]['_auditStatus_'] == 'same':
                    auditRows[i]['_side2score_'] = ''
                    auditRows[i]['_side1score_'] = ''
                #--see if split rows are related
                elif auditRows[i]['_auditStatus_'] == 'new negative':
                    ent1str = same_side1clusterID
                    ent2str = auditRows[i]['_side1clusterID_']
                    relKey = ent1str + '-' + ent2str if ent1str < ent2str else ent2str + '-' + ent1str
                    if relKey in fileMap1['relationships']:
                        auditRows[i]['_side1score_'] = 'related on: ' + fileMap1['relationships'][relKey]
                    #else:
                    #    auditRows[i]['_side1score_'] = 'no relation'
                elif auditRows[i]['_auditStatus_'] == 'new positive':
                    if not auditRows[i]['_side1score_']: #--maybe statisize this
                        if len(same_side1matchKeys) == 1:
                            auditRows[i]['_side1score_'] = same_side1matchKeys[0]
                        #else:
                        #    auditRows[i]['_side1score_'] = 'not_logged'

        #--write the record
        scoreCounts = {}
        statpack['AUDIT'][auditCategory]['COUNT'] += 1
        nextAuditID += 1
        sampleRows = []
        score1List = {} #--will be matchKey for senzing
        for auditData in auditRows:
            csvRow = []
            csvRow.append(nextAuditID)
            csvRow.append(auditCategory)
            csvRow.append(auditData['_auditStatus_'])
            recordIDsplit = auditData['_recordID_'].split('|DS=')
            auditData['_dataSource_'] = recordIDsplit[1]
            auditData['_recordID_'] = recordIDsplit[0]
            csvRow.append(auditData['_dataSource_'])
            csvRow.append(auditData['_recordID_'])
            csvRow.append(auditData['_side2clusterID_'])
            csvRow.append(auditData['_side2score_'] if '_side2score_' in auditData else '')
            csvRow.append(auditData['_side1clusterID_'])
            csvRow.append(auditData['_side1score_'] if '_side1score_' in auditData else '')
            if auditData['_auditStatus_'] == 'new negative':
                statpack['ACCURACY']['NEW_NEGATIVE'] += 1
            elif auditData['_auditStatus_'] == 'new positive':
                statpack['ACCURACY']['NEW_POSITIVE'] += 1
            if auditData['_auditStatus_'] in ('new negative', 'new positive') and auditData['_side1score_']:
                if auditData['_side1score_'] not in scoreCounts:
                    scoreCounts[auditData['_side1score_']] = 1
                else:
                    scoreCounts[auditData['_side1score_']] += 1
            if debugOn:
                print(auditData)
            sampleRows.append(dict(zip(csvHeaders,csvRow)))

            try: csvHandle.write(','.join(map(str, csvRow)) + '\n')
            except IOError as err:
                print(err)
                print('could not write to output file %s' % outputCsvFile)
                return 
            #print(','.join(map(str, csvRow)))

        #--assign the best score (most used)
        if True:
            if len(scoreCounts) == 0:
                bestScore = 'none'
            elif len(scoreCounts) == 1:
                bestScore = list(scoreCounts.keys())[0]
            else:
                bestScore = 'multiple'
        #--assign the best score (most used)
        else:
            bestScore = 'none'
            bestCount = 0
            for score in scoreCounts:
                if scoreCounts[score] > bestCount:
                    bestScore = score
                    bestCount = scoreCounts[score]

        #--initialize sub category
        if bestScore not in statpack['AUDIT'][auditCategory]['SUB_CATEGORY']:
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore] = {}
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['COUNT'] = 0
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE'] = []
        statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['COUNT'] += 1

        #--place in the sample list
        if len(statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE']) < 100:
            statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE'].append(sampleRows)
        else:
            randomSampleI = random.randint(1,99)
            if randomSampleI % 10 != 0:                   
                statpack['AUDIT'][auditCategory]['SUB_CATEGORY'][bestScore]['SAMPLE'][randomSampleI] = sampleRows

        if debugOn:
            pause()

    csvHandle.close()

    #--completion display
    now = datetime.now().strftime('%I:%M%p').lower()
    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    eps = int(float(sqlCommitSize) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
    batchStartTime = time.time()
    print(' %s entities processed at %s, %s per second, complete!' % (entityCnt, now, eps))

    #--compute the side 1 (result set) cluster and pair count
    print('computing statistics ...')

    #--get all cluster counts for both sides

    #--get cluster and pair counts for side1
    for side1clusterID in fileMap1['clusters']:
        statpack['ENTITY']['RESULT_COUNT'] += 1
        side1recordCnt = len(fileMap1['clusters'][side1clusterID])
        if side1recordCnt == 1:
            continue
        statpack['CLUSTERS']['RESULT_COUNT'] += 1
        statpack['PAIRS']['RESULT_COUNT'] += ((side1recordCnt * (side1recordCnt - 1)) / 2)

    #--entity precision and recall
    statpack['ENTITY']['PRECISION'] = 0
    statpack['ENTITY']['RECALL'] = 0
    statpack['ENTITY']['F1-SCORE'] = 0
    if statpack['ENTITY']['RESULT_COUNT'] and statpack['ENTITY']['STANDARD_COUNT']:
        statpack['ENTITY']['PRECISION'] = round((statpack['ENTITY']['COMMON_COUNT'] + .0) / (statpack['ENTITY']['RESULT_COUNT'] + .0), 5)
        statpack['ENTITY']['RECALL'] = round(statpack['ENTITY']['COMMON_COUNT'] / (statpack['ENTITY']['STANDARD_COUNT'] + .0), 5)
        if (statpack['ENTITY']['PRECISION'] + statpack['ENTITY']['RECALL']) != 0:
            statpack['ENTITY']['F1-SCORE'] = round(2 * ((statpack['ENTITY']['PRECISION'] * statpack['ENTITY']['RECALL']) / (statpack['ENTITY']['PRECISION'] + statpack['ENTITY']['RECALL'] + .0)), 5)

    #--cluster precision and recall
    statpack['CLUSTERS']['PRECISION'] = 0
    statpack['CLUSTERS']['RECALL'] = 0
    statpack['CLUSTERS']['F1-SCORE'] = 0
    if statpack['CLUSTERS']['RESULT_COUNT'] and statpack['CLUSTERS']['STANDARD_COUNT']:
        statpack['CLUSTERS']['PRECISION'] = round((statpack['CLUSTERS']['COMMON_COUNT'] + .0) / (statpack['CLUSTERS']['RESULT_COUNT'] + .0), 5)
        statpack['CLUSTERS']['RECALL'] = round(statpack['CLUSTERS']['COMMON_COUNT'] / (statpack['CLUSTERS']['STANDARD_COUNT'] + .0), 5)
        if (statpack['CLUSTERS']['PRECISION'] + statpack['CLUSTERS']['RECALL']) != 0:
            statpack['CLUSTERS']['F1-SCORE'] = round(2 * ((statpack['CLUSTERS']['PRECISION'] * statpack['CLUSTERS']['RECALL']) / (statpack['CLUSTERS']['PRECISION'] + statpack['CLUSTERS']['RECALL'] + .0)), 5)

    #--pairs precision and recall
    statpack['PAIRS']['PRECISION'] = 0
    statpack['PAIRS']['RECALL'] = 0
    statpack['PAIRS']['F1-SCORE'] = 0
    if statpack['PAIRS']['RESULT_COUNT'] and statpack['PAIRS']['STANDARD_COUNT']:
        statpack['PAIRS']['PRECISION'] = round(statpack['PAIRS']['COMMON_COUNT'] / (statpack['PAIRS']['RESULT_COUNT'] + .0), 5)
        statpack['PAIRS']['RECALL'] = round(statpack['PAIRS']['COMMON_COUNT'] / (statpack['PAIRS']['STANDARD_COUNT'] + .0), 5)
        if (statpack['PAIRS']['PRECISION'] + statpack['PAIRS']['RECALL']) != 0:
            statpack['PAIRS']['F1-SCORE'] = round(2 * ((statpack['PAIRS']['PRECISION'] * statpack['PAIRS']['RECALL']) / (statpack['PAIRS']['PRECISION'] + statpack['PAIRS']['RECALL'] + .0)), 5)

    #--accruacy precision and recall
    statpack['ACCURACY']['PRECISION'] = 0
    statpack['ACCURACY']['RECALL'] = 0
    statpack['ACCURACY']['F1-SCORE'] = 0
    if statpack['ACCURACY']['PRIOR_POSITIVE']:
        statpack['ACCURACY']['PRECISION'] = round(statpack['ACCURACY']['PRIOR_POSITIVE'] / (statpack['ACCURACY']['PRIOR_POSITIVE'] + statpack['ACCURACY']['NEW_POSITIVE'] + .0), 5)
        statpack['ACCURACY']['RECALL'] =    round(statpack['ACCURACY']['PRIOR_POSITIVE'] / (statpack['ACCURACY']['PRIOR_POSITIVE'] + statpack['ACCURACY']['NEW_NEGATIVE'] + .0), 5)
        if (statpack['ACCURACY']['PRECISION'] + statpack['ACCURACY']['RECALL']) != 0:
            statpack['ACCURACY']['F1-SCORE'] = round(2 * ((statpack['ACCURACY']['PRECISION'] * statpack['ACCURACY']['RECALL']) / (statpack['ACCURACY']['PRECISION'] + statpack['ACCURACY']['RECALL'] + .0)), 5)

    #--dump the stats to screen and file
    with open(outputJsonFile, 'w') as outfile:
        json.dump(statpack, outfile)    

    print ('')
    print ('%s prior positives ' % statpack['ACCURACY']['PRIOR_POSITIVE'])
    print ('%s new positives ' % statpack['ACCURACY']['NEW_POSITIVE'])
    print ('%s new negatives ' % statpack['ACCURACY']['NEW_NEGATIVE'])
    print ('%s precision ' % statpack['ACCURACY']['PRECISION'])
    print ('%s recall ' % statpack['ACCURACY']['RECALL'])
    print ('%s f1-score ' % statpack['ACCURACY']['F1-SCORE'])
    print ('')
    print ('%s prior entities ' % statpack['ENTITY']['STANDARD_COUNT'])
    print ('%s new entities ' % statpack['ENTITY']['RESULT_COUNT'])
    print ('%s common entities ' % statpack['ENTITY']['COMMON_COUNT'])
    print ('%s merged entities ' % (statpack['AUDIT']['MERGE']['COUNT'] if 'MERGE' in statpack['AUDIT'] else 0))
    print ('%s split entities ' % (statpack['AUDIT']['SPLIT']['COUNT'] if 'SPLIT' in statpack['AUDIT'] else 0))
    print ('%s split+merge entities ' % (statpack['AUDIT']['SPLIT+MERGE']['COUNT'] if 'SPLIT+MERGE' in statpack['AUDIT'] else 0))
    print ('')
    #print ('%s slice edit distance ' % statpack['SLICE']['COST'])
    #print('')
    if statpack['MISSING_RECORD_COUNT']:
        print ('%s ** missing clusters **' % statpack['MISSING_RECORD_COUNT'])
        print('')
    if shutDown:
        print('** process was aborted **')
    else:
        print('process completed successfully!')
    print('')
    return


# ===== The main function =====
if __name__ == '__main__':
    global shutDown
    shutDown = False
    signal.signal(signal.SIGINT, signal_handler)
    procStartTime = time.time()

    sqlCommitSize = 10000 #-this is really just for stat display

    #--capture the command line arguments
    argParser = argparse.ArgumentParser()
    argParser.add_argument('-n', '--newer_csv_file', dest='newerFile', default=None, help='the latest entity map file')
    argParser.add_argument('-p', '--prior_csv_file', dest='priorFile', default=None, help='the prior entity map file')
    argParser.add_argument('-o', '--output_file_root', dest='outputRoot', default=None, help='the ouputfile root name (both a .csv and a .json file will be created')
    argParser.add_argument('-D', '--debug', dest='debug', action='store_true', default=False, help='print debug statements')
    args = argParser.parse_args()
    newerFile = args.newerFile
    priorFile = args.priorFile
    outputRoot = args.outputRoot
    debugOn = args.debug

    #--validations
    if not newerFile:
        print('ERROR: A newer entity map file must be specified with -n')
        sys.exit(1)
    if not priorFile:
        print('ERROR: A prior entity map file must be specified with -p')
        sys.exit(1)
    if not outputRoot:
        print('ERROR: An output root must be specified with -o')
        sys.exit(1)
    if os.path.splitext(outputRoot)[1]:
        print("Please don't use a file extension as both a .json and a .csv file will be created")
        sys.exit(1)

    erCompare(newerFile, priorFile, outputRoot)
    
    sys.exit(0)
