#! /usr/bin/env python3

import argparse
import csv
import json
import os
import random
import signal
import sys
import time
import math
from datetime import datetime, timedelta
import configparser

#--concurrency
from multiprocessing import Process, Queue, Value
from queue import Empty, Full
import threading

#--senzing python classes
try:
    import G2Paths
    from senzing import G2ConfigMgr, G2Diagnostic, G2Engine, G2EngineFlags, G2Exception, G2IniParams, G2Product
    from G2Database import G2Database
except:
    print('')
    print('Please export PYTHONPATH=<path to senzing python directory>')
    print('')
    sys.exit(1)

#---------------------------------------
def queue_read(queue):
    try: return queue.get(True, 1)
    except Empty:
        time.sleep(.01)
        return None

#---------------------------------------
def queue_write(queue, message):
    while True:
        try: queue.put(message, True, 1)
        except Full:
            time.sleep(.01)
            continue
        break

#---------------------------------------
def setup_entity_queue_db(thread_id, threadStop, entity_queue, resume_queue):
    local_dbo = G2Database(g2dbUri)
    if not local_dbo.success:
        print('\nCould not connect to database\n')
        with shutDown.get_lock():
            shutDown.value = 1
        return
    process_entity_queue_db(thread_id, threadStop, entity_queue, resume_queue, local_dbo)
    local_dbo.close()

#---------------------------------------
def setup_entity_queue_api(thread_count, threadStop, entity_queue, resume_queue):
    #--multi-threaded API
    try:
        g2Engine = G2Engine()
        g2Engine.init('G2Snapshot', iniParams, False)
    except G2Exception as ex:
        printWithNewLines(f'G2Exception: {ex}', 'B')
        with shutDown.get_lock():
            shutDown.value = 1
        return
    thread_list = []
    for thread_id in range(thread_count):
        thread_list.append(threading.Thread(target=process_entity_queue_api, args=(thread_id, threadStop, entity_queue, resume_queue, g2Engine)))
    for thread in thread_list:
        thread.start()
    for thread in thread_list:
        thread.join()
    g2Engine.destroy()

#---------------------------------------
def setup_entity_queue_api_single(thread_id, threadStop, entity_queue, resume_queue):
    #--single-threaded API
    try:
        g2Engine = G2Engine()
        g2Engine.init('G2Snapshot%s' % thread_id, iniParams, False)
    except G2Exception as ex:
        printWithNewLines(f'G2Exception: {ex}', 'B')
        with shutDown.get_lock():
            shutDown.value = 1
        return
    process_entity_queue_api(thread_id, threadStop, entity_queue, resume_queue, g2Engine)
    g2Engine.destroy()

#---------------------------------------
def process_entity_queue_db(thread_id, threadStop, entity_queue, resume_queue, local_dbo):
    while threadStop.value == 0: #or entity_queue.empty() == False:
        queue_data = queue_read(entity_queue)
        if queue_data:
            #print('read entity_queue %s' % row)
            resume_rows = get_resume_db(local_dbo, queue_data)
            if resume_rows:
                queue_write(resume_queue, resume_rows)
    #print('process_entity_queue %s shut down with %s left in the queue' % (thread_id, entity_queue.qsize()))

#---------------------------------------
def process_entity_queue_api(thread_id, threadStop, entity_queue, resume_queue, g2_engine):
    while threadStop.value == 0: #entity_queue.empty() == False:
        queue_data = queue_read(entity_queue)
        if queue_data:
            #print('read entity_queue %s' % row)
            resume_rows = get_resume_api(g2_engine, queue_data)
            if resume_rows:
                queue_write(resume_queue, resume_rows)
    #print('process_entity_queue2 %s shut down with %s left in the queue' % (thread_id, entity_queue.qsize()))

#---------------------------------------
def setup_resume_queue(statPack, thread_id, threadStop, resume_queue):

    if exportCsv:
        columnHeaders = []
        columnHeaders.append('RESOLVED_ENTITY_ID')
        columnHeaders.append('RELATED_ENTITY_ID')
        columnHeaders.append('MATCH_LEVEL')
        columnHeaders.append('MATCH_KEY')
        columnHeaders.append('DATA_SOURCE')
        columnHeaders.append('RECORD_ID')
        try:
            exportFileHandle = open(csvFilePath, 'a')
            exportFileHandle.write(','.join(columnHeaders) + '\n')
        except IOError as err:
            print('\nERROR: cannot write to %s \n%s\n' % (csvFilePath, err))
            with shutDown.get_lock():
                shutDown.value = 1
            return
    else:
        exportFileHandle = None

    process_resume_queue(thread_id, threadStop, resume_queue, statPack, exportFileHandle)

    if exportCsv:
        exportFileHandle.close()


#---------------------------------------
def process_resume_queue(thread_id, threadStop, resume_queue, statPack, exportFileHandle):
    while threadStop.value == 0: #or resume_queue.empty() == False:
        queue_data = queue_read(resume_queue)
        if queue_data:
            #print('read resume_queue', row)

            #--its a resume to process
            if type(queue_data) == list:
                statPack = process_resume(statPack, queue_data, exportFileHandle)

            #--its a status write request
            else:
                statPack = write_stat_pack(statPack, queue_data)

    #print('process_resume_queue %s shut down with %s left in the queue' % (thread_id, resume_queue.qsize()))

#---------------------------------------
def get_resume_db(local_dbo, resolved_id):

    resume_rows = []

    sqlEntities = 'select ' + \
                  ' a.RES_ENT_ID as RESOLVED_ENTITY_ID, '\
                  ' a.ERRULE_ID, '\
                  ' a.MATCH_KEY, '\
                  ' b.DSRC_ID, '\
                  ' c.RECORD_ID '\
                  'from RES_ENT_OKEY a '\
                  'join OBS_ENT b on b.OBS_ENT_ID = a.OBS_ENT_ID '\
                  'join DSRC_RECORD c on c.ENT_SRC_KEY = b.ENT_SRC_KEY and c.DSRC_ID = b.DSRC_ID and c.ETYPE_ID = b.ETYPE_ID '\
                  'where a.RES_ENT_ID = ?'

    if not exportCsv: #--don't need related record_id
        sqlRelations = 'select '\
                       ' a.RES_ENT_ID as RESOLVED_ENTITY_ID, '\
                       ' a.REL_ENT_ID as RELATED_ENTITY_ID, '\
                       ' b.LAST_ERRULE_ID as ERRULE_ID, '\
                       ' b.IS_DISCLOSED, '\
                       ' b.IS_AMBIGUOUS, '\
                       ' b.MATCH_KEY, '\
                       ' d.DSRC_ID '\
                       'from RES_REL_EKEY a '\
                       'join RES_RELATE b on b.RES_REL_ID = a.RES_REL_ID '\
                       'join RES_ENT_OKEY c on c.RES_ENT_ID = a.REL_ENT_ID '\
                       'join OBS_ENT d on d.OBS_ENT_ID = c.OBS_ENT_ID '\
                       'where a.RES_ENT_ID = ?'
    else:
       sqlRelations = 'select '\
                       ' a.RES_ENT_ID as RESOLVED_ENTITY_ID, '\
                       ' a.REL_ENT_ID as RELATED_ENTITY_ID, '\
                       ' b.LAST_ERRULE_ID as ERRULE_ID, '\
                       ' b.IS_DISCLOSED, '\
                       ' b.IS_AMBIGUOUS, '\
                       ' b.MATCH_KEY, '\
                       ' d.DSRC_ID, '\
                       ' e.RECORD_ID '\
                       'from RES_REL_EKEY a '\
                       'join RES_RELATE b on b.RES_REL_ID = a.RES_REL_ID '\
                       'join RES_ENT_OKEY c on c.RES_ENT_ID = a.REL_ENT_ID '\
                       'join OBS_ENT d on d.OBS_ENT_ID = c.OBS_ENT_ID '\
                       'join DSRC_RECORD e on e.ENT_SRC_KEY = d.ENT_SRC_KEY and e.DSRC_ID = d.DSRC_ID and e.ETYPE_ID = d.ETYPE_ID '\
                       'where a.RES_ENT_ID = ?'

    queryStartTime = time.time()
    cursor1 = local_dbo.sqlExec(sqlEntities, [resolved_id,])
    for rowData in local_dbo.fetchAllDicts(cursor1):
        rowData = complete_resume_db(rowData)
        resume_rows.append(rowData)
    #print('   fetching entities took %s seconds' % str(round(time.time() - queryStartTime,2)))

    #--get relationships if not an orphaned entity_id
    if resume_rows and relationshipFilter in (2,3):
        queryStartTime = time.time()
        cursor1 = local_dbo.sqlExec(sqlRelations, [resolved_id,])
        for rowData in local_dbo.fetchAllDicts(cursor1):
            rowData = complete_resume_db(rowData)
            resume_rows.append(rowData)
        #print('   fetching relationships took %s seconds' % str(round(time.time() - queryStartTime,2)))

    return resume_rows

#---------------------------------------
def complete_resume_db(rowData):

    if 'RELATED_ENTITY_ID' not in rowData:
        rowData['RELATED_ENTITY_ID'] = 0
        rowData['IS_DISCLOSED'] = 0
        rowData['IS_AMBIGUOUS'] = 0
    if 'RECORD_ID' not in rowData:
        rowData['RECORD_ID'] = 'n/a'

    try: rowData['DATA_SOURCE'] = dsrcLookup[rowData['DSRC_ID']]['DSRC_CODE']
    except: rowData['DATA_SOURCE'] = 'unk'
    try: rowData['ERRULE_CODE'] = erruleLookup[rowData['ERRULE_ID']]['ERRULE_CODE']
    except: rowData['ERRULE_CODE'] = 'unk'

    if rowData['RELATED_ENTITY_ID'] == 0:
        rowData['MATCH_LEVEL'] = 1 if rowData['ERRULE_CODE'] else 0
        rowData['MATCH_CATEOGRY'] = 'RESOLVED'
    elif rowData['IS_DISCLOSED'] != 0:
        rowData['MATCH_LEVEL'] = 11
        rowData['MATCH_CATEGORY'] = 'DISCLOSED_RELATION'
    elif rowData['IS_AMBIGUOUS'] != 0:
        rowData['MATCH_LEVEL'] = 11
        rowData['MATCH_CATEGORY'] = 'AMBIGUOUS_MATCH'
    else:
        try: rowData['MATCH_LEVEL'] = erruleLookup[rowData['ERRULE_ID']]['RTYPE_ID']
        except: rowData['MATCH_LEVEL'] = 3
        if rowData['MATCH_LEVEL'] == 2:
            rowData['MATCH_CATEGORY'] = 'POSSIBLE_MATCH'
        else:
            rowData['MATCH_CATEGORY'] = 'POSSIBLY_RELATED'

    return rowData

#---------------------------------------
def get_resume_api(g2Engine, resolved_id):

    resume_rows = []

    getFlags = 0
    #getFlags = G2EngineFlags.G2_ENTITY_DEFAULT_FLAGS
    #getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_ENTITY_NAME
    getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_RECORD_DATA
    #getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_RECORD_MATCHING_INFO
    #getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_RECORD_FORMATTED_DATA
    if relationshipFilter in (2,3):
        getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_ALL_RELATIONS
        #getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_RELATED_ENTITY_NAME
        getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_RELATED_MATCHING_INFO
        getFlags = getFlags | G2EngineFlags.G2_ENTITY_INCLUDE_RELATED_RECORD_SUMMARY

    try:
        response = bytearray()
        retcode = g2Engine.getEntityByEntityID(int(resolved_id), response, getFlags)
        response = response.decode() if response else ''
    except G2Exception as err:
        print(str(err))
        with shutDown.get_lock():
            shutDown.value = 1
        return resume_rows

    if not response:
        return resume_rows

    try: jsonData = json.loads(response)
    except:
        print('json parse error:\n\t ', response)
        return resume_rows

    for record in jsonData['RESOLVED_ENTITY']['RECORDS']:
        resume_rows.append({
            'RESOLVED_ENTITY_ID': jsonData['RESOLVED_ENTITY']['ENTITY_ID'],
            'RELATED_ENTITY_ID': 0,
            'MATCH_LEVEL': 1,
            'IS_AMBIGUOUS': 0,
            'IS_DISCLOSED': 0,
            'MATCH_KEY': 'n/a',
            'DATA_SOURCE': record['DATA_SOURCE'],
            'RECORD_ID': record['RECORD_ID']
        })
    if 'RELATED_ENTITIES' in jsonData:
        for relation in jsonData['RELATED_ENTITIES']:
            for record in relation['RECORD_SUMMARY']:
                resume_rows.append({
                    'RESOLVED_ENTITY_ID': jsonData['RESOLVED_ENTITY']['ENTITY_ID'],
                    'RELATED_ENTITY_ID': relation['ENTITY_ID'],
                    'MATCH_LEVEL': relation['MATCH_LEVEL'],
                    'IS_AMBIGUOUS': relation['IS_AMBIGUOUS'],
                    'IS_DISCLOSED': relation['IS_DISCLOSED'],
                    'MATCH_KEY': relation['MATCH_KEY'],
                    'DATA_SOURCE': record['DATA_SOURCE'],
                    'RECORD_ID': 'n/a'
                })

    return resume_rows

#---------------------------------------
def process_resume(statPack, resume_rows, exportFileHandle):

    if not resume_rows:
        return statPack

    categoryTotalStat = {}
    categoryTotalStat['AMBIGUOUS_MATCH'] = 'TOTAL_AMBIGUOUS_MATCHES'
    categoryTotalStat['POSSIBLE_MATCH'] = 'TOTAL_POSSIBLE_MATCHES'
    categoryTotalStat['POSSIBLY_RELATED'] = 'TOTAL_POSSIBLY_RELATEDS'
    categoryTotalStat['DISCLOSED_RELATION'] = 'TOTAL_DISCLOSED_RELATIONS'

    calcStartTime = time.time()

    entitySize = 0
    recordList = []
    resumeData = {}

    #--summarize entity resume
    entityID = resume_rows[0]['RESOLVED_ENTITY_ID']
    for rowData in resume_rows:
        relatedID = str(rowData['RELATED_ENTITY_ID'])
        dataSource = rowData['DATA_SOURCE']
        recordID = rowData['RECORD_ID']

        if relatedID == '0':
            matchCategory = 'RESOLVED'
            entitySize += 1
            recordList.append(dataSource + ':' + recordID)
        elif rowData['IS_DISCLOSED'] != 0:
            matchCategory = 'DISCLOSED_RELATION'
        elif rowData['IS_AMBIGUOUS'] != 0:
            matchCategory = 'AMBIGUOUS_MATCH'
        elif rowData['MATCH_LEVEL'] == 2:
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

        if exportFileHandle:
             writeCsvRecord(rowData, exportFileHandle)

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
        statPack['ENTITY_SIZE_SAMPLE_COUNT'] += 1
    elif randomIndex != 0:
        statPack['ENTITY_SIZE_BREAKDOWN'][strEntitySize]['SAMPLE'][randomIndex] = entityID

    #--resolved entity stats
    statPack['TOTAL_ENTITY_COUNT'] += 1
    statPack['TOTAL_RECORD_COUNT'] += entitySize
    for dataSource1 in resumeData['0']['dataSources']:
        recordCount = resumeData['0']['dataSources'][dataSource1]

        #--this just updates entity and record count for the data source
        statPack = updateStatpack(statPack, dataSource1, None, None, 1, recordCount, None)

        if recordCount == 1:
            statPack = updateStatpack(statPack, dataSource1, None, 'SINGLE', 1, 0, entityID)
        else:
            statPack = updateStatpack(statPack, dataSource1, None, 'DUPLICATE', 1, recordCount, entityID)

        #--cross matches
        for dataSource2 in resumeData['0']['dataSources']:
            if dataSource2 == dataSource1:
                continue
            statPack = updateStatpack(statPack, dataSource1, dataSource2, 'MATCH', 1, recordCount, entityID)

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
                if entityID < int(relatedID) or dataSource2:
                    statPack = updateStatpack(statPack, dataSource1, dataSource2, matchCategory, 1, recordCount, str(entityID) + ' ' + relatedID)
    return statPack

#---------------------------------------
def randomSampleIndex():
    targetIndex = random.randint(1, sampleSize)
    if targetIndex % round(sampleSize/10) == 0:
        return 0
    return targetIndex

#---------------------------------------
def updateStatpack(statPack, dataSource1, dataSource2, statPrefix, entityCount, recordCount, sampleValue):
    randomIndex = randomSampleIndex()

    if datasourceFilter and dataSource1 != datasourceFilter:
        return statPack

    if dataSource1 not in statPack['DATA_SOURCES']:
        statPack = initDataSourceStats(statPack, dataSource1)
    if dataSource2 and dataSource2 not in statPack['DATA_SOURCES'][dataSource1]['CROSS_MATCHES']:
        statPack = initDataSourceStats(statPack, dataSource1, dataSource2)

    #--special case for entity/record count at the data source level with no sample value
    if not statPrefix:
        statPack['DATA_SOURCES'][dataSource1]['ENTITY_COUNT'] += entityCount
        statPack['DATA_SOURCES'][dataSource1]['RECORD_COUNT'] += recordCount
        return statPack

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

    return statPack

#---------------------------------------
def initDataSourceStats(statPack, dataSource1, dataSource2 = None):

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
    return statPack

#---------------------------------------
def writeCsvRecord(csvData, exportFileHandle):
    columnValues = []
    columnValues.append(str(csvData['RESOLVED_ENTITY_ID']))
    columnValues.append(str(csvData['RELATED_ENTITY_ID']))
    columnValues.append(str(csvData['MATCH_LEVEL']))
    columnValues.append('"' + (csvData['MATCH_KEY'][1:] if csvData['MATCH_KEY'] else '') + '"')
    columnValues.append('"' + csvData['DATA_SOURCE'] + '"')
    columnValues.append('"' + csvData['RECORD_ID'] + '"')
    try: exportFileHandle.write(','.join(columnValues) + '\n')
    except IOError as err:
        print('\nERROR: cannot write to %s \n%s\n' % (csvFilePath, err))
        with shutDown.get_lock():
            shutDown.value = 1

#---------------------------------------
def processEntities():

    newStatPack = True
    if os.path.exists(statsFilePath):
        statPack = json.load(open(statsFilePath))
        if 'PROCESS' in statPack:
            priorStatus = statPack['PROCESS']['STATUS']
            lastEntityID = statPack['PROCESS']['LAST_ENTITY_ID'] if type(statPack['PROCESS']['LAST_ENTITY_ID']) == int else 0
        else:
            priorStatus = 'Unknown'
            lastEntityID = 0
        print ('\n%s snapshot file exists with %s entities processed!' % (priorStatus, statPack['TOTAL_ENTITY_COUNT']))

        if priorStatus != 'Complete' and lastEntityID != 0:
            reply = input('\nDo you want to pick up where it left off (yes/no)? ')
            if reply in ['y','Y', 'yes', 'YES']:
                newStatPack = False

        if newStatPack:
            reply = input('\nAre you sure you want to overwrite it (yes/no)? ')
            if reply not in ['y','Y', 'yes', 'YES']:
                with shutDown.get_lock():
                    shutDown.value = 1
                return
        print()

    if newStatPack and os.path.exists(csvFilePath):
        print('\nThe %s file still exists.  Please either rename or remove it as well.\n' % csvFilePath)
        with shutDown.get_lock():
            shutDown.value = 1
        return

    if newStatPack:
        statPack = {}
        statPack['SOURCE'] = 'G2Snapshot'
        statPack['PROCESS'] = {}
        statPack['PROCESS']['STATUS'] = 'Incomplete'
        statPack['PROCESS']['START_TIME'] = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        statPack['PROCESS']['LAST_ENTITY_ID'] = 0
        statPack['TOTAL_RECORD_COUNT'] = 0
        statPack['TOTAL_ENTITY_COUNT'] = 0
        statPack['TOTAL_AMBIGUOUS_MATCHES'] = 0
        statPack['TOTAL_POSSIBLE_MATCHES'] = 0
        statPack['TOTAL_POSSIBLY_RELATEDS'] = 0
        statPack['TOTAL_DISCLOSED_RELATIONS'] = 0
        statPack['DATA_SOURCES'] = {}
        statPack['ENTITY_SIZE_BREAKDOWN'] = {}
        statPack['ENTITY_SIZE_SAMPLE_COUNT'] = 0

    g2Dbo = G2Database(g2dbUri)
    if not g2Dbo.success:
        print('\nCould not connect to database\n')
        return 1

    entity_queue = Queue(threadCount * 10)
    resume_queue = Queue(threadCount * 10)
    thread_list = []

    print('starting %s %s threads ...' % (threadCount, ('api' if use_api else 'database')))
    process_list = []

    if not use_api: #--sql only mode
        for thread_id in range(threadCount - 1):
            process_list.append(Process(target=setup_entity_queue_db, args=(thread_id, threadStop, entity_queue, resume_queue)))

    else: #--api
        unusedThreads = threadCount - 1
        while unusedThreads > 0:
            if unusedThreads > 16:
                thisThreadCount = 16
                unusedThreads -= 16
            else:
                thisThreadCount = unusedThreads
                unusedThreads = 0
            process_list.append(Process(target=setup_entity_queue_api, args=(thisThreadCount, threadStop, entity_queue, resume_queue)))

    process_list.append(Process(target=setup_resume_queue, args=(statPack, 99, threadStop, resume_queue)))
    for process in process_list:
        process.start()

    procStartTime = time.time()

    if not datasourceFilter:
        maxEntityId = g2Dbo.fetchRow(g2Dbo.sqlExec('select max(RES_ENT_ID) from RES_ENT'))[0]
        sql0 = 'select RES_ENT_ID from RES_ENT where RES_ENT_ID between ? and ?'
    else:
        print(f'\ndetermining entity_id range for {datasourceFilter} ... ', end='', flush=True)
        sql = 'select  ' \
              ' min(b.RES_ENT_ID), ' \
              ' max(b.RES_ENT_ID) ' \
              'from OBS_ENT a ' \
              'join RES_ENT_OKEY b on b.OBS_ENT_ID = a.OBS_ENT_ID ' \
              'where a.DSRC_ID = ' + str(datasourceFilterID)
        minEntityId, maxEntityId = g2Dbo.fetchRow(g2Dbo.sqlExec(sql))
        print(f'from {minEntityId} to {maxEntityId}\n')
        if newStatPack:
            statPack['PROCESS']['LAST_ENTITY_ID'] = minEntityId - 1

        sql0 = 'select distinct' \
               ' a.RES_ENT_ID ' \
               'from RES_ENT_OKEY a ' \
               'join OBS_ENT b on b.OBS_ENT_ID = a.OBS_ENT_ID ' \
               'where a.RES_ENT_ID between ? and ? and b.DSRC_ID = ' + str(datasourceFilterID)

    batchStartTime = time.time()
    entityCount = 0
    batchEntityCount = 0

    begEntityId = statPack['PROCESS']['LAST_ENTITY_ID'] + 1
    endEntityId = begEntityId + chunkSize

    while True:
        if not datasourceFilter:
            print('Getting entities from %s to %s ...' % (begEntityId, endEntityId))
        else:
            print('Getting entities from %s to %s with %s records ...' % (begEntityId, endEntityId, datasourceFilter))
        entity_rows = g2Dbo.fetchAllRows(g2Dbo.sqlExec(sql0, (begEntityId, endEntityId)))

        if entity_rows:
            last_row_entity_id = entity_rows[len(entity_rows)-1][0]
        for entity_row in entity_rows:
            queue_write(entity_queue, entity_row[0])
            #print('put queue1 %s' % row['RES_ENT_ID'])

            #--status display
            entityCount += 1
            batchEntityCount += 1
            if entityCount % progressInterval == 0 or entity_row[0] == last_row_entity_id:
                threadsRunning = 0
                for process in process_list:
                    if process.is_alive():
                        threadsRunning += 1
                qdepth = entity_queue.qsize() + resume_queue.qsize()
                now = datetime.now().strftime('%I:%M%p').lower()
                elapsedMins = round((time.time() - procStartTime) / 60, 1)
                eps = int(float(entityCount) / (float(time.time() - procStartTime if time.time() - procStartTime != 0 else 1)))
                eps2 = int(float(batchEntityCount) / (float(time.time() - batchStartTime if time.time() - batchStartTime != 0 else 1)))
                print('%s entities processed at %s after %s minutes, %s / %s per second, %s processes, %s entity queue, %s resume queue' % (entityCount, now, elapsedMins, eps2, eps, threadsRunning, entity_queue.qsize(), resume_queue.qsize()))
                batchStartTime = time.time()
                batchEntityCount = 0

            #--get out if errors hit or out of records
            if shutDown.value:
                if shutDown.value == 9:
                    print('USER INTERUPT! Shutting down ... ')
                break

        #--get out if errors hit
        if shutDown.value:
            break
        else: #--set next batch

            if endEntityId >= maxEntityId:
                break

            #--write interim snapshot file
            queuesEmpty = wait_for_queues(entity_queue, resume_queue)
            statData = {
                'writeStatus': 'Interim',
                'lastEntityId': endEntityId,
                'queuesEmpty': queuesEmpty,
                'statsFileName': statsFilePath
            }
            queue_write(resume_queue, statData)
            queuesEmpty = wait_for_queues(entity_queue, resume_queue)

            #--get next chunk
            begEntityId += chunkSize
            endEntityId += chunkSize

    #--write final snapshot file
    print('Finishing up ...')
    queuesEmpty = wait_for_queues(entity_queue, resume_queue)
    if not shutDown.value:
        statData = {
            'writeStatus': 'Final',
            'lastEntityId': maxEntityId if not datasourceFilter else datasourceFilter,
            'queuesEmpty': queuesEmpty,
            'statsFileName': statsFilePath
        }
        queue_write(resume_queue, statData)
        queuesEmpty = wait_for_queues(entity_queue, resume_queue)

    #--stop the threads
    print('stopping threads ...')
    with threadStop.get_lock():
        threadStop.value = 1
    start = time.time()
    while time.time() - start <= 15:
        if not any(process.is_alive() for process in process_list):
            break
        time.sleep(1)
    for process in process_list:
        if process.is_alive():
            print(process.name, 'did not terminate gracefully')
            process.terminate()
        process.join()
    entity_queue.close()
    resume_queue.close()

#---------------------------------------
def wait_for_queues(entity_queue, resume_queue):
    waits = 0
    while entity_queue.qsize() or resume_queue.qsize():
        time.sleep(1)
        waits += 1
        if waits >= 10:
            break
        elif entity_queue.qsize() or resume_queue.qsize():
            print(' waiting for %s entity_queue and %s resume_queue records' % (entity_queue.qsize(), resume_queue.qsize()))

    if (entity_queue.qsize() or resume_queue.qsize()):
        print(' warning: queues are not empty!')
        return False
    return True

#---------------------------------------
def write_stat_pack(statPack, statData):
    writeStatus = statData['writeStatus']
    lastEntityId = statData['lastEntityId']
    queuesEmpty = statData['queuesEmpty']
    statsFileName = statData['statsFileName']

    print(' %s stats written to %s' % (writeStatus, statsFileName))

    if writeStatus == 'Interim':
        statPack['PROCESS']['STATUS'] = 'Interim'
    elif shutDown.value == 0:
        statPack['PROCESS']['STATUS'] = 'Complete'
    elif shutDown.value == 9:
        statPack['PROCESS']['STATUS'] = 'Aborted by user'
    else:
        statPack['PROCESS']['STATUS'] = 'Error!'

    if lastEntityId != 0:
        statPack['PROCESS']['LAST_ENTITY_ID'] = lastEntityId
    statPack['PROCESS']['QUEUES'] = 'empty' if queuesEmpty else 'NOT EMPTY!'
    statPack['PROCESS']['END_TIME'] = datetime.now().strftime('%m/%d/%Y %H:%M:%S')

    diff = datetime.now() - datetime.strptime(statPack['PROCESS']['START_TIME'], '%m/%d/%Y %H:%M:%S')
    days, seconds = diff.days, diff.seconds
    hours = days * 24 + seconds // 3600
    minutes = (seconds % 3600) // 60
    seconds = seconds % 60
    statPack['PROCESS']['RUN_TIME'] = '%s:%s:%s' % (hours, minutes, seconds)
    #statPack['PROCESS']['ENTITIES_PER_SECOND'] = int(float(statPack['TOTAL_ENTITY_COUNT']) / float(diff.seconds))
    #--actual final stat
    #elapsedMins = diff = data2 - data1
    #eps = int(float(entityCount) / (float(time.time() - procStartTime if time.time() - procStartTime != 0 else 1)))

    #--calculate some percentages
    if statPack['TOTAL_RECORD_COUNT']:
        statPack['TOTAL_COMPRESSION'] = str(round(100.00-((float(statPack['TOTAL_ENTITY_COUNT']) / float(statPack['TOTAL_RECORD_COUNT'])) * 100.00), 2)) + '%'
    for dataSource in statPack['DATA_SOURCES']:
        statPack['DATA_SOURCES'][dataSource]['COMPRESSION'] = str(round(100.00-((float(statPack['DATA_SOURCES'][dataSource]['ENTITY_COUNT']) / float(statPack['DATA_SOURCES'][dataSource]['RECORD_COUNT'])) * 100.00), 2)) + '%'

    with open(statsFileName, 'w') as outfile:
        json.dump(statPack, outfile, indent=4)

    return statPack

#----------------------------------------
def signal_handler(signal, frame):
    with shutDown.get_lock():
        shutDown.value = 9
    return

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    try: response = input(question)
    except: response = None
    return response

#----------------------------------------
if __name__ == '__main__':
    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))

    shutDown = Value('i', 0)
    threadStop = Value('i', 0)
    signal.signal(signal.SIGINT, signal_handler)

    procStartTime = time.time()
    progressInterval = 10000

    #--defaults
    try: configFileName = G2Paths.get_G2Module_ini_path()
    except: configFileName = ''
    outputFileRoot = os.getenv('SENZING_OUTPUT_FILE_ROOT') if os.getenv('SENZING_OUTPUT_FILE_ROOT', None) else None
    sampleSize = int(os.getenv('SENZING_SAMPLE_SIZE')) if os.getenv('SENZING_SAMPLE_SIZE', None) and os.getenv('SENZING_SAMPLE_SIZE').isdigit() else 1000
    datasourceFilter = os.getenv('SENZING_DATASOURCE_FILTER', None)
    relationshipFilter = int(os.getenv('SENZING_RELATIONSHIP_FILTER')) if os.getenv('SENZING_RELATIONSHIP_FILTER', None) and os.getenv('SENZING_RELATIONSHIP_FILTER').isdigit() else 3
    chunkSize = int(os.getenv('SENZING_CHUNK_SIZE')) if os.getenv('SENZING_CHUNK_SIZE', None) and os.getenv('SENZING_CHUNK_SIZE').isdigit() else 1000000
    threadCount = int(os.getenv('SENZING_THREAD_COUNT')) if os.getenv('SENZING_THREAD_COUNT', None) and os.getenv('SENZING_THREAD_COUNT').isdigit() else 0

    #--capture the command line arguments
    argParser = argparse.ArgumentParser()
    argParser.add_argument('-c', '--config_file_name', default=configFileName, help='name of the senzing config file, defaults to %s' % configFileName)
    argParser.add_argument('-o', '--output_file_root', default=outputFileRoot, help='root name for files created such as "/project/snapshots/snapshot1"')
    argParser.add_argument('-s', '--sample_size', type=int, default=sampleSize, help='defaults to %s' % sampleSize)
    argParser.add_argument('-d', '--datasource_filter', help='data source code to analayze, defaults to all')
    argParser.add_argument('-f', '--relationship_filter', type=int, default=relationshipFilter, help='filter options 1=No Relationships, 2=Include possible matches, 3=Include possibly related and disclosed. Defaults to %s' % relationshipFilter)
    argParser.add_argument('-a', '--for_audit', action='store_true', default=False, help='export csv file for audit')
    argParser.add_argument('-D', '--debug', action='store_true', default=False, help='print debug statements')
    argParser.add_argument('-k', '--chunk_size', type=int, default=chunkSize, help='defaults to %s' % chunkSize)
    argParser.add_argument('-t', '--thread_count', type=int, default=threadCount, help='defaults to %s' % threadCount)
    argParser.add_argument('-u', '--use_api', action='store_true', default=False, help='use api instead of sql to get resume')

    args = argParser.parse_args()
    configFileName = args.config_file_name
    outputFileRoot = args.output_file_root
    sampleSize = args.sample_size
    datasourceFilter = args.datasource_filter
    relationshipFilter = args.relationship_filter
    exportCsv = args.for_audit
    debugOn = args.debug
    chunkSize = args.chunk_size
    threadCount = args.thread_count
    use_api = args.use_api

    #--get parameters from ini file
    if not os.path.exists(configFileName):
        print('')
        print('An ini file was not found, please supply with the -c parameter.')
        print('')
        sys.exit(1)
    iniParser = configparser.ConfigParser()
    iniParser.read(configFileName)
    try: g2dbUri = iniParser.get('SQL', 'CONNECTION')
    except:
        print('')
        print('CONNECTION parameter not found in [SQL] section of the ini file')
        print('')
        sys.exit(1)

    #--g2 engine stuff
    try:
        g2iniParams = G2IniParams()
        iniParams = g2iniParams.getJsonINIParams(configFileName)
        g2Product = G2Product()
        apiVersion = json.loads(g2Product.version())
        g2Product.destroy()
    except G2Exception as err:
        print('\n%s\n' % str(err))
        sys.exit(1)

    if not threadCount:
        try:
            g2Diag = G2Diagnostic()
            g2Diag.init('pyG2Diagnostic', iniParams, False)
            physical_cores = g2Diag.getPhysicalCores()
            logical_cores = g2Diag.getLogicalCores()
            calc_cores_factor = 4 #--if physical_cores != logical_cores else 1.2
            threadCount = math.ceil(logical_cores * calc_cores_factor)
            print(f'\nPhysical cores: {logical_cores}, logical cores: {logical_cores}, default threads: {threadCount}')
        except G2Exception as err:
            print('\n%s\n' % str(err))
            sys.exit(1)

    #--get needed config data
    try:
        g2ConfigMgr = G2ConfigMgr()
        g2ConfigMgr.init('pyG2ConfigMgr', iniParams, False)
        defaultConfigID = bytearray()
        g2ConfigMgr.getDefaultConfigID(defaultConfigID)
        defaultConfigDoc = bytearray()
        g2ConfigMgr.getConfig(defaultConfigID, defaultConfigDoc)
        cfgData = json.loads(defaultConfigDoc.decode())
        g2ConfigMgr.destroy()

        ftypeCodeLookup = {}
        for cfgRecord in cfgData['G2_CONFIG']['CFG_FTYPE']:
            ftypeCodeLookup[cfgRecord['FTYPE_CODE']] = cfgRecord

        dsrcLookup = {}
        dsrcLookupByCode = {}
        for cfgRecord in cfgData['G2_CONFIG']['CFG_DSRC']:
            dsrcLookup[cfgRecord['DSRC_ID']] = cfgRecord
            dsrcLookupByCode[cfgRecord['DSRC_CODE']] = cfgRecord

        erruleLookup = {}
        for cfgRecord in cfgData['G2_CONFIG']['CFG_ERRULE']:
            erruleLookup[cfgRecord['ERRULE_ID']] = cfgRecord

        ftypeLookup = {}
        ambiguousFtypeID = 0
        for cfgRecord in cfgData['G2_CONFIG']['CFG_FTYPE']:
            ftypeLookup[cfgRecord['FTYPE_ID']] = cfgRecord
            if cfgRecord['FTYPE_CODE'] == 'AMBIGUOUS_ENTITY':
                ambiguousFtypeID = cfgRecord['FTYPE_ID']

    except G2Exception as err:
        print('\n%s\n' % str(err))
        sys.exit(1)
    g2ConfigMgr.destroy()

    #--validate data source filter if supplied
    if datasourceFilter:
        if datasourceFilter.upper() not in dsrcLookupByCode:
            print(f'\nData source code {datasourceFilter} is not valid\n')
            sys.exit(1)
        else:
            datasourceFilterID = dsrcLookupByCode[datasourceFilter.upper()]['DSRC_ID']

    #--check the output file
    if not outputFileRoot:
        print('\nPlease use -o to select and output path and root file name such as /project/audit/snap1\n')
        sys.exit(1)
    if os.path.splitext(outputFileRoot)[1]:
        print("\nPlease don't use a file extension as both a .json and a .csv file will be created\n")
        sys.exit(1)

    statsFilePath = outputFileRoot + '.json'
    csvFilePath = outputFileRoot + '.csv'

    statsFileExisted = os.path.exists(statsFilePath)
    try:
        with open(statsFilePath, 'a') as f:
            pass
    except IOError as err:
        print(f'\nCannot write to {statsFilePath}.  \n{err}\n')
        sys.exit(1)
    if not statsFileExisted:
        os.remove(statsFilePath)

    #--process the entities
    processEntities()

    elapsedMins = round((time.time() - procStartTime) / 60, 1)
    if shutDown.value == 0:
        print('Process completed successfully in %s minutes' % elapsedMins)
    else:
        print('Process aborted after %s minutes!' % elapsedMins)
    print('')

    sys.exit(shutDown.value)
