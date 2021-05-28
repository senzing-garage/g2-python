#--python imports
import optparse
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
from multiprocessing import Value
from operator import itemgetter

#--project classes
from G2Database import G2Database
from G2ConfigTables import G2ConfigTables
from G2Project import G2Project
from G2Module import G2Module
import G2Exception

#---------------------------------------------------------------------
#-- g2 report
#---------------------------------------------------------------------

#---------------------------------------
def buildReport():
    procStartTime = time.time()
    print('Rebuilding report ...')

    # initialize G2
    try:
        g2_module = G2Module('pyG2Export', g2iniPath, False)
        g2_module.init()
    except G2Exception.G2ModuleException as ex:
        print('ERROR: could not start the G2 module at ' + g2iniPath)
        print(ex)
        shutDown()
        return

    #--initialize the g2module export
    try: exportHandle = g2_module.getExportHandle('CSV', 4) # 4=include all relationships
    except G2ModuleException as ex:
        print('ERROR: could not initialize export')
        print(ex)
        shutDown()
        return

    #--clear the reporting database
    try:
        odsDbo.truncateTable('DSRC_RECORD')
        odsDbo.truncateTable('RESOLVED_ENTITY')
        odsDbo.truncateTable('ENTITY_RESUME')
        odsDbo.truncateTable('DBREPORT_MATCHES')
        odsDbo.truncateTable('DBREPORT_CATEGORY')
        odsDbo.truncateTable('DBREPORT_SUMMARY')
    except:
        print('ERROR: could not truncate reporting tables')
        shutDown()
        return

    #--initialize report level counters
    dbreportCounters = {}

    #--first row is header for CSV
    exportColumnHeaders = g2_module.fetchExportRecord(exportHandle).split(',')

    #--start processing rows
    resolvedEntityCount = 0
    rowData = g2_module.fetchCsvExportRecord(exportHandle, exportColumnHeaders)
    while rowData:

        categoryCounters = {}
        resolvedID = rowData['RESOLVED_ENTITY_ID']
        lensID = rowData['LENS_ID']
        resumeRows = []
        while rowData and rowData['RESOLVED_ENTITY_ID'] == resolvedID and rowData['LENS_ID'] == lensID:    
            rowData['LENS_ID'] = int(rowData['LENS_ID'])
            rowData['RESOLVED_ENTITY_ID'] = int(rowData['RESOLVED_ENTITY_ID'])
            rowData['RELATED_ENTITY_ID'] = int(rowData['RELATED_ENTITY_ID'])
            rowData['MATCH_LEVEL'] = int(rowData['MATCH_LEVEL'])
            rowData['MATCH_SCORE'] = int(rowData['MATCH_SCORE']) if rowData['MATCH_SCORE'] else None
            #--update resolved entity counters
            if rowData['RELATED_ENTITY_ID'] == 0:
                if rowData['DATA_SOURCE'] not in categoryCounters:
                    categoryCounters[rowData['DATA_SOURCE']] = 1
                else:
                    categoryCounters[rowData['DATA_SOURCE']] += 1

            resumeRows.append(rowData)
            rowData = g2_module.fetchCsvExportRecord(exportHandle, exportColumnHeaders)
            
        #--update dbreport category counts
        for category in categoryCounters:
            if category in dbreportCounters:
                dbreportCounters[category]['ENTITY_COUNT'] += 1
                dbreportCounters[category]['RECORD_COUNT'] += categoryCounters[category]
            else:
                dbreportCounters[category] = {}
                dbreportCounters[category]['ENTITY_COUNT'] = 1
                dbreportCounters[category]['RECORD_COUNT'] = categoryCounters[category]
                dbreportCounters[category]['SINGLE_COUNT'] = 0
            if categoryCounters[category] == 1:
                dbreportCounters[category]['SINGLE_COUNT'] += 1

        #--process matches if more than one record for the entity
        if len(resumeRows) > 1:
            processEntityResume(resumeRows)

        #--status display
        resolvedEntityCount += 1
        if resolvedEntityCount % sqlCommitSize == 0 or not rowData:
            elapsedMins = round((time.time() - procStartTime) / 60, 1)
            if rowData:
                print(' %s resolved entities processed' % resolvedEntityCount)
            else:
                print(' %s resolved entities completed in %s minutes' % (resolvedEntityCount, elapsedMins))

        #--shut down if errors hit
        if shutDownStatus.value:
            break

    #--build report category summary
    if not shutDownStatus.value:
        dbreportSummaryList = []
        for category in dbreportCounters:
            dbreportSummaryRecord = []
            dbreportSummaryRecord.append(category)
            dbreportSummaryRecord.append(dbreportCounters[category]['ENTITY_COUNT'])
            dbreportSummaryRecord.append(dbreportCounters[category]['RECORD_COUNT'])
            dbreportSummaryRecord.append(dbreportCounters[category]['SINGLE_COUNT'])
            dbreportSummaryList.append(dbreportSummaryRecord)
        if dbreportSummaryList:
            dbReportSummaryInsert = 'insert into DBREPORT_CATEGORY (ENTITY_CATEGORY, ENTITY_COUNT, RECORD_COUNT, SINGLE_COUNT) values (?, ?, ?, ?)'
            try: odsDbo.execMany(dbReportSummaryInsert, dbreportSummaryList)
            except G2Exception.G2DBException as err:
                for x in dbreportSummaryList:
                    print(x)
                print('')
                print(err)
                print('ERROR: could not insert into DBREPORT_CATEGORY table')
                shutDown()

    #--build dbreport summary
    if not shutDownStatus.value:
        buildReportSummary()

    return

#---------------------------------------
def processEntityResume(resumeRows):
    ''' processing for an entity resume '''

    #--get sql statements and clear insert lists
    dsrcRecordInsert = getSqlStatment('dsrcRecordInsert',odsDbo.dbType) 
    resolvedEntityInsert = getSqlStatment('resolvedEntityInsert',odsDbo.dbType) 
    entityResumeInsert = getSqlStatment('entityResumeInsert',odsDbo.dbType)
    dbReportMatchInsert = getSqlStatment('dbReportMatchInsert',odsDbo.dbType)
    dsrcRecordList = []
    resolvedEntityList = []
    entityResumeList = []
    dbReportMatchList = []

    #--get dsrc_records to insert
    for rowData in resumeRows:
        try: jsonDict = json.loads(rowData['JSON_DATA'])
        except:
            pass
        else:
            mappingResponse = g2Project.mapJsonRecord(jsonDict)
            valuesByClass = mappingResponse[2]
            dsrcRecordData = []
            dsrcRecordData.append(rowData['DATA_SOURCE'])
            dsrcRecordData.append(rowData['RECORD_ID'])
            dsrcRecordData.append(rowData['ENTITY_TYPE'])
            dsrcRecordData.append(rowData['ENTITY_KEY']) 
            dsrcRecordData.append(rowData['ENTITY_KEY']) #--really obs_ent_hash 
            dsrcRecordData.append(valuesByClass['NAME'] if 'NAME' in valuesByClass else None)
            dsrcRecordData.append(valuesByClass['ATTRIBUTE'] if 'ATTRIBUTE' in valuesByClass else None)
            dsrcRecordData.append(valuesByClass['IDENTIFIER'] if 'IDENTIFIER' in valuesByClass else None)
            dsrcRecordData.append(valuesByClass['ADDRESS'] if 'ADDRESS' in valuesByClass else None)
            dsrcRecordData.append(valuesByClass['PHONE'] if 'PHONE' in valuesByClass else None)
            dsrcRecordData.append(valuesByClass['RELATIONSHIP'] if 'RELATIONSHIP' in valuesByClass else None)
            dsrcRecordData.append(valuesByClass['ENTITY'] if 'ENTITY' in valuesByClass else None)
            dsrcRecordData.append(valuesByClass['OTHER'] if 'OTHER' in valuesByClass else None)
            dsrcRecordData.append(rowData['JSON_DATA'])
            dsrcRecordList.append(dsrcRecordData)

    #--reorganize the rows for an entity
    resumeData = organizeResume(resumeRows)

    #--for each record in the entity
    for relatedIDstr in resumeData['RECORDS']:
        if relatedIDstr == '0': #--resolved entity
            processResolvedEntity(resumeData, relatedIDstr, resolvedEntityList, entityResumeList, dbReportMatchList)
        else:
            processRelatedEntity(resumeData, relatedIDstr, entityResumeList, dbReportMatchList)

    #--update the database
    try: odsDbo.execMany(dsrcRecordInsert, dsrcRecordList)
    except G2Exception.G2DBException as err:
        for x in dsrcRecordList:
            print(x)
        print('')
        print(err)
        print('ERROR: could not insert into DSRC_RECORD table')
        shutDown()
    
    try: odsDbo.execMany(resolvedEntityInsert, resolvedEntityList)
    except G2Exception.G2DBException as err:
        for x in resolvedEntityList:
            print(x)
        print('')
        print(err)
        print('ERROR: could not insert into RESOLVED_ENTITY table')
        shutDown()
    
    #--de-dupe entity resume insert list due to bug!!! (remove when bug fixed)
    entityResumeList = [list(i) for i in set(tuple(i) for i in entityResumeList)]
    #--de-dupe entity resume insert list due to bug!!! (remove when bug fixed)

    try: odsDbo.execMany(entityResumeInsert, entityResumeList)
    except G2Exception.G2DBException as err:
        for x in entityResumeList:
            print(x)
        print('')
        print(err)
        print('ERROR: could not insert into ENTITY_RESUME table')
        shutDown()

    if dbReportMatchList:
        try: odsDbo.execMany(dbReportMatchInsert, dbReportMatchList)
        except G2Exception.G2DBException as err:
            for x in dbReportMatchList:
                print(x)
            print('')
            print(err)
            print('ERROR: could not insert into DBREPORT_MATCHES table')
            shutDown()

    return 

#---------------------------------------
def organizeResume(resumeRows):
    ''' re-organizes the rows for an entity by category '''
    resumeData = {}
    resumeData['RESOLVED_ENTITY_ID'] = resumeRows[0]['RESOLVED_ENTITY_ID']
    resumeData['LENS_ID'] = resumeRows[0]['LENS_ID']
    resumeData['RECORDS'] = {}
    for rowData in resumeRows:

        #--compute audit score and reorganize match_key
        matchScore = 0
        if rowData['MATCH_KEY']:
            matchKey = rowData['MATCH_KEY']
            plusFeats = ''
            minusFeats = ''
            while len(matchKey) > 0:
                nextStart1 = matchKey.find('+',1)
                nextStart2 = matchKey.find('-',1)
                nextStart = min(nextStart1 if nextStart1 > 0 else 999, nextStart2 if nextStart2 >0 else 999)
                if nextStart != 999:
                    featCode = matchKey[0:nextStart]
                    matchKey = matchKey[nextStart:]
                else:
                    featCode = matchKey
                    matchKey = matchKey = ''
                if featCode[0] == '+':
                    plusFeats += featCode[1:] if len(plusFeats) == 0 else featCode
                else:            
                    minusFeats += featCode
                if featCode[0] == '+'and 'NAME' in featCode and 'NAME' in rowData['MATCH_KEY']:
                    matchScore += 10
                elif featCode in ('+SSN','+TAX_ID', '+NATIONAL_ID', '+TRUSTED_ID'):
                    matchScore += 4
                elif featCode in ('+DRLIC', '+PASSPORT', '+LOGIN_ID', '+ACCT_NUM', '+OTHER_ID'):
                    matchScore += 3
                elif featCode in ('+ADDRESS', '+PHONE', '+EMAIL_ADDR', '+WEBSITE_ADDR'):
                    matchScore += 2
                elif featCode in ('+DOB', '+GENDER'):
                    matchScore += 1
                elif featCode == ('+DISCLOSED'):
                    matchScore += 20
                #elif featCode in ('-DOB', '-GENDER', '-SSN', any exclusives wish I c)
                #    matchScore -= 1
            rowData['MATCH_KEY'] = plusFeats + minusFeats

        rowData['MATCH_SCORE'] = matchScore

        #--add related id structure if needed (will be 0 for resolved records)
        relatedID = rowData['RELATED_ENTITY_ID']
        relatedIDstr = str(relatedID)
        if relatedIDstr not in resumeData['RECORDS']:
            resumeData['RECORDS'][relatedIDstr] = {}
            resumeData['RECORDS'][relatedIDstr]['RELATED_ENTITY_ID'] = rowData['RELATED_ENTITY_ID']
            resumeData['RECORDS'][relatedIDstr]['MATCH_LEVEL'] = rowData['MATCH_LEVEL']
            if relatedID != 0: #--these values are only valid at this level for true related entities
                resumeData['RECORDS'][relatedIDstr]['MATCH_KEY'] = rowData['MATCH_KEY']
                resumeData['RECORDS'][relatedIDstr]['MATCH_SCORE'] = rowData['MATCH_SCORE']
                resumeData['RECORDS'][relatedIDstr]['REF_SCORE'] = rowData['REF_SCORE']
                resumeData['RECORDS'][relatedIDstr]['ERRULE_CODE'] = rowData['ERRULE_CODE']
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'] = {}

        #--set the entity category to be by data source
        rowData['ENTITY_CATEGORY'] = rowData['DATA_SOURCE']

        #--add related id category structure if needed
        entityCategory = rowData['ENTITY_CATEGORY']
        if entityCategory not in resumeData['RECORDS'][relatedIDstr]['CATEGORIES']:
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory] = {}
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['ENTITY_CATEGORY'] = entityCategory
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['DATA_SOURCE'] = rowData['DATA_SOURCE']
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['ENTITY_TYPE'] = rowData['ENTITY_TYPE']
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_IDS'] = []
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST'] = []

        #--append the data source record
        resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_IDS'].append(rowData['RECORD_ID'])
        resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST'].append(rowData)

    #--sort all record lists
    for relatedIDstr in resumeData['RECORDS']:
        for entityCategory in resumeData['RECORDS'][relatedIDstr]['CATEGORIES']:
            recordIdList = sorted(resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_IDS'])
            recordRowList = sorted(resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST'], key=itemgetter('MATCH_KEY', 'RECORD_ID'))
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_IDS'] = recordIdList
            resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST'] = recordRowList

    return resumeData

#---------------------------------------
def processResolvedEntity(resumeData, relatedIDstr, resolvedEntityList, entityResumeList, dbReportMatchList):

    #--processing for the resolved entity
    for entityCategory in resumeData['RECORDS'][relatedIDstr]['CATEGORIES']:

        #--choose minimum record ID and name as the best values to represent the resolved entity
        resolvedFirstRecordID = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST'][0]['RECORD_ID']
        resolvedName = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST'][0]['ENTITY_NAME']
        auditKey = resumeData['RESOLVED_ENTITY_ID']   #--resolvedFirstRecordID (needs more work)  
        #--get max match_score for the category
        auditScore = 20
        #--attempt to find best score for a duplicate that may have muliple (disabled for now as too confusing)
        #for resumeRow in resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST']:
        #    if resumeRow['MATCH_SCORE'] > maxMatchScore:
        #        auditScore = resumeRow['MATCH_SCORE']

        #--resolved entity inserts
        recordIdList = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_IDS']
        resolvedEntityData = []
        resolvedEntityData.append(resumeData['RESOLVED_ENTITY_ID'])
        resolvedEntityData.append(resumeData['LENS_ID'])
        resolvedEntityData.append(resolvedName if resolvedName else 'unknown') 
        resolvedEntityData.append(resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['DATA_SOURCE'])
        resolvedEntityData.append(resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['ENTITY_TYPE'])
        resolvedEntityData.append(len(recordIdList))
        resolvedEntityData.append(recordIdList[0] if recordIdList else 'none')
        resolvedEntityList.append(resolvedEntityData)

        #--process each record 
        recordSeq = 0
        recordRowList = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST']
        for resumeRow in recordRowList:
            recordSeq += 1
            entityResumeData = []
            entityResumeData.append(resumeRow['RESOLVED_ENTITY_ID'])
            entityResumeData.append(resumeRow['LENS_ID'])
            entityResumeData.append(resumeRow['RELATED_ENTITY_ID'])
            entityResumeData.append(resumeRow['DATA_SOURCE'])
            entityResumeData.append(resumeRow['RECORD_ID'])
            entityResumeData.append(resumeRow['ENTITY_TYPE'])
            entityResumeData.append(0 if not resumeRow['ERRULE_CODE'] else 1)
            entityResumeData.append(resumeRow['MATCH_KEY'])
            entityResumeData.append(resumeRow['REF_SCORE'])
            entityResumeData.append(resumeRow['ERRULE_CODE'])
            entityResumeList.append(entityResumeData)

            #--determine which reports to put it on
            dbReportMatchData = []
            dbReportMatchData.append('tbd')
            dbReportMatchData.append('tbd')
            dbReportMatchData.append('Duplicates')
            dbReportMatchData.append(auditKey) #--audit key
            dbReportMatchData.append(resolvedName if resolvedName else 'unknown') #--audit name
            dbReportMatchData.append(auditScore) #--audit score
            dbReportMatchData.append(recordSeq) 
            dbReportMatchData.append(0 if not resumeRow['ERRULE_CODE'] else 1) #--match_type
            dbReportMatchData.append('Base record' if not resumeRow['ERRULE_CODE'] else 'Duplicate') #--match_level
            dbReportMatchData.append(resumeRow['MATCH_KEY'])
            dbReportMatchData.append(resumeRow['REF_SCORE'])
            dbReportMatchData.append(resumeRow['ERRULE_CODE'])
            dbReportMatchData.append(resumeRow['RESOLVED_ENTITY_ID'])
            dbReportMatchData.append(resumeRow['LENS_ID'])
            dbReportMatchData.append(resumeRow['RELATED_ENTITY_ID'])
            dbReportMatchData.append(resumeRow['DATA_SOURCE'])
            dbReportMatchData.append(resumeRow['RECORD_ID'])
            dbReportMatchData.append(resumeRow['ENTITY_TYPE'])

            #--put it on the duplicate report if more than one entity in the same category
            if len(resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_IDS']) > 1:
                entityCategory1 = resumeRow['ENTITY_CATEGORY']
                entityCategory2 = resumeRow['ENTITY_CATEGORY']
                dbReportMatchDataCopy = list(dbReportMatchData) 
                dbReportMatchDataCopy[0] = entityCategory1
                dbReportMatchDataCopy[1] = entityCategory2
                dbReportMatchDataCopy[6] = 1000 + dbReportMatchDataCopy[6]
                dbReportMatchList.append(dbReportMatchDataCopy)
            
            #--put it on both sides of the cross category duplicate report if there are records in other resumeData['RECORDS'][relatedIDstr]['CATEGORIES']
            for crossCategory in resumeData['RECORDS'][relatedIDstr]['CATEGORIES']:
                crossFirstRecordID = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][crossCategory]['RECORD_LIST'][0]['RECORD_ID']
                crossName = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][crossCategory]['RECORD_LIST'][0]['ENTITY_NAME']
                crossAuditKey = resumeData['RESOLVED_ENTITY_ID']   
                #--crossAuditKey = resolvedFirstRecordID (needs more work)  

                if resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][crossCategory]['ENTITY_CATEGORY'] != resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['ENTITY_CATEGORY']:
                    #--add side 1
                    dbReportMatchDataCopy = list(dbReportMatchData) 
                    dbReportMatchDataCopy[0] = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['ENTITY_CATEGORY']
                    dbReportMatchDataCopy[1] = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][crossCategory]['ENTITY_CATEGORY']
                    dbReportMatchDataCopy[6] = 2000 + dbReportMatchDataCopy[6]
                    dbReportMatchList.append(dbReportMatchDataCopy)
                    #--add side 2
                    dbReportMatchDataCopy = list(dbReportMatchData) 
                    dbReportMatchDataCopy[1] = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['ENTITY_CATEGORY']
                    dbReportMatchDataCopy[0] = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][crossCategory]['ENTITY_CATEGORY']
                    dbReportMatchDataCopy[3] = crossAuditKey
                    dbReportMatchDataCopy[4] = crossName if crossName else 'unknown'
                    dbReportMatchDataCopy[6] = 3000 + dbReportMatchDataCopy[6]
                    dbReportMatchList.append(dbReportMatchDataCopy)

    return

#---------------------------------------
def processRelatedEntity(resumeData, relatedIDstr, entityResumeList, dbReportMatchList):
    ''' add related entities to the resume and report '''

    #--add the related entity to this entity's resume
    for entityCategory in resumeData['RECORDS'][relatedIDstr]['CATEGORIES']:
        recordRowList = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][entityCategory]['RECORD_LIST']
        for resumeRow in recordRowList:
            entityResumeData = []
            entityResumeData.append(resumeRow['RESOLVED_ENTITY_ID'])
            entityResumeData.append(resumeRow['LENS_ID'])
            entityResumeData.append(resumeRow['RELATED_ENTITY_ID'])
            entityResumeData.append(resumeRow['DATA_SOURCE'])
            entityResumeData.append(resumeRow['RECORD_ID'])
            entityResumeData.append(resumeRow['ENTITY_TYPE'])
            entityResumeData.append(resumeRow['MATCH_LEVEL'])
            entityResumeData.append(resumeRow['MATCH_KEY'])
            entityResumeData.append(resumeRow['REF_SCORE'])
            entityResumeData.append(resumeRow['ERRULE_CODE'])
            entityResumeList.append(entityResumeData)

    resolvedIDstr = '0'
    relatedID = resumeData['RECORDS'][relatedIDstr]['RELATED_ENTITY_ID']

    #--these values overide the resume row values
    matchLevel = resumeData['RECORDS'][relatedIDstr]['MATCH_LEVEL']
    matchKey = resumeData['RECORDS'][relatedIDstr]['MATCH_KEY']
    erruleCode = resumeData['RECORDS'][relatedIDstr]['ERRULE_CODE']
    refScore = resumeData['RECORDS'][relatedIDstr]['REF_SCORE']
    matchScore = resumeData['RECORDS'][relatedIDstr]['MATCH_SCORE']

    #--convert match level to string
    if matchLevel <= 2:
        reportMatchLevel = 'Possible matches'
        relatedMatchType = 2
        relatedMatchLevel = 'Possible match'
    elif matchLevel >= 3:
        reportMatchLevel = 'Relationships'
        relatedMatchType = 3
        relatedMatchLevel = 'Relationship'
    #elif matchLevel == 4:
    #    reportMatchLevel = 'Disclosed relationships'
    #    relatedMatchType = 4
    #    relatedMatchLevel = 'Disclosed relationship'
    #else:
    #    reportMatchLevel = str(matchLevel) + 'Unknown'
    #    relatedMatchType = 4
    #    relatedMatchLevel = str(matchLevel) + 'Unknown'

    #--compare each resolved entity category to the related entity categories 
    for resolvedCategory in resumeData['RECORDS'][resolvedIDstr]['CATEGORIES']:

        #--choose minimum record ID and name as the best values to represent the resolved entity
        resolvedFirstRecordID = resumeData['RECORDS'][resolvedIDstr]['CATEGORIES'][resolvedCategory]['RECORD_LIST'][0]['RECORD_ID']
        resolvedName = resumeData['RECORDS'][resolvedIDstr]['CATEGORIES'][resolvedCategory]['RECORD_LIST'][0]['ENTITY_NAME']

        for relatedCategory in resumeData['RECORDS'][relatedIDstr]['CATEGORIES']:

            #--choose minimum record ID and name as the best values to represent the resolved entity
            relatedFirstRecordID = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][relatedCategory]['RECORD_LIST'][0]['RECORD_ID']

            #--only show relationships from same category from lowest record_id's point of view
            okToContinue = True
            if resolvedCategory == relatedCategory:
               okToContinue = resolvedFirstRecordID < relatedFirstRecordID
            if okToContinue:    

                #--assign the audit key
                auditKey = str(resumeData['RESOLVED_ENTITY_ID']) + '-' + str(resumeData['RECORDS'][relatedIDstr]['RELATED_ENTITY_ID'])
                #auditKey = resolvedFirstRecordID + '-' + relatedFirstRecordID (works but must fix resolved side first)

                #--add all the resolved entity records as the "base" entity
                recordSeq = 0
                recordRowList = resumeData['RECORDS'][resolvedIDstr]['CATEGORIES'][resolvedCategory]['RECORD_LIST']
                for resumeRow in recordRowList:
                    recordSeq += 1
                    dbReportMatchData = []
                    dbReportMatchData.append(resolvedCategory)
                    dbReportMatchData.append(relatedCategory)
                    dbReportMatchData.append(reportMatchLevel)
                    dbReportMatchData.append(auditKey) #--audit key
                    dbReportMatchData.append(resolvedName if resolvedName else 'unknown') #--audit name
                    dbReportMatchData.append(matchScore) #--audit score
                    dbReportMatchData.append(1000 + recordSeq) 
                    dbReportMatchData.append(0) #--match_type
                    dbReportMatchData.append('Base record') #--match_level
                    dbReportMatchData.append(None) #--match_key
                    dbReportMatchData.append(0) #--ref_score
                    dbReportMatchData.append(None) #--errule_code
                    dbReportMatchData.append(resumeRow['RESOLVED_ENTITY_ID'])
                    dbReportMatchData.append(resumeRow['LENS_ID'])
                    dbReportMatchData.append(resumeRow['RELATED_ENTITY_ID'])
                    dbReportMatchData.append(resumeRow['DATA_SOURCE'])
                    dbReportMatchData.append(resumeRow['RECORD_ID'])
                    dbReportMatchData.append(resumeRow['ENTITY_TYPE'])
                    dbReportMatchList.append(dbReportMatchData)

                #--add all the related entity records as the "matched" entity
                recordSeq = 0
                recordRowList = resumeData['RECORDS'][relatedIDstr]['CATEGORIES'][relatedCategory]['RECORD_LIST']
                for resumeRow in recordRowList:
                    recordSeq += 1
                    dbReportMatchData = []
                    dbReportMatchData.append(resolvedCategory)
                    dbReportMatchData.append(relatedCategory)
                    dbReportMatchData.append(reportMatchLevel)
                    dbReportMatchData.append(auditKey) #--audit key
                    dbReportMatchData.append(resolvedName if resolvedName else 'unknown') #--audit name
                    dbReportMatchData.append(matchScore) #--audit score
                    dbReportMatchData.append(2000 + recordSeq) 
                    dbReportMatchData.append(relatedMatchType) #--match_type
                    dbReportMatchData.append(relatedMatchLevel) #--match_level
                    dbReportMatchData.append(matchKey) #--match_key
                    dbReportMatchData.append(refScore) #--ref_score
                    dbReportMatchData.append(erruleCode) #--errule_code
                    dbReportMatchData.append(resumeRow['RESOLVED_ENTITY_ID'])
                    dbReportMatchData.append(resumeRow['LENS_ID'])
                    dbReportMatchData.append(resumeRow['RELATED_ENTITY_ID'])
                    dbReportMatchData.append(resumeRow['DATA_SOURCE'])
                    dbReportMatchData.append(resumeRow['RECORD_ID'])
                    dbReportMatchData.append(resumeRow['ENTITY_TYPE'])
                    dbReportMatchList.append(dbReportMatchData)

    return

#---------------------------------------
def buildReportSummary():

    #--truncate the ods table being populated
    if not odsDbo.truncateTable('DBREPORT_SUMMARY'):
        print('ERROR: could not truncate DBREPORT_SUMMARY table')
        shutDown()
        return

    selectStmt = 'select '
    selectStmt += ' a.ENTITY_CATEGORY1, '
    selectStmt += ' a.ENTITY_CATEGORY2, '
    selectStmt += " sum(case when a.REPORT_CATEGORY = 'Duplicates' then AUDIT_COUNT else 0 end) as DUPLICATE_COUNT, "
    selectStmt += " sum(case when a.REPORT_CATEGORY = 'Possible matches' then AUDIT_COUNT else 0 end) as POSSIBLE_MATCHES, "
    selectStmt += " sum(case when a.REPORT_CATEGORY = 'Relationships' then AUDIT_COUNT else 0 end) as POSSIBLE_RELATIONSHIPS, "
    selectStmt += " sum(case when a.REPORT_CATEGORY = 'Disclosed relationships' then AUDIT_COUNT else 0 end) as DISCLOSED_RELATIONSHIPS "
    selectStmt += 'from ( '
    selectStmt += ' select '
    selectStmt += '  ENTITY_CATEGORY1, '
    selectStmt += '  ENTITY_CATEGORY2, '
    selectStmt += '  REPORT_CATEGORY, '
    selectStmt += '  count(distinct AUDIT_KEY) as audit_count '
    selectStmt += ' from DBREPORT_MATCHES '
    selectStmt += ' group by '
    selectStmt += '  ENTITY_CATEGORY1, '
    selectStmt += '  ENTITY_CATEGORY2, '
    selectStmt += '  REPORT_CATEGORY '
    selectStmt += ' ) a '
    selectStmt += 'group by '
    selectStmt += ' a.ENTITY_CATEGORY1, '
    selectStmt += ' a.ENTITY_CATEGORY2 '
    selectStmt += 'order by '
    selectStmt += ' a.ENTITY_CATEGORY1, '
    selectStmt += ' a.ENTITY_CATEGORY2 '

    insertStmt = 'insert into DBREPORT_SUMMARY ('
    insertStmt += 'ENTITY_CATEGORY1, '
    insertStmt += 'ENTITY_CATEGORY2, '
    insertStmt += 'DUPLICATE_COUNT, '
    insertStmt += 'POSSIBLE_MATCHES, '
    insertStmt += 'POSSIBLE_RELATIONSHIPS, '
    insertStmt += 'DISCLOSED_RELATIONSHIPS) '
    insertStmt += 'values (?, ?, ?, ?, ?, ?)'

    print('')
    print('Calculating report summary statistics ...')
    rowCursor = odsDbo.sqlExec(selectStmt)
    rowList = odsDbo.fetchAllRows(rowCursor)
    if len(rowList) == 0:
        return

    sqlResponse = odsDbo.execMany(insertStmt, rowList)
    if sqlResponse:
        print(' %s report summary records written, complete!' % len(rowList))
    else:
        print('ERROR: could not insert into DBREPORT_SUMMARY table')
        shutDown()
        return

    return

#---------------------------------------
def exportToCsv():

    print('')    
    print('Exporting to %s ...' % outputFilePath)

    headerList1 = []
    headerList1.append('ENTITY_CATEGORY1')
    headerList1.append('ENTITY_COUNT1')
    headerList1.append('SINGLE_COUNT1')
    headerList1.append('RECORD_COUNT1')
    headerList1.append('ENTITY_CATEGORY2')
    headerList1.append('ENTITY_COUNT2')
    headerList1.append('RECORD_COUNT2')
    headerList1.append('DUPLICATE_COUNT')
    headerList1.append('POSSIBLE_MATCHES')
    headerList1.append('POSSIBLE_RELATIONSHIPS')
    headerList1.append('DISCLOSED_RELATIONSHIPS')

    headerList2 = []
    headerList2.append('ENTITY_CATEGORY1') 
    headerList2.append('ENTITY_CATEGORY2')
    headerList2.append('REPORT_CATEGORY')
    headerList2.append('AUDIT_KEY')
    headerList2.append('AUDIT_NAME')
    headerList2.append('AUDIT_SCORE')
    headerList2.append('RECORD_SEQ')
    headerList2.append('MATCH_TYPE')
    headerList2.append('MATCH_LEVEL')
    headerList2.append('MATCH_KEY')
    headerList2.append('ERRULE_CODE')
    headerList2.append('REF_SCORE')
    headerList2.append('RESOLVED_ENTITY_ID')
    headerList2.append('RELATED_ENTITY_ID')
    headerList2.append('DATA_SOURCE')
    headerList2.append('RECORD_ID')
    headerList2.append('ENTITY_TYPE')
    headerList2.append('NAME_DATA')
    headerList2.append('ATTRIBUTE_DATA')
    headerList2.append('IDENTIFIER_DATA')
    headerList2.append('ADDRESS_DATA')
    headerList2.append('PHONE_DATA')
    headerList2.append('RELATIONSHIP_DATA')
    headerList2.append('ENTITY_DATA')
    headerList2.append('OTHER_DATA')

    #--open the combined output file for writing
    try: 
        outputFileHandle = open(outputFilePath, "w")
        outputFileWriter = csv.writer(outputFileHandle, dialect=csv.excel, quoting=csv.QUOTE_ALL)
    except csv.Error as err:
        print(err)
        print('ERROR: Could not open %s for writing' % outputFilePath)
        shutDown()
        return
    
    #--write the summary records
    try: outputFileWriter.writerow(headerList1)
    except csv.Error as err:
        print(err)
        print('ERROR: could not write to %s' % (outputFilePath))
        shutDown()
    else:

        cursor = odsDbo.sqlExec('select * from DBREPORT_SUMMARY_VIEW order by ENTITY_CATEGORY1, ENTITY_CATEGORY2')
        row = odsDbo.fetchRow(cursor)
        while row:
            try: outputFileWriter.writerow(row)
            except csv.Error as err:
                print(err)
                print('ERROR: could not write to %s' % (outputFilePath))
                shutDown()
                break
            else:
                row = odsDbo.fetchRow(cursor)

    #--write separator to the data file only
    if not shutDownStatus.value:
        try: outputFileHandle.write('-' * 50 + '\n')
        except:
            print('ERROR: could not write to %s' % (outputFilePath))
            shutDown()
    
    #--write the detail records
    if not shutDownStatus.value:
        try: outputFileWriter.writerow(headerList2)
        except csv.Error as err:
            print(err)
            print('ERROR: could not write to %s' % (outputFilePath))
            shutDown()

    if not shutDownStatus.value:
        categoryCounter = {}
        cursor = odsDbo.sqlExec('select * from DBREPORT_DETAIL_VIEW order by ENTITY_CATEGORY1, ENTITY_CATEGORY2, REPORT_CATEGORY, AUDIT_NAME, AUDIT_KEY, RECORD_SEQ')
        row = odsDbo.fetchRow(cursor)
        while row:

            #--export limit calculation
            categoryKey = row[0] + row[1] + row[2]
            lastAuditKey = row[3]
            lastErruleCode = 'eof'
            recordList = []
            while row and row[3] == lastAuditKey:
                if row[10] != '':
                    lastErruleCode = str(row[10])+str(row[9])
                recordList.append(row)
                row = odsDbo.fetchRow(cursor)

            categoryKey += lastErruleCode
            if categoryKey in categoryCounter:
                categoryCounter[categoryKey] +=1
            else:
                categoryCounter[categoryKey] = 1             

            #--only export if within limit
            if categoryCounter[categoryKey] <= reportCategoryLimit or reportCategoryLimit == 0:
                for record in recordList:
                    try: outputFileWriter.writerow(record)
                    except csv.Error as err:
                        print(err)
                        print('ERROR: could not write to %s' % (outputFilePath))
                        shutDown()

            #--shut down if errors hit 
            if shutDownStatus.value:
                break

    #--close the output file
    outputFileHandle.close()

    return

#---------------------------------------
def getSqlStatment(statementName, dbType):

    if statementName == 'dsrcRecordInsert':
        if dbType == 'MYSQL':
            sql = 'insert into DSRC_RECORD ('
            sql += ' DATA_SOURCE, '
            sql += ' RECORD_ID, '
            sql += ' ENTITY_TYPE,'
            sql += ' ENTITY_KEY, '
            sql += ' OBS_ENT_HASH, '
            sql += ' NAME_DATA,'
            sql += ' ATTRIBUTE_DATA,'
            sql += ' IDENTIFIER_DATA,'
            sql += ' ADDRESS_DATA,'
            sql += ' PHONE_DATA,'
            sql += ' RELATIONSHIP_DATA,'
            sql += ' ENTITY_DATA,'
            sql += ' OTHER_DATA,'
            sql += ' JSON_DATA)'
            sql += ' values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            sql += ' on duplicate key update '
            sql += ' DATA_SOURCE=VALUES(DATA_SOURCE),'
            sql += ' RECORD_ID=VALUES(RECORD_ID),'
            sql += ' ENTITY_TYPE=VALUES(ENTITY_TYPE),'
            sql += ' ENTITY_KEY=VALUES(ENTITY_KEY),'
            sql += ' OBS_ENT_HASH=VALUES(OBS_ENT_HASH),'
            sql += ' NAME_DATA=VALUES(NAME_DATA),'
            sql += ' ATTRIBUTE_DATA=VALUES(ATTRIBUTE_DATA),'
            sql += ' IDENTIFIER_DATA=VALUES(IDENTIFIER_DATA),'
            sql += ' ADDRESS_DATA=VALUES(ADDRESS_DATA),'
            sql += ' PHONE_DATA=VALUES(PHONE_DATA),'
            sql += ' RELATIONSHIP_DATA=VALUES(RELATIONSHIP_DATA),'
            sql += ' ENTITY_DATA=VALUES(ENTITY_DATA),'
            sql += ' OTHER_DATA=VALUES(OTHER_DATA),'
            sql += ' JSON_DATA=VALUES(JSON_DATA)'
        elif dbType == 'DB2':
            sql = 'merge into DSRC_RECORD AS T'
            sql += ' using (VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) as DAT(DATA_SOURCE,RECORD_ID,ENTITY_TYPE,ENTITY_KEY,OBS_ENT_HASH,NAME_DATA,ATTRIBUTE_DATA,IDENTIFIER_DATA,ADDRESS_DATA,PHONE_DATA,RELATIONSHIP_DATA,ENTITY_DATA,OTHER_DATA,JSON_DATA)'
            sql += ' on T.DATA_SOURCE = DAT.DATA_SOURCE and T.RECORD_ID = DAT.RECORD_ID'
            sql += ' when matched then update '
            sql += ' set T.DATA_SOURCE = DAT.DATA_SOURCE, '
            sql += ' T.RECORD_ID = DAT.RECORD_ID, '
            sql += ' T.ENTITY_TYPE = DAT.ENTITY_TYPE,'
            sql += ' T.ENTITY_KEY = DAT.ENTITY_KEY, '
            sql += ' T.OBS_ENT_HASH = DAT.OBS_ENT_HASH, '
            sql += ' T.NAME_DATA = DAT.NAME_DATA,'
            sql += ' T.ATTRIBUTE_DATA = DAT.ATTRIBUTE_DATA,'
            sql += ' T.IDENTIFIER_DATA = DAT.IDENTIFIER_DATA,'
            sql += ' T.ADDRESS_DATA = DAT.ADDRESS_DATA,'
            sql += ' T.PHONE_DATA = DAT.PHONE_DATA,'
            sql += ' T.RELATIONSHIP_DATA = DAT.RELATIONSHIP_DATA,'
            sql += ' T.ENTITY_DATA = DAT.ENTITY_DATA,'
            sql += ' T.OTHER_DATA = DAT.OTHER_DATA,'
            sql += ' T.JSON_DATA = DAT.JSON_DATA'
            sql += ' when not matched then insert (DATA_SOURCE,RECORD_ID,ENTITY_TYPE,ENTITY_KEY,OBS_ENT_HASH,NAME_DATA,ATTRIBUTE_DATA,IDENTIFIER_DATA,ADDRESS_DATA,PHONE_DATA,RELATIONSHIP_DATA,ENTITY_DATA,OTHER_DATA,JSON_DATA)'
            sql += ' values (DAT.DATA_SOURCE,DAT.RECORD_ID,DAT.ENTITY_TYPE,DAT.ENTITY_KEY,DAT.OBS_ENT_HASH,DAT.NAME_DATA,DAT.ATTRIBUTE_DATA,DAT.IDENTIFIER_DATA,DAT.ADDRESS_DATA,DAT.PHONE_DATA,DAT.RELATIONSHIP_DATA,DAT.ENTITY_DATA,DAT.OTHER_DATA,DAT.JSON_DATA)'
        else:
            sql = 'insert or replace into DSRC_RECORD ('
            sql += ' DATA_SOURCE, '
            sql += ' RECORD_ID, '
            sql += ' ENTITY_TYPE,'
            sql += ' ENTITY_KEY, '
            sql += ' OBS_ENT_HASH, '
            sql += ' NAME_DATA,'
            sql += ' ATTRIBUTE_DATA,'
            sql += ' IDENTIFIER_DATA,'
            sql += ' ADDRESS_DATA,'
            sql += ' PHONE_DATA,'
            sql += ' RELATIONSHIP_DATA,'
            sql += ' ENTITY_DATA,'
            sql += ' OTHER_DATA,'
            sql += ' JSON_DATA)'
            sql += ' values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    if statementName == 'dsrcRecordSelect':
        sql = 'select '
        sql += ' RECORD_ID '
        sql += 'from DSRC_RECORD '
        sql += 'where ENTITY_KEY = ? and DATA_SOURCE = ? and ENTITY_TYPE = ?'

    if statementName == 'resolvedEntityInsert': 
        sql = 'insert into RESOLVED_ENTITY ('
        sql += ' RESOLVED_ID,'
        sql += ' LENS_ID,'
        sql += ' RESOLVED_NAME,'
        sql += ' DATA_SOURCE,'
        sql += ' ENTITY_TYPE, '
        sql += ' RECORD_COUNT,'
        sql += ' MIN_RECORD_ID) '
        sql += 'values (?, ?, ?, ?, ?, ?, ?)'

    if statementName == 'entityResumeInsert': 
        sql = 'insert into ENTITY_RESUME ('
        sql += ' RESOLVED_ID,'
        sql += ' LENS_ID,'
        sql += ' RELATED_ID,'
        sql += ' DATA_SOURCE,'
        sql += ' RECORD_ID,'
        sql += ' ENTITY_TYPE,'
        sql += ' MATCH_LEVEL,'
        sql += ' MATCH_KEY, '
        sql += ' REF_SCORE,'
        sql += ' ERRULE_CODE) '
        sql += 'values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    if statementName == 'dbReportMatchInsert': 
        sql = 'insert into DBREPORT_MATCHES ('
        sql += ' ENTITY_CATEGORY1,'
        sql += ' ENTITY_CATEGORY2,'
        sql += ' REPORT_CATEGORY,'
        sql += ' AUDIT_KEY,'
        sql += ' AUDIT_NAME,'
        sql += ' AUDIT_SCORE,'
        sql += ' RECORD_SEQ,'
        sql += ' MATCH_TYPE,'
        sql += ' MATCH_LEVEL,'
        sql += ' MATCH_KEY,'
        sql += ' REF_SCORE,'
        sql += ' ERRULE_CODE,'
        sql += ' RESOLVED_ID,' 
        sql += ' LENS_ID,' 
        sql += ' RELATED_ID,'
        sql += ' DATA_SOURCE,'
        sql += ' RECORD_ID,'  
        sql += ' ENTITY_TYPE)'
        sql += 'values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    return sql

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
def shutDown():
    with shutDownStatus.get_lock():
        shutDownStatus.value = 1
    return

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    with shutDownStatus.get_lock():
        shutDownStatus.value = 9
    return

#----------------------------------------
def pause(question='PRESS ENTER TO CONTINUE ...'):
    response = input(question)
    return response

#----------------------------------------
if __name__ == '__main__':
    shutDownStatus = Value('i', 0)    
    signal.signal(signal.SIGINT, signal_handler)

    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))
    iniFileName = appPath + os.path.sep + 'G2Project.ini'
    if not os.path.exists(iniFileName):
        print('ERROR: The G2Project.ini file is missing from the application path!')
        sys.exit(1)

    #--get parameters from ini file
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)
    try: g2iniPath = os.path.expanduser(iniParser.get('g2', 'iniPath'))
    except: g2iniPath = None
    try: g2dbUri = iniParser.get('g2', 'G2Connection')
    except: g2dbUri = None
    try: odsDbUri = iniParser.get('g2', 'ODSConnection')
    except: odsDbUri = None
    try: configTableFile = iniParser.get('g2', 'G2ConfigFile')
    except: configTableFile = None
    try: collapsedTableSchema = iniParser.get('g2', 'collapsedTableSchema').upper() == 'Y'
    except: collapsedTableSchema = False
    try: sqlCommitSize = int(iniParser.get('report', 'sqlCommitSize'))
    except: sqlCommitSize = 1000
    try: reportCategoryLimit = int(iniParser.get('report', 'reportCategoryLimit'))
    except: reportCategoryLimit = 1000

    #--capture the command line arguments
    noRebuild = False
    outputFilePath = 'g2report.g2v'
    if len(sys.argv) > 1:
        optParser = optparse.OptionParser()
        optParser.add_option('-o', '--outputFilePath', dest='outputFilePath', help='the full path to the output file name to write')
        optParser.add_option('-l', '--sampleLimit', dest='sampleLimit', type='int', default=1000, help='limit the number of sample record to (n) per match key')
        optParser.add_option('-N', '--noRebuild', dest='noRebuild', action='store_true', default=False, help='do not purge and rebuild, export to csv only')
        (options, args) = optParser.parse_args()
        if options.outputFilePath:
            outputFilePath = options.outputFilePath
        if options.sampleLimit:
            reportCategoryLimit = options.sampleLimit
        if options.noRebuild:
            noRebuild = options.noRebuild

    #--validations
    if not g2dbUri:
        print('ERROR: A G2 database connection not specified!')
        sys.exit(1)
    if not odsDbUri:
        print('ERROR: A ODS database connection not specified!')
        sys.exit(1)
    if not configTableFile:
        print('ERROR: A G2 setup configuration file is not specified')
        sys.exit(1)

    #--attempt to open the ods database
    odsDbo = G2Database(odsDbUri)
    if not odsDbo.success:
        print('ERROR: could not open database at %s' % odsDbUri)
        sys.exit(1)

    #-- Load the G2 configuration file
    g2ConfigTables = G2ConfigTables(configTableFile,g2iniPath)
    cfg_attr = g2ConfigTables.loadConfig('CFG_ATTR')

    #--open the project
    g2Project = G2Project(cfg_attr)
    if not g2Project.success:
        print('error initializing project')
        sys.exit(1)

    #--build resolved entities
    if not noRebuild:
        buildReport()

    #--export to csv
    if not shutDownStatus.value:
        exportToCsv()

    print('')
    if not shutDownStatus.value:
        print('SUCCESS: Process completed successfully!')
    else:
        print('ERROR: Process did NOT complete!')
    print('')
    odsDbo.close()

    sys.exit(shutDownStatus.value)

