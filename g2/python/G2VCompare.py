#!/usr/bin/python

import os
import sys
import optparse
import sqlite3
from operator import itemgetter

hasUnicodeCsv = True
try: import unicodecsv as csv
except: 
	import csv
	hasUnicodeCsv = False

#----------------------------------------
def pause(question = 'PRESS ENTER TO CONTINUE ...'):
    response = input(question)
    return response

#----------------------------------------
def sqlExec(conn, sql, parmList=None):
    ''' make a database call '''
    cursorData = {}
    try:
        if parmList:
            cursorObj = conn.cursor().execute(sql, parmList)
        else:
            cursorObj = conn.cursor().execute(sql)
    except sqlite3.DatabaseError as err:
        print('ERROR: %s' % err)
        
    else:
        if cursorObj:
            cursorData['OBJECT'] = cursorObj
            cursorData['ROWS_AFFECTED'] = cursorObj.rowcount
            if cursorObj.description:
                cursorData['COLUMN_HEADERS'] = [columnData[0] for columnData in cursorObj.description]
    return cursorData

#----------------------------------------
def sqlExecMany(conn, sql, parmList):
    ''' make a many database call '''
    execSuccess = False
    try: cursor = conn.cursor().executemany(sql, parmList)
    except sqlite3.DatabaseError as err:
        print('ERROR: %s' % err)
    else:
        execSuccess = True
    return execSuccess

#----------------------------------------
def fetchNext(cursorData):
    ''' fetch the next row from a cursor '''
    if 'COLUMN_HEADERS' in cursorData:
        rowValues = cursorData['OBJECT'].fetchone()
        if rowValues:
            rowData = dict(list(zip(cursorData['COLUMN_HEADERS'], rowValues)))
        else:
            rowData = None
    else:
        print('WARNING: Previous SQL was not a query.')
        rowData = None

    return rowData

#----------------------------------------
def pad(strval, strlen):
    if not strval:
        strval = ' '
    strval = strval.replace('\n',';') + ' ' * strlen
    return strval[0:strlen]

#----------------------------------------
def safeReader(csv_reader, csv_header = None): 
    try: 
        tmprow = next(csv_reader)
    except: 
        rtnval = None
    else: 
        if csv_header:
            rtnval = dict(list(zip(csv_header, tmprow)))
        else:
            rtnval = tmprow
    return rtnval


#----------------------------------------
def addAuditInfo(conn, isg2vFile, auditName):

    if isg2vFile:
        tableName = 'DBREPORT_DETAIL_VIEW'
    else:
        tableName = 'DBREPORT_MATCHES'
    dbreportUpdate1 = 'update ' + tableName + ' set AUDIT_GROUP = ? where ENTITY_CATEGORY1 = ? and ENTITY_CATEGORY2 =? and REPORT_CATEGORY = ? and AUDIT_KEY = ?'
    dbreportUpdate2 = 'update ' + tableName + ' set AUDIT_DISPOSITION = ? where ENTITY_CATEGORY1 = ? and ENTITY_CATEGORY2 =? and REPORT_CATEGORY = ? and AUDIT_KEY = ? and RECORD_ID = ?'

    cnt = 0
    with open(auditName) as f:
        reader = csv.reader(f, dialect=csv.excel, quoting=csv.QUOTE_ALL, encoding='utf-8')
        rowHeader = safeReader(reader)
        rowData = safeReader(reader, rowHeader)
        while rowData:
            entityCategory1 = rowData['ENTITY_CATEGORY1']
            entityCategory2 = rowData['ENTITY_CATEGORY2']
            reportCategory = rowData['REPORT_CATEGORY']
            auditKey = rowData['AUDIT_KEY']
            auditGroup = 'OK'
            while rowData and rowData['ENTITY_CATEGORY1'] == entityCategory1 and rowData['ENTITY_CATEGORY2'] == entityCategory2 and rowData['REPORT_CATEGORY'] == reportCategory and rowData['AUDIT_KEY'] == auditKey:
                if rowData['ROW_TYPE'] == 'AUDIT_DETAIL' and rowData['EXCEPTION'] not in ('NOT_APPLICABLE', 'NONE'):
                    if auditGroup == 'OK':
                        auditGroup = rowData['EXCEPTION']
                    else:
                        if auditGroup != rowData['EXCEPTION']:
                            auditGroup = rowData['MULTIPLE']

                    updateRow = []
                    updateRow.append(rowData['EXCEPTION'])
                    updateRow.append(rowData['ENTITY_CATEGORY1']) 
                    updateRow.append(rowData['ENTITY_CATEGORY2'])
                    updateRow.append(rowData['REPORT_CATEGORY'])
                    updateRow.append(rowData['AUDIT_KEY'])
                    updateRow.append(rowData['RECORD_ID'])
                    if not sqlExec(conn, dbreportUpdate2, updateRow):
                        return False

                rowData = safeReader(reader, rowHeader)

            updateRow = []
            updateRow.append(auditGroup)
            updateRow.append(entityCategory1) 
            updateRow.append(entityCategory2)
            updateRow.append(reportCategory)
            updateRow.append(auditKey)
            if not sqlExec(conn, dbreportUpdate1, updateRow):
                return False

    return True

#----------------------------------------
def loadFile(fileName, auditName = None):

    if not fileName:
        return None

    print('')
    print('Loading %s ...' % fileName)

    #--create or use database
    dummy, fileExtension = os.path.splitext(fileName)
    isg2vFile = fileExtension.upper() == '.G2V'
    if isg2vFile:
        dbFileName = fileName + '.db'
    else:
        dbFileName = fileName

    try: conn = sqlite3.connect(dbFileName, isolation_level=None)
    except:
        print('ERROR: could not open file %s ' % dbFileName)
        return False
    else:
        conn.cursor().execute("PRAGMA journal_mode=wal")
        conn.cursor().execute("PRAGMA synchronous=0")

    #--load db report from g2v file
    if isg2vFile:

        sql = 'drop table if exists DBREPORT_DETAIL_VIEW'
        if not sqlExec(conn, sql):
            return False

        sql = 'create table DBREPORT_DETAIL_VIEW ( '
        sql += ' ENTITY_CATEGORY1 varchar(50) not null, '
        sql += ' ENTITY_CATEGORY2 varchar(50) not null, '
        sql += ' REPORT_CATEGORY varchar(25) not null, '
        sql += ' AUDIT_KEY varchar(500) not null, '
        sql += ' AUDIT_NAME varchar(250) not null, '
        sql += ' AUDIT_SCORE integer not null, '
        sql += ' RECORD_SEQ integer not null, '
        sql += ' MATCH_TYPE integer not null, '
        sql += ' MATCH_LEVEL varchar(25) not null, '
        sql += ' MATCH_KEY varchar(100), '
        sql += ' REF_SCORE integer, '
        sql += ' ERRULE_CODE varchar(25), '
        sql += ' RESOLVED_ENTITY_ID integer not null,  '
        sql += ' LENS_ID integer not null,  '
        sql += ' RELATED_ENTITY_ID integer not null, '
        sql += ' DATA_SOURCE varchar(25) not null, '
        sql += ' RECORD_ID varchar(250) not null,   '
        sql += ' ENTITY_TYPE varchar(25) not null, '
        sql += ' NAME_DATA varchar(5000), '
        sql += ' ATTRIBUTE_DATA varchar(250), '
        sql += ' IDENTIFIER_DATA varchar(500), '
        sql += ' ADDRESS_DATA varchar(5000), '
        sql += ' PHONE_DATA varchar(250), '
        sql += ' RELATIONSHIP_DATA varchar(250), '
        sql += ' ENTITY_DATA varchar(100), '
        sql += ' OTHER_DATA varchar(250), '
        sql += ' AUDIT_GROUP varchar(25), '
        sql += ' AUDIT_DISPOSITION varchar(25), '
        sql += ' AUDIT_CORRECTED integer, '
        sql += ' primary key (ENTITY_CATEGORY1, ENTITY_CATEGORY2, REPORT_CATEGORY, AUDIT_KEY, RECORD_SEQ) '
        sql += ');'
        if not sqlExec(conn, sql):
            return False

        hitSeparator = False
        readyToRoll = False
        columnHeaders = None
        dbreportInsert = 'insert into DBREPORT_DETAIL_VIEW values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

        cnt = 0
        with open(fileName) as f:
            reader = csv.reader(f, dialect=csv.excel, quoting=csv.QUOTE_ALL, encoding='utf-8')
            for row in reader:
                if readyToRoll:
                    rowDict = dict(list(zip(columnHeaders, row)))
                    cnt += 1
                    if cnt % statInterval == 0:
                        print(' %s records loaded' % cnt)

                    insertRow = []
                    insertRow.append(rowDict['ENTITY_CATEGORY1']) 
                    insertRow.append(rowDict['ENTITY_CATEGORY2'])
                    insertRow.append(rowDict['REPORT_CATEGORY'])
                    insertRow.append(rowDict['AUDIT_KEY'])
                    insertRow.append(rowDict['AUDIT_NAME'])
                    insertRow.append(rowDict['AUDIT_SCORE'] if 'AUDIT_SCORE' in rowDict else 0)
                    insertRow.append(rowDict['RECORD_SEQ'])
                    insertRow.append(rowDict['MATCH_TYPE'])
                    insertRow.append(rowDict['MATCH_LEVEL'])
                    insertRow.append(rowDict['MATCH_KEY'])
                    insertRow.append(rowDict['REF_SCORE'])
                    insertRow.append(rowDict['ERRULE_CODE'])
                    insertRow.append(rowDict['RESOLVED_ENTITY_ID'])
                    insertRow.append(rowDict['LENS_ID'] if 'LENS_ID' in rowDict else 1)
                    insertRow.append(rowDict['RELATED_ENTITY_ID'])
                    insertRow.append(rowDict['DATA_SOURCE'])
                    insertRow.append(rowDict['RECORD_ID'])
                    insertRow.append(rowDict['ENTITY_TYPE'])
                    insertRow.append(rowDict['NAME_DATA'])
                    insertRow.append(rowDict['ATTRIBUTE_DATA'])
                    insertRow.append(rowDict['IDENTIFIER_DATA'])
                    insertRow.append(rowDict['ADDRESS_DATA'])
                    insertRow.append(rowDict['PHONE_DATA'])
                    insertRow.append(rowDict['RELATIONSHIP_DATA'])
                    insertRow.append(rowDict['ENTITY_DATA'])
                    insertRow.append(rowDict['OTHER_DATA'])
                    insertRow.append(None) #--AUDIT_GROUP
                    insertRow.append(None) #--AUDIT_DISPOSITION
                    insertRow.append(None) #--AUDIT_RESULT
                    if not sqlExec(conn, dbreportInsert, insertRow):
                        return False

                #--still not ready
                else:
                    if hitSeparator:
                        columnHeaders = row
                        readyToRoll = True
                    else:
                        hitSeparator = row[0][0:10] == '-' * 10

            print(' %s records loaded, complete' % cnt)

    #--apply the audit information
    if auditName:
        addAuditInfo(conn, isg2vFile, auditName)

    #--create audit table
    sql = 'drop table if exists AUDIT_DETAIL'
    if not sqlExec(conn, sql):
        return False

    sql = 'create table AUDIT_DETAIL ( '
    sql += ' RECORD_ID1 varchar(250) not null, '
    sql += ' RECORD_ID2 varchar(250) not null, '
    sql += ' DATA_SOURCE1 varchar(25) not null, '
    sql += ' DATA_SOURCE2 varchar(25) not null, '
    sql += ' RESOLVED_ENTITY_ID integer not null, '
    sql += ' RELATED_ENTITY_ID integer not null, '
    sql += ' REPORT_CATEGORY varchar(25) not null, '
    sql += ' ERRULE_CODE varchar(25), '
    sql += ' MATCH_KEY varchar(100), '
    sql += ' DIFF_CATEGORY varchar(100), '
    sql += ' AUDIT_DISPOSITION varchar(25), '
    sql += ' AUDIT_KEY varchar(50), '
    sql += ' AUDIT_NAME varchar(250), '
    sql += ' NAME_DATA1 varchar(5000), '
    sql += ' NAME_DATA2 varchar(5000), '
    sql += ' ATTRIBUTE_DATA1 varchar(250), '
    sql += ' ATTRIBUTE_DATA2 varchar(250), '
    sql += ' IDENTIFIER_DATA1 varchar(500), '
    sql += ' IDENTIFIER_DATA2 varchar(500), '
    sql += ' ADDRESS_DATA1 varchar(5000), '
    sql += ' ADDRESS_DATA2 varchar(5000), '
    sql += ' PHONE_DATA1 varchar(250), '
    sql += ' PHONE_DATA2 varchar(250), '
    sql += ' RELATIONSHIP_DATA1 varchar(250), '
    sql += ' RELATIONSHIP_DATA2 varchar(250), '
    sql += ' primary key (RECORD_ID1, RECORD_ID2, DATA_SOURCE1, DATA_SOURCE2)'
    sql += ');'
    if not sqlExec(conn, sql):
        return False

    auditInsert = 'insert into AUDIT_DETAIL values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'

    sql = 'select '
    sql += ' RESOLVED_ENTITY_ID, '
    sql += ' AUDIT_KEY, '
    sql += ' AUDIT_NAME, '
    sql += ' RELATED_ENTITY_ID, '
    sql += ' REPORT_CATEGORY, '
    sql += ' MATCH_TYPE, '
    sql += ' ERRULE_CODE, '
    sql += ' MATCH_KEY, '
    sql += ' AUDIT_DISPOSITION, '
    sql += ' DATA_SOURCE, '
    sql += ' RECORD_ID, '
    sql += ' NAME_DATA, '
    sql += ' ATTRIBUTE_DATA, '
    sql += ' IDENTIFIER_DATA, '
    sql += ' ADDRESS_DATA, '
    sql += ' PHONE_DATA, '
    sql += ' RELATIONSHIP_DATA '
    sql += 'from DBREPORT_DETAIL_VIEW '
    sql += 'where ENTITY_CATEGORY1 <= ENTITY_CATEGORY2 '  #--removes duplicate cross category reports (a to B and b to a)
    #--sql += ' and (RESOLVED_ENTITY_ID < RELATED_ENTITY_ID or RELATED_ENTITY_ID = 0) ' #--removes duplicate relationships within a category
    sql += 'order by AUDIT_KEY'

    cnt = 0
    cursor = sqlExec(conn, sql)
    rowData = fetchNext(cursor)
    while rowData:

        #--gather all the rows for an audit_key
        reportCategory = rowData['REPORT_CATEGORY']
        resolvedId = rowData['RESOLVED_ENTITY_ID']
        relatedId = rowData['RELATED_ENTITY_ID']
        auditKey = rowData['AUDIT_KEY']
        auditName = rowData['AUDIT_NAME']
        resolvedRows = []    
        relatedRows = []
        while rowData and rowData['AUDIT_KEY'] == auditKey:
            if rowData['MATCH_TYPE'] <= 1:
                resolvedRows.append(rowData)
            else:
                relatedRows.append(rowData)
            rowData = fetchNext(cursor)

        cnt += 1
        if cnt % statInterval == 0:
            print(' %s entities processed' % cnt)

        #--print auditKey, auditName, reportCategory, len(resolvedRows), len(relatedRows)
        diffCategory = None
        insertList = []

        #--explode resolved rows if a duplicate
        if reportCategory == 'Duplicates':
            for resolvedRow1 in resolvedRows:
                for resolvedRow2 in [x for x in resolvedRows if resolvedRow1['DATA_SOURCE'] + resolvedRow1['RECORD_ID'] < x['DATA_SOURCE'] + x['RECORD_ID']]:
                    if resolvedRow1['DATA_SOURCE'] + resolvedRow1['RECORD_ID'] != resolvedRow2['DATA_SOURCE'] + resolvedRow2['RECORD_ID']:
                        if resolvedRow1['DATA_SOURCE'] + resolvedRow1['RECORD_ID'] < resolvedRow2['DATA_SOURCE'] + resolvedRow2['RECORD_ID']:
                            recordId1 = resolvedRow1['RECORD_ID']
                            dataSource1 = resolvedRow1['DATA_SOURCE']
                            nameData1 = resolvedRow1['NAME_DATA']
                            attributeData1 = resolvedRow1['ATTRIBUTE_DATA']
                            identifierData1 = resolvedRow1['IDENTIFIER_DATA']
                            addressData1 = resolvedRow1['ADDRESS_DATA']
                            phoneData1 = resolvedRow1['PHONE_DATA']
                            relationshipData1 = resolvedRow1['RELATIONSHIP_DATA']

                            recordId2 = resolvedRow2['RECORD_ID']
                            dataSource2 = resolvedRow2['DATA_SOURCE']
                            nameData2 = resolvedRow2['NAME_DATA']
                            attributeData2 = resolvedRow2['ATTRIBUTE_DATA']
                            identifierData2 = resolvedRow2['IDENTIFIER_DATA']
                            addressData2 = resolvedRow2['ADDRESS_DATA']
                            phoneData2 = resolvedRow2['PHONE_DATA']
                            relationshipData2 = resolvedRow2['RELATIONSHIP_DATA']
                        else:
                            recordId1 = resolvedRow2['RECORD_ID']
                            dataSource1 = resolvedRow2['DATA_SOURCE']
                            nameData1 = resolvedRow2['NAME_DATA']
                            attributeData1 = resolvedRow2['ATTRIBUTE_DATA']
                            identifierData1 = resolvedRow2['IDENTIFIER_DATA']
                            addressData1 = resolvedRow2['ADDRESS_DATA']
                            phoneData1 = resolvedRow2['PHONE_DATA']
                            relationshipData1 = resolvedRow2['RELATIONSHIP_DATA']

                            recordId2 = resolvedRow1['RECORD_ID']
                            dataSource2 = resolvedRow1['DATA_SOURCE']
                            nameData2 = resolvedRow1['NAME_DATA']
                            attributeData2 = resolvedRow1['ATTRIBUTE_DATA']
                            identifierData2 = resolvedRow1['IDENTIFIER_DATA']
                            addressData2 = resolvedRow1['ADDRESS_DATA']
                            phoneData2 = resolvedRow1['PHONE_DATA']
                            relationshipData2 = resolvedRow1['RELATIONSHIP_DATA']

                        #if len(resolvedRows) > 2:
                        #    erruleCode = "a resolve rule"
                        if resolvedRow1['ERRULE_CODE'] or resolvedRow1['AUDIT_DISPOSITION']:
                            erruleCode = resolvedRow1['ERRULE_CODE']
                            matchKey = resolvedRow1['MATCH_KEY']
                            auditDisposition = resolvedRow1['AUDIT_DISPOSITION']
                        else:
                            erruleCode = resolvedRow2['ERRULE_CODE']
                            matchKey = resolvedRow2['MATCH_KEY']
                            auditDisposition = resolvedRow2['AUDIT_DISPOSITION']

                        insertRow = [recordId1, recordId2, dataSource1, dataSource2, resolvedId, relatedId, reportCategory, erruleCode, matchKey, diffCategory, auditDisposition, auditKey, auditName, nameData1, nameData2, attributeData1, attributeData2, identifierData1, identifierData2, addressData1, addressData2, phoneData1, phoneData2, relationshipData1, relationshipData2]
                        if insertRow not in insertList:
                            insertList.append(insertRow)

        #--explode related rows
        for relatedRow in relatedRows:
            auditKey = relatedRow['AUDIT_KEY']
            relatedId = relatedRow['RELATED_ENTITY_ID']
            reportCategory = relatedRow['REPORT_CATEGORY']
            erruleCode = relatedRow['ERRULE_CODE']
            matchKey = relatedRow['MATCH_KEY']
            auditDisposition = relatedRow['AUDIT_DISPOSITION']
            for resolvedRow in resolvedRows:
                if relatedRow['DATA_SOURCE'] + relatedRow['RECORD_ID'] < resolvedRow['DATA_SOURCE'] + resolvedRow['RECORD_ID'] :
                    recordId1 = relatedRow['RECORD_ID']
                    dataSource1 = relatedRow['DATA_SOURCE']
                    nameData1 = relatedRow['NAME_DATA']
                    attributeData1 = relatedRow['ATTRIBUTE_DATA']
                    identifierData1 = relatedRow['IDENTIFIER_DATA']
                    addressData1 = relatedRow['ADDRESS_DATA']
                    phoneData1 = relatedRow['PHONE_DATA']
                    relationshipData1 = relatedRow['RELATIONSHIP_DATA']

                    recordId2 = resolvedRow['RECORD_ID']
                    dataSource2 = resolvedRow['DATA_SOURCE']
                    nameData2 = resolvedRow['NAME_DATA']
                    attributeData2 = resolvedRow['ATTRIBUTE_DATA']
                    identifierData2 = resolvedRow['IDENTIFIER_DATA']
                    addressData2 = resolvedRow['ADDRESS_DATA']
                    phoneData2 = resolvedRow['PHONE_DATA']
                    relationshipData2 = resolvedRow['RELATIONSHIP_DATA']
                else:
                    recordId1 = resolvedRow['RECORD_ID']
                    dataSource1 = resolvedRow['DATA_SOURCE']
                    nameData1 = resolvedRow['NAME_DATA']
                    attributeData1 = resolvedRow['ATTRIBUTE_DATA']
                    identifierData1 = resolvedRow['IDENTIFIER_DATA']
                    addressData1 = resolvedRow['ADDRESS_DATA']
                    phoneData1 = resolvedRow['PHONE_DATA']
                    relationshipData1 = resolvedRow['RELATIONSHIP_DATA']

                    recordId2 = relatedRow['RECORD_ID']
                    dataSource2 = relatedRow['DATA_SOURCE']
                    nameData2 = relatedRow['NAME_DATA']
                    attributeData2 = relatedRow['ATTRIBUTE_DATA']
                    identifierData2 = relatedRow['IDENTIFIER_DATA']
                    addressData2 = relatedRow['ADDRESS_DATA']
                    phoneData2 = relatedRow['PHONE_DATA']
                    relationshipData2 = relatedRow['RELATIONSHIP_DATA']

                insertRow = [recordId1, recordId2, dataSource1, dataSource2, resolvedId, relatedId, reportCategory, erruleCode, matchKey, diffCategory, auditDisposition, auditKey, auditName, nameData1, nameData2, attributeData1, attributeData2, identifierData1, identifierData2, addressData1, addressData2, phoneData1, phoneData2, relationshipData1, relationshipData2]
                if insertRow not in insertList:
                    insertList.append(insertRow)

        if not sqlExecMany(conn, auditInsert, insertList):
            print('ERROR: Inserting into AUDIT_DETAIL table')
            for insertRow in insertList:
                print(insertRow)

            return False
                    
    print(' %s entities processed, complete' % cnt)

    return conn

#----------------------------------------
def compareFiles(conn1, conn2, fileName1, fileName2, auditName1):

    diffCategories = {}
    diffCategory3s = {}
    diffRecords = []

    print('')
    print('Comparing %s to %s ... ' % (fileName1, fileName2))
    auditCnt1 = 0
    sql1 = 'select * from AUDIT_DETAIL'
    #if auditName1:
    #    sql1 += ' where AUDIT_DISPOSITION is not null'

    cursor1 = sqlExec(conn1, sql1)
    rowData1 = fetchNext(cursor1)
    while rowData1:
        auditKey = rowData1['AUDIT_KEY']

        #--check for this record in the second report
        if conn2:
            sql2 = 'select '
            sql2 += ' RESOLVED_ENTITY_ID, '
            sql2 += ' RELATED_ENTITY_ID, '
            sql2 += ' AUDIT_KEY, '
            sql2 += ' AUDIT_NAME, '
            sql2 += ' REPORT_CATEGORY, '
            sql2 += ' ERRULE_CODE '
            sql2 += 'from AUDIT_DETAIL '
            sql2 += 'where RECORD_ID1 = ? '
            sql2 += '  and RECORD_ID2 = ? '
            sql2 += '  and DATA_SOURCE1 = ? '
            sql2 += '  and DATA_SOURCE2 = ? ' 
            parmData2 = [rowData1['RECORD_ID1'], rowData1['RECORD_ID2'], rowData1['DATA_SOURCE1'], rowData1['DATA_SOURCE2']]
            cursor2 = sqlExec(conn2, sql2, parmData2)
            rowData2 = fetchNext(cursor2)
        else:
            rowData2 = None

        diffCategory1 = '0=same'
        diffCategory2 = '0=same'
        diffCategory3 = '0=same'
        if not rowData2:
            diffCategory1 = '1=missing %s on %s' % (rowData1['REPORT_CATEGORY'], rowData1['ERRULE_CODE'])
            diffCategory2 = '1=missing'
            diffCategory3 = '1=missing'
        elif rowData2['REPORT_CATEGORY'] != rowData1['REPORT_CATEGORY']:
            diffCategory1 = '2=moved from %s on %s to %s on %s' % (rowData1['REPORT_CATEGORY'], rowData1['ERRULE_CODE'], rowData2['REPORT_CATEGORY'], rowData2['ERRULE_CODE'])
            diffCategory2 = '2=moved to %s on %s' % (rowData2['REPORT_CATEGORY'], rowData2['ERRULE_CODE'])
            diffCategory3 = '2=moved from %s to %s' % (rowData1['REPORT_CATEGORY'], rowData2['REPORT_CATEGORY'])
        elif rowData2['ERRULE_CODE'] != rowData1['ERRULE_CODE']:
            diffCategory1 = '3=changed from %s on %s to %s on %s' % (rowData1['REPORT_CATEGORY'], rowData1['ERRULE_CODE'], rowData2['REPORT_CATEGORY'], rowData2['ERRULE_CODE'])
            diffCategory2 = '3=changed to %s on %s' % (rowData2['REPORT_CATEGORY'], rowData2['ERRULE_CODE'])
            diffCategory3 = '3=changed from %s to %s' % (rowData1['REPORT_CATEGORY'], rowData2['REPORT_CATEGORY'])

        if diffCategory1 not in diffCategories:
            diffCategories[diffCategory1] = 1
        else:
            diffCategories[diffCategory1] += 1

        if diffCategory3 not in diffCategory3s:
            diffCategory3s[diffCategory3] = 1
        else:
            diffCategory3s[diffCategory3] += 1

        if diffCategory1[0:1] != '0':
            rowData1['DIFF_CATEGORY'] = diffCategory2
            if rowData2:
                rowData1['NEW_AUDIT_KEY'] = rowData2['AUDIT_KEY']
            else:
                rowData1['NEW_AUDIT_KEY'] = None
            diffRecords.append(rowData1)

        #--update the record
        updateParms = [diffCategory2[1:], rowData1['RECORD_ID1'], rowData1['RECORD_ID2'], rowData1['DATA_SOURCE1'], rowData1['DATA_SOURCE2']]
        sqlExec(conn1, 'update AUDIT_DETAIL set DIFF_CATEGORY = ? where RECORD_ID1 = ? and RECORD_ID2 = ? and DATA_SOURCE1 = ? and DATA_SOURCE2 = ?', updateParms)

        #--next record
        rowData1 = fetchNext(cursor1)
        auditCnt1 += 1
        if auditCnt1 % statInterval == 0 or not rowData1:
            print(' %s records audited' % auditCnt1)

    #--reverse search
    auditCnt2 = 0
    if conn2: # and not auditName1:
        print('')
        print('Comparing %s to %s ... ' % (fileName2, fileName1))
        sql1 = 'select * from AUDIT_DETAIL'
        cursor1 = sqlExec(conn2, sql1)
        rowData1 = fetchNext(cursor1)
        while rowData1:
            auditKey = rowData1['AUDIT_KEY']

            #--check for this record in the second report
            sql2 = 'select 1 '
            sql2 += 'from AUDIT_DETAIL '
            sql2 += 'where RECORD_ID1 = ? '
            sql2 += '  and RECORD_ID2 = ? '
            sql2 += '  and DATA_SOURCE1 = ? '
            sql2 += '  and DATA_SOURCE2 = ? ' 
            parmData2 = [rowData1['RECORD_ID1'], rowData1['RECORD_ID2'], rowData1['DATA_SOURCE1'], rowData1['DATA_SOURCE2']]
            cursor2 = sqlExec(conn1, sql2, parmData2)
            rowData2 = fetchNext(cursor2)

            if not rowData2:
                diffCategory = '4=new %s on %s' % (rowData1['REPORT_CATEGORY'], rowData1['ERRULE_CODE'])
                diffCategory3 = '4=new %s' % rowData1['REPORT_CATEGORY']

                if diffCategory not in diffCategories:
                    diffCategories[diffCategory] = 1
                else:
                    diffCategories[diffCategory] += 1

                if diffCategory3 not in diffCategory3s:
                    diffCategory3s[diffCategory3] = 1
                else:
                    diffCategory3s[diffCategory3] += 1


                rowData1['DIFF_CATEGORY'] = '4-New'
                rowData1['NEW_AUDIT_KEY'] = rowData1['AUDIT_KEY']
                rowData1['AUDIT_KEY'] = None
                diffRecords.append(rowData1)

            #--update the record
            updateParms = ['New', rowData1['RECORD_ID1'], rowData1['RECORD_ID2'], rowData1['DATA_SOURCE1'], rowData1['DATA_SOURCE2']]
            sqlExec(conn2, 'update AUDIT_DETAIL set DIFF_CATEGORY = ? where RECORD_ID1 = ? and RECORD_ID2 = ? and DATA_SOURCE1 = ? and DATA_SOURCE2 = ?', updateParms)

            #--next record
            rowData1 = fetchNext(cursor1)
            auditCnt2 += 1
            if auditCnt2 % statInterval == 0 or not rowData1:
                print(' %s records audited' % auditCnt2)

    #--display results
    print('') 
    print('COMPARED ...')
    print(' %s  (%s records) ' % (fileName1, auditCnt1))
    if conn2:
        print(' %s  (%s records) ' % (fileName2, auditCnt2))
    print('')
    print('RULE SUMMARY ...')
    fileWrite(summaryFileHandle, '')
    fileWrite(summaryFileHandle, 'RULE SUMMARY ...')
    for diffCategory in sorted(diffCategories):
        line = ' %s %s' % (diffCategories[diffCategory], diffCategory[2:])
        print(line)
        fileWrite(summaryFileHandle, line)

    print('')
    print('CATEGORY SUMMARY ...')
    fileWrite(summaryFileHandle, '')
    fileWrite(summaryFileHandle, 'CATEGORY SUMMARY ...')
    for diffCategory in sorted(diffCategory3s):
        line = ' %s %s' % (diffCategory3s[diffCategory], diffCategory[2:])
        print(line)
        fileWrite(summaryFileHandle, line)

    line = '%s resolved entities affected' % len(set([x['RESOLVED_ENTITY_ID'] for x in diffRecords]))
    print('') 
    print(line)
    fileWrite(summaryFileHandle, '')
    fileWrite(summaryFileHandle, line)

    line = 'Writing results to %s ...' % outputFileName
    print('') 
    print(line)
    fileWrite(summaryFileHandle, '')
    fileWrite(summaryFileHandle, line)

    #--write the header row
    row = []
    row.append('REPORT_CATEGORY')
    row.append('AUDIT_DISPOSITION')
    row.append('ERRULE_CODE')
    row.append('MATCH_KEY')
    row.append('AUDIT_NAME')
    row.append('AUDIT_KEY')
    row.append('DIFF_CATEGORY')
    row.append('NEW_AUDIT_KEY')
    row.append('DATA_SOURCE1')
    row.append('RECORD_ID1')
    row.append('DATA_SOURCE2')
    row.append('RECORD_ID2')
    row.append('NAME_DATA1')
    row.append('NAME_DATA2')
    row.append('ATTRIBUTE_DATA1')
    row.append('ATTRIBUTE_DATA2')
    row.append('IDENTIFIER_DATA1')
    row.append('IDENTIFIER_DATA2')
    row.append('ADDRESS_DATA1')
    row.append('ADDRESS_DATA2')
    row.append('PHONE_DATA1')
    row.append('PHONE_DATA2')
    row.append('RELATIONSHIP_DATA1')
    row.append('RELATIONSHIP_DATA2')
    outputFileWriter.writerow(row)

    sortedDiffRecords = sorted(diffRecords, key=itemgetter('REPORT_CATEGORY', 'AUDIT_DISPOSITION', 'ERRULE_CODE', 'MATCH_KEY', 'AUDIT_NAME', 'AUDIT_KEY', 'DATA_SOURCE1', 'RECORD_ID1', 'DATA_SOURCE2', 'RECORD_ID2'))
    for diffRecord in sortedDiffRecords:
        row = []
        row.append(diffRecord['REPORT_CATEGORY'])
        row.append(diffRecord['AUDIT_DISPOSITION'])
        row.append(diffRecord['ERRULE_CODE'])
        row.append(diffRecord['MATCH_KEY'])
        row.append(diffRecord['AUDIT_NAME'])
        row.append(diffRecord['AUDIT_KEY'])
        row.append(diffRecord['DIFF_CATEGORY'][2:])
        row.append(diffRecord['NEW_AUDIT_KEY'])
        row.append(diffRecord['DATA_SOURCE1'])
        row.append(diffRecord['RECORD_ID1'])
        row.append(diffRecord['DATA_SOURCE2'])
        row.append(diffRecord['RECORD_ID2'])
        row.append(diffRecord['NAME_DATA1'].replace('\n','; ') if diffRecord['NAME_DATA1'] else '')
        row.append(diffRecord['NAME_DATA2'].replace('\n','; ') if diffRecord['NAME_DATA2'] else '')
        row.append(diffRecord['ATTRIBUTE_DATA1'].replace('\n','; ') if diffRecord['ATTRIBUTE_DATA1'] else '')
        row.append(diffRecord['ATTRIBUTE_DATA2'].replace('\n','; ') if diffRecord['ATTRIBUTE_DATA2'] else '')
        row.append(diffRecord['IDENTIFIER_DATA1'].replace('\n','; ') if diffRecord['IDENTIFIER_DATA1'] else '')
        row.append(diffRecord['IDENTIFIER_DATA2'].replace('\n','; ') if diffRecord['IDENTIFIER_DATA2'] else '')
        row.append(diffRecord['ADDRESS_DATA1'].replace('\n','; ') if diffRecord['ADDRESS_DATA1'] else '')
        row.append(diffRecord['ADDRESS_DATA2'].replace('\n','; ') if diffRecord['ADDRESS_DATA2'] else '')
        row.append(diffRecord['PHONE_DATA1'].replace('\n','; ') if diffRecord['PHONE_DATA1'] else '')
        row.append(diffRecord['PHONE_DATA2'].replace('\n','; ') if diffRecord['PHONE_DATA2'] else '')
        row.append(diffRecord['RELATIONSHIP_DATA1'].replace('\n','; ') if diffRecord['RELATIONSHIP_DATA1'] else '') 
        row.append(diffRecord['RELATIONSHIP_DATA2'].replace('\n','; ') if diffRecord['RELATIONSHIP_DATA2'] else '')
        outputFileWriter.writerow(row)

    print('')
    print('done!')
    fileWrite(summaryFileHandle, '')
    fileWrite(summaryFileHandle, 'done!')

    return

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
    try: fileHandle = open(fileName, 'a', 0)
    except:
        print('ERROR: could not open %s' % (fileName))
        return None

    return fileHandle

#----------------------------------------
def fileWrite(fileHandle, fileLine):
    ''' write a line to the file '''
    try: fileHandle.write(fileLine + '\n')
    except:
        success = False
    else:
        success = True

    return success

#----------------------------------------
if __name__ == '__main__':
    shutDown = False

    #--capture the command line arguments
    statInterval = 10000
    outputFilter = 0
    fileName1 = None
    fileName2 = None
    auditName1 = None

    outputFileName = './g2vcompare.csv'
    if len(sys.argv) > 1:
        optParser = optparse.OptionParser()
        optParser.add_option('-1', '--inputFile1', dest='fileName1', help='the name of a prior g2v or sqlite db file to audit')
        optParser.add_option('-2', '--inputFile2', dest='fileName2', help='the name of a prior g2v or sqlite db file to audit')
        optParser.add_option('-a', '--auditFile1', dest='auditName1', help='the name of a prior g2a audit file')
        optParser.add_option('-o', '--outputFile', dest='outputFileName', default=outputFileName, help='the name of a file to write the results to')
        (options, args) = optParser.parse_args()
        if options.fileName1:
            fileName1 = options.fileName1
        if options.auditName1:
            auditName1 = options.auditName1
        if options.fileName2:
            fileName2 = options.fileName2
        if options.outputFileName:
            outputFileName = options.outputFileName
    else:
        #print 'ERROR: No parameters specified'
        #sys.exit(1)

        #--input files
        fileName1 = './patientReport-r1-s2.g2v'
        fileName2 = './patientReport-r2-s1.g2v'
        auditName1 = './patientReport-r1-s2.g2a'

        #fileName1 = './G2_REPORT-r1-s2.db'
        #fileName2 = './G2_REPORT-r2-s1.db'

       
        #fileName1 = './test/crayon/crayon_021517.g2v'
        #fileName2 = './test/crayon/crayon_030317.g2v'
        #fileName1 = './demoReport1.g2v'
        #fileName2 = './demoReport2.g2v'

    #--creates the csv output file
    try: 
        outputFileHandle = open(outputFileName, "wb")
        if hasUnicodeCsv:
            outputFileWriter = csv.writer(outputFileHandle, dialect=csv.excel, quoting=csv.QUOTE_ALL, encoding='utf-8')
        else:
            outputFileWriter = csv.writer(outputFileHandle, dialect=csv.excel, quoting=csv.QUOTE_ALL)
    except csv.Error as err:
        print(err)
        print('ERROR: Could not open %s for writing' % outputFileName)
        sys.exit(1)
    
    #--creates summary output file
    fileName, fileExtension = os.path.splitext(outputFileName)
    summaryFileName = fileName + '-summary.txt'
    try: summaryFileHandle = fileCreate(summaryFileName)
    except:
        print('ERROR: could not open output file %s for writing' % summaryFileName)
        sys.exit(1)

    conn1 = loadFile(fileName1, auditName1)
    if not conn1:
        sys.exit(1)

    #--second file is now optional
    conn2 = None
    if fileName2:
        conn2 = loadFile(fileName2)
        if not conn2:
            sys.exit(1)

    compareFiles(conn1, conn2, fileName1, fileName2, auditName1)

    outputFileHandle.close()
    summaryFileHandle.close()

    sys.exit(0)

