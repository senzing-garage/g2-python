#--python imports
import optparse
import sys
import os
from importlib import import_module

#--project classes
import G2Exception

#======================
class G2Database:
#======================

    #----------------------------------------
    def __init__(self, dbUri):
        """ open the database """
        self.success = False

        #--parse the uri
        try: self.dburiParse(dbUri)
        except G2Exception.G2UnsupportedDatabaseType as err:
            print(err)
            return

        #--import correct modules for DB type
        if self.dbType in ('MYSQL', 'DB2', 'POSTGRESQL'):
            try:
                self.pyodbc = import_module('pyodbc')
            except ImportError as err:
                raise ImportError('ERROR: could not import pyodbc module\n\nPlease check the Senzing help center: https://senzing.zendesk.com/hc/en-us/search?utf8=%E2%9C%93&query=pyodbc\n\t')
        else:
            try:
                self.sqlite3 = import_module('sqlite3')
            except ImportError as err:
                raise ImportError('ERROR: could not import sqlite3 module\n\nPlease ensure the python sqlite3 module is available')

        #--attempt to open the database
        try:
            self.Connect()
        except G2Exception.G2DBMNotStarted as err:
            print('ERROR: Database Manager not started')
            print(err)
            return
        except G2Exception.G2DBNotFound as err:
            print('ERROR: Database not found')
            print(err)
            return
        except Exception as err:
            print(self)
            print('ERROR: could not open database ' + self.dsn)
            print(type(err))
            print(err)
            return
        else:
            #--attempt to set the schema (if there is one)import 
            if self.schema != None and len(self.schema) != 0:
                if not self.SetSchema():
                    print(self)
                    print('ERROR: could not connect to schema')
            
                    return

            #--handle utf-8 issues for sqlite3
            if self.dbType == 'SQLITE3':
                self.dbo.text_factory = str

            self.success = True

        return

    #----------------------------------------
    def Connect(self):
        try:
            if self.dbType == 'MYSQL':
                self.dbo = self.pyodbc.connect('DRIVER={' + self.dbType + '};SERVER=' + self.dsn + ';PORT=' + self.port + ';DATABASE=' + self.schema + ';UID=' + self.userId + '; PWD=' + self.password, autocommit = True)
            elif self.dbType == 'SQLITE3':
                if not os.path.isfile(self.dsn):
                    raise G2Exception.G2DBNotFound('ERROR: sqlite3 database file not found ' + self.dsn)
                self.dbo = self.sqlite3.connect(self.dsn, isolation_level=None)
                c = self.dbo.cursor()
                c.execute("PRAGMA journal_mode=wal")
                c.execute("PRAGMA synchronous=0")
            elif self.dbType == 'DB2':
                self.dbo = self.pyodbc.connect('DSN=' + self.dsn + '; UID=' + self.userId + '; PWD=' + self.password, autocommit=True)
            elif self.dbType == 'POSTGRESQL':
              conn_str = 'DSN=' + self.dsn + ';UID=' + self.userId + ';PWD=' + self.password + ';'
              self.dbo = self.pyodbc.connect(conn_str, autocommit=True)
            else:
                print('ERROR: Unsupported DB Type: ' + self.dbType)
                return False
        except Exception as err:
            raise self.TranslateException(err)
        except self.sqlite3.DatabaseError as err:
            raise self.TranslateException(err)

        return

    #----------------------------------------
    def __str__(self):
        ''' return the database we connected to '''
        return "dbType:" + str(self.dbType) + " dsn:" + str(self.dsn) + " port:" + str(self.port) +" userId:" + str(self.userId) + " password:" + str(self.password) + " schema:" + str(self.schema) + " table:" + str(self.table)

    #----------------------------------------
    #-- basic database functions
    #----------------------------------------

    #----------------------------------------
    def sqlExec(self, sql, parmList=None):
        ''' make a database call '''
        cursorData = {}
        try:
            if parmList:
                exec_cursor = self.dbo.cursor().execute(sql, parmList)
            else:
                exec_cursor = self.dbo.cursor().execute(sql)
        except Exception as err:
            raise self.TranslateException(err)
        except self.sqlite3.DatabaseError as err:
            raise self.TranslateException(err)
        else:
            if exec_cursor:
                cursorData['OBJECT'] = exec_cursor
                cursorData['ROWS_AFFECTED'] = exec_cursor.rowcount
                if exec_cursor.description:
                    cursorData['COLUMN_HEADERS'] = [columnData[0] for columnData in exec_cursor.description]
        return cursorData

    #----------------------------------------
    def execMany(self, sql, parmList):
        ''' make a database call '''
        execSuccess = False
        try: cursor = self.dbo.cursor().executemany(sql, parmList)
        except Exception as err:
            raise self.TranslateException(err)
        except self.sqlite3.DatabaseError as err:
            raise self.TranslateException(err)
        else:
            execSuccess = True
        return execSuccess

    #----------------------------------------
    def close(self):
        self.dbo.close()

        return

    #----------------------------------------
    def fetchNext(self, cursorData):
        ''' fetch the next row from a cursor '''
        if 'COLUMN_HEADERS' in cursorData:
            rowValues = cursorData['OBJECT'].fetchone()
            if rowValues:
                type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in rowValues])
                rowData = dict(list(zip(cursorData['COLUMN_HEADERS'], type_fixed_row)))
            else:
                rowData = None
        else:
            print('WARNING: Previous SQL was not a query.')
            rowData = None

        return rowData

    #----------------------------------------
    def fetchRow(self, cursorData):
        ''' fetch the next row from a cursor '''
        if 'COLUMN_HEADERS' in cursorData:
            rowData = cursorData['OBJECT'].fetchone()
        else:
            print('WARNING: Previous SQL was not a query.')
            rowData = None

        return rowData

    #----------------------------------------
    def fetchAllRows(self, cursorData):
        ''' fetch all the rows without column names '''
        return cursorData['OBJECT'].fetchall()

    #----------------------------------------
    def fetchAllDicts(self, cursorData):
        ''' fetch all the rows with column names '''
        rowList = []
        rowData = self.fetchNext(cursorData)
        while rowData:
            rowList.append(rowData)
            rowData = self.fetchNext(cursorData)

        return rowList

    #---------------------------------------    
    def truncateTable(self, tableName_):
        if self.dbType == 'SQLITE3':
            sql = 'DELETE FROM ' + tableName_
        else:
            sql = 'truncate table ' + tableName_
            if self.dbType == 'DB2':
                sql += ' immediate'
        
        cursor = self.sqlExec(sql)

        return cursor 

    #---------------------------------------    
    def SetSchema(self):
        if self.dbType == 'SQLITE3':
            print('''WARNING: SQLITE3 doesn't support schema URI argument''')
            return False

        try:
            if self.dbType == 'MYSQL':
                self.sqlExec('use ' + self.schema)
            elif self.dbType == 'DB2':
                self.sqlExec('set current schema ' + self.schema)
                #--note: for some reason pyodbc not throwing error with set to invalid schema!
            elif self.dbType == 'POSTGRESQL':
                self.sqlExec('SET search_path TO ' + self.schema)
        except G2Exception.G2DBException as err:
            print(err)
            return False

        return True 

    #--------------------
    #--utility functions
    #--------------------

    #---------------------------------------    
    def dburiParse(self, dbUri):
        ''' parse the database uri string '''
        #--pull off the table parameter if supplied
        tbl = None
        self.schema = None
        if '/?' in dbUri:
            parm = dbUri.split('/?')[1]
            dbUri = dbUri.split('/?')[0]
            for item in parm.split('&'):
                parmType = item.split('=')[0]
                parmValue = item.split('=')[1]
                if parmType.upper() == 'TABLE':
                    tbl = parmValue
                elif parmType.upper() == 'SCHEMA':
                    self.schema = parmValue

        #--get database type
        if '://' in dbUri:
            dbtype = dbUri.split('://')[0].upper()
            dbUriData = dbUri.split('://')[1]
        else:
            dbtype = 'UNKNOWN'
            dbUriData = dbUri

        #--separate login and dsn info
        if '@' in dbUriData:
            justUidPwd = dbUriData.split('@')[0]
            justDsnSch = dbUriData.split('@')[1]
        else: #--just dsn with trusted connection?
            justUidPwd = ':'
            justDsnSch = dbUriData
        justDsnSch = justDsnSch.rstrip('/')
 
        #--separate uid and password
        if ':' in justUidPwd:
            uid = justUidPwd.split(':')[0]
            pwd = justUidPwd.split(':')[1]
        else: #--just uid with no password?
            uid = justUidPwd
            pwd = ''
 
        #--separate dsn and port
        if justDsnSch[1:3] == ":\\":
            dsn = justDsnSch
            self.port = None
        elif ':' in justDsnSch:
            dsn = justDsnSch.split(':')[0]
            self.port = justDsnSch.split(':')[1]
            if dbtype == 'POSTGRESQL':
              dsn = justDsnSch.split(':')[2]
        else: #--just dsn with no port
            dsn = justDsnSch
            self.port = None
 
        #--create the return dictionary
        self.dbType = dbtype
        self.dsn = dsn
        self.userId = uid
        self.password = pwd
        self.table = tbl

        if self.dbType not in ('DB2', 'MYSQL', 'SQLITE3', 'POSTGRESQL'):
            raise G2Exception.G2UnsupportedDatabaseType('ERROR: ' + self.dbType + ' is an unsupported database type')

        return

    #----------------------------------------
    def pause(self, question = None):
        if not question:
            v_wait = input("PRESS ENTER TO CONTINUE ... ")
        else:
            v_wait = input(question)
        return v_wait

    #----------------------------------------
    def TranslateException(self, ex):
        '''Translate DB specific exception into a common exception'''
        errMessage = ex.message #--default for all database types
        if self.dbType == 'DB2':
            errMessage = ex.args[1] #--ex.message is empty for db2!
            if ex.args[0] == '42S02':
                return G2Exception.G2DBUnknownException(errMessage)
            if ex.args[0] == '42704':
                return G2Exception.G2TableNoExist(errMessage)
            elif ex.args[0] == '23505':
                return G2Exception.G2DBUniqueConstraintViolation(errMessage)

        elif self.dbType == 'SQLITE3':
            if type(ex) == self.sqlite3.OperationalError:
                if errMessage.startswith('no such table'):
                    return G2Exception.G2TableNoExist(errMessage)
            if type(ex) == self.sqlite3.IntegrityError:
                if 'not unique' in errMessage:
                    return G2Exception.G2DBUniqueConstraintViolation(errMessage)
        elif self.dbType == 'MYSQL':
            errMessage = ex.args[1]
            pass
        elif self.dbType == 'POSTGRESQL':
            errMessage = ex.args[1]
            pass
        else:
            raise G2Exception.G2UnsupportedDatabaseType('ERROR: ' + self.dbType + ' is an unsupported database type')
        raise G2Exception.G2DBUnknownException(errMessage)

#----------------------------------------
if __name__ == "__main__":

    #--running in debug mode - no parameters
    if len(sys.argv) == 1:
        dbUri = 'db2://db2inst1:db2admin@g2'

    #--capture the command line arguments
    else:
        optParser = optparse.OptionParser()
        optParser.add_option('-d', '--dbUri', dest='dbUri', default='', help='a database uri such as: db2://user:pwd@dsn:schema')
        (options, args) = optParser.parse_args()
        dbUri = options.dbUri
 
    #--create an instance
    testDbo = g2Database(dbUri)
    if testDbo.success:
        print('SUCCESS: connection to ' + testDbo.dsn + ' successful!')
        cursor = testDbo.sqlExec('''select * from SRD_PRODUCT_VERSIONS where PRODUCT = '%s' ''' % ('PIPELINE',))
        if not cursor:
            print('ERROR: ' + testDbo.dsn + ' does not appear to be a g2 database')

    else:
        print('ERROR: connection to ' + testDbo.dsn + ' failed!')

    sys.exit()

