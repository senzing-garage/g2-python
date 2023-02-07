#! /usr/bin/env python3

import json
import optparse
import os
import sys
import textwrap
import urllib.parse
from importlib import import_module
from senzing import G2Exception


# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------


class G2DBException(Exception):
    '''Base exception for G2 DB related python code'''

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class G2DBMNotStarted(G2DBException):

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class G2DBNotFound(G2DBException):

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class G2DBUniqueConstraintViolation(G2DBException):

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class G2DBUnknownException(G2DBException):

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class G2TableNoExist(G2DBException):

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


class G2UnsupportedDatabaseType(G2DBException):

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)


# ======================
class G2Database:

    # ----------------------------------------
    def __init__(self, dbUri):
        """ open the database """
        self.success = False

        # --parse the uri
        try:
            self.dburi_parse(dbUri)
        except G2UnsupportedDatabaseType as err:
            print(err)
            return

        # --import correct modules for DB type
        self.using_pyscopg2 = False
        self.using_cx_Oracle = False
        if self.dbType in ('MYSQL', 'DB2', 'POSTGRESQL', 'MSSQL'):
            if self.dbType == 'POSTGRESQL':
                # Ensure have required args
                try:
                    self.psycopg2 = import_module('psycopg2')
                    self.using_pyscopg2 = True
                except ImportError as ex:
                    print('WARNING: postgres database driver (psycopg2) recommended')
            if not self.using_pyscopg2:
                try:
                    self.pyodbc = import_module('pyodbc')
                except ImportError as err:
                    raise ImportError('ERROR: could not import pyodbc module\n\nPlease check the Senzing help center: https://senzing.zendesk.com/hc/en-us/search?utf8=%E2%9C%93&query=pyodbc\n\t')
        elif self.dbType == 'OCI':
            try:
                self.cx_Oracle = import_module('cx_Oracle')
                self.using_cx_Oracle = True
            except ImportError as ex:
                raise ImportError('ERROR: could not import cx_Oracle')
        else:
            try:
                self.sqlite3 = import_module('sqlite3')
            except ImportError as err:
                raise ImportError('ERROR: could not import sqlite3 module\n\nPlease ensure the python sqlite3 module is available')

        # --attempt to open the database
        try:
            self.Connect()
        except G2DBMNotStarted as err:
            print('ERROR: Database Manager not started')
            print(err)
            return
        except G2DBNotFound as err:
            print('ERROR: Database not found')
            print(err)
            return
        except Exception as err:
            # print(self)
            raise Exception(err)
            # print('ERROR: could not open database ' + self.dsn)
            # print(err)
            # print(type(err))
            # print()
            # return
        else:
            # --attempt to set the schema (if there is one)import
            if self.schema is not None and len(self.schema) != 0:
                if not self.SetSchema():
                    print(self)
                    print('ERROR: could not connect to schema')

                    return

            # --handle utf-8 issues for sqlite3
            if self.dbType == 'SQLITE3':
                self.dbo.text_factory = str

            self.success = True

        return

    # ----------------------------------------
    def Connect(self):

        try:
            if self.dbType == 'MYSQL':
                self.dbo = self.pyodbc.connect('DRIVER={' + self.dbType + '};SERVER=' + self.dsn + ';PORT=' + self.port + ';DATABASE=' + self.schema + ';UID=' + self.userId + '; PWD=' + self.password, autocommit=True)
            elif self.dbType == 'SQLITE3':
                if not os.path.isfile(self.dsn):
                    raise G2DBNotFound('ERROR: sqlite3 database file not found ' + self.dsn)
                self.dbo = self.sqlite3.connect(self.dsn, isolation_level=None)
                c = self.dbo.cursor()
                c.execute("PRAGMA journal_mode=wal")
                c.execute("PRAGMA synchronous=0")
            elif self.dbType == 'DB2':
                self.dbo = self.pyodbc.connect('DSN=' + self.dsn + '; UID=' + self.userId + '; PWD=' + self.password, autocommit=True)
            elif self.dbType == 'POSTGRESQL':
                conn_str = 'DSN=' + self.dsn + ';UID=' + self.userId + ';PWD=' + self.password + ';'
                if self.using_pyscopg2:
                    self.dbo = self.psycopg2.connect(host=self.host, port=self.port, dbname=self.dsn, user=self.userId, password=self.password)
                    self.dbo.set_session(autocommit=True, isolation_level='READ UNCOMMITTED', readonly=True)
                else:
                    self.dbo = self.pyodbc.connect(conn_str, autocommit=True)
            elif self.dbType == 'MSSQL':
                self.dbo = self.pyodbc.connect('DSN=' + self.dsn + '; UID=' + self.userId + '; PWD=' + self.password, autocommit=True)
            elif self.dbType == 'OCI':
                self.dbo = self.cx_Oracle.connect(user=self.userId, password=self.password, dsn=f"{self.host}:{self.port}/{self.schema}", encoding="UTF-8")
            else:
                raise Exception('Unsupported DB Type: ' + self.dbType)
        except Exception as err:
            raise self.TranslateException(err)
        except self.sqlite3.DatabaseError as err:
            raise self.TranslateException(err)

        return

    # ----------------------------------------
    # -- basic database functions
    # ----------------------------------------

    # ----------------------------------------
    def sqlPrep(self, sql):
        if self.using_pyscopg2:
            sql = sql.replace('?', '%s')
        elif self.using_cx_Oracle:
            i = 0
            while '?' in sql:
                i += 1
                sql = sql.replace('?', f":{i}", 1)
        return sql

    # ----------------------------------------
    def sqlExec(self, sql, parmList=None, **kwargs):
        ''' make a database call '''
        if parmList and type(parmList) not in (list, tuple):
            parmList = [parmList]
        # --name and itersize are postgres server side cursor settings
        cursorData = {}
        cursorData['NAME'] = kwargs['name'] if 'name' in kwargs else None
        cursorData['ITERSIZE'] = kwargs['itersize'] if 'itersize' in kwargs else None

        try:
            if cursorData['NAME'] and self.using_pyscopg2:
                exec_cursor = self.dbo.cursor(cursorData['NAME'])
                if cursorData['ITERSIZE']:
                    exec_cursor.itersize = cursorData['ITERSIZE']
            else:
                exec_cursor = self.dbo.cursor()
            if parmList:
                exec_cursor.execute(sql, parmList)
            else:
                exec_cursor.execute(sql)

        except Exception as err:
            # print('ERR: ' + str(err))
            # print('SQL: ' + sql)
            # if parmList:
            #    print('PARMS:', type(parmList), parmList)
            raise err

        if exec_cursor:
            cursorData['OBJECT'] = exec_cursor
            cursorData['ROWS_AFFECTED'] = exec_cursor.rowcount
            if exec_cursor.description:
                cursorData['COLUMN_HEADERS'] = [columnData[0].upper() for columnData in exec_cursor.description]
        return cursorData

    # ----------------------------------------
    def close(self):
        self.dbo.close()

        return

    # ----------------------------------------
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

    # ----------------------------------------
    def fetchRow(self, cursorData):
        ''' fetch the next row from a cursor '''
        if 'COLUMN_HEADERS' in cursorData:
            rowData = cursorData['OBJECT'].fetchone()
        else:
            print('WARNING: Previous SQL was not a query.')
            rowData = None

        return rowData

    # ----------------------------------------
    def fetchAllRows(self, cursorData):
        ''' fetch all the rows without column names '''
        return cursorData['OBJECT'].fetchall()

    # ----------------------------------------
    def fetchAllDicts(self, cursorData):
        ''' fetch all the rows with column names '''
        rowList = []
        for rowValues in cursorData['OBJECT'].fetchall():
            type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in rowValues])
            rowData = dict(list(zip(cursorData['COLUMN_HEADERS'], type_fixed_row)))
            rowList.append(rowData)

        return rowList

    # ----------------------------------------
    def fetchManyRows(self, cursorData, rowCount):
        ''' fetch all the rows without column names '''
        return cursorData['OBJECT'].fetchmany(rowCount)

    # ----------------------------------------
    def fetchManyDicts(self, cursorData, rowCount):
        ''' fetch all the rows with column names '''
        rowList = []
        for rowValues in cursorData['OBJECT'].fetchmany(rowCount):
            type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in rowValues])
            rowData = dict(list(zip(cursorData['COLUMN_HEADERS'], type_fixed_row)))
            rowList.append(rowData)

        return rowList

    # ---------------------------------------
    def truncateTable(self, tableName_):
        if self.dbType == 'SQLITE3':
            sql = 'DELETE FROM ' + tableName_
        else:
            sql = 'truncate table ' + tableName_
            if self.dbType == 'DB2':
                sql += ' immediate'

        cursor = self.sqlExec(sql)

        return cursor

    # ---------------------------------------
    def SetSchema(self):
        if self.dbType == 'SQLITE3':
            print('''WARNING: SQLITE3 doesn't support schema URI argument''')
            return False

        try:
            if self.dbType == 'MYSQL':
                self.sqlExec('use ' + self.schema)
            elif self.dbType == 'DB2':
                self.sqlExec('set current schema ' + self.schema)
                # --note: for some reason pyodbc not throwing error with set to invalid schema!
            elif self.dbType == 'POSTGRESQL':
                self.sqlExec('SET search_path TO ' + self.schema)
        except G2DBException as err:
            print(err)
            return False

        return True

    # --------------------
    # --utility functions
    # --------------------

    def dburi_parse(self, dbUri):
        ''' Parse the database uri string '''

        uri_dict = {}

        try:

            # Pull off the table parameter if supplied
            uri_dict['TABLE'] = uri_dict['SCHEMA'] = uri_dict['PORT'] = None

            if '/?' in dbUri:
                (dbUri, parm) = tuple(dbUri.split('/?'))
                for item in parm.split('&'):
                    (parmType, parmValue) = tuple(item.split('='))
                    uri_dict['TABLE'] = parmValue if parmType.upper() == 'TABLE' else None
                    uri_dict['SCHEMA'] = parmValue if parmType.upper() == 'SCHEMA' else None

            # Get database type
            (uri_dict['DBTYPE'], dbUriData) = dbUri.split('://') if '://' in dbUri else ('UNKNOWN', dbUri)
            uri_dict['DBTYPE'] = uri_dict['DBTYPE'].upper()

            # Separate login and dsn info
            (justUidPwd, justDsnSch) = dbUriData.split('@') if '@' in dbUriData else (':', dbUriData)
            justDsnSch = justDsnSch.rstrip('/')

            # Separate uid and password
            (uri_dict['USERID'], uri_dict['PASSWORD']) = justUidPwd.split(':') if ':' in justUidPwd else (justUidPwd, '')

            # Separate dsn and port
            if justDsnSch[1:3] == ":\\":
                uri_dict['DSN'] = justDsnSch
            elif ':' in justDsnSch:
                uri_dict['DSN'] = justDsnSch.split(':')[0]
                uri_dict['PORT'] = justDsnSch.split(':')[1]
                # PostgreSQL & MySQL use a slightly different connection string
                # e.g. CONNECTION=postgresql://userid:password@localhost:5432:postgres
                # postgres == odbc.ini datasource entry
                if uri_dict['DBTYPE'] in ('POSTGRESQL', 'MYSQL'):
                    uri_dict['HOST'] = uri_dict['DSN']
                    uri_dict['DSN'] = justDsnSch.split(':')[2]
                # oracle syntax is port/database or sid (placing in schema field)
                # e.g. CONNECTION=oci://userid:password@//192.168.1.111:1521/G2PDB
                elif uri_dict['DBTYPE'] == 'OCI':
                    uri_dict['HOST'] = uri_dict['DSN'].replace('/','') #--get rid of the // if they used it
                    items = uri_dict['PORT'].split('/')
                    uri_dict['PORT'] = items[0]
                    uri_dict['SCHEMA'] = items[1]

            else:  # Just dsn with no port
                uri_dict['DSN'] = justDsnSch

        except (IndexError, ValueError) as ex:
            raise G2DBException(f'Failed to parse database URI, check the connection string(s) in your G2Module INI file.') from None

        if not uri_dict['DSN']:
            raise G2DBException(f'Missing database DSN. \n{self.show_connection(uri_dict, False, False)}')

        self.dbType = uri_dict['DBTYPE'] if 'DBTYPE' in uri_dict else None
        self.dsn = uri_dict['DSN'] if 'DSN' in uri_dict else None
        self.host = uri_dict['HOST'] if 'HOST' in uri_dict else None
        self.port = uri_dict['PORT'] if 'PORT' in uri_dict else None
        self.userId = urllib.parse.unquote(uri_dict['USERID']) if 'USERID' in uri_dict else None
        self.password = urllib.parse.unquote(uri_dict['PASSWORD']) if 'PASSWORD' in uri_dict else None
        self.table = uri_dict['TABLE'] if 'TABLE' in uri_dict else None
        self.schema = uri_dict['SCHEMA'] if 'SCHEMA' in uri_dict else None

        if self.dbType not in ('DB2', 'MYSQL', 'SQLITE3', 'POSTGRESQL', 'MSSQL', 'OCI'):
            print(json.dumps(uri_dict, indent=4))
            raise G2UnsupportedDatabaseType('ERROR: ' + self.dbType + ' is an unsupported database type')

        return uri_dict

    # ----------------------------------------
    def pause(self, question=None):
        if not question:
            v_wait = input("PRESS ENTER TO CONTINUE ... ")
        else:
            v_wait = input(question)
        return v_wait

    # ----------------------------------------
    def TranslateException(self, ex):
        '''Translate DB specific exception into a common exception'''

        # Python3 no longer supports message attribute, uses args
        # errMessage = ex.message #--default for all database types

        if self.dbType == 'DB2':
            errMessage = ex.args[1]
            if ex.args[0] == '42S02':
                return G2DBUnknownException(errMessage)
            if ex.args[0] == '42704':
                return G2TableNoExist(errMessage)
            elif ex.args[0] == '23505':
                return G2DBUniqueConstraintViolation(errMessage)

        elif self.dbType == 'SQLITE3':
            # if type(ex) == self.sqlite3.OperationalError:
            #     if errMessage.startswith('no such table'):
            #         return G2TableNoExist(errMessage)
            # if type(ex) == self.sqlite3.IntegrityError:
            #     if 'not unique' in errMessage:
            # SQLITE3 only returns 1 arg
            errMessage = ex.args[0]
        elif self.dbType in ('MYSQL', 'POSTGRESQL'):
            errMessage = ex.args[1] if len(ex.args) > 1 else ex.args[0]
        else:
            errMessage = ex

        # return G2DBUnknownException(errMessage)
        return G2DBException(errMessage)


# ----------------------------------------
if __name__ == "__main__":

    # --running in debug mode - no parameters
    if len(sys.argv) == 1:
        dbUri = 'db2://db2inst1:db2admin@g2'

    # --capture the command line arguments
    else:
        optParser = optparse.OptionParser()
        optParser.add_option('-d', '--dbUri', dest='dbUri', default='', help='a database uri such as: db2://user:pwd@dsn:schema')
        (options, args) = optParser.parse_args()
        dbUri = options.dbUri

    # --create an instance
    testDbo = g2Database(dbUri)
    if testDbo.success:
        print('SUCCESS: connection to ' + testDbo.dsn + ' successful!')
        cursor = testDbo.sqlExec('''select * from SRD_PRODUCT_VERSIONS where PRODUCT = '%s' ''' % ('PIPELINE',))
        if not cursor:
            print('ERROR: ' + testDbo.dsn + ' does not appear to be a g2 database')

    else:
        print('ERROR: connection to ' + testDbo.dsn + ' failed!')

    sys.exit()
