#! /usr/bin/env python3

import os
import sys
import json
import urllib.parse
from importlib import import_module

# ======================
class G2Database:

    # ----------------------------------------
    def __init__(self, param_str):
        self.success = False
        self.imports = []
        if not param_str.startswith('{'): # for backwards compatibility
            param_data = {'SQL': {"CONNECTION": param_str}}
        else:
            param_data = json.loads(param_str)

        self.connections = {'MAIN': {}}
        self.tables_by_connection = {}
        self.statement_cache = {}

        self.Connect('MAIN', param_data['SQL']['CONNECTION'])

        for table_name in param_data.get('HYBRID', {}).keys():
            node = param_data['HYBRID'][table_name]
            if node not in self.connections:
                self.connections[node] = {}
                self.Connect(node, param_data[node]['DB_1'])
            self.tables_by_connection[table_name] = node

        self.success = True

    # ----------------------------------------
    def Connect(self, node, dburi):
        """ connect to the database """

        # --parse the uri
        try:
            self.dburi_parse(node, dburi)
        except Exception as err:
            raise Exception(err)

        # --import correct modules for DB type
        self.connections[node]['psycopg2'] = False
        self.connections[node]['cx_Oracle'] = False
        if self.connections[node]['dbtype'] in ('MYSQL', 'DB2', 'POSTGRESQL', 'MSSQL'):
            if self.connections[node]['dbtype'] == 'POSTGRESQL':
                # Ensure have required args
                try:
                    if 'psycopg2' not in self.imports:
                        self.psycopg2 = import_module('psycopg2')
                        self.imports.append('psycopg2')
                    self.connections[node]['psycopg2'] = True
                except ImportError as ex:
                    print('WARNING: postgres database driver (psycopg2) recommended')
            if not self.connections[node]['psycopg2']:
                try:
                    if 'pyodbc' not in self.imports:
                        self.pyodbc = import_module('pyodbc')
                        self.imports.append('pyodbc')
                except ImportError as err:
                    raise ImportError('ERROR: could not import pyodbc module\n\nPlease check the Senzing help center: https://senzing.zendesk.com/hc/en-us/search?utf8=%E2%9C%93&query=pyodbc\n\t')
        elif self.connections[node]['dbtype'] == 'OCI':
            try:
                if 'cx_Oracle' not in self.imports:
                    self.cx_Oracle = import_module('cx_Oracle')
                    self.imports.append('cx_Oracle')
                self.connections[node]['cx_Oracle'] = True
            except ImportError as ex:
                raise ImportError('ERROR: could not import cx_Oracle')
        else:
            try:
                if 'sqlite3' not in self.imports:
                    self.sqlite3 = import_module('sqlite3')
                    self.imports.append('sqlite3')
            except ImportError as err:
                raise ImportError('ERROR: could not import sqlite3 module\n\nPlease ensure the python sqlite3 module is available')

        try:
            if self.connections[node]['dbtype'] == 'MYSQL':
                self.connections[node]['dbo'] = self.pyodbc.connect('DRIVER={' + self.connections[node]['dbtype'] + '};SERVER=' + self.connections[node]['dsn'] + ';PORT=' + self.connections[node]['port'] + ';DATABASE=' + self.connections[node]['schema'] + ';UID=' + self.connections[node]['userid'] + '; PWD=' + self.connections[node]['password'], autocommit=True)
            elif self.connections[node]['dbtype'] == 'SQLITE3':
                if not os.path.isfile(self.connections[node]['dsn']):
                    raise Exception('ERROR: sqlite3 database file not found ' + self.connections[node]['dsn'])
                self.connections[node]['dbo'] = self.sqlite3.connect(self.connections[node]['dsn'], isolation_level=None)
                self.connections[node]['dbo'].text_factory = str
                c = self.connections[node]['dbo'].cursor()
                c.execute("PRAGMA journal_mode=wal")
                c.execute("PRAGMA synchronous=0")
            elif self.connections[node]['dbtype'] == 'DB2':
                self.connections[node]['dbo'] = self.pyodbc.connect('DSN=' + self.connections[node]['dsn'] + '; UID=' + self.connections[node]['userid'] + '; PWD=' + self.connections[node]['password'], autocommit=True)
            elif self.connections[node]['dbtype'] == 'POSTGRESQL':
                conn_str = 'DSN=' + self.connections[node]['dsn'] + ';UID=' + self.connections[node]['userid'] + ';PWD=' + self.connections[node]['password'] + ';'
                if self.connections[node]['psycopg2']:
                    self.connections[node]['dbo'] = self.psycopg2.connect(host=self.connections[node]['host'], port=self.connections[node]['port'], dbname=self.connections[node]['dsn'], user=self.connections[node]['userid'], password=self.connections[node]['password'])
                    self.connections[node]['dbo'].set_session(autocommit=True, isolation_level='READ UNCOMMITTED')
                else:
                    self.connections[node]['dbo'] = self.pyodbc.connect(conn_str, autocommit=True)
            elif self.connections[node]['dbtype'] == 'MSSQL':
                self.connections[node]['dbo'] = self.pyodbc.connect('DSN=' + self.connections[node]['dsn'] + '; UID=' + self.connections[node]['userid'] + '; PWD=' + self.connections[node]['password'], autocommit=True)
            elif self.connections[node]['dbtype'] == 'OCI':
                self.connections[node]['dbo'] = self.cx_Oracle.connect(user=self.connections[node]['userid'], password=self.connections[node]['password'], dsn=f"{self.connections[node]['host']}:{self.connections[node]['port']}/{self.connections[node]['schema']}", encoding="UTF-8")
            else:
                raise Exception('Unsupported DB Type: ' + self.connections[node]['dbtype'])
        except Exception as err:
            raise Exception(err) # self.TranslateException(err)
        except self.sqlite3.DatabaseError as err:
            raise Exception(err) # self.TranslateException(err)

        if self.connections[node]['schema'] is not None and len(self.connections[node]['schema']) != 0:
            if self.connections[node]['dbtype'] == 'SQLITE3':
                raise Exception('''WARNING: SQLITE3 doesn't support schema URI argument''')
            try:
                if self.connections[node]['dbtype'] == 'MYSQL':
                    self.sqlExec('use ' + self.connections[node]['schema'])
                elif self.connections[node]['dbtype'] == 'DB2':
                    self.sqlExec('set current schema ' + self.connections[node]['schema'])
                    # --note: for some reason pyodbc not throwing error with set to invalid schema!
                elif self.connections[node]['dbtype'] == 'POSTGRESQL':
                    self.sqlExec('SET search_path TO ' + self.connections[node]['schema'])
            except Exception as err:
                raise Exception(err)


    def set_node(self, sql):
        if len(self.connections) == 1:
            return 'MAIN'

        node_list = []
        for table in self.tables_in_query(sql):
            node_list.append(self.tables_by_connection.get(table.upper(), 'MAIN'))

        #print('-' * 10)
        #print(sql)
        #print(self.tables_in_query(sql))
        #print(node_list)

        if len(node_list) == 0:
            raise Exception(f"Could not determine tables from sql statement\n{sql}")
        if len(set(node_list)) > 1:
            raise Exception(f"Cannot query across nodes in hybrid setup\n{sql}\n{self.tables_in_query(sql)}\n{node_list}")
        return node_list[0]


    def tables_in_query(self, sql):
        result = []
        tokens = sql.split()
        i = 0
        while True:
            if tokens[i].upper() in ('FROM', 'JOIN'):
                i += 1
                result.append(tokens[i])
            i += 1
            if i >= len(tokens):
                break
        return result

    # ----------------------------------------
    def close(self):
        for node in self.connections.keys():
            self.connections[node]['dbo'].close()

        return

    # ----------------------------------------
    def sqlPrep(self, sql):  # left in for backwards compatibility
        node = self.set_node(sql)

        if self.connections[node]['psycopg2']:
            sql = sql.replace('?', '%s')
        elif self.connections[node]['cx_Oracle']:
            i = 0
            while '?' in sql:
                i += 1
                sql = sql.replace('?', f":{i}", 1)
        return sql

    def sqlPrep2(self, sql):
        node = self.set_node(sql)

        if self.connections[node]['psycopg2']:
            sql = sql.replace('?', '%s')
        elif self.connections[node]['cx_Oracle']:
            i = 0
            while '?' in sql:
                i += 1
                sql = sql.replace('?', f":{i}", 1)
        return sql, node

    # ----------------------------------------
    # basic database functions
    # ----------------------------------------

    # ----------------------------------------
    def sqlExec(self, rawsql, parmList=None, **kwargs):
        ''' make a database call '''
        if rawsql in self.statement_cache:
            sql = self.statement_cache[rawsql]['sql']
            node = self.statement_cache[rawsql]['node']
        else:
            sql, node = self.sqlPrep2(rawsql)
            self.statement_cache[rawsql] = {'sql': sql, 'node': node}

        if parmList and type(parmList) not in (list, tuple):
            parmList = [parmList]

        # --name and itersize are postgres server side cursor settings
        cursorData = {}
        cursorData['NAME'] = kwargs['name'] if 'name' in kwargs else None
        cursorData['ITERSIZE'] = kwargs['itersize'] if 'itersize' in kwargs else None

        try:
            if cursorData['NAME'] and self.connections[node]['psycopg2']:
                exec_cursor = self.connections[node]['dbo'].cursor(cursorData['NAME'])
                if cursorData['ITERSIZE']:
                    exec_cursor.itersize = cursorData['ITERSIZE']
            else:
                exec_cursor = self.connections[node]['dbo'].cursor()
            if parmList:
                exec_cursor.execute(sql, parmList)
            else:
                exec_cursor.execute(sql)

        except Exception as err:
            raise Exception(f"sqlerror: {err}\n{sql}\n")

        if exec_cursor:
            cursorData['CURSOR'] = exec_cursor
            cursorData['ROWS_AFFECTED'] = exec_cursor.rowcount
            if exec_cursor.description:
                cursorData['COLUMN_HEADERS'] = [columnData[0].upper() for columnData in exec_cursor.description]
        return cursorData

    # ----------------------------------------
    def fetchNext(self, cursorData):
        ''' fetch the next row from a cursor '''
        if 'COLUMN_HEADERS' in cursorData:
            rowValues = cursorData['CURSOR'].fetchone()
            if rowValues:
                type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in rowValues])
                rowData = dict(list(zip(cursorData['COLUMN_HEADERS'], type_fixed_row)))
            else:
                rowData = None
        else:
            raise Exception('WARNING: Previous SQL was not a query.')

        return rowData

    # ----------------------------------------
    def fetchRow(self, cursorData):
        ''' fetch the next row from a cursor '''
        if 'COLUMN_HEADERS' in cursorData:
            rowData = cursorData['CURSOR'].fetchone()
        else:
            raise Exception('WARNING: Previous SQL was not a query.')

        return rowData

    # ----------------------------------------
    def fetchAllRows(self, cursorData):
        ''' fetch all the rows without column names '''
        return cursorData['CURSOR'].fetchall()

    # ----------------------------------------
    def fetchAllDicts(self, cursorData):
        ''' fetch all the rows with column names '''
        rowList = []
        for rowValues in cursorData['CURSOR'].fetchall():
            type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in rowValues])
            rowData = dict(list(zip(cursorData['COLUMN_HEADERS'], type_fixed_row)))
            rowList.append(rowData)

        return rowList

    # ----------------------------------------
    def fetchManyRows(self, cursorData, rowCount):
        ''' fetch all the rows without column names '''
        return cursorData['CURSOR'].fetchmany(rowCount)

    # ----------------------------------------
    def fetchManyDicts(self, cursorData, rowCount):
        ''' fetch all the rows with column names '''
        rowList = []
        for rowValues in cursorData['CURSOR'].fetchmany(rowCount):
            type_fixed_row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in rowValues])
            rowData = dict(list(zip(cursorData['COLUMN_HEADERS'], type_fixed_row)))
            rowList.append(rowData)

        return rowList

    # ---------------------------------------
    def truncateTable(self, tableName_):
        node = self.set_node('from ' + tableName_)

        if self.connections[node]['dbtype'] == 'SQLITE3':
            sql = 'DELETE FROM ' + tableName_
        else:
            sql = 'truncate table ' + tableName_
            if self.connections[node]['dbtype'] == 'DB2':
                sql += ' immediate'

        cursor = self.sqlExec(sql)

        return cursor

    # --------------------
    # --utility functions
    # --------------------

    def dburi_parse(self, node, dburi):
        ''' Parse the database uri string '''

        uri_dict = {}

        try:

            # Pull off the table parameter if supplied
            uri_dict['TABLE'] = uri_dict['SCHEMA'] = uri_dict['PORT'] = None

            if '/?' in dburi:
                (dburi, parm) = tuple(dburi.split('/?'))
                for item in parm.split('&'):
                    (parmType, parmValue) = tuple(item.split('='))
                    uri_dict['TABLE'] = parmValue if parmType.upper() == 'TABLE' else None
                    uri_dict['SCHEMA'] = parmValue if parmType.upper() == 'SCHEMA' else None

            # Get database type
            (uri_dict['DBTYPE'], dburiData) = dburi.split('://') if '://' in dburi else ('UNKNOWN', dburi)
            uri_dict['DBTYPE'] = uri_dict['DBTYPE'].upper()

            # Separate login and dsn info
            (justUidPwd, justDsnSch) = dburiData.split('@') if '@' in dburiData else (':', dburiData)
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
            raise Exception(f'Failed to parse database URI, check the connection string(s) in your G2Module INI file.') from None

        if not uri_dict['DSN']:
            raise Exception(f'Missing database DSN. \n{self.show_connection(uri_dict, False, False)}')

        self.connections[node]['dbtype'] = uri_dict['DBTYPE'] if 'DBTYPE' in uri_dict else None
        self.connections[node]['dsn'] = uri_dict['DSN'] if 'DSN' in uri_dict else None
        self.connections[node]['host'] = uri_dict['HOST'] if 'HOST' in uri_dict else None
        self.connections[node]['port'] = uri_dict['PORT'] if 'PORT' in uri_dict else None
        self.connections[node]['userid'] = urllib.parse.unquote(uri_dict['USERID']) if 'USERID' in uri_dict else None
        self.connections[node]['password'] = urllib.parse.unquote(uri_dict['PASSWORD']) if 'PASSWORD' in uri_dict else None
        self.connections[node]['table'] = uri_dict['TABLE'] if 'TABLE' in uri_dict else None
        self.connections[node]['schema'] = uri_dict['SCHEMA'] if 'SCHEMA' in uri_dict else None

        if self.connections[node]['dbtype'] not in ('DB2', 'MYSQL', 'SQLITE3', 'POSTGRESQL', 'MSSQL', 'OCI'):
            raise Exception(self.connections[node]['dbtype'] + ' is an unsupported database type')

        return uri_dict


# ----------------------------------------
if __name__ == "__main__":


    if os.getenv("SENZING_ENGINE_CONFIGURATION_JSON"):
        g2module_params = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON")
    else:
        import G2Paths
        from G2IniParams import G2IniParams
        ini_file_name = G2Paths.get_G2Module_ini_path()
        ini_param_creator = G2IniParams()
        g2module_params = ini_param_creator.getJsonINIParams(ini_file_name)

    try:
        g2dbo = G2Database(g2module_params)
    except Exception as err:
        print(err)
        sys.exit(1)

    # --create an instance
    print('\nConnection successful!\n')

    # --running in debug mode - no parameters
    if len(sys.argv) > 1:
        sql = ' '.join(sys.argv[1:])

        try:
            cursor = g2dbo.sqlExec(sql)
        except Exception as err:
            print(err)
            sys.exit(1)

        if cursor.get('COLUMN_HEADERS'):
            print(','.join(cursor.get('COLUMN_HEADERS')))
            for row in cursor['CURSOR']:
                print(','.join(row))
            print()
    if cursor.get('ROWS_AFFECTED', 0) > 0:
        print(cursor.get('ROWS_AFFECTED'), 'rows affected')
        print()

    sys.exit(0)
