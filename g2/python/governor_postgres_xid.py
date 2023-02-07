# --------------------------------------------------------------------------------------------------------------
# Class: Governor
#
# Sample to demonstrate G2Loader calling Govern after each record read. Detects XID age in Postgres database(s),
# if the threshold age is detected all threads are paused until the resume age value (or less) is detected,
# upon this detection threads and processing is resumed.
#
# XID age is reduced with the Postgres vacuum command. This sample doesn't attempt to issue the vacuum command;
# the user running G2Loader may not have the privileges to do so. When the age threshold is detected and G2Loader
# pauses, manually issue a vacuum command.
#
# Senzing can run in a clustered database mode where the engine handles up to 3 separate DBs itself:
# https://senzing.zendesk.com/hc/en-us/articles/360010599254-Scaling-Out-Your-Database-With-Clustering
#
# This sample uses the native Python Postgres driver psycopg2.
# Full details on installation: https://www.psycopg.org/docs/install.html
# Basic installation: pip3 install psycopg2 --user
#
# --------------------------------------------------------------------------------------------------------------

import json
import logging
import sys
import textwrap
import threading
import time
import urllib.parse
from datetime import datetime
from importlib import import_module

# -----------------------------------------------------------------------------
# Exceptions
# -----------------------------------------------------------------------------


class G2DBException(Exception):
    """ Base exception for G2 DB related python code """


# -----------------------------------------------------------------------------
# Class: Governor
# -----------------------------------------------------------------------------


class Governor:

    def __init__(self, thread_stop, *args, **kwargs):

        # Setup logging if caller uses it - Stream Loader uses logging, G2Loader doesn't yet
        self.gov_logger = logging.getLogger(__name__)
        self.gov_logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        gov_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(gov_formatter)
        self.gov_logger.addHandler(console_handler)

        # General
        self.kwargs = kwargs
        self.valid_frequencies = ['record', 'source']
        self.ignore_ini_stanzas = ['PIPELINE', 'HYBRID']
        self.connect_strs = []
        self.connect_dict = {}
        self.next_check_time = time.time()
        self.sql_stmt = "SELECT age(datfrozenxid) FROM pg_database WHERE datname = (%s);"
        self.threads_lock = threading.Lock()

        # To check if main threads from G2Loader stopped, or CTRL-C pressed to exit
        self.thread_stop = thread_stop

        # Set defaults, override at governor init if required
        #
        # frequency = row or source. The frequency the governor runs at, this must be specfied
        # interval = Governor called every row x number of records - used for per record or redo governor not per source
        # check_time = In addition to checking every interval, check every x seconds too
        # xid_age = Value of Postgres XID age to pause at for a vacuum to be completed (highwater mark to pause at)
        # wait_time = Period in seconds to pause processing for between each check of XID age once triggered
        # resume_age = XID age to resume processing at (lowwater mark lower than xid_age)
        # govern_debug = To call debug functions (if used)
        # use_logging = Future use
        # pre_post_msgs = Show pre and post messages?

        self.type = kwargs.get('type', '-- Unspecified --')
        self.frequency = kwargs.get('frequency', None)
        self.interval = kwargs.get('interval', 10000)
        self.check_time_interval = kwargs.get('check_time_interval', 5)
        self.xid_age = kwargs.get('xid_age', 1500000000)
        self.wait_time = kwargs.get('wait_time', 60)
        self.resume_age = kwargs.get('resume_age', 1200000000)
        self.govern_debug = kwargs.get('govern_debug', False)
        self.use_logging = kwargs.get('use_logging', False)
        self.pre_post_msgs = kwargs.get('pre_post_msgs', True)

        # Required parms
        self.g2module_params = kwargs.get('g2module_params', None)

        # Perform any pre tasks
        self.govern_pre()

        # Ensure have required args
        try:
            self.psycopg2 = import_module('psycopg2')
        except ImportError:
            raise G2DBException(textwrap.dedent(f'''\n\

                - The Postgres native Python connector psycopg2 is required to use this governor.
                  This governor is used by default with G2Loader.py when using Postgres as the Senzing database.

                - Connector psycopg2 can usually be installed with the following command, see also: https://www.psycopg.org/docs/install.html

                    - pip3 install psycopg2 --user

                - If you are just getting started and seeing this message you can disable this governor with the -gpd argument with G2Loader.py.
                  This isn't recommended for normal use but is acceptable for testing and low data volumes.
        '''))

        if not self.g2module_params:
            raise ValueError(f'Creating governor, g2module_params must be specified.')
        else:
            try:
                self.g2module_params = json.loads(self.g2module_params)
            except JSONDecodeError:
                raise ValueError(f'Couldn\'t decode parameters to JSON for G2Module.')

        if not self.frequency:
            raise ValueError(f'Creating governor, frequency=<freq> must be specified. Where <freq> is one of {self.valid_frequencies}')

        if type(self.frequency) != str:
            raise ValueError(f'Creating governor, invalid frequency type. Should be a string not {type(self.frequency)}')

        self.frequency = self.frequency.lower()

        if self.frequency not in self.valid_frequencies:
            raise ValueError(f'Creating governor, invalid frequency {self.frequency}. Should be one of {self.valid_frequencies}')

        # Check argument types are correct
        type_check = [
            ('interval', self.interval, int, 'integer'),
            ('check time interval', self.check_time_interval, int, 'integer'),
            ('xidage', self.xid_age, int, 'integer'),
            ('waittime', self.wait_time, int, 'integer'),
            ('resumeage', self.resume_age, int, 'integer'),
            ('debug', self.govern_debug, bool, 'boolean')
        ]

        for check in type_check:
            if type(check[1]) != check[2]:
                try:
                    check[1] = check[2](check[1])
                except:
                    raise ValueError(f'Creating governor, invalid {check[0]} {check[1]}. Should be {check[3]} not {type(check[1])}')

        # Track number of per record calls when governor is frequency = row
        self.record_num = 1

        # Check G2Module parms, are we configured for running a Senzing clustered DB instance?
        self.sql_backend = self.g2module_params.get("SQL", {}).get("BACKEND", False)
        self.connect_hybrid = True if self.sql_backend and self.sql_backend.strip() == 'HYBRID' else False

        # Collect connection string(s)
        for stanza in {k: v for (k, v) in self.g2module_params.items() if k.upper() not in self.ignore_ini_stanzas}:
            for line_value in self.g2module_params[stanza].values():
                if 'postgresql://' in line_value.lower():
                    self.connect_strs.append(line_value)

        # Make connection strings unique, possible to config cluster to use 2 (or 1) DB for each node!
        self.connect_strs = list(set(self.connect_strs))

        if not self.connect_strs:
            raise G2DBException('No database connection string in g2module_params or not a PostgreSQL string. Please check your G2Module INI file')

        if len(self.connect_strs) > 1 and not self.connect_hybrid:
            print(f'\nWARNING: Multiple ({len(self.connect_strs)}) connection strings found without BACKEND=HYBRID. Please check your G2Module INI file\n')

        # Build data structure for each connection, a cursor to use against the connection, and it's DSN (for messages)
        for connect_str in self.connect_strs:
            try:
                # Today: {'TABLE': None, 'SCHEMA': None, 'PORT': '5432', 'DBTYPE': 'POSTGRESQL', 'USERID': 'senzing', 'PASSWORD': 'pass%25word', 'DSN': 'g2', 'HOST': 'ant76'}
                # New: {'dbname': 'g2', 'host': 'ant76', 'password': 'pass%2bword', 'port': 5432, 'user': 'senzing'}
                connect_parsed = self.dburi_parse(connect_str)
            except G2DBException as ex:
                raise ex
            else:
                (dbo, cursor) = self.connect_cursor(connect_parsed)
                self.connect_dict[connect_str] = [dbo, cursor, connect_parsed['dbname']]

                # String of all the DB names without other connection details. k[2] is the dsn
                self.db_names = ', '.join(k[2] for k in self.connect_dict.values())

        self.govern_post()

        return

    def __del__(self):
        self.govern_cleanup()

    def govern(self, *args, **kwargs):
        """ Trigger action based on frequency """

        if self.frequency == 'record':
            self.record_num += 1
            # If either the record interval or time_interval has passed call record_action
            if (self.record_num % self.interval == 0) or (time.time() > self.next_check_time):
                self.record_action()
        elif self.frequency == 'source':
            self.source_action()

        return

    def govern_pre(self, *args, **kwargs):
        """ Tasks to perform before creating governor """

        if self.pre_post_msgs:

            self.print_or_log(textwrap.dedent(f'''\
                Governor Details
                -------------------
            '''))

        return

    def govern_post(self, *args, **kwargs):
        """  Tasks to perform after creating governor """

        if self.pre_post_msgs:

            self.print_or_log(textwrap.indent(textwrap.dedent(f'''\
                  Successfully created:

                    Type:               {self.type}
                    Frequency:          {self.frequency}
                    Interval:           {self.interval if self.frequency == 'record' else 'None - Only used for frequency type of record'}
                    Check n seconds:    {self.check_time_interval if self.frequency == 'record' else 'None - Only used for frequency type of record'}
                    XID trigger age:    {self.xid_age}
                    XID resume age:     {self.resume_age}
                    Wait Time(s):       {self.wait_time}
                    Database(s):        {self.db_names}
                '''), '  '))

        return

    def govern_cleanup(self, *args, **kwargs):
        """  Tasks to perform when shutting down, e.g., close DB connections """

        for db_objs in self.connect_dict.values():
            db_objs[1].close()
            db_objs[0].close()

        return

    def record_action(self):
        """ Action to be performed when record or time interval is met, for each DB (single or clustered)
            db_objs - 0 = connection, 1 = cursor, 2 = DSN
        """

        # Serialize threads to perform expensive work and check if a pause and vacuum is needed
        with self.threads_lock:

            # Check each database connection, in hybrid mode will be > 1
            for db_objs in self.connect_dict.values():

                try:
                    db_objs[1].execute(self.sql_stmt, (db_objs[2],))
                    current_age = db_objs[1].fetchone()[0]
                except self.psycopg2.DatabaseError as ex:
                    raise ex
                except Exception as ex:
                    raise ex

                if current_age > self.xid_age:
                    self.print_or_log(textwrap.indent(textwrap.dedent(f'''\n\
                        WARNING: Transaction ID (XID) age threshold reached. Ingestion paused, vacuum required on database(s) to resume

                              Triggering governor:    {self.type}
                              XID age trigger:        {self.xid_age}
                              XID age currently:      {current_age}
                              XID target to resume:   {self.resume_age}
                              Triggered on DB:        {db_objs[2]}
                              DB Clustering:          {self.connect_hybrid}
                              DB(s) to vacuum:        {self.db_names}
                              '''), '  '), 'WARN')

                    # Wait for a manual vacuum to lower XID age < resume_age
                    while current_age > self.resume_age:
                        
                        self.print_or_log(f'\t{datetime.now().strftime("%I:%M%p")} - Database: {db_objs[2]}, Current XID age: {current_age}, Target age: {self.resume_age} - Sleeping for {self.wait_time}s...', 'WARN')
                        time.sleep(self.wait_time)
                        db_objs[1].execute(self.sql_stmt, [db_objs[2]])
                        current_age = db_objs[1].fetchone()[0]
                        # If G2Loader fails or catches CTRL-C, break to end this loop and exit governor
                        if self.thread_stop.value != 0:
                            break

                    print()

            # Update the last checked time
            self.next_check_time = time.time() + self.check_time_interval

    def source_action(self):
        """ Action to be performed when triggered for each source, e.g. in multi-source project  """

        return

    def connect_cursor(self, parsed_uri):
        """ For each DB return connection and cursor """

        try:
            dbo = self.psycopg2.connect(**parsed_uri)
            dbo.set_session(autocommit=True, isolation_level='READ UNCOMMITTED', readonly=True)
            cursor = dbo.cursor()
        except self.psycopg2.Error as ex:
            raise ex
        except KeyError as ex:
            self.print_or_log(f'\nERROR: Parsed database connection string appears to be incomplete:', 'ERROR')
            self.print_or_log(self.showConn(parsed_uri, False, False), 'ERROR')
            self.print_or_log('', 'ERROR')
            raise ex
        except Exception as ex:
            raise ex

        return dbo, cursor

    def dburi_parse(self, dburi):
        """ Parse the database uri string """

        def parse_error_msg(parse):
            print(textwrap.dedent(f'''\n\

                  ERROR: parsing database connection string, this is usually caused by special characters being
                  used in the password field without being percent encoded. For example @ was specified instead
                  of %40.

                  Parse result: {parse}
                  '''))

        # Split main URI and schema if schema is present
        if '/?schema' in dburi:
            first_half, schema_str = dburi.split('/?')
        else:
            first_half = dburi
            schema_str = ''

        # Replace the last : with / for urlparse to work
        last_colon = first_half.rfind(':')
        first_half_list = list(first_half)
        first_half_list[last_colon] = '/'
        first_half = ''.join(first_half_list)

        conn_string_parsed = urllib.parse.urlparse(first_half)

        try:
            result = {
                'dbname': conn_string_parsed.path.strip('/'),
                'host': conn_string_parsed.hostname,
                'password': urllib.parse.unquote(conn_string_parsed.password),
                'port': conn_string_parsed.port,
                'user': urllib.parse.unquote(conn_string_parsed.username)
            }
        except TypeError:
            parse_error_msg(conn_string_parsed)
            sys.exit(-1)

        dbtype = conn_string_parsed.scheme

        # Reconstruct the URI and test if it matches input URI, if it doesn't something went wrong parsing
        # and likely percent encoding issue
        reconstruct = dbtype + '://' + conn_string_parsed.netloc + ':' + conn_string_parsed.path.strip('/')
        if schema_str:
            reconstruct += '/?' + schema_str

        if reconstruct != dburi:
            result['password'] = 'REDACTED'
            parse_error_msg(result)
            sys.exit(-1)

        return result

    def show_connection(self, uri_dict, show_pwd=False, print_not_return=True):
        """ Show connection details and redact password if requested. Print directly or return only the parsed URI dict """

        if not show_pwd:
            orig_pwd = uri_dict['PASSWORD']
            uri_dict['PASSWORD'] = '<--REDACTED-->'
            uri_dict['PASSWORD'] = orig_pwd

        conn_string = f'{dict(sorted(uri_dict.items()))}'

        if print_not_return:
            print(f'\nDB URI Parse: {conn_string}')
            return

        return conn_string

    def print_or_log(self, msg, level_='INFO'):
        """ Use logging or print for output """

        if self.use_logging:
            self.gov_logger.log(getattr(logging, level_.upper()), msg.strip())
        else:
            print(f'{msg}')
