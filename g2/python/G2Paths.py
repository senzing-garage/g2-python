import errno
import os
import sys
import textwrap

# The Senzing python tools call G2Paths before starting engine to locate the engine configuration. Error if can't locate
# a G2Module.ini or SENZING_ENGINE_CONFIGURATION_JSON
if 'SENZING_ETC_PATH' not in os.environ and 'SENZING_ROOT' not in os.environ and 'SENZING_ENGINE_CONFIGURATION_JSON' not in os.environ:

    print(textwrap.dedent('''\n\
    ERROR: SENZING_ROOT or SENZING_ENGINE_CONFIGURATION_JSON environment variable is not set:
    
           - If using a Senzing project on a bare metal install, source the setupEnv file in the project root path. 
               
                https://senzing.zendesk.com/hc/en-us/articles/115002408867-Introduction-G2-Quickstart
               
           - If running within a container set the SENZING_ENGINE_CONFIGURATION_JSON environment variable.
            
                https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_engine_configuration_json
    '''))
    sys.exit(1)

# Search paths checked for INI file requested. Path can be for a local Senzing project
# (created with G2CreateProject.py) or a 'system install' path - for example using an
# asset from Senzing Git Hub mounting the path from the host into a container.

# Check current path
search_paths = [os.path.dirname(os.path.abspath(sys.argv[0]))]

# Senzing container assets set SENZING_ETC_PATH, check too if set. This needs to be checked first
if 'SENZING_ETC_PATH' in os.environ:
    search_paths.append(os.environ.get('SENZING_ETC_PATH'))

# SENZING_ROOT is set by setupEnv and should be set
if 'SENZING_ROOT' in os.environ:
    search_paths.append(os.path.join(os.environ.get('SENZING_ROOT'), 'etc'))


class TooManyFilesException(Exception):
    pass


def get_G2Module_ini_path():
    return __get_file_path('G2Module.ini')


def show_paths(paths):

    [print(f'\t{path}') for path in paths]
    print()


def check_file_exists_and_readable(filename):

    if not os.path.exists(filename):
        print(f'\nERROR: {filename} not found\n')
        sys.exit(1)

    if not os.access(filename, os.R_OK):
        print(f'\nERROR: {filename} not readable\n')
        sys.exit(1)


def __get_file_path(filename):

    ini_file_locations = []
    msg_args = f'Use command line argument -c (--inifile) to specify the path & filename for {filename}.'
    msg_multi_loc = 'found in multiple locations'

    for path in search_paths:
        candidate_path = os.path.normpath(os.path.join(path, filename))

        if os.path.exists(candidate_path):
            ini_file_locations.append(candidate_path)

    if len(ini_file_locations) == 0:
        print(f'ERROR: {filename} is missing from the search path location(s). Searched: ')
        show_paths(search_paths)

        raise FileNotFoundError(f'[Errno {errno.ENOENT}] {os.strerror(errno.ENOENT)}: {filename} - {msg_args}')

    if len(ini_file_locations) > 1:
        print(f'ERROR: {filename} {msg_multi_loc}: ')
        show_paths(ini_file_locations)

        raise TooManyFilesException(f'{filename} {msg_multi_loc}. {msg_args}')

    return ini_file_locations[0]
