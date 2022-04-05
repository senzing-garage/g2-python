import errno
import os
import sys

# Search paths checked for INI file requested. Path can be for a local Senzing project
# (created with G2CreateProject.py) or a 'system install' path - for example using an
# asset from Senzing Git Hub mounting the path from the host into a container.

search_paths = []

# Check where running from
search_paths.append(os.path.dirname(os.path.abspath(sys.argv[0])))

# Senzing container assets set SENZING_ETC_PATH, check too if set. This needs to be checked first
if 'SENZING_ETC_PATH' in os.environ:
    search_paths.append(os.environ.get('SENZING_ETC_PATH'))

# SENZING_ROOT is set by setupEnv and should be set
if 'SENZING_ROOT' in os.environ:
    search_paths.append(os.path.join(os.environ.get('SENZING_ROOT'), 'etc'))

# Some utilities call G2Paths before starting engine etc. If we're not in a container and
# SENZING_ROOT isn't set catch here instead of in G2Engine.py for example
if 'SENZING_ETC_PATH' not in os.environ and 'SENZING_ROOT' not in os.environ:
    print("\nERROR: Environment variable SENZING_ROOT is not set. Did you remember to setup your environment by sourcing the setupEnv file?")
    print("ERROR: For more information see https://senzing.zendesk.com/hc/en-us/articles/115002408867-Introduction-G2-Quickstart")
    print("ERROR: If you are running Ubuntu or Debian please also review the ssl and crypto information at https://senzing.zendesk.com/hc/en-us/articles/115010259947-System-Reuirements")
    sys.exit(1)


class TooManyFilesException(Exception):
    pass


def get_G2Project_ini_path():
    return __get_file_path('G2Project.ini')


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
