import os
import sys
import errno


try:
    search_paths = [
        os.path.dirname(os.path.abspath(sys.argv[0])),
        os.path.join(os.environ["SENZING_ROOT"], "../../etc/opt/senzing"),  # To catch G2CreateInstance created instances
        "/etc/opt/senzing",
        "/opt/senzing/g2/python"  # backwards compataibility
    ]
except KeyError:
    print("SENZING_ROOT is not in the environment. Did you remember to setup your environment by sourcing the setupEnv file?")
    sys.exit(1)


def get_G2Project_ini_path():
    return __get_file_path('G2Project.ini')


def get_G2Module_ini_path():
    return __get_file_path('G2Module.ini')


def __get_file_path(filename):
    iniFileName = None
    for path in search_paths:
        candidate_path = os.path.normpath(os.path.join(path, filename))
        if os.path.exists(candidate_path):
            iniFileName = candidate_path
            break

    if iniFileName is None:
        print('ERROR: The ' + filename + ' file is missing from the application path!')
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)

    return iniFileName

