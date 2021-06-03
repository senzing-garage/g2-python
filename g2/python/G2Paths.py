import os
import sys
import errno


current_path = os.path.dirname(os.path.abspath(sys.argv[0]))
search_paths = [
    current_path,
    os.path.abspath(os.path.join(current_path, '..', 'etc'))
]

class TooManyFilesException(Exception):
    pass

def get_G2Project_ini_path():
    return __get_file_path('G2Project.ini')


def get_G2Module_ini_path():
    return __get_file_path('G2Module.ini')


def __get_file_path(filename):
    ini_file_locations = []
    for path in search_paths:
        candidate_path = os.path.normpath(os.path.join(path, filename))
        if os.path.exists(candidate_path):
            ini_file_locations.append(candidate_path)

    if len(ini_file_locations) == 0:
        print('ERROR: The ' + filename + ' file is missing from the application path!')
        raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), filename)
    
    if len(ini_file_locations) > 1:
        print('ERROR: Found ' + filename + ' In more than one location. Please clean up extra copies or pass the file on the command line. ' + filename + ' found in ')
        for location in ini_file_locations:
            print(location)
        raise TooManyFilesException(filename + " found in more than one location")

    return ini_file_locations[0]

