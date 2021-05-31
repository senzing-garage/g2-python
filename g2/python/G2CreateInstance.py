#! /usr/bin/env python

import sys
import os
import argparse
import errno
import shutil

def find_replace_in_file(filename, old_string, new_string):
    # Safely read the input filename using 'with'
    with open(filename) as f:
        s = f.read()

    # Safely write the changed content, if found in the file
    with open(filename, 'w') as f:
        s = s.replace(old_string, new_string)
        f.write(s)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a per-user instance of Senzing in a new folder with the specified name.')
    parser.add_argument('folder', metavar='F', nargs='?', default='~/senzing', help='the name of the folder to create (default is "~/senzing"). It must not already exist')
    args = parser.parse_args()

    target_path = os.path.normpath(os.path.join(os.getcwd(), os.path.expanduser(args.folder)))
    
    # check if folder exists. It shouldn't
    if os.path.exists(target_path):
        print('"' + target_path + '" already exists. Please specify a folder that does not already exist.')
        sys.exit(1)
    
    # Folders to copy from
    senzing_paths = [
        '/opt/senzing',
        '/etc/opt/senzing',
        '/var/opt/senzing'
    ]
    
    print("Creating Senzing instance at " + target_path )
    for path in senzing_paths:
        specific_target_path = os.path.join(target_path, path[1:])
        shutil.copytree(path, specific_target_path)

    files_to_update = [
        'opt/senzing/g2/setupEnv',
        'etc/opt/senzing/G2Module.ini',
        'etc/opt/senzing/G2Project.ini'
    ]
    
    for f in files_to_update:
        for p in senzing_paths:
            # Anchor the replace on the character that comes before the path. This ensures that we are only 
            # replacing the begining of the path and not a substring of the path.
            find_replace_in_file(os.path.join(target_path, f), '=' + p, '=' + os.path.join(target_path, p[1:]))
            find_replace_in_file(os.path.join(target_path, f), '@' + p, '@' + os.path.join(target_path, p[1:]))

