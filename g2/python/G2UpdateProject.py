#! /usr/bin/env python3

import sys
import os
import argparse
import errno
import shutil
import json
from distutils.dir_util import copy_tree

# files to update
files_to_update = [
    'setupEnv',
    'etc/G2Module.ini',
    'etc/G2Project.ini'
]

def find_replace_in_file(filename, old_string, new_string):
    # Safely read the input filename using 'with'
    with open(filename) as f:
        s = f.read()

    # Safely write the changed content, if found in the file
    with open(filename, 'w') as f:
        s = s.replace(old_string, new_string)
        f.write(s)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update an existing Senzing project with the installed version of Senzing.')
    parser.add_argument('folder', metavar='Project',help='the path of the folder to update. It must already exist and be a Senzing project folder.')
    args = parser.parse_args()

    target_path = os.path.normpath(os.path.join(os.getcwd(), os.path.expanduser(args.folder)))

    # List items in target path
    files_and_folders = os.listdir(target_path)

    # Check that it is a project folder
    if 'g2BuildVersion.json' not in files_and_folders:
        print(target_path + " is not a project path. (Cannot find g2BuildVersion.json)")

    # Get current version info
    with open(os.path.join(target_path, 'g2BuildVersion.json')) as json_file:
        start_version_info = json.load(json_file)
    
    # Get new version info
    with open(os.path.join('/opt/senzing/g2', 'g2BuildVersion.json')) as json_file:
        end_version_info = json.load(json_file)

    answer = input("Update Senzing instance at '%s' from version %s to %s? (y/N) " % (target_path, start_version_info['VERSION'], end_version_info['VERSION']))
    answer = answer.strip()
    if answer != 'y' and answer != 'Y':
        sys.exit(0)

    print("Updating...")

    # we want to keep etc and var, so take them out of list
    while 'etc' in files_and_folders:
        files_and_folders.remove('etc')

    # we want to keep etc and var, so take them out of list
    while 'var' in files_and_folders:
        files_and_folders.remove('var')

    for the_file in files_and_folders:
        file_path = os.path.join(target_path, the_file)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path): 
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)

    # copy opt
    copy_tree('/opt/senzing/g2', target_path)
    
    # copy etc to resources/templates but don't touch <target_path>/etc
    shutil.copytree('/etc/opt/senzing', os.path.join(target_path, 'resources/templates'))

    # soft link in data
    os.symlink('/opt/senzing/data/1.0.0', os.path.join(target_path, 'data'))

    # paths to substitute
    senzing_path_subs = [
        ('/opt/senzing/g2', target_path),
        ('/opt/senzing', target_path)        
    ]

    for f in files_to_update:
        for p in senzing_path_subs:
            # Anchor the replace on the character that comes before the path. This ensures that we are only 
            # replacing the begining of the path and not a substring of the path.
            find_replace_in_file(os.path.join(target_path, f), '=' + p[0], '=' + os.path.join(target_path, p[1]))
            find_replace_in_file(os.path.join(target_path, f), '@' + p[0], '@' + os.path.join(target_path, p[1]))

    print("Project successfully updated from %s to %s. Please refer to https://senzing.com/releases/#api-releases for any additional upgrade instructions." % (start_version_info['VERSION'], end_version_info['VERSION']))
