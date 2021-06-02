#! /usr/bin/env python3

import sys
import os
import argparse
import errno
import shutil
import json
from distutils.dir_util import copy_tree
try: import configparser
except: import ConfigParser as configparser

# files to update
files_to_update = [
    'setupEnv'
]

senzing_path = '/opt/senzing/g2'

paths_to_exclude = [os.path.join(senzing_path, 'resources' , 'config'), os.path.join(senzing_path, 'resources' , 'schema')]
files_to_exclude = ['G2CreateProject.py', 'G2UpdateProject.py']

def get_ignored(path, filenames):        
    ret = []
    for filename in filenames:
        if os.path.join(path, filename) in paths_to_exclude:
            ret.append(filename)
        elif filename in files_to_exclude:
            ret.append(filename)
    return ret

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
    with open(os.path.join(senzing_path, 'g2BuildVersion.json')) as json_file:
        end_version_info = json.load(json_file)

    answer = input("Update Senzing instance at '%s' from version %s to %s? (y/N) " % (target_path, start_version_info['VERSION'], end_version_info['VERSION']))
    answer = answer.strip()
    if answer != 'y' and answer != 'Y':
        sys.exit(0)

    print("Updating...")

    # we want to keep etc and var, so take them out of list
    while 'etc' in files_and_folders:
        files_and_folders.remove('etc')
    while 'var' in files_and_folders:
        files_and_folders.remove('var')

    # remove the existing (old version) files
    for the_file in files_and_folders:
        file_path = os.path.join(target_path, the_file)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path): 
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)

    
    # Update most of the files from opt
    files_and_folders = os.listdir(senzing_path)
    for the_file in files_and_folders:
        target_file_path = os.path.join(target_path, the_file)
        source_file_path = os.path.join(senzing_path, the_file)
        if os.path.isfile(source_file_path) or os.path.islink(source_file_path):
            shutil.copyfile(source_file_path, target_file_path)
        elif os.path.isdir(source_file_path): 
            shutil.copytree(source_file_path, target_file_path, ignore=get_ignored)

    # soft link in data
    os.symlink('/opt/senzing/data/1.0.0', os.path.join(target_path, 'data'))

    # soft link in the two resouces folders
    os.symlink(os.path.join(senzing_path, 'resources', 'config'), os.path.join(target_path, 'resources', 'config'))
    os.symlink(os.path.join(senzing_path, 'resources', 'schema'), os.path.join(target_path, 'resources', 'schema'))

    # paths to substitute
    senzing_path_subs = [
        (senzing_path, target_path),
        ('/opt/senzing', target_path)        
    ]

    # New files copied in, now update some of the new files.
    for f in files_to_update:
        for p in senzing_path_subs:
            # Anchor the replace on the character that comes before the path. This ensures that we are only 
            # replacing the begining of the path and not a substring of the path.
            find_replace_in_file(os.path.join(target_path, f), '=' + p[0], '=' + os.path.join(target_path, p[1]))
            find_replace_in_file(os.path.join(target_path, f), '@' + p[0], '@' + os.path.join(target_path, p[1]))

    # fixups - any edits to etc files, like adding new INI tokens
    # add in RESOURCEPATH
    ini_content = None
    g2_module_ini_path = os.path.join(target_path,'etc','G2Module.ini')
    with open(g2_module_ini_path, 'r') as configfile:
        ini_content = configfile.readlines()
    
    try:        
        resource_path_exists = False
        for line in ini_content:
            if line.strip().startswith('RESOURCEPATH='):
                resource_path_exists = True
                break

        if not resource_path_exists:
            index = ini_content.index('[PIPELINE]\n')
            ini_content.insert(index+1, ' RESOURCEPATH=' + os.path.join(target_path, 'resources') + '\n')
            with open(g2_module_ini_path, 'w') as configfile:
                configfile.writelines(ini_content)
    except ValueError:
        print("Could not find the [PIPELINE] section in G2Module.ini. Add RESOURCEPATH to the [PIPELINE] of G2Module.ini and set it to '" + os.path.join(target_path, 'resources') + "'")

    print("Project successfully updated from %s to %s. Please refer to https://senzing.com/releases/#api-releases for any additional upgrade instructions." % (start_version_info['VERSION'], end_version_info['VERSION']))
