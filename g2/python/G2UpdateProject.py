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

def dirShouldBeSymlink(source_file_path):
    if source_file_path == senzing_path + "/data":
        return True
    if source_file_path == senzing_path + "/resources/config":
        return True
    if source_file_path == senzing_path + "/resources/schema":
        return True
    return False

def overlayFiles(sourcePath,destPath):
    files_and_folders = os.listdir(sourcePath)
    for the_file in files_and_folders:
        source_file_path = os.path.join(sourcePath, the_file)
        target_file_path = os.path.join(destPath, the_file)
        try:
            # clear out the old file
            if os.path.isfile(target_file_path):
                os.remove(target_file_path)
            elif os.path.islink(target_file_path):
                os.unlink(target_file_path)
            elif os.path.isdir(target_file_path):
                pass

            # put in the new file
            if os.path.isfile(source_file_path):
                shutil.copy(source_file_path, target_file_path)
            elif os.path.islink(source_file_path):
                shutil.copy(source_file_path, target_file_path)
            elif os.path.isdir(source_file_path): 
                if dirShouldBeSymlink(source_file_path) == True:
                    pass
                else:
                    os.makedirs(target_file_path, exist_ok=True)
                    overlayFiles(source_file_path,target_file_path)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update an existing Senzing project with the installed version of Senzing.')
    parser.add_argument('folder', metavar='Project',help='the path of the folder to update. It must already exist and be a Senzing project folder.')
    parser.add_argument('-y', '--yes', action='store_true', help='Accept and skip the prompt to update the project')
    args = parser.parse_args()

    target_path = os.path.normpath(os.path.join(os.getcwd(), os.path.expanduser(args.folder)))

    # Check that we don't try to overwrite our master folder
    if target_path == senzing_path:
        print(target_path + " is the same as the main installation path.  Invalid operation.")
        sys.exit(1)

    # List items in target path
    files_and_folders = os.listdir(target_path)

    # Check that it is a project folder
    if 'g2BuildVersion.json' not in files_and_folders:
        print(target_path + " is not a project path. (Cannot find g2BuildVersion.json)")
        sys.exit(1)

    # Get current version info
    with open(os.path.join(target_path, 'g2BuildVersion.json')) as json_file:
        start_version_info = json.load(json_file)
    
    # Get new version info
    with open(os.path.join(senzing_path, 'g2BuildVersion.json')) as json_file:
        end_version_info = json.load(json_file)

    if args.yes == True:
        print("Updating Senzing instance at '%s' from version %s to %s." % (target_path, start_version_info['VERSION'], end_version_info['VERSION']))
    else:
        answer = input("Update Senzing instance at '%s' from version %s to %s? (y/n) " % (target_path, start_version_info['VERSION'], end_version_info['VERSION']))
        answer = answer.strip()
        if answer != 'y' and answer != 'Y':
            sys.exit(0)

    print("Updating...")

    # we want to keep etc and var, so take them out of list
    while 'etc' in files_and_folders:
        files_and_folders.remove('etc')
    while 'var' in files_and_folders:
        files_and_folders.remove('var')
    
    # Update most of the files from opt
    overlayFiles(senzing_path,target_path)

    # soft link in data
    try:
        if os.path.exists(os.path.join(target_path, 'data')):
            os.remove(os.path.join(target_path, 'data'))
        os.symlink('/opt/senzing/data/1.0.0', os.path.join(target_path, 'data'))
    except Exception as e:
        print(e)

    # soft link in the two resource folders
    try:
        if os.path.exists(os.path.join(target_path, 'resources', 'config')):
            os.remove(os.path.join(target_path, 'resources', 'config'))
        os.symlink(os.path.join(senzing_path, 'resources', 'config'), os.path.join(target_path, 'resources', 'config'))
        if os.path.exists(os.path.join(target_path, 'resources', 'schema')):
            os.remove(os.path.join(target_path, 'resources', 'schema'))
        os.symlink(os.path.join(senzing_path, 'resources', 'schema'), os.path.join(target_path, 'resources', 'schema'))
    except Exception as e:
        print(e)

    # files to update
    files_to_update = [
        'setupEnv'
    ]

    # paths to substitute
    senzing_path_subs = [
        ('${SENZING_DIR}', target_path),
        ('${SENZING_CONFIG_DIR}', os.path.join(target_path, 'etc'))
    ]

    # New files copied in, now update some of the new files.
    for f in files_to_update:
        for p in senzing_path_subs:
            try:
                # Anchor the replace on the character that comes before the path. This ensures that we are only 
                # replacing the begining of the path and not a substring of the path.
                find_replace_in_file(os.path.join(target_path, f), '=' + p[0], '=' + os.path.join(target_path, p[1]))
                find_replace_in_file(os.path.join(target_path, f), '@' + p[0], '@' + os.path.join(target_path, p[1]))
            except Exception as e:
                print(e)

    # fixups - any edits to existing files, like adding new INI tokens
    # 1) Add in RESOURCEPATH
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

    # End of fixups

    print("Project successfully updated from %s to %s. Please refer to https://senzing.com/releases/#api-releases for any additional upgrade instructions." % (start_version_info['VERSION'], end_version_info['VERSION']))
