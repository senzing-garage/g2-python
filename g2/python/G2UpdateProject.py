#! /usr/bin/env python3

import sys
import os
import argparse
import errno
import shutil
import json
import pathlib
from distutils.dir_util import copy_tree
try: import configparser
except: import ConfigParser as configparser


senzing_path = '/opt/senzing/g2'
jre_dir_name = 'jdk-11.0.10+9-jre'
past_jre_dir_names = [
    'jdk-11.0.10+9-jre'
]
files_to_exclude = ['G2CreateProject.py', 'G2UpdateProject.py']
paths_to_remove = [
    os.path.join('extras', 'poc'),
    os.path.join('extras'),
    os.path.join('sdk', 'python', 'old'),
    os.path.join('python', 'demo', 'ofac'),
]
files_to_remove = [
    os.path.join('extras', 'poc', 'poc_audit.py'),
    os.path.join('extras', 'poc', 'poc_snapshot.py'),
    os.path.join('extras', 'poc', 'poc_viewer.py'),
    os.path.join('lib', 'libDefaultRelationship.so'),
    os.path.join('sdk', 'c', 'libg2audit.h'),
    os.path.join('sdk', 'c', 'libg2audit.h'),
    os.path.join('sdk', 'python', 'G2Audit.py'),
    os.path.join('sdk', 'python', 'old', 'G2AuditModule.py'),
    os.path.join('sdk', 'python', 'old', 'G2ConfigModule.py'),
    os.path.join('sdk', 'python', 'old', 'G2Exception.py'),
    os.path.join('sdk', 'python', 'old', 'G2Module.py'),
    os.path.join('sdk', 'python', 'old', 'G2ProductModule.py'),
    os.path.join('sdk', 'java', 'com', 'senzing', 'g2', 'engine', 'G2Audit.java'),
    os.path.join('sdk', 'java', 'com', 'senzing', 'g2', 'engine', 'G2AuditJNI.java'),
    os.path.join('python', 'demo', 'ofac', 'cust.json'),
    os.path.join('python', 'demo', 'ofac', 'ofac.json'),
    os.path.join('python', 'demo', 'ofac', 'project.json'),
]
symlinks = [
    os.path.join(senzing_path, 'data')
]
folders_to_ignore = [
    pathlib.Path(os.path.join(senzing_path, 'lib', 'jdk-11.0.10+9-jre'))
]

def find_replace_in_file(filename, old_string, new_string):
    # Safely read the input filename using 'with'
    with open(filename) as f:
        s = f.read()

    # Safely write the changed content, if found in the file
    with open(filename, 'w') as f:
        s = s.replace(old_string, new_string)
        f.write(s)

def dirShouldBeSymlink(source_file_path):
    return source_file_path in symlinks

def ignore_folder(dir_to_test):
    test_path = pathlib.Path(dir_to_test)
    for this_dir in folders_to_ignore:
        if this_dir in (test_path, *test_path.parents):
            return True
    return False

def overlayFiles(sourcePath,destPath):
    files_and_folders = os.listdir(sourcePath)
    for the_file in files_and_folders:
        if os.path.basename(the_file) in files_to_exclude:
            continue
        source_file_path = os.path.join(sourcePath, the_file)
        target_file_path = os.path.join(destPath, the_file)
        if ignore_folder(source_file_path):
            continue
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
                shutil.copy(source_file_path, target_file_path, follow_symlinks=False)
            elif os.path.isdir(source_file_path): 

                if dirShouldBeSymlink(source_file_path) == True:
                    pass
                else:
                    os.makedirs(target_file_path, exist_ok=True)
                    overlayFiles(source_file_path,target_file_path)
        except Exception as e:
            print(e)

def change_permissions_recursive(path, mode):
    os.chmod(path, mode)
    for root, dirs, files in os.walk(path, topdown=False):
        for dir in [os.path.join(root,d) for d in dirs]:
            if not os.path.islink(dir):
                os.chmod(dir, mode)
        for file in [os.path.join(root, f) for f in files]:
            if not os.path.islink(file):
                os.chmod(file, mode)

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
        if answer != 'y' and answer != 'Y' and answer.lower() != 'yes':
            print("Update declined")
            sys.exit(0)

    print("Updating...")

    # we want to keep etc and var, so take them out of list
    while 'etc' in files_and_folders:
        files_and_folders.remove('etc')
    while 'var' in files_and_folders:
        files_and_folders.remove('var')

    # Clean up old files/folders from old releases that are not in the current release
    for f in files_to_remove:
        try:
            os.remove(os.path.join(target_path, f))
        except (FileNotFoundError, OSError):
            # ok if file doesn't exist, or can't be removed for some other reason (permissions, etc)
            pass

    for p in paths_to_remove:
        try:
            os.rmdir(os.path.join(target_path, p))
        except (FileNotFoundError, OSError):
            # ok if file doesn't exist or the folder is not empty
            pass

    # Remove JRE (if it exists)
    jre_to_remove = None
    for jre in past_jre_dir_names:
        test_path = os.path.join(target_path, 'lib', jre)
        if os.path.exists(test_path):
            jre_to_remove = test_path
            break

    if jre_to_remove is not None:
        shutil.rmtree(jre_to_remove)

    # Update most of the files from opt
    overlayFiles(senzing_path,target_path)

    # copy over new JRE
    jre_source_path = os.path.join(senzing_path, 'lib', jre_dir_name)
    jre_target_path = os.path.join(target_path, 'lib', jre_dir_name)

    shutil.copytree(jre_source_path, jre_target_path)

    # soft link in data
    try:
        if os.path.exists(os.path.join(target_path, 'data')):
            os.remove(os.path.join(target_path, 'data'))
        os.symlink('/opt/senzing/data/3.0.0', os.path.join(target_path, 'data'))
    except Exception as e:
        print(e)

    # files to update
    files_to_update = [
        'setupEnv'
    ]

    # paths to substitute
    senzing_path_subs = [
        ('${SENZING_DIR}', target_path),
        ('${SENZING_CONFIG_PATH}', os.path.join(target_path, 'etc')),
        ('${SENZING_DATA_DIR}', os.path.join(target_path, 'data')),
        ('${SENZING_RESOURCES_DIR}', os.path.join(target_path, 'resources')),
        ('${SENZING_VAR_DIR}', os.path.join(target_path, 'var'))
    ]

    # New files copied in, now update some of the new files.
    for f in files_to_update:
        for p in senzing_path_subs:
            try:
                find_replace_in_file(os.path.join(target_path, f), p[0], p[1])
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

    # Set permissions to 750
    change_permissions_recursive(target_path, 0o750)

    print("Project successfully updated from %s to %s. Please refer to https://senzing.com/releases/#api-releases for any additional upgrade instructions." % (start_version_info['VERSION'], end_version_info['VERSION']))
