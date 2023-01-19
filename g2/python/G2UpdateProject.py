#! /usr/bin/env python3

import sys
import os
import argparse
import errno
import shutil
import json
import pathlib
from contextlib import suppress

try:
    import configparser
except:
    import ConfigParser as configparser

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
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.9-to-1.10.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.10-to-1.10.1.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.10-to-1.11.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.10.1-to-1.11.2.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.11-to-1.11.2.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.11.2-to-1.12.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.12-to-1.13.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.13-to-1.14.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.14-to-1.15.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-1.15-to-2.0.gtc'),
    os.path.join('resources', 'config', 'g2core-config-upgrade-2.0-to-2.5.gtc'),
]
paths_to_move = [
    (os.path.join('sdk', 'python'), os.path.join('sdk', 'python_prior_to_3.0')),
    (os.path.join('python', 'demo', 'truth'), os.path.join('python', 'demo', 'truth_prior_to_3.0')),
]
files_to_move = [
    ('G2Config.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2ConfigMgr.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2Diagnostic.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2Engine.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2Exception.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2Hasher.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2Health.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2IniParams.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('G2Product.py', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
    ('g2purge.umf', os.path.join('python'), os.path.join('python', 'prior_to_3.0')),
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


def overlayFiles(sourcePath, destPath):
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

                if dirShouldBeSymlink(source_file_path):
                    pass
                else:
                    os.makedirs(target_file_path, exist_ok=True)
                    overlayFiles(source_file_path, target_file_path)
        except Exception as e:
            print(e)


def set_folder_permissions_recursive(project_root_path, folder_permissions, folders_to_ignore=[]):
    os.chmod(project_root_path, folder_permissions)
    for root, dirs, _ in os.walk(project_root_path, topdown=True):
        dirs[:] = [d for d in dirs if d not in folders_to_ignore]
        for dir in [os.path.join(root, d) for d in dirs]:
            if not os.path.islink(dir):
                os.chmod(dir, folder_permissions)


def set_permissions_on_files_in_folder(folder_path, permissions, files_to_ignore=[]):
    for file in os.listdir(folder_path):
        if file in files_to_ignore:
            continue
        file_path = os.path.join(folder_path, file)
        if os.path.isfile(file_path):
            os.chmod(file_path, permissions)


def set_permissions_on_files_in_folder_recursive(path, mode, files_to_ignore=[]):
    for root, _, files in os.walk(path, topdown=False):
        for file in [os.path.join(root, f) for f in files]:
            if file in files_to_ignore:
                continue
            if not os.path.islink(file):
                os.chmod(file, mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update an existing Senzing project with the installed version of Senzing.')
    parser.add_argument('folder', metavar='Project', help='the path of the folder to update. It must already exist and be a Senzing project folder.')
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

    if args.yes:
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
        with suppress(FileNotFoundError, OSError):
            os.remove(os.path.join(target_path, f))

    for p in paths_to_remove:
        with suppress(FileNotFoundError, OSError):
            os.rmdir(os.path.join(target_path, p))

    for p in paths_to_move:
        backup_path = os.path.join(target_path, p[1])
        new_backup_path_template = backup_path + '_{}'
        i = 0
        while os.path.exists(backup_path):
            backup_path = new_backup_path_template.format(str(i))
            i = i + 1
            
        with suppress(FileNotFoundError, OSError):
            shutil.move(os.path.join(target_path, p[0]), backup_path)

    for f in files_to_move:
        os.makedirs(os.path.join(target_path,f[2]), exist_ok=True)
        with suppress(FileNotFoundError, OSError):
            shutil.move(os.path.join(target_path, f[1], f[0]), os.path.join(target_path, f[2], f[0]))

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
    overlayFiles(senzing_path, target_path)

    # copy over new JRE
    jre_source_path = os.path.join(senzing_path, 'lib', jre_dir_name)
    jre_target_path = os.path.join(target_path, 'lib', jre_dir_name)

    shutil.copytree(jre_source_path, jre_target_path)

    # soft link in data
    with suppress(FileNotFoundError, OSError):
        os.remove(os.path.join(target_path, 'data'))

    try:        
        os.symlink(os.path.join(os.sep, 'opt', 'senzing', 'data', end_version_info['DATA_VERSION']), os.path.join(target_path, 'data'))
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
    g2_module_ini_path = os.path.join(target_path, 'etc', 'G2Module.ini')
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
            ini_content.insert(index + 1, ' RESOURCEPATH=' + os.path.join(target_path, 'resources') + '\n')
            with open(g2_module_ini_path, 'w') as configfile:
                configfile.writelines(ini_content)
    except ValueError:
        print("Could not find the [PIPELINE] section in G2Module.ini. Add RESOURCEPATH to the [PIPELINE] of G2Module.ini and set it to '" + os.path.join(target_path, 'resources') + "'")

    # End of fixups

    # Folder permissions
    set_folder_permissions_recursive(target_path, 0o770, folders_to_ignore=['jdk-11.0.10+9-jre'])

    # root
    set_permissions_on_files_in_folder(target_path, 0o660)
    os.chmod(os.path.join(target_path, 'setupEnv'), 0o770)

    # bin
    set_permissions_on_files_in_folder(os.path.join(target_path, 'bin'), 0o770)

    # etc
    set_permissions_on_files_in_folder(os.path.join(target_path, 'etc'), 0o660)

    # lib
    set_permissions_on_files_in_folder(os.path.join(target_path, 'lib'), 0o660, files_to_ignore=['g2.jar'])
    os.chmod(os.path.join(target_path, 'lib', 'g2.jar'), 0o664)

    # python
    set_permissions_on_files_in_folder_recursive(os.path.join(target_path, 'python'), 0o660)
    os.chmod(os.path.join(target_path, 'python', 'G2Audit.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2Command.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2ConfigTool.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2Database.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2Explorer.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2Export.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2Loader.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2SetupConfig.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'G2Snapshot.py'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'SenzingGo.py'), 0o770)

    # resources
    set_permissions_on_files_in_folder_recursive(os.path.join(target_path, 'resources'), 0o660)
    os.chmod(os.path.join(target_path, 'resources', 'templates', 'setupEnv'), 0o770)

    # sdk
    set_permissions_on_files_in_folder_recursive(os.path.join(target_path, 'sdk'), 0o664)

    # var
    set_permissions_on_files_in_folder_recursive(os.path.join(target_path, 'var'), 0o660)

    print("Project successfully updated from %s to %s. Please refer to https://senzing.com/releases/#api-releases for any additional upgrade instructions." % (start_version_info['VERSION'], end_version_info['VERSION']))
