#! /usr/bin/env python3

import argparse
import errno
import json
import os
import shutil
import sys
import textwrap


def find_replace_in_file(filename, old_string, new_string):
    ''' Replace strings in new project files  '''

    try:
        with open(filename) as f:
            s = f.read()
    except IOError as ex:
        raise ex 

    try:
        with open(filename, 'w') as f:
            s = s.replace(old_string, new_string)
            f.write(s)
    except IOError as ex:
        raise ex


def get_version():
    ''' Return version of currently installed Senzing  '''

    version = build_version = None

    try:
        with open(os.path.join(senzing_path, 'g2BuildVersion.json')) as file_version:
            version_details = json.load(file_version)
    except Exception as ex:
        pass
    else:
        version = version_details.get('VERSION', None)
        build_version = version_details.get('BUILD_VERSION', None)

    return f'{version} - ({build_version})' if version and build_version else 'Unknown, error reading build details!'


def get_ignored(path, filenames):
    '''  Return list of paths/files to ignore for copying '''

    ret = []
    for filename in filenames:
        if os.path.join(path, filename) in paths_to_exclude:
            ret.append(filename)
        elif filename in files_to_exclude:
            ret.append(filename)
    return ret


if __name__ == '__main__':

    senzing_path = '/opt/senzing/g2'
    senz_install_path = '/opt/senzing'
    paths_to_exclude = []
    files_to_exclude = ['G2CreateProject.py', 'G2UpdateProject.py']

    parser = argparse.ArgumentParser(description='Create a per-user instance of Senzing in a new folder with the specified name.')
    parser.add_argument('folder', metavar='F', nargs='?', default='~/senzing', help='the name of the folder to create, it must not already exist (Default: %(default)s)')
    args = parser.parse_args()

    target_path = os.path.normpath(os.path.join(os.getcwd(), os.path.expanduser(args.folder)))
    
    if os.path.exists(target_path) or os.path.isfile(target_path):
        print(f'\n{target_path} already exists or is a file. Please specify a folder that does not already exist.')
        sys.exit(1)

    if target_path.startswith(senz_install_path):
        print(f'\nProject cannot be created in {senz_install_path}. Please specify a different folder.')
        sys.exit(1)

    print(textwrap.dedent(f'''\n\
        Creating Senzing instance at: {target_path}
        Senzing version: {get_version()}
    '''))
    
    # Copy senzing_path to new project path
    shutil.copytree(senzing_path, target_path, ignore=get_ignored)
    
    # Copy resources/templates to etc
    files_to_ignore = shutil.ignore_patterns('G2C.db', 'setupEnv', '*.template')
    shutil.copytree(os.path.join(senzing_path,'resources', 'templates'), os.path.join(target_path, 'etc'), ignore=files_to_ignore)

    ##project_etc_path = os.path.join(target_path, 'etc')

    # Copy setupEnv
    shutil.copyfile(os.path.join(senzing_path, 'resources', 'templates', 'setupEnv'), os.path.join(target_path, 'setupEnv'))

    # Copy G2C.db to runtime location
    os.makedirs(os.path.join(target_path, 'var', 'sqlite'))
    shutil.copyfile(os.path.join(senzing_path, 'resources', 'templates', 'G2C.db'), os.path.join(target_path, 'var', 'sqlite','G2C.db'))
    
    # Soft link in data
    os.symlink('/opt/senzing/data/1.0.0', os.path.join(target_path, 'data'))

    # Files to modify in new project
    files_to_update = [
        'setupEnv',
        'etc/G2Module.ini'
    ]

    senzing_path_subs = [
        ('${SENZING_DIR}', target_path),
        ('${SENZING_CONFIG_PATH}', os.path.join(target_path, 'etc')),
        ('${SENZING_DATA_DIR}', os.path.join(target_path, 'data')),
        ('${SENZING_RESOURCES_DIR}', os.path.join(target_path, 'resources')),
        ('${SENZING_VAR_DIR}', os.path.join(target_path, 'var'))
    ]

    for f in files_to_update:
        for p in senzing_path_subs:
            find_replace_in_file(os.path.join(target_path, f), p[0], p[1])

    print('Succesfully created.')
