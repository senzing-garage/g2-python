#! /usr/bin/env python3

import sys
import os
import argparse
import errno
import shutil

senzing_path = '/opt/senzing/g2'

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
    if os.path.exists(target_path) or os.path.isfile(target_path):
        print('"' + target_path + '" already exists or is a path to a file. Please specify a folder that does not already exist.')
        sys.exit(1)

    if target_path.startswith('/opt/senzing'):
        print('Project cannot be created at "' + target_path + '". Projects cannot be created in /opt/senzing ')
        sys.exit(1)

    print("Creating Senzing instance at " + target_path )
    
    # copy opt
    paths_to_exclude = []
    files_to_exclude = ['G2CreateProject.py', 'G2UpdateProject.py']
    def get_ignored(path, filenames):        
        ret = []
        for filename in filenames:
            if os.path.join(path, filename) in paths_to_exclude:
                ret.append(filename)
            elif filename in files_to_exclude:
                ret.append(filename)
        return ret

    shutil.copytree(senzing_path, target_path, ignore=get_ignored)
    
    # copy resources/templates to etc
    files_to_ignore = shutil.ignore_patterns('G2C.db', 'setupEnv', '*.template')
    shutil.copytree(os.path.join(senzing_path,'resources', 'templates'), os.path.join(target_path, 'etc'), ignore=files_to_ignore)

    project_etc_path = os.path.join(target_path, 'etc')

    # copy setupEnv
    shutil.copyfile(os.path.join(senzing_path, 'resources', 'templates', 'setupEnv'), os.path.join(target_path, 'setupEnv'))

    # copy G2C.db to runtime location
    os.makedirs(os.path.join(target_path, 'var', 'sqlite'))
    shutil.copyfile(os.path.join(senzing_path, 'resources', 'templates', 'G2C.db'), os.path.join(target_path, 'var', 'sqlite','G2C.db'))
    
    # soft link in data
    os.symlink('/opt/senzing/data/1.0.0', os.path.join(target_path, 'data'))

    # files to update
    files_to_update = [
        'setupEnv',
        'etc/G2Module.ini',
        'etc/G2Project.ini'
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

