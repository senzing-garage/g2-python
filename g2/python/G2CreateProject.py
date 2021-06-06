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
    paths_to_exclude = [os.path.join(senzing_path, 'resources', 'config'), os.path.join(senzing_path, 'resources', 'schema')]
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
    
    # copy resources/templates to etc, then cut off the .template extension
    files_to_ignore = shutil.ignore_patterns('G2C.db.template')
    shutil.copytree(os.path.join(senzing_path,'resources', 'templates'), os.path.join(target_path, 'etc'))

    project_etc_path = os.path.join(target_path, 'etc')

    for file in os.listdir(project_etc_path):
        if file.endswith('.template'):
            os.rename(os.path.join(project_etc_path, file), os.path.join(project_etc_path, file).replace('.template',''))

    # copy G2C.db to runtime location
    os.makedirs(os.path.join(target_path, 'var', 'sqlite'))
    shutil.copyfile(os.path.join(senzing_path, 'resources', 'templates', 'G2C.db.template'), os.path.join(target_path, 'var', 'sqlite','G2C.db'))
    
    # soft link in data
    os.symlink('/opt/senzing/data/1.0.0', os.path.join(target_path, 'data'))

    # soft link in the two resouces folders
    os.symlink(os.path.join(senzing_path, 'resources', 'config'), os.path.join(target_path, 'resources', 'config'))
    os.symlink(os.path.join(senzing_path, 'resources', 'schema'), os.path.join(target_path, 'resources', 'schema'))

    # files to update
    files_to_update = [
        'setupEnv',
        'etc/G2Module.ini',
        'etc/G2Project.ini'
    ]

    # paths to substitute
    senzing_path_subs = [
        (senzing_path, target_path),
        (os.path.join(senzing_path, 'data'), os.path.join(target_path, 'data')),
        ('/etc/opt/senzing', os.path.join(target_path, 'etc')),
        ('/var/opt/senzing', os.path.join(target_path, 'var')),
        ('/opt/senzing', target_path),
        ('${SENZING_DIR}', target_path),
        ('${SENZING_CONFIG_DIR}', os.path.join(target_path, 'etc'))
    ]

    for f in files_to_update:
        for p in senzing_path_subs:
            # Anchor the replace on the character that comes before the path. This ensures that we are only 
            # replacing the begining of the path and not a substring of the path.
            find_replace_in_file(os.path.join(target_path, f), '=' + p[0], '=' + os.path.join(target_path, p[1]))
            find_replace_in_file(os.path.join(target_path, f), '@' + p[0], '@' + os.path.join(target_path, p[1]))

