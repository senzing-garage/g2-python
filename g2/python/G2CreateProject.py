#! /usr/bin/env python3

import argparse
import json
import shutil
import sys
import textwrap
from pathlib import Path


def find_replace_in_file(filename, old_string, new_string):
    ''' Replace strings in new project files  '''

    try:
        with open(filename) as fr:
            s = fr.read()
        with open(filename, 'w') as fw:
            s = s.replace(old_string, new_string)
            fw.write(s)
    except IOError as ex:
        raise ex


def get_version():
    ''' Return version of currently installed Senzing  '''

    version = build_version = None

    try:
        build_version_file = senzing_path.joinpath('g2BuildVersion.json')
        with open(build_version_file) as file_version:
            version_details = json.load(file_version)
    except IOError as ex:
        print(f'\nERROR: Unable to read {build_version_file} to retrieve version details!')
        print(f'       {ex}')
        # Exit if version details can't be read. g2BuildVersion.json should be available and is copied to project
        return False

    version = version_details.get('VERSION', None)
    build_version = version_details.get('BUILD_VERSION', None)

    return f'{version} - ({build_version})' if version and build_version else 'Unable to determine version!'


def get_ignored(path, filenames):
    '''  Return list of paths/files to ignore for copying '''

    ret = []

    for filename in filenames:
        if Path(path).joinpath(filename) in paths_to_exclude:
            ret.append(filename)
        elif filename in files_to_exclude:
            ret.append(filename)

    return ret


if __name__ == '__main__':

    # senzing_path on normal rpm/deb install = /opt/senzing/g2
    # senzing_install_root would then = /opt/senzing
    senzing_path = Path(__file__).resolve().parents[1]
    senz_install_root = Path(__file__).resolve().parents[2]

    version = get_version()
    if not version:
        sys.exit(1)

    # Example: paths_to_exclude = [senzing_path.joinpath('python')]
    paths_to_exclude = []
    files_to_exclude = ['G2CreateProject.py', 'G2UpdateProject.py']

    parser = argparse.ArgumentParser(description='Create a new instance of a Senzing project in a path')
    parser.add_argument('path', metavar='PATH', nargs='?', default='~/senzing', help='path to create new Senzing project in, it must not already exist (Default: %(default)s)')
    args = parser.parse_args()

    target_path = Path(args.path).expanduser().resolve()

    if target_path.exists() and target_path.samefile(senz_install_root):
        print(f'\nProject cannot be created in {senz_install_root}. Please specify a different path.')
        sys.exit(1)

    if target_path.exists() or target_path.is_file():
        print(f'\n{target_path} already exists or is a file. Please specify a path that does not already exist.')
        sys.exit(1)

    print(textwrap.dedent(f'''\n\
        Creating Senzing instance at: {target_path}
        Senzing version: {version}
    '''))

    # Copy senzing_path to new project path
    shutil.copytree(senzing_path, target_path, ignore=get_ignored)

    # Copy resources/templates to etc
    files_to_ignore = shutil.ignore_patterns('G2C.db', 'setupEnv', '*.template')
    shutil.copytree(senzing_path.joinpath('resources', 'templates'), target_path.joinpath('etc'), ignore=files_to_ignore)

    # Copy setupEnv
    shutil.copyfile(senzing_path.joinpath('resources', 'templates', 'setupEnv'), target_path.joinpath('setupEnv'))

    # Copy G2C.db to runtime location
    Path.mkdir(target_path.joinpath('var', 'sqlite'), parents=True)
    shutil.copyfile(senzing_path.joinpath('resources', 'templates', 'G2C.db'), target_path.joinpath('var', 'sqlite', 'G2C.db'))

    # Soft link data
    target_path.joinpath('data').symlink_to(senz_install_root.joinpath('data', '1.0.0'))

    # Files & strings to modify in new project
    files_to_update = [
        target_path.joinpath('setupEnv'),
        target_path.joinpath('etc', 'G2Module.ini'),
    ]

    senzing_path_subs = [
        ('${SENZING_DIR}', target_path),
        ('${SENZING_CONFIG_PATH}', target_path.joinpath('etc')),
        ('${SENZING_DATA_DIR}', target_path.joinpath('data')),
        ('${SENZING_RESOURCES_DIR}', target_path.joinpath('resources')),
        ('${SENZING_VAR_DIR}', target_path.joinpath('var'))
    ]

    for f in files_to_update:
        for p in senzing_path_subs:
            find_replace_in_file(f, p[0], str(p[1]))

    print('Succesfully created.')
