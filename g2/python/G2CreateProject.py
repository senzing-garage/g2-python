#! /usr/bin/env python3

import argparse
import json
import os
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


def get_version_details():
    ''' Return version details as json of currently installed Senzing  '''

    try:
        build_version_file = senzing_path.joinpath('g2BuildVersion.json')
        with open(build_version_file) as file_version:
            version_details = json.load(file_version)
    except IOError as ex:
        print(f'\nERROR: Unable to read {build_version_file} to retrieve version details!')
        print(f'       {ex}')
        # Exit if version details can't be read. g2BuildVersion.json should be available and is copied to project
        return False

    return version_details


def get_version():
    ''' Return version of currently installed Senzing  '''

    version = build_version = None

    version_details = get_version_details()
    if not version_details:
        sys.exit(1)

    version = version_details.get('VERSION', None)
    build_version = version_details.get('BUILD_VERSION', None)

    return f'{version} - ({build_version})' if version and build_version else 'Unable to determine version!'


def get_data_version():
    ''' Return data version of currently installed data files  '''

    data_version = None

    version_details = get_version_details()
    if not version_details:
        sys.exit(1)

    data_version = version_details.get('DATA_VERSION', None)
    return data_version if data_version else 'Unable to determine data version!'

def get_ignored(path, filenames):
    '''  Return list of paths/files to ignore for copying '''

    ret = []

    for filename in filenames:
        if Path(path).joinpath(filename) in paths_to_exclude:
            ret.append(filename)
        elif filename in files_to_exclude:
            ret.append(filename)

    return ret


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

    # senzing_path on normal rpm/deb install = /opt/senzing/g2
    # senzing_install_root would then = /opt/senzing
    senzing_path = Path(__file__).resolve().parents[1]
    senz_install_root = Path(__file__).resolve().parents[2]

    version = get_version()
    if not version:
        sys.exit(1)

    data_version = get_data_version()
    if not data_version:
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
        Data version: {data_version}
    '''))

    # Copy senzing_path to new project path
    shutil.copytree(senzing_path, target_path, ignore=get_ignored, symlinks=True)

    # Copy resources/templates to etc
    files_to_ignore = shutil.ignore_patterns('G2C.db', 'setupEnv', '*.template', 'g2config.json')
    shutil.copytree(senzing_path.joinpath('resources', 'templates'), target_path.joinpath('etc'), ignore=files_to_ignore)

    # Copy setupEnv
    shutil.copyfile(senzing_path.joinpath('resources', 'templates', 'setupEnv'), target_path.joinpath('setupEnv'))

    # Copy G2C.db to runtime location
    Path.mkdir(target_path.joinpath('var', 'sqlite'), parents=True)
    shutil.copyfile(senzing_path.joinpath('resources', 'templates', 'G2C.db'), target_path.joinpath('var', 'sqlite', 'G2C.db'))

    # Soft link data
    target_path.joinpath('data').symlink_to(senz_install_root.joinpath('data', data_version))

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
    os.chmod(os.path.join(target_path, 'python', 'demo', 'truth', 'truthset-load1.sh'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'demo', 'truth', 'truthset-load2.sh'), 0o770)
    os.chmod(os.path.join(target_path, 'python', 'demo', 'truth', 'truthset-load3.sh'), 0o770)

    # resources
    set_permissions_on_files_in_folder_recursive(os.path.join(target_path, 'resources'), 0o660)
    os.chmod(os.path.join(target_path, 'resources', 'templates', 'setupEnv'), 0o770)

    # sdk
    set_permissions_on_files_in_folder_recursive(os.path.join(target_path, 'sdk'), 0o664)

    # var
    set_permissions_on_files_in_folder_recursive(os.path.join(target_path, 'var'), 0o660)

    print('Successfully created.')
