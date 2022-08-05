#! /usr/bin/env python3

import argparse
import os
import pathlib
import sys
import G2Paths
from senzing import G2Config, G2ConfigMgr, G2Exception
from G2IniParams import G2IniParams


def setup_config(ini_params, auto_mode):

    # Determine if a default/initial G2 configuration already exists
    default_config_id = bytearray()

    try:
        g2_config_mgr = G2ConfigMgr()
        g2_config_mgr.init("g2ConfigMgr", ini_params, False)
        g2_config_mgr.getDefaultConfigID(default_config_id)
    except G2Exception as ex:
        print('\nERROR: Could not init G2ConfigMgr or get default config ID.')
        print(f'\n\t{ex}')
        return -1

    # If not in auto mode prompt user
    if not auto_mode:

        if default_config_id:
            if not input('\nA configuration document already exists in the database. Do you want to replace it (yes/no)?  ') in ['y', 'Y', 'yes', 'YES']:
                return -1
        else:

            if not input('\nInstalling template configuration to database. Do you want to continue (yes/no)?  ') in ['y', 'Y', 'yes', 'YES']:
                return -1

    # Apply a default configuration
    try:
        g2_config = G2Config()
        g2_config.init("g2Config", ini_params, False)
        config_handle = g2_config.create()
    except G2Exception as ex:
        print('\nERROR: Could not init G2Config or get template config from G2Config.')
        print(f'\n\t{ex}')
        return -1

    new_configuration_bytearray = bytearray()
    g2_config.save(config_handle, new_configuration_bytearray)
    g2_config.close(config_handle)
    config_json = new_configuration_bytearray.decode()

    # Save configuration JSON into G2 database.
    new_config_id = bytearray()

    try:
        g2_config_mgr.addConfig(config_json, 'Configuration added from G2SetupConfig.', new_config_id)
    except G2Exception as ex:
        ex_info = g2_config_mgr.getLastException().split('|', 1)
        # The engine configuration compatibility version [{0}] does not match the version of the provided config[{1}]
        if ex_info[0] == '0040E':
            print("\nERROR: Failed to add config to the repository. Please ensure your config is updated to the current version.")
        else:
            print("\nERROR: Failed to add config to the repository.")
        print(f'\n\t{ex}')
        return -1

    # Set the default configuration ID.
    try:
        g2_config_mgr.setDefaultConfigID(new_config_id)
    except G2Exception as ex:
        print("\nERROR: Failed to set config as default.")
        print(f'\n\t{ex}')
        return -1

    # Shut down
    g2_config_mgr.destroy()
    g2_config.destroy()

    print("\nConfiguration successfully added.")

    return 0


if __name__ == '__main__':

    argParser = argparse.ArgumentParser()
    argParser.add_argument('-c',
                           '--iniFile',
                           dest='ini_file_name',
                           default=None,
                           help='Path and file name of optional G2Module.ini to use.')

    # Run in non-interactive mode for Senzing team testing
    argParser.add_argument('-a',
                           '--auto',
                           action='store_true',
                           help=argparse.SUPPRESS)

    args = argParser.parse_args()

    #Check if INI file or env var is specified, otherwise use default INI file
    iniFileName = None

    if args.ini_file_name:
        iniFileName = pathlib.Path(args.ini_file_name)
    elif os.getenv("SENZING_ENGINE_CONFIGURATION_JSON"):
        ini_params = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON")
    else:
        iniFileName = pathlib.Path(G2Paths.get_G2Module_ini_path())

    if iniFileName:
        G2Paths.check_file_exists_and_readable(iniFileName)
        iniParamCreator = G2IniParams()
        ini_params = iniParamCreator.getJsonINIParams(iniFileName)

    sys.exit(setup_config(ini_params, args.auto))
