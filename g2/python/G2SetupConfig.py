#! /usr/bin/env python3

#--python imports
import argparse
import os
import json

import sys
if sys.version[0] == '2':
    reload(sys)
    sys.setdefaultencoding('utf-8')

#--project classes
import G2Paths

from senzing import G2Config, G2ConfigMgr, G2Exception, G2IniParams


#---------------------------------------------------------------------
#-- Setup Config
#---------------------------------------------------------------------

def setupConfig(iniFileName,autoMode):

    #-- Load the G2 configuration file
    iniParamCreator = G2IniParams.G2IniParams()
    iniParams = iniParamCreator.getJsonINIParams(iniFileName)

    # Connect to the needed API
    g2ConfigMgr = G2ConfigMgr.G2ConfigMgr()
    g2ConfigMgr.init("g2ConfigMgr", iniParams, False)

    # Determine if a default/initial G2 configuration already exists.
    default_config_id = bytearray()
    g2ConfigMgr.getDefaultConfigID(default_config_id)

    # If a default configuration exists, there is nothing more to do.
    if default_config_id:
        shouldContinue = False
        reply = ''
        if autoMode == False:
            reply = userInput('\nA configuration document already exists in the database.  Do you want to replace it (yes/no)?  ')
        else:
            reply = os.environ.get("G2SETUPCONFIG_OVERWRITE_CONFIGURATION_DOC")
        if reply in ['y','Y', 'yes', 'YES']:
            shouldContinue = True
        if shouldContinue == False:
            print('Error:  Will not replace config in database.')
            return -1

    # See if "[SQL]G2ConfigFile" is configured
    g2ConfigFilePath = iniParamCreator.getINIParam(iniFileName,'Sql','G2ConfigFile')
    g2ConfigFileExistsInINIParams = False;
    if len(g2ConfigFilePath) > 0:
        g2ConfigFileExistsInINIParams = True;

    # get data to import
    configJsonToUse = ''
    if g2ConfigFileExistsInINIParams == True:
        shouldContinue = False
        reply = ''
        if autoMode == False:
            reply = userInput('\nMigrating configuration from file to database.  Do you want to continue (yes/no)?  ')
        else:
            reply = os.environ.get("G2SETUPCONFIG_MIGRATE_CONFIG_TO_DATABASE")
        if reply in ['y','Y', 'yes', 'YES']:
            shouldContinue = True
        if shouldContinue == False:
            print('Error:  Will not migrate config from file to database.')
            return -1
        jsonData = json.load(open(g2ConfigFilePath), encoding="utf-8")
        configJsonToUse = json.dumps(jsonData)
    else:
        shouldContinue = False
        reply = ''
        if autoMode == False:
            reply = userInput('\nInstalling template configuration to database.  Do you want to continue (yes/no)?  ')
        else:
            reply = os.environ.get("G2SETUPCONFIG_INSTALL_TEMPLATE_CONFIG_TO_DATABASE")
        if reply in ['y','Y', 'yes', 'YES']:
            shouldContinue = True
        if shouldContinue == False:
            print('Error:  Will not migrate config from file to database.')
            return -1
        g2Config = G2Config.G2Config()
        g2Config.init("g2Config", iniParams, False)
        config_handle = g2Config.create()
        if config_handle == None:
            print('Error:  Could not get template config.')
            return -1
        new_configuration_bytearray = bytearray()
        g2Config.save(config_handle, new_configuration_bytearray)
        g2Config.close(config_handle)
        g2Config.destroy()
        configJsonToUse = new_configuration_bytearray.decode()

    # Save configuration JSON into G2 database.
    config_comment = "Configuration added from G2SetupConfig."
    new_config_id = bytearray()
    try:
        g2ConfigMgr.addConfig(configJsonToUse, config_comment, new_config_id)
    except G2Exception.G2ModuleException as exc:
        print(exc)
        exceptionInfo = g2ConfigMgr.getLastException()
        exInfo = exceptionInfo.split('|', 1)
        if exInfo[0] == '0040E':
            print ("Error:  Failed to add config to the datastore.  Please ensure your config is updated to the current version.")
            return -1
        print ("Error:  Failed to add config to the datastore.")
        return -1

    # Set the default configuration ID.
    try:
        g2ConfigMgr.setDefaultConfigID(new_config_id)
    except G2Exception.G2Exception as err:
        print ("Error:  Failed to set config as default.")
        return -1

    # Remove the parameter from the INI file.
    if g2ConfigFileExistsInINIParams == True:
        iniParamCreator.removeINIParam(iniFileName,'Sql','G2ConfigFile','Removed by G2SetupConfig.py')

    # shut down the API's
    g2ConfigMgr.destroy()

    # We completed successfully
    print ("Config added successfully.")
    exitCode = 0
    return exitCode


#---------------------------------------------------------------------
#-- Main function
#---------------------------------------------------------------------

#----------------------------------------
if __name__ == '__main__':

    #--python3 uses input
    userInput = input

    argParser = argparse.ArgumentParser()
    argParser.add_argument('-c', '--iniFile', dest='iniFile', default='', help='the name of a G2Module.ini file to use', nargs='?')
    argParser.add_argument('-a', '--auto', action='store_true', help='should run in non-interactive mode')

    args = argParser.parse_args()
    #print(args)

    ini_file_name = ''

    if args.iniFile and len(args.iniFile) > 0:
        ini_file_name = os.path.abspath(args.iniFile)

    if ini_file_name == '':
        ini_file_name = G2Paths.get_G2Module_ini_path()

    G2Paths.check_file_exists_and_readable(ini_file_name)

    autoMode = args.auto == True

    exitCode = setupConfig(ini_file_name,autoMode)
    sys.exit(exitCode)

