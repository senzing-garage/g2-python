#! /usr/bin/env python3

#--python imports
from G2IniParams import G2IniParams


#======================
class G2Health:
#======================

    #----------------------------------------
    def checkIniParams(self,iniFileName):
        ''' Checks the INI parameters. '''

        iniParamCreator = G2IniParams()
        hasG2configfileParamValue = iniParamCreator.hasINIParam(iniFileName,'Sql','G2ConfigFile')
        if hasG2configfileParamValue == True:
            print('Warning!!!  The INI parameter \'[SQL] G2ConfigFile\' is currently in use.  This causes the configuration to be loaded from a file, rather than from the database.  This functionality is deprecated and will be removed in future versions.  The config should be migrated to the system database.  See https://senzing.zendesk.com/hc/en-us/articles/360036587313 for more information.')

