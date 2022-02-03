#! /usr/bin/env python3

import textwrap

from senzing import G2IniParams


#======================
class G2Health:
#======================

    #----------------------------------------
    def checkIniParams(self, iniFileName):
        ''' Checks the INI parameters. '''

        iniParamCreator = G2IniParams.G2IniParams()
        hasG2configfileParamValue = iniParamCreator.hasINIParam(iniFileName,'SQL','G2CONFIGFILE')

        if hasG2configfileParamValue:

            print(textwrap.dedent(f'''\n\
                WARN: INI parameter \'[SQL] G2CONFIGFILE\' is in use, this is deprecated and will be removed in future versions.
                      This causes the configuration to be loaded from a file, rather than the Senzing repository.
                      The config should be migrated to the Senzing repository: https://senzing.zendesk.com/hc/en-us/articles/360036587313 for more information.

                      Config file being used: {iniParamCreator.getINIParam(iniFileName,'SQL','G2CONFIGFILE')}
                      '''))
