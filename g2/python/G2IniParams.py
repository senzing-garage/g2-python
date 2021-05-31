#! /usr/bin/env python3

#--python imports
try: import configparser
except: import ConfigParser as configparser
import json

#--project classes

#======================
class G2IniParams:
#======================

    #----------------------------------------
    def getJsonINIParams(self,iniFileName):
        ''' Creates a JSON INI parameter string from an INI file. '''

        iniParser = configparser.ConfigParser(empty_lines_in_values=False)
        iniParser.read(iniFileName)

        paramDict = {}
        for groupName in iniParser.sections():  
            normalizedGroupName = groupName.upper()
            paramDict[normalizedGroupName] = {}
            for varName in iniParser[groupName]:  
                normalizedVarName = varName.upper()
                paramDict[normalizedGroupName][normalizedVarName] = iniParser[groupName][varName]

        jsonIniString = json.dumps(paramDict)
        return jsonIniString

