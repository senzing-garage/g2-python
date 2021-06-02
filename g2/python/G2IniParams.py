#! /usr/bin/env python3

#--python imports
try: import configparser
except: import ConfigParser as configparser
import json
import os
from shutil import copyfile

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

    #----------------------------------------
    def getINIParam(self,iniFileName,requestedGroupName,requestedParamName):
        ''' Gets an INI parameter string from an INI file. '''

        iniParser = configparser.ConfigParser(empty_lines_in_values=False)
        iniParser.read(iniFileName)

        paramDict = {}
        for groupName in iniParser.sections():  
            normalizedGroupName = groupName.upper()
            paramDict[normalizedGroupName] = {}
            for varName in iniParser[groupName]:  
                normalizedVarName = varName.upper()
                paramDict[normalizedGroupName][normalizedVarName] = iniParser[groupName][varName]

        paramValue = ''
        normalizedRequestedGroupName = requestedGroupName.upper()
        normalizedRequestedParamName = requestedParamName.upper()
        if normalizedRequestedGroupName in paramDict:
            if normalizedRequestedParamName in paramDict[normalizedRequestedGroupName]:
                paramValue = paramDict[normalizedRequestedGroupName][normalizedRequestedParamName]
        return paramValue

    #----------------------------------------
    def hasINIParam(self,iniFileName,requestedGroupName,requestedParamName):
        ''' Determines whether an INI parameter exists in an INI file. '''

        iniParser = configparser.ConfigParser(empty_lines_in_values=False)
        iniParser.read(iniFileName)

        paramDict = {}
        for groupName in iniParser.sections():  
            normalizedGroupName = groupName.upper()
            paramDict[normalizedGroupName] = {}
            for varName in iniParser[groupName]:  
                normalizedVarName = varName.upper()
                paramDict[normalizedGroupName][normalizedVarName] = iniParser[groupName][varName]

        hasParam = False
        normalizedRequestedGroupName = requestedGroupName.upper()
        normalizedRequestedParamName = requestedParamName.upper()
        if normalizedRequestedGroupName in paramDict:
            if normalizedRequestedParamName in paramDict[normalizedRequestedGroupName]:
                hasParam = True
        return hasParam

    #----------------------------------------
    def removeINIParam(self,iniFileName,requestedGroupName,requestedParamName,commentString):
        ''' Removes an INI parameter from a file, by commenting it out. '''

        normalizedRequestedGroupName = requestedGroupName.upper()
        normalizedRequestedParamName = requestedParamName.upper()
        if os.path.exists(iniFileName):
            try: copyfile(iniFileName, iniFileName + '.bk')
            except:
                print("Could not create %s" % iniFileName + '.bk')
                return
            with open(iniFileName, 'w') as fp:
                with open(iniFileName + '.bk') as data_in:
                    insideGroupSection = False
                    for fileLine in data_in:
                        line = fileLine.strip()
                        if len(line) > 0:
                            if line[0:1] not in ('#'):
                               if line[0] == '[' and line[-1] == ']':
                                   sectionName = line[1:-1]
                                   normalizedSectionName = sectionName
                                   if normalizedSectionName == normalizedRequestedGroupName:
                                       insideGroupSection = True;
                                   else:
                                       insideGroupSection = False;
                               if insideGroupSection == True:
                                   normalizedLine = line.upper()
                                   if normalizedLine.startswith(normalizedRequestedParamName + '='):
                                        fileLine = '# ' + fileLine
                                        endOfLineCharTrimmed = False
                                        if fileLine[-1] == '\n':
                                            fileLine = fileLine[0:-1]
                                            endOfLineCharTrimmed = True
                                        fileLine = fileLine + '     ##' + commentString
                                        if endOfLineCharTrimmed == True:
                                            fileLine = fileLine + '\n'
                        fp.write(fileLine)

