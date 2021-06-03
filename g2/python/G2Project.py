#! /usr/bin/env python3

#--python imports
import optparse
import sys
import os
import json
import csv
import glob
import fnmatch
from operator import itemgetter

#--project classes
from G2Exception import G2UnsupportedFileTypeException
from G2Exception import G2InvalidFileTypeContentsException
from CompressedFile import openPossiblyCompressedFile, fileRowParser
from G2S3 import G2S3

try: from dateutil.parser import parse as dateParser
except: pass

try: from nameparser import HumanName as nameParser
except: pass

#======================
class G2Project:
#======================

    #----------------------------------------
    def __init__(self, g2ConfigTables, projectFileName = None, projectFileUri = None, tempFolderPath = None):
        """ open and validate a g2 generic configuration and project file """

        self.projectSourceList = []             
        self.mappingFiles = {}

        self.attributeDict = g2ConfigTables.loadConfig('CFG_ATTR')
        self.dataSourceDict = g2ConfigTables.loadConfig('CFG_DSRC')
        self.entityTypeDict = g2ConfigTables.loadConfig('CFG_ETYPE')

        self.success = True 
        self.mappingCache = {}
        self.tempFolderPath = tempFolderPath

        self.clearStatPack()
        self.loadAttributes()
        if self.success and projectFileName:
            self.loadProjectFile(projectFileName)
        elif self.success and projectFileUri:
            self.loadProjectUri(projectFileUri)

        return

    #-----------------------------
    #--mapping functions
    #-----------------------------

    #----------------------------------------
    def loadAttributes(self):
        ''' creates a feature/element structure out of the flat attributes in the mapping file '''

        #--create the list of elements per feature so that we can validate the mappings
        attributeDict = self.attributeDict
        self.featureDict = {}
        for configAttribute in attributeDict:
            if attributeDict[configAttribute]['FTYPE_CODE']:
                if attributeDict[configAttribute]['FTYPE_CODE'] not in self.featureDict:
                    self.featureDict[attributeDict[configAttribute]['FTYPE_CODE']] = {}
                    self.featureDict[attributeDict[configAttribute]['FTYPE_CODE']]['FEATURE_ORDER'] = attributeDict[configAttribute]['ATTR_ID']
                    self.featureDict[attributeDict[configAttribute]['FTYPE_CODE']]['ATTR_CLASS'] = attributeDict[configAttribute]['ATTR_CLASS']
                    self.featureDict[attributeDict[configAttribute]['FTYPE_CODE']]['ELEMENT_LIST'] = []
                if attributeDict[configAttribute]['ATTR_ID'] < self.featureDict[attributeDict[configAttribute]['FTYPE_CODE']]['FEATURE_ORDER']:
                    self.featureDict[attributeDict[configAttribute]['FTYPE_CODE']]['FEATURE_ORDER'] = attributeDict[configAttribute]['ATTR_ID']

        #--reclass dict no longer valid
        self.reclassDict = {}

        return

    #----------------------------------------
    def lookupAttribute(self, attrName):
        ''' determine if a valid config attribute '''
        attrName = attrName.upper()
        if attrName in self.mappingCache:
            attrMapping = dict(self.mappingCache[attrName]) #--dict() to create a new instance!
        else:
            usageType = ''
            configMapping = None
            if attrName in self.attributeDict:
                configMapping = self.attributeDict[attrName]
            elif '_' in attrName:
                usageType = attrName[0:attrName.find('_')]
                tryColumn = attrName[attrName.find('_') + 1:]
                if tryColumn in self.attributeDict:
                    configMapping = self.attributeDict[tryColumn]
                else:  
                   usageType = attrName[attrName.rfind('_') + 1:]
                   tryColumn = attrName[0:attrName.rfind('_')]
                   if tryColumn in self.attributeDict:
                        configMapping = self.attributeDict[tryColumn]

            attrMapping = {}
            if configMapping:
                attrMapping['ATTR_CLASS'] = configMapping['ATTR_CLASS']
                attrMapping['ATTR_CODE'] = configMapping['ATTR_CODE']
                attrMapping['ATTR_ID'] = configMapping['ATTR_ID']
                attrMapping['FTYPE_CODE'] = configMapping['FTYPE_CODE']
                attrMapping['UTYPE_CODE'] = usageType
                attrMapping['FELEM_CODE'] = configMapping['FELEM_CODE']
                attrMapping['FELEM_REQ'] = configMapping['FELEM_REQ']
            else:
                attrMapping['ATTR_CLASS'] = 'OTHER'
                attrMapping['ATTR_CODE'] = None
                attrMapping['ATTR_ID'] = 0
                attrMapping['FTYPE_CODE'] = None
                attrMapping['UTYPE_CODE'] = None
                attrMapping['FELEM_CODE'] = None
                attrMapping['FELEM_REQ'] = None

            self.mappingCache[attrName] = attrMapping
                
        return attrMapping

    #---------------------------------------
    def mapAttribute(self, attrName, attrValue):
        ''' places mapped column values into a feature structure '''

        #--strip spaces and ensure a string
        if attrValue:
            if type(attrValue) in (str, str):
                attrValue = attrValue.strip()
                #if attrValue.upper() == 'NULL' or self.containsOnly(attrValue, '.,-?'):
                #    attrValue = None
            else:
                attrValue = str(attrValue)

        #--if still a column value
        if attrValue:
            attrMapping = self.lookupAttribute(attrName)
            attrMapping['ATTR_NAME'] = attrName
            attrMapping['ATTR_VALUE'] = attrValue
        else:
            attrMapping = None

        return attrMapping

    #----------------------------------------
    def mapJsonRecord(self, jsonDict):
        '''checks for mapping errors'''
        mappingErrors = []
        mappedAttributeList = []
        valuesByClass = {}
        entityName = None
        self.recordCount += 1

        #--parse the json
        for columnName in jsonDict:
            if type(jsonDict[columnName]) == list: 
                childRowNum = -1
                for childRow in jsonDict[columnName]:
                    childRowNum += 1
                    if type(childRow) != dict:
                        mappingErrors.append('expected {records} in list under %s' % columnName)
                        break
                    else:
                        for childColumn in childRow:
                            if type(jsonDict[columnName][childRowNum][childColumn]) not in (dict, list): #--safeguard:
                                mappedAttribute = self.mapAttribute(childColumn, jsonDict[columnName][childRowNum][childColumn])
                                if mappedAttribute:
                                    mappedAttribute['ATTR_LEVEL'] = columnName + '#' + str(childRowNum)
                                    mappedAttributeList.append(mappedAttribute)
                            else:
                                mappingErrors.append('unexpected {records} under %s of %s' % (childColumn, columnName))
                                break

            elif type(jsonDict[columnName]) != dict: #--safeguard
                columnValue = jsonDict[columnName]
                mappedAttribute = self.mapAttribute(columnName, jsonDict[columnName])
                if mappedAttribute:
                    mappedAttribute['ATTR_LEVEL'] = 'ROOT'
                    mappedAttributeList.append(mappedAttribute)
            else:
                mappingErrors.append('unexpected {records} under %s' % columnName)

        if not mappingErrors:

            #--create a feature key to group elements that go together
            elementKeyVersions = {}
            for i in range(len(mappedAttributeList)):
                if mappedAttributeList[i]['FTYPE_CODE']:
                    if mappedAttributeList[i]['UTYPE_CODE']:
                        featureKey = mappedAttributeList[i]['ATTR_LEVEL'] + '-' + mappedAttributeList[i]['FTYPE_CODE'] + '-' + mappedAttributeList[i]['UTYPE_CODE']
                        elementKey = mappedAttributeList[i]['ATTR_LEVEL'] + '-' + mappedAttributeList[i]['FTYPE_CODE'] + '-' + mappedAttributeList[i]['UTYPE_CODE'] + '-' + mappedAttributeList[i]['FELEM_CODE']
                    else:
                        featureKey = mappedAttributeList[i]['ATTR_LEVEL'] + '-' + mappedAttributeList[i]['FTYPE_CODE']
                        elementKey = mappedAttributeList[i]['ATTR_LEVEL'] + '-' + mappedAttributeList[i]['FTYPE_CODE'] + '-' + mappedAttributeList[i]['FELEM_CODE']

                    if elementKey in elementKeyVersions:
                        elementKeyVersions[elementKey] += 1
                    else:
                        elementKeyVersions[elementKey] = 0
                    mappedAttributeList[i]['FEATURE_KEY'] = featureKey + '-' + str(elementKeyVersions[elementKey])
                else:
                    mappedAttributeList[i]['FEATURE_KEY'] = ""

            #--validate and count the features
            featureCount = 0
            mappedAttributeList = sorted(mappedAttributeList, key=itemgetter('FEATURE_KEY', 'ATTR_ID'))
            mappedAttributeListLength = len(mappedAttributeList) 
            i = 0
            while i < mappedAttributeListLength:
                if mappedAttributeList[i]['FEATURE_KEY']:
                    featureKey = mappedAttributeList[i]['FEATURE_KEY']
                    ftypeClass = mappedAttributeList[i]['ATTR_CLASS']
                    ftypeCode = mappedAttributeList[i]['FTYPE_CODE']
                    utypeCode = mappedAttributeList[i]['UTYPE_CODE']
                    featureDesc = ''
                    completeFeature = False
                    while i < mappedAttributeListLength and mappedAttributeList[i]['FEATURE_KEY'] == featureKey:
                        if mappedAttributeList[i]['FELEM_CODE'] == 'USAGE_TYPE':
                            utypeCode = mappedAttributeList[i]['ATTR_VALUE']
                        elif mappedAttributeList[i]['FELEM_CODE'].upper() not in ('USED_FROM_DT', 'USED_THRU_DT'):
                            featureDesc += ('' if len(featureDesc) == 0 else ' ') + mappedAttributeList[i]['ATTR_VALUE']
                            if mappedAttributeList[i]['FELEM_REQ'].upper() in ('YES', 'ANY'):
                                completeFeature = True
                        i += 1

                    if completeFeature:
                        featureCount += 1

                        #--update mapping stats
                        statCode = ftypeCode + ('-' + utypeCode if utypeCode else '')
                        if statCode in self.featureStats:
                            self.featureStats[statCode] += 1
                        else:
                            self.featureStats[statCode] = 1
                        
                        #--yse first name encountered as the entity description
                        if ftypeCode == 'NAME' and not entityName:
                            entityName = featureDesc

                        #--update values by class
                        if utypeCode:
                            featureDesc = utypeCode +': ' + featureDesc
                        if ftypeCode not in ('NAME', 'ADDRESS', 'PHONE'):
                            featureDesc = ftypeCode +': ' + featureDesc
                        if ftypeClass in valuesByClass:
                            valuesByClass[ftypeClass] += '\n' + featureDesc
                        else:
                            valuesByClass[ftypeClass] = featureDesc
                        
                else:
                    #--this is an unmapped attribute
                    if mappedAttributeList[i]['ATTR_CLASS'] == 'OTHER':

                        #--update mapping stats
                        statCode = mappedAttributeList[i]['ATTR_NAME']
                        if statCode in self.unmappedStats:
                            self.unmappedStats[statCode] += 1
                        else:
                            self.unmappedStats[statCode] = 1
                        
                        #--update values by class
                        attrClass = mappedAttributeList[i]['ATTR_CLASS']
                        attrDesc = mappedAttributeList[i]['ATTR_NAME'] +': ' + mappedAttributeList[i]['ATTR_VALUE']
                        if attrClass in valuesByClass:
                            valuesByClass[attrClass] += '\n' + attrDesc
                        else:
                            valuesByClass[attrClass] = attrDesc

                    elif mappedAttributeList[i]['ATTR_CLASS'] == 'OBSERVATION':
                        pass #--no need as not creating umf

                    i += 1

            if featureCount == 0:
                mappingErrors.append('no features mapped')

        return [mappingErrors, mappedAttributeList, valuesByClass, entityName]

    #----------------------------------------
    def validateJsonMessage(self, msg):
        ''' validates a json message and return the mappings '''
        jsonErrors = []
        jsonMappings = {}
        if type(msg) == dict:
            jsonDict = msg
        else: 
            try: jsonDict = json.loads(msg, encoding="utf-8")
            except:
                jsonErrors.append('ERROR: could not parse as json')
                jsonDict = None

        if not jsonErrors:
            ##print '-' * 50
            for columnName in jsonDict:
                ##print columnName+'-'*10, type(jsonDict[columnName])
                if type(jsonDict[columnName]) == dict:
                    jsonErrors.append('ERROR: single value expected: %s : %s' % (columnName, json.dumps(jsonDict[columnName])))
                elif type(jsonDict[columnName]) == list:
                    childRowNum = 0
                    for childRow in jsonDict[columnName]:
                        childRowNum += 1
                        if type(childRow) != dict:
                            jsonErrors.append('ERROR: dict expected: %s : %s' % (columnName, json.dumps(jsonDict[columnName])))
                            break
                        else:
                            for childColumn in childRow:
                                ##print str(childRowNum) + ' ' + childColumn + '-'*8, type(childColumn)
                                if type(childColumn) in (dict, list):
                                    jsonErrors.append('ERROR: single value expected: %s : %s' % (columnName, json.dumps(jsonDict[columnName])))
                                    break
                                elif childColumn not in jsonMappings:
                                    jsonMappings[childColumn] = self.lookupAttribute(childColumn)

                elif columnName not in jsonMappings:
                    jsonMappings[columnName] = self.lookupAttribute(columnName)

        return jsonErrors, jsonDict, jsonMappings 

    #---------------------------------------
    def clearStatPack(self):
        ''' clear the statistics on demand '''
        self.recordCount = 0
        self.featureStats = {}
        self.unmappedStats = {}
        return
    
    #---------------------------------------
    def getStatPack(self):

        ''' return the statistics for each feature '''
        statPack = {}

        featureStats = []
        for feature in self.featureStats:
            featureStat = {}
            featureStat['FEATURE'] = feature
            if '-' in feature:
                featureSplit = feature.split('-')
                featureStat['FTYPE_CODE'] = featureSplit[0] 
                featureStat['UTYPE_CODE'] = featureSplit[1]
            else:
                featureStat['FTYPE_CODE'] = feature 
                featureStat['UTYPE_CODE'] = ''
            featureStat['FEATURE_ORDER'] = self.featureDict[featureStat['FTYPE_CODE']]['FEATURE_ORDER']
            featureStat['COUNT'] = self.featureStats[feature]
            if self.recordCount == 0:
                featureStat['PERCENT'] = 0
            else:
                featureStat['PERCENT'] = round((float(featureStat['COUNT']) / self.recordCount) * 100,2)
            featureStats.append(featureStat)
        statPack['FEATURES'] = sorted(featureStats, key=itemgetter('FEATURE_ORDER', 'UTYPE_CODE'))
 
        unmappedStats = []
        for attribute in self.unmappedStats:
            unmappedStat = {}
            unmappedStat['ATTRIBUTE'] = attribute
            unmappedStat['COUNT'] = self.unmappedStats[attribute]
            if self.recordCount == 0:
                unmappedStat['PERCENT'] = 0
            else:
                unmappedStat['PERCENT'] = round((float(unmappedStat['COUNT']) / self.recordCount) * 100,2)
            unmappedStats.append(unmappedStat)
        statPack['UNMAPPED'] = sorted(unmappedStats, key=itemgetter('ATTRIBUTE'))
 
        return statPack

    #---------------------------------------
    def featureToJson(self, featureList):
        ''' turns database feature (felem_values) strings back into json attributes '''
        jsonString = None

        return jsonString


    #-----------------------------
    #--project functions
    #-----------------------------

    #----------------------------------------
    def loadProjectUri(self, fileSpec):
        ''' creates a project dictionary from a file spec '''
        parmDict = {}
        if '/?' in fileSpec:
            parmString = fileSpec.split('/?')[1]
            fileSpec = fileSpec.split('/?')[0]
            parmList = parmString.split(',')
            for parm in parmList:
                if '=' in parm:
                    parmType = parm.split('=')[0].strip().upper()
                    parmValue = parm.split('=')[1].strip().replace('"','').replace("'",'').upper()
                    parmDict[parmType] = parmValue

        #--try to determine file_format
        if 'FILE_FORMAT' not in parmDict:
            if 'FILE_TYPE' in parmDict:
                parmDict['FILE_FORMAT'] = parmDict['FILE_TYPE']
            else:
                dummy, fileExtension = os.path.splitext(fileSpec)
                parmDict['FILE_FORMAT'] = fileExtension.replace('.','').upper()

        if parmDict['FILE_FORMAT'] not in ('JSON', 'CSV', 'UMF', 'TAB', 'TSV', 'PIPE'):
            print('ERROR: File format must be either JSON, CSV UMF, TAB, TSV or PIPE to use the file specification!')
            self.success = False
        else:
            if G2S3.isS3Uri(fileSpec):
                s3list = G2S3.ListOfS3UrisOfFilesInBucket(fileSpec, os.path.dirname(G2S3.getFilePathFromUri(fileSpec)))
                fileList = fnmatch.filter(s3list, fileSpec)
            else:
                if fileSpec.upper().startswith('FILE://'):
                    fileSpec = fileSpec[7:]
                try: fileList = glob.glob(fileSpec)
                except: fileList = []
            if not fileList:
                print('ERROR: file specification did not return any files!')
                self.success = False
            else:
                self.projectFileName = 'n/a'
                self.projectFilePath = os.path.dirname(os.path.abspath(fileList[0]))
                for fileName in fileList:
                    sourceDict = {} 
                    sourceDict['FILE_NAME'] = fileName
                    sourceDict['FILE_FORMAT'] = parmDict['FILE_FORMAT']
                    if 'DATA_SOURCE' in parmDict:
                        sourceDict['DATA_SOURCE'] = parmDict['DATA_SOURCE'] 
                    self.projectSourceList.append(sourceDict)

                self.prepareSourceFiles()

        return

    #----------------------------------------
    def loadProjectFile(self, projectFileName):
        ''' ensures the project file exists, is valid and kicks off correct processor - csv or json '''
        #--hopefully its a valid project file
        if os.path.exists(projectFileName):

            self.projectFileName = projectFileName
            self.projectFilePath = os.path.dirname(os.path.abspath(projectFileName))
            fileName, fileExtension = os.path.splitext(projectFileName)
            if fileExtension.upper() == '.JSON':
                self.projectFileFormat = 'JSON'
            elif fileExtension.upper() in ('.CSV', '.TSV', '.TAB', '.PIPE'):
                self.projectFileFormat = fileExtension[1:].upper()
            else:
                print('ERROR: Invalid project file extension [%s]' % fileExtension)
                print(' Supported project file extensions include: .json, .csv, .tsv, .tab, and .pipe')
                self.success = False 
                return

            #--load a json project file
            if self.projectFileFormat == 'JSON':
                self.loadJsonProject()

            #--its gotta be a csv dialect
            else:
                self.loadCsvProject()

            if self.success:
                self.prepareSourceFiles()
        else:
            print('ERROR: project file ' + projectFileName + ' not found!')
            self.success = False 

        return

    #----------------------------------------
    def dictKeysUpper(self, in_dict):
        if type(in_dict) is dict:
            out_dict = {}
            for key, item in in_dict.items():
                out_dict[key.upper()] = self.dictKeysUpper(item)
            return out_dict
        elif type(in_dict) is list:
            return [self.dictKeysUpper(obj) for obj in in_dict]
        else:
            return in_dict

    #----------------------------------------
    def loadJsonProject(self):
        ''' validates and loads a json project file into memory '''
        try: projectData = json.load(open(self.projectFileName), encoding="utf-8")
        except Exception as err:
            print('ERROR: project file ' + repr(err))
            self.success = False 
        else:
            projectData = self.dictKeysUpper(projectData)
            if type(projectData) == list:
                projectData = {"DATA_SOURCES": projectData}

            if "DATA_SOURCES" in projectData:
                sourceRow = 0
                for sourceDict in projectData['DATA_SOURCES']:
                    sourceRow += 1
                    if 'FILE_NAME' not in sourceDict:
                        print('ERROR: project file entry ' + str(sourceRow) + ' does not contain an entry for FILE_NAME!')
                        self.success = False
                        break
                    self.projectSourceList.append(sourceDict)             

        if len(self.projectSourceList) == 0:
            print('ERROR: project file does not contain any data sources!')

        return

    #----------------------------------------
    def loadCsvProject(self):
        fileData = {}
        fileData['FILE_NAME'] = self.projectFileName
        fileData['FILE_FORMAT'] = self.projectFileFormat
        if self.projectFileFormat == 'CSV':
            fileData['DELIMITER'] = ','
        elif self.projectFileFormat in ('TSV', 'TAB'):
            fileData['DELIMITER'] = '\t'
        elif self.projectFileFormat == 'PIPE':
            fileData['DELIMITER'] = '|'
        fileData['MULTICHAR_DELIMITER'] = False

        csvFile = openPossiblyCompressedFile(self.projectFileName, 'r')
        fileData['HEADER_ROW'] = [x.strip().upper() for x in fileRowParser(next(csvFile), fileData)]
        if not(fileData['HEADER_ROW']):
            print('ERROR: project file does not contain a header row!')
            self.success = False
        elif not 'FILE_NAME' in fileData['HEADER_ROW']:
            print('ERROR: project file does not contain a column for FILE_NAME!')
            self.success = False
        else:

            for line in csvFile:
                rowData = fileRowParser(line, fileData) 
                if rowData: #--skip blank lines
                    self.projectSourceList.append(rowData)
        csvFile.close()

        return

    #----------------------------------------
    def prepareSourceFiles(self):
        ''' ensure project files referenced exist and are valid '''
        print('')
        self.sourceList = []
        sourceRow = 0
        for sourceDict in self.projectSourceList:
            sourceRow += 1

            #--bypass if disabled
            if 'ENABLED' in sourceDict and str(sourceDict['ENABLED']).upper() in ('0', 'N','NO'):
                continue

            #--validate source file
            sourceDict['FILE_NAME'] = sourceDict['FILE_NAME'].strip()
            if len(sourceDict['FILE_NAME']) == 0:
                print('ERROR: project file entry ' + str(sourceRow) + ' does not contain a FILE_NAME!')
                self.success = False

            if 'DATA_SOURCE' in sourceDict:
                sourceDict['DATA_SOURCE'] = sourceDict['DATA_SOURCE'].strip().upper()
                if 'ENTITY_TYPE' not in sourceDict:
                    sourceDict['ENTITY_TYPE'] = sourceDict['DATA_SOURCE']

            if 'FILE_FORMAT' not in sourceDict:
                fileName, fileExtension = os.path.splitext(sourceDict['FILE_NAME'])
                sourceDict['FILE_FORMAT'] = fileExtension[1:].upper()
            else:
                sourceDict['FILE_FORMAT'] = sourceDict['FILE_FORMAT'].upper()

            if sourceDict['FILE_FORMAT'] not in ('JSON', 'CSV', 'TSV', 'TAB', 'PIPE'):
                print('ERROR: project file entry ' + str(sourceRow) + ' does not contain a valid file format!')
                self.success = False

            #--csv stuff
            sourceDict['ENCODING'] = sourceDict['ENCODING'] if 'ENCODING' in sourceDict else 'utf-8-sig'
            sourceDict['DELIMITER'] = sourceDict['DELIMITER'] if 'DELIMITER' in sourceDict else None
            if not sourceDict['DELIMITER']:
                if sourceDict['FILE_FORMAT'] == 'CSV':
                    sourceDict['DELIMITER'] = ','
                elif sourceDict['FILE_FORMAT'] in ('TSV', 'TAB'):
                    sourceDict['DELIMITER'] = '\t'
                elif sourceDict['FILE_FORMAT'] == 'PIPE':
                    sourceDict['DELIMITER'] = '|'
                else:
                    sourceDict['DELIMITER'] = ''
            else:
                sourceDict['DELIMITER'] = str(sourceDict['DELIMITER'])
            sourceDict['MULTICHAR_DELIMITER'] = len(sourceDict['DELIMITER']) > 1
            sourceDict['QUOTECHAR'] = sourceDict['QUOTECHAR'] if 'QUOTECHAR' in sourceDict else None

            #--csv mapping stuff
            if 'MAPPING_FILE' in sourceDict:
                if not os.path.exists(sourceDict['MAPPING_FILE']):
                    sourceDict['MAPPING_FILE'] = self.projectFilePath + os.path.sep + sourceDict['MAPPING_FILE']
                if sourceDict['MAPPING_FILE'] not in self.mappingFiles:
                    if not os.path.exists(sourceDict['MAPPING_FILE']):
                        print('ERROR: Mapping file %s does not exist for project file entry %s' % (sourceDict['MAPPING_FILE'], str(sourceRow)))
                        self.success = False
                    else:
                        try: mappingFileDict = json.load(open(sourceDict['MAPPING_FILE']))
                        except ValueError as err: 
                            print('ERROR: Invalid json in mapping file  %s' % (sourceDict['MAPPING_FILE']))
                            print(err)
                            self.success = False
                        else:
                            self.mappingFiles[sourceDict['MAPPING_FILE']] = self.dictKeysUpper(mappingFileDict)
            else:
                sourceDict['MAPPING_FILE'] = None

            #--validate and map the files for this source
            if self.success:
                if G2S3.isS3Uri(sourceDict['FILE_NAME']):
                    #--an S3 path so download the file to the temp location
                    downloader = G2S3(sourceDict['FILE_NAME'], self.tempFolderPath)
                    downloader.downloadFile()
                    sourceDict['FILE_PATH'] = downloader.tempFilePath
                    sourceDict['FILE_NAME'] = downloader.fileName
                    sourceDict['FILE_SOURCE'] = "S3"
                elif os.path.exists(sourceDict['FILE_NAME']):
                    #--adjustment if they gave us full path as file name
                    sourceDict['FILE_PATH'] = sourceDict['FILE_NAME']
                    sourceDict['FILE_NAME'] = os.path.basename(sourceDict['FILE_PATH'])
                    sourceDict['FILE_SOURCE'] = 'local'
                else:  #--append the project file path
                    sourceDict['FILE_PATH'] = self.projectFilePath + os.path.sep + sourceDict['FILE_NAME']
                    sourceDict['FILE_SOURCE'] = 'local'

                print('Validating %s ...' % sourceDict['FILE_PATH'])

                if not os.path.exists(sourceDict['FILE_PATH']):
                    print(' ERROR: File does not exist!')
                    self.success = False
                else:

                    #--test first 100 rows
                    rowCnt = 0
                    badCnt = 0
                    fileReader = openPossiblyCompressedFile(sourceDict['FILE_PATH'], 'r', sourceDict['ENCODING'])

                    #--get header row if csv
                    if sourceDict['FILE_FORMAT'] not in ('JSON', 'UMF'):
                        sourceDict['HEADER_ROW'] = [x.strip().upper() for x in fileRowParser(next(fileReader), sourceDict)]

                    for row in fileReader:
                        rowCnt += 1
                        if rowCnt > 100:
                            rowCnt -= 1
                            break

                        rowData = fileRowParser(row, sourceDict, rowCnt)
                        if not rowData:
                            badCnt += 1
                            continue

                        if sourceDict['FILE_FORMAT'] == 'UMF':
                            if not (rowData.upper().startswith('<UMF_DOC') and rowData.upper().endswith('/UMF_DOC>')):
                                print(' WARNING: invalid UMF in row %s (%s)' % (rowCnt, rowData[0:50]))
                                badCnt += 1
                        else: #--json or csv

                            #--perform csv mapping if needed
                            if not sourceDict['MAPPING_FILE']:
                                recordList = [rowData]
                            else:
                                rowData['_MAPPING_FILE'] = sourceDict['MAPPING_FILE']
                                recordList, errorCount = self.csvMapper(rowData)
                                if errorCount:
                                    badCnt += 1
                                    recordList = []

                            #--ensure output(s) have mapped features
                            for rowData in recordList:
                                if 'DATA_SOURCE' not in rowData and 'DATA_SOURCE' not in sourceDict:
                                    print(' WARNING: data source missing in row %s and not specified at the file level' % rowCnt)
                                    badCnt += 1
                                elif 'DATA_SOURCE' in rowData and rowData['DATA_SOURCE'] not in self.dataSourceDict:
                                    print(' WARNING: invalid data_source in row %s (%s)' % (rowCnt, rowData['DATA_SOURCE']))
                                    badCnt += 1
                                elif 'ENTITY_TYPE' in rowData and rowData['ENTITY_TYPE'] not in self.entityTypeDict:
                                    print(' WARNING: invalid entity_type in row %s (%s)' % (rowCnt, rowData['ENTITY_TYPE']))
                                    badCnt += 1
                                elif 'DSRC_ACTION' in rowData and rowData['DSRC_ACTION'].upper() == 'X':
                                    pass
                                else:
                                    #--other mapping errors
                                    mappingResponse = self.mapJsonRecord(rowData)
                                    if mappingResponse[0]:
                                        badCnt += 1
                                        for mappingError in mappingResponse[0]:
                                            print(' WARNING: mapping error in row %s (%s)' % (rowCnt, mappingError))

                    #--fails if too many bad records (more than 10 of 100 or all bad)
                    if badCnt >= 10 or badCnt == rowCnt:
                        print(' ERROR: Pre-test failed %s bad records in first %s' % (badCnt, rowCnt))
                        self.success = False

                    fileReader.close()

            self.sourceList.append(sourceDict)
        return

    #----------------------------------------
    def csvMapper(self, rowData):
        outputRows = []

        #--clean garbage values
        for key in rowData:
            try:
                if rowData[key].upper() in ('NULL', 'NONE', 'N/A', '\\N'):
                    rowData[key] = ''
            except: pass

        mappingErrors = 0
        csvMap = self.mappingFiles[rowData['_MAPPING_FILE']]

        if 'CALCULATIONS' in csvMap:

            if type(csvMap['CALCULATIONS']) == dict:
                for newField in csvMap['CALCULATIONS']:
                    try: rowData[newField] = eval(csvMap['CALCULATIONS'][newField])
                    except Exception as e: 
                        print('  error: %s [%s]' % (newField, e)) 
                        mappingErrors += 1

            elif type(csvMap['CALCULATIONS']) == list:
                for calcDict in csvMap['CALCULATIONS']:
                    try: rowData[calcDict['NAME']] = eval(calcDict['EXPRESSION'])
                    except Exception as e: 
                        print('  error: %s [%s]' % (calcDict['NAME'], e)) 
                        mappingErrors += 1

        #--for each mapping (output record)
        for mappingData in csvMap['MAPPINGS']:
            mappedData = {}
            for columnName in mappingData:

                #--perform the mapping
                try: columnValue = mappingData[columnName] % rowData
                except: 
                    print('  error: could not map %s' % mappingData[columnName]) 
                    mappingErrors += 1
                    columnValue = ''

                #--clear nulls
                if not columnValue or columnValue.upper() in ('NONE', 'NULL', '\\N'):
                    columnValue = ''

                #--dont write empty tags
                if columnValue: 
                    mappedData[columnName] = columnValue

            outputRows.append(mappedData)

        return outputRows, mappingErrors

#--------------------
#--utility functions
#--------------------

#----------------------------------------
def pause(question = None):
    if not question:
        v_wait = input("PRESS ENTER TO CONTINUE ... ")
    else:
        v_wait = input(question)
    return v_wait

#----------------------------------------
def containsOnly(seq, aset):
    ''' Check whether sequence seq contains ONLY items in aset '''
    for c in seq:
        if c not in aset: return False
    return True

#----------------------------------------
def calcNameKey(fullNameStr, keyType):

    if type(fullNameStr) == list:
        newStr = ''
        for namePart in fullNameStr:
            if namePart:
                newStr += (' ' + namePart)
        fullNameStr = newStr.strip()

    try: 
        values = []
        parsedName = nameParser(fullNameStr)
        if keyType.upper() == 'FULL':
            if len(parsedName.last) != 0:
                values.append(parsedName.last)
            if len(parsedName.first) != 0:
                values.append(parsedName.first)
            if len(parsedName.middle) != 0:
                values.append(parsedName.middle)
        else:
            if len(parsedName.last) != 0 and len(parsedName.first) != 0 and len(parsedName.middle) != 0:
                values = [parsedName.last, parsedName.first]
    except:
        values = fullNameStr.upper().replace('.',' ').replace(',',' ').split()

    return '|'.join(sorted([x.upper() for x in values]))

#----------------------------------------
def calcOrgKey(fullNameStr):
    values = fullNameStr.upper().replace('.',' ').replace(',',' ').split()
    return ' '.join(values)

#----------------------------------------
def compositeKeyBuilder(rowData, keyList):
    values = []
    for key in keyList:
        if key in rowData and rowData[key]:
            values.append(str(rowData[key]))
        else:
            return ''
    return '|'.join(values)

#----------------------------------------
if __name__ == "__main__":

    #--running in debug mode - no parameters
    if len(sys.argv) == 1:
        mappingFileName = './g2Generic.map'
        projectFileName = './test/input/project.json'

    #--capture the command line arguments
    else:
        optParser = optparse.OptionParser()
        optParser.add_option('-m', '--mappingFile', dest='mappingFileName', default='', help='the name of a g2 attribute mapping file')
        optParser.add_option('-p', '--projectFile', dest='projectFileName', default='', help='the name of a g2 project csv or json file')
        (options, args) = optParser.parse_args()
        mappingFileName = options.mappingFileName
        projectFileName = options.projectFileName
  
    #--create an instance
    myProject = g2Mapper('self', mappingFileName, projectFileName)
    if myProject.success:
        print('SUCCESS: project ' + projectFileName + ' is valid!')

    #--delete the instance
    del myProject

    sys.exit()

