#--python imports
import optparse
import sys
import os
import json
import csv
import glob 
from operator import itemgetter

#--project classes
from G2Exception import G2UnsupportedFileTypeException
from G2Exception import G2InvalidFileTypeContentsException
from CompressedFile import openPossiblyCompressedFile

#======================
class G2Project:
#======================

    #----------------------------------------
    def __init__(self, attributeDict, projectFileName = None, projectFileUri = None):
        """ open and validate a g2 generic configuration and project file """

        self.success = True 
        self.mappingCache = {}

        self.clearStatPack()
        self.loadAttributes(attributeDict)
        if self.success and projectFileName:
            self.loadProjectFile(projectFileName)
        elif self.success and projectFileUri:
            self.loadProjectUri(projectFileUri)

        return

    #-----------------------------
    #--mapping functions
    #-----------------------------

    #----------------------------------------
    def loadAttributes(self, attributeDict):
        ''' creates a feature/element structure out of the flat attributes in the mapping file '''
        self.attributeDict = attributeDict

        #--create the list of elements per feature so that we can validate the mappings
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
        '''create umf from a json dictionary '''
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
                featureStat['UTYPE_CODE'] = None
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
        self.projectSourceList = []
        if '/?' in fileSpec:
            parmDict = {}
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

            #--convert TSV to TAB
            if parmDict['FILE_FORMAT'] == 'TSV':
                parmDict['FILE_FORMAT'] = 'TAB'

            #--ensure we have all we need
            if 'DATA_SOURCE' not in parmDict:
                print('ERROR: must include "data_source=?" in the /? parameters to use the file specification!')
                self.success = False 
            elif 'FILE_FORMAT' not in parmDict:
                print('ERROR: file_format=? must be included in the /? parameters to use the file specification!')
                self.success = False
            elif parmDict['FILE_FORMAT'] not in ('JSON', 'CSV', 'UMF', 'TAB', 'TSV', 'PIPE'):
                print('ERROR: File format must be either JSON, CSV UMF, TAB, TSV or PIPE to use the file specification!')
                self.success = False
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
                        sourceDict['FILE_NAME'] = os.path.basename(fileName)
                        sourceDict['DATA_SOURCE'] = parmDict['DATA_SOURCE']
                        sourceDict['FILE_FORMAT'] = parmDict['FILE_FORMAT']
                        self.projectSourceList.append(sourceDict)

                    self.prepareSourceFiles()

        else:
            print('ERROR: must include /?data_source=?,file_format=? to use the file specification!')
            self.success = False 
        
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
            elif fileExtension.upper() in ('.CSV', '.TAB', '.PIPE'):
                self.projectFileFormat = fileExtension[1:].upper()
            else:
                raise G2UnsupportedFileTypeException('Unknown project file type: ' + fileExtension)

            #--load a json project file
            if self.projectFileFormat == 'JSON':
                self.loadJsonProject()
            #--load a csv json file
            elif self.projectFileFormat == 'CSV':
                self.loadCsvProject()

            if self.success:
                self.prepareSourceFiles()
        else:
            print('ERROR: project file ' + projectFileName + ' not found!')
            self.success = False 

        return

    #----------------------------------------
    def loadJsonProject(self):
        ''' validates and loads a json project file into memory '''
        try: self.projectSourceList = json.load(open(self.projectFileName), encoding="utf-8")
        except:
            print('ERROR: project file ' + self.projectFileName + ' could not be opened as a json file!')
            self.success = False 
        else:
            sourceRow = 0
            for sourceDict in self.projectSourceList:
                sourceRow += 1
                if not 'DATA_SOURCE' in sourceDict:
                    print('ERROR: project file entry ' + str(sourceRow) + ' does not contain an entry for DATA_SOURCE!')
                    self.success = False
                    break
                elif not 'FILE_NAME' in sourceDict:
                    print('ERROR: project file entry ' + str(sourceRow) + ' does not contain an entry for FILE_NAME!')
                    self.success = False
                    break
        return

    #----------------------------------------
    def loadCsvProject(self):
        try: csvFile, csvReader = self.openCsv(self.projectFileName, self.projectFileFormat)
        except:
            print('ERROR: project file ' + self.projectFileName + ' could not be opened as a ' + self.projectFileFormat + ' file!')
            self.success = False 
        else:
            csvRows = []
            csvHeaders = [x.strip().upper() for x in next(csvReader)]
            if not 'DATA_SOURCE' in csvHeaders:
                print('ERROR: project file does not contain a column for DATA_SOURCE!')
                self.success = False
            elif not 'FILE_NAME' in csvHeaders:
                print('ERROR: project file does not contain a column for FILE_NAME!')
                self.success = False
            else:
                self.projectSourceList = []
                for csvRow in csvReader:
                    if csvRow: #--skip blank lines
                        sourceDict = dict(list(zip(csvHeaders, csvRow)))
                        self.projectSourceList.append(sourceDict)

            csvFile.close()

        return

    #----------------------------------------
    def prepareSourceFiles(self):
        ''' ensure project files referenced exist and are valid '''
        self.sourceList = []
        sourceRow = 0
        for sourceDict in self.projectSourceList:
            sourceRow += 1

            #--validate source record
            sourceDict['DATA_SOURCE'] = sourceDict['DATA_SOURCE'].strip().upper()
            sourceDict['FILE_NAME'] = sourceDict['FILE_NAME'].strip()
            if 'FILE_FORMAT' not in sourceDict:
                sourceDict['FILE_FORMAT'] = self.projectFileFormat
            else:
                sourceDict['FILE_FORMAT'] = sourceDict['FILE_FORMAT'].upper()

            if len(sourceDict['DATA_SOURCE']) == 0:
                print('ERROR: project file entry ' + str(sourceRow) + ' does not contain an DATA_SOURCE!')
                self.success = False
            if len(sourceDict['FILE_NAME']) == 0:
                print('ERROR: project file entry ' + str(sourceRow) + ' does not contain a FILE_NAME!')
                self.success = False

            #--validate and map the files for this source
            if self.success:
                if 'ENTITY_TYPE' not in sourceDict:
                    sourceDict['ENTITY_TYPE'] = sourceDict['DATA_SOURCE'] 
                else:
                    sourceDict['ENTITY_TYPE'] = sourceDict['ENTITY_TYPE'].upper() 

                #--currently file name cannot contain wildcards.  We should add support for this in the future
                sourceDict['FILE_LIST'] = []
                fileDict = {}
                fileDict['FILE_ORDER'] = 1
                fileDict['FILE_FORMAT'] = sourceDict['FILE_FORMAT']

                #--adjustment if they gave us full path as file name
                if os.path.exists(sourceDict['FILE_NAME']):
                    fileDict['FILE_PATH'] = sourceDict['FILE_NAME']
                    sourceDict['FILE_NAME'] = os.path.basename(fileDict['FILE_PATH'])    
                else:  #--append the project file path
                    fileDict['FILE_PATH'] = self.projectFilePath + os.path.sep + sourceDict['FILE_NAME'] 

                if not os.path.exists(fileDict['FILE_PATH']):
                    print('ERROR: ' + fileDict['FILE_PATH'] + ' does not exist!')
                    self.success = False
                else:
                    if sourceDict['FILE_FORMAT'] == 'JSON':
                        if self.verifyJsonFile(fileDict) == False:
                            raise G2InvalidFileTypeContentsException('File %s contents appear invalid for file type: %s' % (fileDict['FILE_PATH'],sourceDict['FILE_FORMAT']))
                        self.mapJsonFile(fileDict)
                    elif sourceDict['FILE_FORMAT'] == 'CSV':
                        if self.verifyCsvFile(fileDict) == False:
                            raise G2InvalidFileTypeContentsException('File %s contents appear invalid for file type: %s' % (fileDict['FILE_PATH'],sourceDict['FILE_FORMAT']))
                        self.mapCsvFile(fileDict)
                    elif sourceDict['FILE_FORMAT'] == 'TAB':
                        if self.verifyCsvFile(fileDict) == False:
                            raise G2InvalidFileTypeContentsException('File %s contents appear invalid for file type: %s' % (fileDict['FILE_PATH'],sourceDict['FILE_FORMAT']))
                        self.mapCsvFile(fileDict)
                    elif sourceDict['FILE_FORMAT'] == 'UMF':
                        if self.verifyUmfFile(fileDict) == False:
                            raise G2InvalidFileTypeContentsException('File %s contents appear invalid for file type: %s' % (fileDict['FILE_PATH'],sourceDict['FILE_FORMAT']))
                        self.mapUmfFile(fileDict)
                    else:
                        raise G2UnsupportedFileTypeException('Unknown input file type: ' + sourceDict['FILE_FORMAT'])

                #--ensure there is at least one feature mapped, unless this is raw UMF
                if self.success and sourceDict['FILE_FORMAT'] is not 'UMF':
                    mappedFields = []
                    unmappedFields = []
                    for mappedColumn in fileDict['MAP']:
                        if fileDict['MAP'][mappedColumn]['FTYPE_CODE'] != 'IGNORE':
                            mappedFields.append(mappedColumn)
                        else:
                            unmappedFields.append(mappedColumn)

                    fileDict['MAPPED_FIELDS'] = mappedFields 
                    fileDict['UNMAPPED_FIELDS'] = unmappedFields 

                    #--ensure there is at least one feature mapped
                    if len(mappedFields) == 0:
                        print('ERROR: No valid mappings for file %s' % sourceDict['FILE_NAME'])
                        self.success = False
                sourceDict['FILE_LIST'].append(fileDict)

            self.sourceList.append(sourceDict)
        return

    #----------------------------------------
    def verifyJsonFile(self, fileDict, sampleSize = 100):
        ''' opens a file to verify it is JSON '''
        dataLooksLikeJSON = True
        try: jsonFile = openPossiblyCompressedFile(fileDict['FILE_PATH'], 'r')
        except:
            print('ERROR: ' + fileDict['FILE_PATH'] + ' could not be opened!')
            dataLooksLikeJSON = False 
        else:
            #--test the first several lines to make sure it looks like JSON data
            rowNum = 0
            for N in range(sampleSize):
                jsonString = jsonFile.readline()
                if jsonString: #--skip blank lines
                    jsonString = jsonString.strip()
                    if len(jsonString) == 0:
                        continue
                    rowNum += 1
                    try: jsonDict = json.loads(jsonString, encoding="utf-8")
                    except:
                        print('Row %d of file %s is not JSON formatted.' % (rowNum, os.path.basename(fileDict['FILE_PATH'])))
                        dataLooksLikeJSON = False
                    if dataLooksLikeJSON == False:
                        break
            jsonFile.close()
        return dataLooksLikeJSON

    def verifyUmfFile(self, fileDict, sampleSize = 100):
        ''' opens a file to verify it is UMF '''
        dataLooksLikeUMF = True
        try: umfFile = openPossiblyCompressedFile(fileDict['FILE_PATH'], 'r')
        except:
            print('ERROR: ' + fileDict['FILE_PATH'] + ' could not be opened!')
            dataLooksLikeUMF = False 
        else:
            #--test the first several lines to make sure it looks like UMF data
            rowNum = 0
            for N in range(sampleSize):
                umfString = umfFile.readline()
                if umfString: #--skip blank lines
                    umfString = umfString.strip()
                    if len(umfString) == 0:
                        continue
                    rowNum += 1
                    if umfString.startswith('<') == False:
                        dataLooksLikeUMF = False
                    if umfString.endswith('>') == False:
                        dataLooksLikeUMF = False
                    if dataLooksLikeUMF == False:
                        break
            umfFile.close()
        return dataLooksLikeUMF

    def verifyCsvFile(self, fileDict, sampleSize = 100):
        ''' opens a file to verify it is CSV '''
        dataLooksLikeCSV = True
        try: csvFile, csvReader = self.openCsv(fileDict['FILE_PATH'], fileDict['FILE_FORMAT'])
        except:
            print('ERROR: ' + fileDict['FILE_PATH'] + ' could not be opened as a ' + fileDict['FILE_FORMAT'] + ' file!')
            dataLooksLikeCSV = False 
        else:
            csvHeaders = [x.strip().upper() for x in next(csvReader)]
            headerFieldCount = len(csvHeaders)
            for columnName in csvHeaders:
                nonCsvChars = set('{}:<>')
                if any((c in nonCsvChars) for c in columnName):
                    print('File %s is not CSV formatted (header invalid).' % (os.path.basename(fileDict['FILE_PATH'])))
                    dataLooksLikeCSV = False
            if dataLooksLikeCSV == True:
                rowNum = 0
                for csvRow in csvReader:
                    if csvRow: #--skip blank lines
                        rowNum += 1
                        itemsInRow = len(list(zip(csvHeaders, csvRow)))
                        if headerFieldCount != itemsInRow:
                            print('Row %d of file %s is not CSV formatted.' % (rowNum, os.path.basename(fileDict['FILE_PATH'])))
                            dataLooksLikeCSV = False
                            break
                        if rowNum >= sampleSize:
                            break
            csvFile.close()
        return dataLooksLikeCSV

    #----------------------------------------
    def mapUmfFile(self, fileDict):
        ''' requisite operations for UMF input file '''
        fileDict['MAP'] = None

    #----------------------------------------
    def mapCsvFile(self, fileDict):
        ''' opens a csv file to validate mappings '''
        try: csvFile, csvReader = self.openCsv(fileDict['FILE_PATH'], fileDict['FILE_FORMAT'])
        except:
            print('ERROR: ' + fileDict['FILE_PATH'] + ' could not be opened as a ' + fileDict['FILE_FORMAT'] + ' file!')
            self.success = False 
        else:
            columnMappings = {}            
            csvRows = []
            csvHeaders = [x.strip().upper() for x in next(csvReader)]
            for columnName in csvHeaders:
                if len(columnName) == 0:
                    print('ERROR: file ' + fileDict['FILE_NAME'] + ' is missing a column header!')
                    self.success = False 
                else:
                    if columnName not in columnMappings:
                        columnMappings[columnName] = self.lookupAttribute(columnName)

            if self.success:
                fileDict['CSV_HEADERS'] = csvHeaders
                fileDict['MAP'] = columnMappings

            csvFile.close()
        return

    #----------------------------------------
    def mapJsonFile(self, fileDict, sampleSize = 100):
        ''' opens a json file to validate mappings '''
        try: jsonFile = openPossiblyCompressedFile(fileDict['FILE_PATH'], 'r')
        except:
            print('ERROR: ' + fileDict['FILE_PATH'] + ' could not be opened!')
            self.success = False 
        else:

            #--test the first 5 lines as not all tags used on every row 
            #--its ok if not, they will be added as used, 
            #--but for performance catch as many as you can 
            columnMappings = {}            
            rowNum = 0
            for N in range(sampleSize):
                jsonString = jsonFile.readline()
                if jsonString: #--skip blank lines
                    jsonString = jsonString.strip()
                    if len(jsonString) == 0:
                        continue
                    rowNum += 1
                    jsonErrors, jsonDict, jsonMappings = self.validateJsonMessage(jsonString)
                    for message in jsonErrors:
                        print(message + ' in row %d of file %s' % (rowNum, os.path.basename(fileDict['FILE_PATH'])))
                    if jsonErrors:
                        self.success = False 
                        break
                    else:
                        #self.mapJsonRecord(jsonDict, jsonMappings)  #--debug test
                        #--add to the column map for the file
                        for columnName in jsonMappings:
                            if columnName not in columnMappings:
                                columnMappings[columnName] = jsonMappings[columnName]

            if self.success:
                fileDict['MAP'] = columnMappings

            jsonFile.close()
        return

    #--------------------
    #--utility functions
    #--------------------

    #----------------------------------------
    def openCsv(self, fileName, fileFormat):
        ''' chooses best dialect to open a csv with (comma, tab, quoted or not) '''
        csvFile = openPossiblyCompressedFile(fileName, 'r')
        csv.register_dialect('CSV', delimiter = ',', quotechar = '"')
        csv.register_dialect('TAB', delimiter = '\t', quotechar = '"')
        csv.register_dialect('PIPE', delimiter = '|', quotechar = '"')
        csvReader = csv.reader(csvFile, fileFormat)

        return csvFile, csvReader

    #----------------------------------------
    def pause(self, question = None):
        if not question:
            v_wait = input("PRESS ENTER TO CONTINUE ... ")
        else:
            v_wait = input(question)
        return v_wait

    #----------------------------------------
    def containsOnly(self, seq, aset):
        ''' Check whether sequence seq contains ONLY items in aset '''
        for c in seq:
            if c not in aset: return False
        return True

    #----------------------------------------
    def xmlEscape(self, rawValue):
        ''' escape xml values '''
        return rawValue.replace('&', '&amp;').replace('<','&lt;').replace('>','&gt;').encode(encoding='UTF-8') #--,errors='strict'


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
        mappingFileName = options.configtFileName
        projectFileName = options.projectFileName
  
    #--create an instance
    myProject = g2Mapper('self', mappingFileName, projectFileName)
    if myProject.success:
        print('SUCCESS: project ' + projectFileName + ' is valid!')

    #--delete the instance
    del myProject

    sys.exit()

