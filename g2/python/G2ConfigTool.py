import cmd
import sys
import json
import os
import platform
from shutil import copyfile

try: import configparser
except: import ConfigParser as configparser

try: 
    from G2Database import G2Database
    import G2Exception
except: 
    pass
    
class G2CmdShell(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)

        # this is how you get command history on windows 
        if platform.system() == 'Windows':
            self.use_rawinput = False

        self.intro = '\nWelcome to the G2 Configuration shell. Type help or ? to list commands.\n'
        self.prompt = '(g2) '

        self.g2configFile = g2configFile
        self.cfgData = json.load(open(self.g2configFile), encoding="utf-8")
        self.configUpdated = False

        self.g2variantFile = g2variantFile
        self.cfgVariant = json.load(open(self.g2variantFile), encoding="utf-8")
        self.variantUpdated = False

        self.attributeClassList = ('NAME', 'ATTRIBUTE', 'IDENTIFIER', 'ADDRESS', 'PHONE', 'RELATIONSHIP', 'OTHER')
        self.lockedFeatureList = ('NAME','ADDRESS', 'PHONE', 'DOB', 'REL_LINK')
        
        self.__hidden_methods = ('do_shell')
        self.doDebug = False
        
    # -----------------------------
    #def do_readme(self, arg):
    #    if os.path.exists('./G2Config.readme'):
    #        with open('./G2Config.readme') as f:
    #            cnt = 0
    #            for line in f:
    #                printWithNewLine(line[0:-1])
    #                cnt += 1
    #                if cnt % 25 == 0:
    #                    pause()
        
    # -----------------------------
    def do_quit(self, arg):

        self.do_save("ask")
        return True

    # -----------------------------
    def emptyline(self):
        return

    # -----------------------------
    def cmdloop(self):

        while True:
            try: 
                cmd.Cmd.cmdloop(self)
                break
            except KeyboardInterrupt:
                ans = userInput('\n\nAre you sure you want to exit?  ')
                if ans in ['y','Y', 'yes', 'YES']:
                    break
            except TypeError as ex:
                print("ERROR: " + str(ex))

    #Hide do_shell from list of APIs. Seperate help section for it
    def get_names(self):
        return [n for n in dir(self.__class__) if n not in self.__hidden_methods]


    def help_KnowledgeCenter(self):
        printWithNewLines('Senzing Knowledge Center: https://senzing.zendesk.com/hc/en-us', 'B')


    def help_Support(self):
        printWithNewLines('Senzing Support Request: https://senzing.zendesk.com/hc/en-us/requests/new', 'B')


    def help_Arguments(self):
        print(
              '\nWhere you see <value> in the help output replace <value> with your value.\n' \
              '\nFor example the help for addAttribute is: \n' \
              '\taddAttribute {"attribute": "<attribute_name>"}\n' \
              '\nReplace <attribute_name> to be the name of your new attribute\n' \
              '\taddAttribute {"attribute": "myNewAttribute"}\n' \
              )

    def help_Shell(self):
        printWithNewLines('Run OS shell commands: ! <command>', 'B')


    def do_shell(self,line):
        '\nRun OS shell commands: !<command>\n'
        output = os.popen(line).read()
        print(output)

    # -----------------------------
    def fileloop(self, fileName):

        if os.path.exists(fileName): 
            with open(fileName) as data_in:
                for line in data_in:
                    line = line.strip()
                    if len(line) > 0:
                        print('-' * 50)
                        print(line)

                        if ' ' in line:
                            cmd = 'do_' + line[0:line.find(' ')]
                            parm = line[line.find(' ')+1:]
                        else:
                            cmd = 'do_' + line
                            parm = ''

                        if cmd not in dir(self):
                            printWithNewLines('Command %s not found' % cmd, 'B')
                        else:
                            execCmd = 'self.' + cmd + "('" + parm + "')"
                            exec(execCmd)
        else:
            print('%s not found' % fileName)

    # -----------------------------
    def getRecord(self, table, field, value):

        for i in range(len(self.cfgData['G2_CONFIG'][table])):
            if type(field) == list:
                matched = True
                for ii in range(len(field)):
                    if self.cfgData['G2_CONFIG'][table][i][field[ii]] != value[ii]:
                        matched = False
                        break
            else:
                matched = self.cfgData['G2_CONFIG'][table][i][field] == value

            if matched:
                return self.cfgData['G2_CONFIG'][table][i]
        return None

    # -----------------------------
    def getRecordList(self, table, field = None, value = None):

        recordList = []
        for i in range(len(self.cfgData['G2_CONFIG'][table])):
            if field and value:
                if self.cfgData['G2_CONFIG'][table][i][field] == value:
                    recordList.append(self.cfgData['G2_CONFIG'][table][i])
            else:
                recordList.append(self.cfgData['G2_CONFIG'][table][i])
        return recordList

# ===== global commands =====

    def do_configReload(self,arg):
        '\n\tReload configuration and discard all unsaved changes\n'

        if self.configUpdated:
            ans = userInput('\nYou have unsaved changes, are you sure you want to discard them? ')
            if ans not in ['y','Y', 'yes', 'YES']:
                printWithNewLines('Configuration wasn\'t reloaded, your changes remain but are still unsaved.')
                return
        
        self.cfgData = json.load(open(self.g2ConfigFile), encoding="utf-8")
        printWithNewLines('%s has been reloaded.' % self.g2ConfigFile, 'B')
        

    # -----------------------------
    def do_save(self, args):
        '\n\tSave changes to g2config.json and attempt a backup of the original\n'

        if self.configUpdated or self.variantUpdated:
            if args == 'ask':
                ans = userInput('\nSave changes? ')
            else: 
                ans = 'y'
            if ans in ['y','Y', 'yes', 'YES']:
            
                if self.configUpdated:
                    try: copyfile(self.g2configFile, self.g2configFile + '.bk')
                    except:
                        printWithNewLines("Could not create %s" % self.g2configFile + '.bk', 'B')
                        return
                    with open(self.g2configFile, 'w') as fp:
                        json.dump(self.cfgData, fp, indent = 4, sort_keys = True)

                    printWithNewLines('Saved!', 'B')
                    self.configUpdated = False

                if self.variantUpdated:
                    try: copyfile(self.g2variantFile, self.g2variantFile + '.bk')
                    except:
                        printWithNewLines("Could not create %s" % self.g2variantFile + '.bk', 'B')
                        return
                    with open(self.g2variantFile, 'w') as fp:
                        json.dump(self.cfgData, fp, indent = 4, sort_keys = True)

                    printWithNewLines('Saved!', 'B')
                    self.variantUpdated = False

# ===== data Source commands =====

    # -----------------------------
    def do_listDataSources(self,arg):
        '\n\tlistDataSources\n'

        print('')
        for dsrcRecord in sorted(self.getRecordList('CFG_DSRC'), key = lambda k: k['DSRC_ID']):
            print('{"id": %i, "dataSource": "%s"}' % (dsrcRecord['DSRC_ID'], dsrcRecord['DSRC_CODE']))
        print('')


    # -----------------------------
    def do_addDataSource(self,arg):
        '\n\taddDataSource {"dataSource": "<dataSource_name>"}'

        if not argCheck('do_addDataSource', arg, self.do_addDataSource.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"DATASOURCE": arg}
            parmData['DATASOURCE'] = parmData['DATASOURCE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_DSRC'])):
                if self.cfgData['G2_CONFIG']['CFG_DSRC'][i]['DSRC_CODE'] == parmData['DATASOURCE']:
                    printWithNewLines('Data source %s already exists!' % parmData['DATASOURCE'], 'B')
                    return
                if 'ID' in parmData and self.cfgData['G2_CONFIG']['CFG_DSRC'][i]['DSRC_ID'] == parmData['ID']:
                    printWithNewLines('Data source ID %s already exists!' % parmData['ID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_DSRC'][i]['DSRC_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_DSRC'][i]['DSRC_ID']
            if 'ID' not in parmData: 
                parmData['ID'] = maxID + 1 if maxID >= 1000 else 1000
                
            newRecord = {}
            newRecord['DSRC_ID'] = parmData['ID'] 
            newRecord['DSRC_CODE'] = parmData['DATASOURCE']
            newRecord['DSRC_DESC'] = parmData['DATASOURCE']
            newRecord['DSRC_RELY'] = 1
            newRecord['RETENTION_LEVEL'] = "Remember"
            newRecord['CONVERSATIONAL'] = 0
            self.cfgData['G2_CONFIG']['CFG_DSRC'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)

    # -----------------------------
    def do_deleteDataSource(self,arg):
        '\n\tdeleteDataSource {"dataSource": "<dataSource_name>"}\n'

        if not argCheck('do_deleteDataSource', arg, self.do_deleteDataSource.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"DATASOURCE": arg} 
            parmData['DATASOURCE'] = parmData['DATASOURCE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if parmData['DATASOURCE'] == 'SEARCH':
                printWithNewLine('Can\'t delete the SEARCH data source!')
                return
    
            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_DSRC'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_DSRC'][i]['DSRC_CODE'] == parmData['DATASOURCE']:
                    del self.cfgData['G2_CONFIG']['CFG_DSRC'][i]        
                    deleteCnt += 1
                    self.configUpdated = True
            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

# ===== entity class commands =====

    # -----------------------------
    def do_listEntityClasses(self,arg):
        '\n\tlistEntityClasses\n'

        print('')
        for eclassRecord in sorted(self.getRecordList('CFG_ECLASS'), key = lambda k: k['ECLASS_ID']):
            print('{"id": %i, "entityClass": "%s"}' % (eclassRecord['ECLASS_ID'], eclassRecord['ECLASS_CODE']))
        print('')

    # -----------------------------
    def do_addEntityClass(self,arg):
        '\n\taddEntityClass {"entityClass": "<entityClass_value>"}'

        if not argCheck('addEntityClass', arg, self.do_addEntityClass.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ENTITYCLASS": arg}
            parmData['ENTITYCLASS'] = parmData['ENTITYCLASS'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if 'RESOLVE' in parmData and parmData['RESOLVE'].upper() not in ('Y','N'):
                printWithNewLines('Resolve flag must be Y or N', 'B')
                return
            if 'ID' in parmData and type(parmData['ID']) is not int:
                parmData['ID'] = int(parmData['ID'])
                   
            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ECLASS'])):
                if self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_CODE'] == parmData['ENTITYCLASS']:
                    printWithNewLines('Entity class %s already exists!' % parmData['ENTITYCLASS'], 'B')
                    return
                if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_ID']) == parmData['ID']:
                    printWithNewLines('Entity class id %s already exists!' % parmData['ID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_ID']
            if 'ID' not in parmData: 
                parmData['ID'] = maxID + 1 if maxID >=1000 else 1000
    
            newRecord = {}
            newRecord['ECLASS_ID'] = parmData['ID']
            newRecord['ECLASS_CODE'] = parmData['ENTITYCLASS']
            newRecord['ECLASS_DESC'] = parmData['ENTITYCLASS']
            newRecord['RESOLVE'] = parmData['RESOLVE'].upper() if 'RESOLVE' in parmData else 'Y'
            self.cfgData['G2_CONFIG']['CFG_ECLASS'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)

    # -----------------------------
    def do_deleteEntityClass(self,arg):
        '\n\tdeleteEntityClass {"entityClass": "<entityClass_value>"}\n'

        if not argCheck('deleteEntityClass', arg, self.do_deleteEntityClass.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ENTITYCLASS": arg}
            parmData['ENTITYCLASS'] = parmData['ENTITYCLASS'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ECLASS'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_CODE'] == parmData['ENTITYCLASS']:
                    del self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]        
                    deleteCnt += 1
                    self.configUpdated = True
            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')
        
# ===== entity type commands =====

    # -----------------------------
    def do_listEntityTypes(self,arg):
        '\n\tlistEntityTypes\n'

        print('')
        for etypeRecord in sorted(self.getRecordList('CFG_ETYPE'), key = lambda k: k['ETYPE_ID']):
            eclassRecord = self.getRecord('CFG_ECLASS', 'ECLASS_ID', etypeRecord['ECLASS_ID'])
            print('{"id": %i, "entityType":"%s", "class": "%s"}' % (etypeRecord['ETYPE_ID'], etypeRecord['ETYPE_CODE'], ('unknown' if not eclassRecord else eclassRecord['ECLASS_CODE'])))
        print('')

    # -----------------------------
    def do_addEntityType(self,arg):
        '\naddEntityType {"entityType": "<entityType_value>"}\n'

        if not argCheck('addEntityType', arg, self.do_addEntityType.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ENTITYTYPE": arg}
            parmData['ENTITYTYPE'] = parmData['ENTITYTYPE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            parmData['CLASS'] = parmData['CLASS'].upper() if 'CLASS' in parmData else 'ACTOR'
            
            eclassRecord = self.getRecord('CFG_ECLASS', 'ECLASS_CODE', parmData['CLASS'])
            if not eclassRecord:
                printWithNewLines('Invalid entity class: %s' % parmData['CLASS'], 'B')
                return

            if 'ID' in parmData and type(parmData['ID']) is not int:
                parmData['ID'] = int(parmData['ID'])

            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ETYPE'])):
                if self.cfgData['G2_CONFIG']['CFG_ETYPE'][i]['ETYPE_CODE'] == parmData['ENTITYTYPE']:
                    printWithNewLines('Entity type %s already exists!' % parmData['ENTITYTYPE'], 'B')
                    return
                if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_ETYPE'][i]['ETYPE_ID']) == parmData['ID']:
                    printWithNewLines('Entity type id %s already exists!' % parmData['ID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_ETYPE'][i]['ETYPE_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_ETYPE'][i]['ETYPE_ID']
            if 'ID' not in parmData: 
                parmData['ID'] = maxID + 1 if maxID >=1000 else 1000
    
            newRecord = {}
            newRecord['ETYPE_ID'] = parmData['ID']
            newRecord['ETYPE_CODE'] = parmData['ENTITYTYPE']
            newRecord['ETYPE_DESC'] = parmData['ENTITYTYPE']
            newRecord['ECLASS_ID'] = eclassRecord['ECLASS_ID']
            self.cfgData['G2_CONFIG']['CFG_ETYPE'].append(newRecord)
            self.configUpdated = True

            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)

    # -----------------------------
    def do_deleteEntityType(self,arg):
        '\n\tdeleteEntityType {"entityType": "<entityType_value>"}\n'

        if not argCheck('deleteEntityType', arg, self.do_deleteEntityType.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ENTITYTYPE": arg}
            parmData['ENTITYTYPE'] = parmData['ENTITYTYPE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ETYPE'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_ETYPE'][i]['ETYPE_CODE'] == parmData['ENTITYTYPE']:
                    del self.cfgData['G2_CONFIG']['CFG_ETYPE'][i]        
                    deleteCnt += 1
                    self.configUpdated = True
            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')
        
# ===== feature commands =====

    # -----------------------------
    def do_listFunctions(self,arg):
        '\n\tlistFunctions\n'

        print('')
        for funcRecord in sorted(self.getRecordList('CFG_SFUNC'), key = lambda k: k['SFUNC_ID']):
            print('{"type": "Standardization", "function": "%s"}' % (funcRecord['SFUNC_CODE']))
        print('')
        for funcRecord in sorted(self.getRecordList('CFG_EFUNC'), key = lambda k: k['EFUNC_ID']):
            print('{"type": "Expression", "function": "%s"}' % (funcRecord['EFUNC_CODE']))
        print('')
        for funcRecord in sorted(self.getRecordList('CFG_CFUNC'), key = lambda k: k['CFUNC_ID']):
            print('{"type": "Comparison", "function": "%s"}' % (funcRecord['CFUNC_CODE']))
        print('')

    # -----------------------------
    def do_listFeatureClasses(self,arg):
        '\n\tlistFeatureClasses\n'

        print('')
        for fclassRecord in sorted(self.getRecordList('CFG_FCLASS'), key = lambda k: k['FCLASS_ID']):
            print('{"id": %i, "class":"%s"}' % (fclassRecord['FCLASS_ID'], fclassRecord['FCLASS_CODE']))
        print('')

    # -----------------------------
    def do_listFeatures(self,arg):
        '\n\tlistFeatures\n'
        ####'\n\tlistFeatures\t\t(displays all features)\n' #\
        ####'listFeatures -n\t\t(to display new features only)\n'

        print('')
        for ftypeRecord in sorted(self.getRecordList('CFG_FTYPE'), key = lambda k: k['FTYPE_ID']):
            if arg != '-n' or ftypeRecord['FTYPE_ID'] >=  1000:
                featureJson = self.getFeatureJson(ftypeRecord)
                print(featureJson)
                if 'ERROR:' in featureJson:
                    print('Corrupted config!  Delete this feature and re-add.')
        print('')

    # -----------------------------
    def do_getFeature(self,arg):
        '\n\tgetFeature {"feature": "<feature_name>"}\n'

        if not argCheck('getFeature', arg, self.do_getFeature.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"FEATURE": arg}
            parmData['FEATURE'] = parmData['FEATURE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
            else:
                printWithNewLines(self.getFeatureJson(ftypeRecord), 'B')

    # -----------------------------
    def do_deleteFeature(self,arg):
        '\n\tdeleteFeature {"feature": "<feature_name>"}\n'

        if not argCheck('deleteFeature', arg, self.do_deleteFeature.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"FEATURE": arg}
            parmData['FEATURE'] = parmData['FEATURE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if parmData['FEATURE'] in ('NAME',):
                printWithNewLines('Can\'t delete feature %s!' %  parmData['FEATURE'], 'B')
                return
    
            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_CODE'] == parmData['FEATURE']:

                    # delete any standardization calls (must loop through backwards when deleting)
                    for i1 in range(len(self.cfgData['G2_CONFIG']['CFG_SFCALL'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_SFCALL'][i1]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            del self.cfgData['G2_CONFIG']['CFG_SFCALL'][i1]        
    
                    # delete any distinct value calls and boms (must loop through backwards when deleting)
                    for i1 in range(len(self.cfgData['G2_CONFIG']['CFG_DFCALL'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_DFCALL'][i1]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_DFBOM'])-1, -1, -1):
                                if self.cfgData['G2_CONFIG']['CFG_DFBOM'][i2]['DFCALL_ID'] == self.cfgData['G2_CONFIG']['CFG_DFCALL'][i1]['DFCALL_ID']:
                                    del self.cfgData['G2_CONFIG']['CFG_DFBOM'][i2] 
        
                            del self.cfgData['G2_CONFIG']['CFG_DFCALL'][i1]        
    
                    # delete any expression calls and boms (must loop through backwards when deleting)
                    for i1 in range(len(self.cfgData['G2_CONFIG']['CFG_EFCALL'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_EFCALL'][i1]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_EFBOM'])-1, -1, -1):
                                if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i2]['EFCALL_ID'] == self.cfgData['G2_CONFIG']['CFG_EFCALL'][i1]['EFCALL_ID']:
                                    del self.cfgData['G2_CONFIG']['CFG_EFBOM'][i2] 
        
                            del self.cfgData['G2_CONFIG']['CFG_EFCALL'][i1]        
    
                    # delete any comparison calls and boms  (must loop through backwards when deleting)
                    for i1 in range(len(self.cfgData['G2_CONFIG']['CFG_CFCALL'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_CFBOM'])-1, -1, -1):
                                if self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]['CFCALL_ID'] == self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]['CFCALL_ID']:
                                    del self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]        
                            del self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]        
    
                    # delete any feature boms (must loop through backwards when deleting)
                    for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_FBOM'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_FBOM'][i2]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            del self.cfgData['G2_CONFIG']['CFG_FBOM'][i2]        
    
                    # delete the feature elements (must loop through backwards when deleting)
                    for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_EBOM'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_EBOM'][i2]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            del self.cfgData['G2_CONFIG']['CFG_EBOM'][i2]        

                    # delete any attributes assigned to this feature (this one is by code, not ID!)
                    for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_ATTR'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_ATTR'][i2]['FTYPE_CODE'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_CODE']:
                            del self.cfgData['G2_CONFIG']['CFG_ATTR'][i2]        
    
                    # delete the feature itself
                    del self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]        
                    deleteCnt += 1
                    self.configUpdated = True

            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    # -----------------------------
    def do_setFeature(self,arg):
        '\n\tsetFeature {"feature": "<feature_name>", "behavior": "<behavior_type>"}' \
        '\n\tsetFeature {"feature": "<feature_name>", "comparison": "<comparison_function>"}\n'
        if not argCheck('setFeature', arg, self.do_setFeature.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:
            printWithNewLines('')

            #--can't alter a locked feature
            if parmData['FEATURE'] in self.lockedFeatureList:
                printWithNewLines('Feature %s is locked!' % parmData['FEATURE'])
                return

            #--lookup feature and error if doesn't exist
            listID = 0
            ftypeID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE'])):
                if self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_CODE'] == parmData['FEATURE']:
                    listID = i
                    ftypeID = self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']
            if listID == 0: 
                printWithNewLines('Feature %s does not exist!' % parmData['FEATURE'])
                return

            #--make the updates
            for parmCode in parmData:
                if parmCode == 'FEATURE':
                    pass

                elif parmCode == 'BEHAVIOR':
                    featureBehaviorDict = parseFeatureBehavior(parmData['BEHAVIOR'])
                    if featureBehaviorDict:
                        self.cfgData['G2_CONFIG']['CFG_FTYPE'][listID]['FTYPE_FREQ'] = featureBehaviorDict['FREQUENCY'] 
                        self.cfgData['G2_CONFIG']['CFG_FTYPE'][listID]['FTYPE_EXCL'] = featureBehaviorDict['EXCLUSIVITY']
                        self.cfgData['G2_CONFIG']['CFG_FTYPE'][listID]['FTYPE_STAB'] = featureBehaviorDict['STABILITY']
                        printWithNewLines('Behavior updated!')
                        self.configUpdated = True
                    else:
                        printWithNewLines('Invalid behavior: %s' % parmData['BEHAVIOR'])
                        
                elif parmCode == 'ANONYMIZE':
                    if parmData['ANONYMIZE'].upper() in ('YES', 'Y', 'NO','N'):
                        self.cfgData['G2_CONFIG']['CFG_FTYPE'][listID]['ANONYMIZE'] = 1 if parmData['ANONYMIZE'].upper() in ('YES', 'Y') else 0
                        printWithNewLines('Anonymize setting updated!')
                        self.configUpdated = True
                    else:
                        printWithNewLines('Invalid anonymize setting: %s' % parmData['ANONYMIZE'])
                    
                elif parmCode == 'STANDARDIZE':
                    sfuncRecord = self.getRecord('CFG_SFUNC', 'SFUNC_CODE', parmData['STANDARDIZE'].upper())
                    if sfuncRecord:
                        sfuncID = sfuncRecord['SFUNC_ID']
                        subListID = 0
                        for i in range(len(self.cfgData['G2_CONFIG']['CFG_SFCALL'])):
                            if self.cfgData['G2_CONFIG']['CFG_SFCALL'][i]['FTYPE_ID'] == ftypeID:
                                subListID = i
                        if subListID != 0:
                            self.cfgData['G2_CONFIG']['CFG_SFCALL'][subListID]['SFUNC_ID'] = sfuncID
                            printWithNewLines('Standardization function updated!')
                            self.configUpdated = True
                        else:
                            printWithNewLines('Standardization call can only be added with the feature, please delete and re-add.')
                    else:
                        printWithNewLines('Invalid standardization code: %s' % parmData['STANDARDIZE'])

                elif parmCode == 'EXPRESSION':
                    efuncRecord = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', parmData['EXPRESSION'].upper())
                    if efuncRecord:
                        efuncID = efuncRecord['EFUNC_ID']
                        subListID = 0
                        for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFCALL'])):
                            if self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['FTYPE_ID'] == ftypeID:
                                subListID = i
                        if subListID != 0:
                            self.cfgData['G2_CONFIG']['CFG_EFCALL'][subListID]['EFUNC_ID'] = efuncID
                            printWithNewLines('Expression function updated!')
                            self.configUpdated = True
                        else:
                            printWithNewLines('Expression call can only be added with the feature, please delete and re-add.')
                    else:
                        printWithNewLines('Invalid expression code: %s' % parmData['EXPRESSION'])
        
                elif parmCode == 'COMPARISON':
                    cfuncRecord = self.getRecord('CFG_CFUNC', 'CFUNC_CODE', parmData['COMPARISON'].upper())
                    if cfuncRecord:
                        cfuncID = cfuncRecord['CFUNC_ID']
                        subListID = 0
                        for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFCALL'])):
                            if self.cfgData['G2_CONFIG']['CFG_CFCALL'][i]['FTYPE_ID'] == ftypeID:
                                subListID = i
                        if subListID != 0:
                            self.cfgData['G2_CONFIG']['CFG_CFCALL'][subListID]['CFUNC_ID'] = cfuncID
                            printWithNewLines('Comparison function updated!')
                            self.configUpdated = True
                        else:
                            printWithNewLines('Comparison call can only be added with the feature, please delete and re-add.')
                    else:
                        printWithNewLines('Invalid comparison code: %s' % parmData['COMPARISON'])
 
                else:
                    printWithNewLines('Cannot set %s on features!' % parmCode)

            printWithNewLines('')
                
    # -----------------------------
    def do_addFeature(self,arg):
        '\n\taddFeature {"feature": "<feature_name>", "behavior": "<behavior_code>", "elementList": ["<element_detail(s)"]}' \
        '\n\n\taddFeature {"feature":"testFeat", "behavior":"FM", "comparison":"exact_comp", "elementlist": [{"compared": "Yes", "expressed": "No", "element": "test"}]}' \
        '\n\n\tFor additional example structures, use getFeature or listFeatures\n'
        
        if not argCheck('addFeature', arg, self.do_addFeature.__doc__):
            return
            
        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            if not 'ELEMENTLIST' in parmData or len(parmData['ELEMENTLIST']) == 0:
                raise ValueError('Element list is required!')
            if type(parmData['ELEMENTLIST']) is not list:
                raise ValueError('Element list should be specified as: "elementlist": ["<values>"]\n\n\tNote the [ and ]')
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            #--lookup feature and error if already exists
            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE'])):
                if self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_CODE'] == parmData['FEATURE']:
                    printWithNewLines('Feature %s already exists!' % parmData['FEATURE'], 'B')
                    return
                if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']) == int(parmData['ID']):
                    printWithNewLines('Feature id %s already exists!' % parmData['ID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']
    
            if 'ID' in parmData: 
                ftypeID = parmData['ID']
            else:
                ftypeID = maxID + 1 if maxID >=1000 else 1000
            
            #--default for missing values
            parmData['ID'] = ftypeID
            parmData['BEHAVIOR'] = parmData['BEHAVIOR'].upper() if 'BEHAVIOR' in parmData else 'FM'
            parmData['ANONYMIZE'] = parmData['ANONYMIZE'].upper() if 'ANONYMIZE' in parmData else 'NO'
            parmData['DERIVED'] = parmData['DERIVED'].upper() if 'DERIVED' in parmData else 'NO'
            parmData['CANDIDATES'] = parmData['CANDIDATES'].upper() if 'CANDIDATES' in parmData else 'NO' if parmData['BEHAVIOR'] == 'FM' else 'YES'
    
            #--parse behavior
            featureBehaviorDict = parseFeatureBehavior(parmData['BEHAVIOR'])
            if not featureBehaviorDict:
                printWithNewLines('Invalid behavior: %s' % parmData['BEHAVIOR'])
                return
                
            if 'CLASS' not in parmData:
                parmData['CLASS'] = 'OTHER'
            fclassRecord = self.getRecord('CFG_FCLASS', 'FCLASS_CODE', parmData['CLASS'].upper())
            if not fclassRecord:
                printWithNewLines('Invalid feature class: %s' % parmData['CLASS'], 'B')
                return
            else:
                fclassID = fclassRecord['FCLASS_ID']
            
            sfuncID = 0  #--standardization function
            if 'STANDARDIZE' in parmData and len(parmData['STANDARDIZE']) != 0:
                parmData['STANDARDIZE'] = parmData['STANDARDIZE'].upper()
                sfuncRecord = self.getRecord('CFG_SFUNC', 'SFUNC_CODE', parmData['STANDARDIZE'])
                if sfuncRecord:
                    sfuncID = sfuncRecord['SFUNC_ID']
                else:
                    printWithNewLines('Invalid standardization code: %s' % parmData['STANDARDIZE'], 'B')
                    return

            efuncID = 0  #--expression function
            if 'EXPRESSION' in parmData and len(parmData['EXPRESSION']) != 0:
                parmData['EXPRESSION'] = parmData['EXPRESSION'].upper()
                efuncRecord = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', parmData['EXPRESSION'])
                if efuncRecord:
                    efuncID = efuncRecord['EFUNC_ID']
                else:
                    printWithNewLines('Invalid expression code: %s' % parmData['EXPRESSION'], 'B')
                    return
    
            cfuncID = 0  #--comparison function
            if 'COMPARISON' in parmData and len(parmData['COMPARISON']) != 0:
                parmData['COMPARISON'] = parmData['COMPARISON'].upper()
                cfuncRecord = self.getRecord('CFG_CFUNC', 'CFUNC_CODE', parmData['COMPARISON'])
                if cfuncRecord:
                    cfuncID = cfuncRecord['CFUNC_ID']
                else:
                    printWithNewLines('Invalid comparison code: %s' % parmData['COMPARISON'], 'B')
                    return
    
            #--ensure elements going to express or compare routines
            ####if efuncID or cfuncID:
            if efuncID > 0 or cfuncID > 0:
                expressedCnt = comparedCnt = 0
                for element in parmData['ELEMENTLIST']:
                    if type(element) == dict:
                        element = dictKeysUpper(element)
                        if 'EXPRESSED' in element and element['EXPRESSED'].upper() == 'YES':
                            expressedCnt += 1
                        if 'COMPARED' in element and element['COMPARED'].upper() == 'YES':
                            comparedCnt += 1
                if efuncID > 0 and expressedCnt == 0:
                    printWithNewLines('No elements marked "expressed" for expression routine', 'B')
                    return
                if cfuncID > 0 and comparedCnt == 0:
                    printWithNewLines('No elements marked "compared" for comparsion routine', 'B')
                    return
    
            #--insert the feature
            newRecord = {}
            newRecord['FTYPE_ID'] = ftypeID
            newRecord['FTYPE_CODE'] = parmData['FEATURE']
            newRecord['FTYPE_DESC'] = parmData['FEATURE']
            newRecord['FCLASS_ID'] = fclassID
            newRecord['FTYPE_FREQ'] = featureBehaviorDict['FREQUENCY'] 
            newRecord['FTYPE_EXCL'] = featureBehaviorDict['EXCLUSIVITY']
            newRecord['FTYPE_STAB'] = featureBehaviorDict['STABILITY']
            newRecord['ANONYMIZE'] = 0 if parmData['ANONYMIZE'].upper() == 'NO' else 1 
            newRecord['DERIVED'] = 0 if parmData['DERIVED'].upper() == 'NO' else 1
            newRecord['USED_FOR_CAND'] = 0 if parmData['CANDIDATES'].upper() == 'NO' else 1
            newRecord['PERSIST_HISTORY'] = 1 
            newRecord['VERSION'] = 1
            newRecord['RTYPE_ID'] = 0
            self.cfgData['G2_CONFIG']['CFG_FTYPE'].append(newRecord)
            if self.doDebug:
                showMeTheThings(newRecord, 'Feature build')

            #--add the standrdization call
            sfcallID = 0
            ####if sfuncID > 1:        
            if sfuncID > 0:        
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_SFCALL'])):
                    if self.cfgData['G2_CONFIG']['CFG_SFCALL'][i]['SFCALL_ID'] > sfcallID:
                        sfcallID = self.cfgData['G2_CONFIG']['CFG_SFCALL'][i]['SFCALL_ID']
                sfcallID = sfcallID + 1 if sfcallID >= 1000 else 1000
                newRecord = {}
                newRecord['SFCALL_ID'] = sfcallID 
                newRecord['SFUNC_ID'] = sfuncID
                newRecord['EXEC_ORDER'] = 1
                newRecord['FTYPE_ID'] = ftypeID
                self.cfgData['G2_CONFIG']['CFG_SFCALL'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'SFCALL build')

            #--add the distinct value call
            dfcallID = 0
            dfuncID = 1  #--force it to this one for now
            if dfuncID > 0:        
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_DFCALL'])):
                    if self.cfgData['G2_CONFIG']['CFG_DFCALL'][i]['DFCALL_ID'] > dfcallID:
                        dfcallID = self.cfgData['G2_CONFIG']['CFG_DFCALL'][i]['DFCALL_ID']
                dfcallID = dfcallID + 1 if dfcallID >= 1000 else 1000
                newRecord = {}
                newRecord['DFCALL_ID'] = dfcallID 
                newRecord['DFUNC_ID'] = dfuncID
                newRecord['EXEC_ORDER'] = 1
                newRecord['FTYPE_ID'] = ftypeID
                self.cfgData['G2_CONFIG']['CFG_DFCALL'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'DFCALL build')

            #--add the expression call
            efcallID = 0
            ####if efuncID > 1:        
            if efuncID > 0:        
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFCALL'])):
                    if self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFCALL_ID'] > efcallID:
                        efcallID = self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFCALL_ID']
                efcallID = efcallID + 1 if efcallID >= 1000 else 1000
                newRecord = {}
                newRecord['EFCALL_ID'] = efcallID 
                newRecord['EFUNC_ID'] = efuncID
                newRecord['EXEC_ORDER'] = 1
                newRecord['FTYPE_ID'] = ftypeID
                self.cfgData['G2_CONFIG']['CFG_EFCALL'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'EFCALL build')

            #--add the comparison call
            cfcallID = 0
            ####if cfuncID > 1: 
            if cfuncID > 0: 
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFCALL'])):
                    if self.cfgData['G2_CONFIG']['CFG_CFCALL'][i]['CFCALL_ID'] > cfcallID:
                        cfcallID = self.cfgData['G2_CONFIG']['CFG_CFCALL'][i]['CFCALL_ID']
                cfcallID = cfcallID + 1 if cfcallID >= 1000 else 1000
                newRecord = {}
                newRecord['CFCALL_ID'] = cfcallID 
                newRecord['CFUNC_ID'] = cfuncID
                newRecord['EXEC_ORDER'] = 1
                newRecord['FTYPE_ID'] = ftypeID
                self.cfgData['G2_CONFIG']['CFG_CFCALL'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'CFCALL build')
    
            #--add elements if not found
            fbomOrder = 0
            for element in parmData['ELEMENTLIST']:
                fbomOrder += 1
    
                if type(element) == dict:
                    elementRecord = dictKeysUpper(element)
                else:
                    elementRecord = {}
                    elementRecord['ELEMENT'] = element
                if 'EXPRESSED' not in elementRecord:
                    elementRecord['EXPRESSED'] = 'No'
                if 'COMPARED' not in elementRecord:
                    elementRecord['COMPARED'] = 'No'

                #--lookup 
                elementRecord['ELEMENT'] = elementRecord['ELEMENT'].upper()
                felemID = 0
                maxID = 0
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])):
                    if self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_CODE'] == elementRecord['ELEMENT']:
                        felemID = self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']
                        break
                    if self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID'] > maxID:
                        maxID = self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']
    
                #--add if not found
                if felemID == 0:
                    felemID = maxID + 1 if maxID >=1000 else 1000
                    newRecord = {}
                    newRecord['FELEM_ID'] = felemID
                    newRecord['FELEM_CODE'] = elementRecord['ELEMENT']
                    newRecord['DATA_TYPE'] = 'STRING'
                    newRecord['TOKENIZE'] = 0
                    self.cfgData['G2_CONFIG']['CFG_FELEM'].append(newRecord)
                    if self.doDebug:
                        showMeTheThings(newRecord, 'FELEM build')

                #--add to distinct value  bom if any 
                if dfcallID > 0:
                    newRecord = {}
                    newRecord['DFCALL_ID'] = dfcallID
                    newRecord['EXEC_ORDER'] = fbomOrder
                    newRecord['FTYPE_ID'] = ftypeID
                    newRecord['FELEM_ID'] = felemID
                    self.cfgData['G2_CONFIG']['CFG_DFBOM'].append(newRecord)
                    if self.doDebug:
                        showMeTheThings(newRecord, 'DFBOM build')

                #--add to expression bom if any
                if efcallID > 0 and elementRecord['EXPRESSED'].upper() == 'YES':
                    newRecord = {}
                    newRecord['EFCALL_ID'] = efcallID
                    newRecord['EXEC_ORDER'] = fbomOrder
                    newRecord['FTYPE_ID'] = ftypeID
                    newRecord['FELEM_ID'] = felemID
                    newRecord['FELEM_REQ'] = 'Yes'
                    self.cfgData['G2_CONFIG']['CFG_EFBOM'].append(newRecord)
                    if self.doDebug:
                        showMeTheThings(newRecord, 'EFBOM build')

                #--add to comparison bom if any 
                if cfcallID > 0 and elementRecord['COMPARED'].upper() == 'YES':
                    newRecord = {}
                    newRecord['CFCALL_ID'] = cfcallID
                    newRecord['EXEC_ORDER'] = fbomOrder
                    newRecord['FTYPE_ID'] = ftypeID
                    newRecord['FELEM_ID'] = felemID
                    self.cfgData['G2_CONFIG']['CFG_CFBOM'].append(newRecord)
                    if self.doDebug:
                        showMeTheThings(newRecord, 'CFBOM build')

                #--add to feature bom always
                newRecord = {}
                newRecord['FTYPE_ID'] = ftypeID
                newRecord['FELEM_ID'] = felemID
                newRecord['EXEC_ORDER'] = fbomOrder
                newRecord['DISPLAY_LEVEL'] = elementRecord['DISPLAY_LEVEL'] if 'DISPLAY_LEVEL' in elementRecord else 1
                newRecord['DISPLAY_DELIM'] = elementRecord['DISPLAY_DELIM'] if 'DISPLAY_DELIM' in elementRecord else ''
                newRecord['DERIVED'] = elementRecord['DERIVED'] if 'DERIVED' in elementRecord else 0
                self.cfgData['G2_CONFIG']['CFG_FBOM'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'FBOM build')
    
            #--guess we made it!
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

    # -----------------------------
    def getFeatureJson(self, ftypeRecord):
        
        fclassRecord = self.getRecord('CFG_FCLASS', 'FCLASS_ID', ftypeRecord['FCLASS_ID'])
        
        sfcallRecord = self.getRecord('CFG_SFCALL', 'FTYPE_ID', ftypeRecord['FTYPE_ID'])
        efcallRecord = self.getRecord('CFG_EFCALL', 'FTYPE_ID', ftypeRecord['FTYPE_ID'])
        cfcallRecord = self.getRecord('CFG_CFCALL', 'FTYPE_ID', ftypeRecord['FTYPE_ID'])
        sfuncRecord = self.getRecord('CFG_SFUNC', 'SFUNC_ID', sfcallRecord['SFUNC_ID']) if sfcallRecord else None
        efuncRecord = self.getRecord('CFG_EFUNC', 'EFUNC_ID', efcallRecord['EFUNC_ID']) if efcallRecord else None
        cfuncRecord = self.getRecord('CFG_CFUNC', 'CFUNC_ID', cfcallRecord['CFUNC_ID']) if cfcallRecord else None
    
        jsonString = '{'
        jsonString += '"id": "%s"' % ftypeRecord['FTYPE_ID']
        jsonString += ', "feature": "%s"' % ftypeRecord['FTYPE_CODE']
        jsonString += ', "class": "%s"' % fclassRecord['FCLASS_CODE'] if fclassRecord else 'OTHER'
        
        jsonString += ', "behavior": "%s"' % getFeatureBehavior(ftypeRecord)
        jsonString += ', "anonymize": "%s"' % ('Yes' if ftypeRecord['ANONYMIZE'] == 1 else 'No')
        jsonString += ', "candidates": "%s"' % ('Yes' if ftypeRecord['USED_FOR_CAND'] == 1 else 'No')
        jsonString += ', "standardize": "%s"' % (sfuncRecord['SFUNC_CODE'] if sfuncRecord else '')
        jsonString += ', "expression": "%s"' % (efuncRecord['EFUNC_CODE'] if efuncRecord else '')
        jsonString += ', "comparison": "%s"' % (cfuncRecord['CFUNC_CODE'] if cfuncRecord else '')

        elementList = []
        fbomRecordList = self.getRecordList('CFG_FBOM', 'FTYPE_ID', ftypeRecord['FTYPE_ID'])
        for fbomRecord in fbomRecordList:
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_ID', fbomRecord['FELEM_ID'])
            if not felemRecord:
                elementList.append('ERROR: FELEM_ID %s' % fbomRecord['FELEM_ID'])
                break
            else:
                if efcallRecord or cfcallRecord:
                    elementRecord = {}
                    elementRecord['element'] = felemRecord['FELEM_CODE']
                    elementRecord['expressed'] = 'No' if not efcallRecord or not self.getRecord('CFG_EFBOM', ['EFCALL_ID', 'FTYPE_ID', 'FELEM_ID'],  [efcallRecord['EFCALL_ID'], fbomRecord['FTYPE_ID'], fbomRecord['FELEM_ID']]) else 'Yes'
                    elementRecord['compared'] = 'No' if not cfcallRecord or not self.getRecord('CFG_CFBOM', ['CFCALL_ID', 'FTYPE_ID', 'FELEM_ID'],  [cfcallRecord['CFCALL_ID'], fbomRecord['FTYPE_ID'], fbomRecord['FELEM_ID']]) else 'Yes'
                    elementList.append(elementRecord)
                else:
                    elementList.append(felemRecord['FELEM_CODE'])

        jsonString += ', "elementList": %s' % json.dumps(elementList)
        jsonString += '}'

        return jsonString
 
# ===== attribute commands =====

    # -----------------------------
    def do_listAttributes(self,arg):
        '\n\tlistAttributes\n'

        print('')
        for attrRecord in sorted(self.getRecordList('CFG_ATTR'), key = lambda k: k['ATTR_ID']):
            print(self.getAttributeJson(attrRecord))
        print('')

    # -----------------------------
    def do_listAttributeClasses(self,arg):
        '\n\tlistAttributeClasses\n'

        print('')
        for attrClass in self.attributeClassList:
            print('{"attributeClass": "%s"}' % attrClass)
        print('')

    # -----------------------------
    def do_getAttribute(self,arg):
        '\n\tgetAttribute {"attribute": "<attribute_name>"}' \
        '\n\tgetAttribute {"feature": "<feature_name>"}\t\tList all the attributes for a feature'

        if not argCheck('getAttribute', arg, self.do_getAttribute.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ATTRIBUTE": arg}
            if 'ATTRIBUTE' in parmData and len(parmData['ATTRIBUTE'].strip()) != 0:
                searchField = 'ATTR_CODE'
                searchValue = parmData['ATTRIBUTE'].upper()
            elif 'ID' in parmData and len(parmData['ID'].strip()) != 0:
                searchField = 'ATTR_ID'
                searchValue = int(parmData['ID'])
            elif 'FEATURE' in parmData and len(parmData['FEATURE'].strip()) != 0:
                searchField = 'FTYPE_CODE'
                searchValue = parmData['FEATURE'].upper()
            else:
                raise ValueError(arg)
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            attrRecords = self.getRecordList('CFG_ATTR', searchField, searchValue)
            if not attrRecords:
                printWithNewLines('Record not found!', 'B')
            else:
                print('')
                for attrRecord in sorted(attrRecords, key = lambda k: k['ATTR_ID']):
                    print(self.getAttributeJson(attrRecord))
                print('')

    # -----------------------------
    def do_deleteAttribute(self,arg):
        '\n\tdeleteAttribute {"attribute": "<attribute_name>"}' \
        '\n\tdeleteAttribute {"feature": "<feature_name>"}\t\tDelete all the attributes for a feature\n'

        if not argCheck('deleteAttribute', arg, self.do_deleteAttribute.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ATTRIBUTE": arg}
            if 'ATTRIBUTE' in parmData and len(parmData['ATTRIBUTE'].strip()) != 0:
                searchField = 'ATTR_CODE'
                searchValue = parmData['ATTRIBUTE'].upper()
            elif 'ID' in parmData and len(parmData['ID'].strip()) != 0:
                searchField = 'ATTR_ID'
                searchValue = int(parmData['ID'])
            elif 'FEATURE' in parmData and len(parmData['FEATURE'].strip()) != 0:
                searchField = 'FTYPE_CODE'
                searchValue = parmData['FEATURE'].upper()
            else:
                raise ValueError(arg)
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ATTR'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_ATTR'][i][searchField] == searchValue:
                    del self.cfgData['G2_CONFIG']['CFG_ATTR'][i]        
                    deleteCnt += 1
                    self.configUpdated = True
            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            printWithNewLines('%s rows deleted!' % deleteCnt, 'B')
    

    # -----------------------------
    def do_addAttribute(self,arg):
        '\n\taddAttribute {"attribute": "<attribute_name>"}' \
        '\n\n\taddAttribute {"attribute": "<attribute_name>", "class": "<class_type>", "feature": "<feature_name>", "element": "<element_type>"}' \
        '\n\n\tFor additional example structures, use getAttribute or listAttributess\n'

        if not argCheck('addAttribute', arg, self.do_addAttribute.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ATTRIBUTE": arg}
            parmData['ATTRIBUTE'] = parmData['ATTRIBUTE'].upper()
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:
            if 'CLASS' in parmData and len(parmData['CLASS']) != 0:
                parmData['CLASS'] = parmData['CLASS'].upper()
                if parmData['CLASS'] not in self.attributeClassList:
                    printWithNewLines('Invalid attribute class: %s' % parmData['CLASS'], 'B')
                    return
            else:
                parmData['CLASS'] = 'OTHER'
    
            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0:
                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s' % parmData['FEATURE'], 'B')
                    return
            else:
                parmData['FEATURE'] = ''
                ftypeRecord = None
    
            if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                if not felemRecord:
                    printWithNewLines('Invalid element: %s' % parmData['ELEMENT'], 'B')
                    return
                else:
                    if not self.getRecord('CFG_FBOM', ['FTYPE_ID', 'FELEM_ID'], [ftypeRecord['FTYPE_ID'], felemRecord['FELEM_ID']]):
                        printWithNewLines('%s is not an element of feature %s'% (parmData['ELEMENT'], parmData['FEATURE']), 'B')
                        return
            else:
                parmData['ELEMENT'] = ''
                felemRecord = None
                
            if (ftypeRecord and not felemRecord) or (felemRecord and not ftypeRecord):
                printWithNewLines('Must have both a feature and an element if either are supplied', 'B')
                return
    
            if 'REQUIRED' not in parmData or len(parmData['REQUIRED'].strip()) == 0:
                parmData['REQUIRED'] = 'No'
            else:
                if parmData['REQUIRED'].upper() not in ('YES', 'NO', 'ANY', 'DESIRED'):
                    printWithNewLines('Invalid required value: %s  (must be "Yes", "No", "Any" or "Desired")' % parmData['REQUIRED'], 'B')
                    return
    
            if 'DEFAULT' not in parmData:
                parmData['DEFAULT'] = ''
            if 'ADVANCED' not in parmData:
                parmData['ADVANCED'] = 'No'
            if 'INTERNAL' not in parmData:
                parmData['INTERNAL'] = 'No'
                
            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ATTR'])):
                if self.cfgData['G2_CONFIG']['CFG_ATTR'][i]['ATTR_CODE'] == parmData['ATTRIBUTE']:
                    printWithNewLines('Attribute %s already exists!' % parmData['ATTRIBUTE'], 'B')
                    return
                if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_ATTR'][i]['ATTR_ID']) == int(parmData['ID']):
                    printWithNewLines('Attribute ID %s already exists!' % parmData['ID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_ATTR'][i]['ATTR_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_ATTR'][i]['ATTR_ID']

            if 'ID' not in parmData: 
                parmData['ID'] = maxID + 1 if maxID >= 2000 else 2000
    
            newRecord = {}
            newRecord['ATTR_ID'] = parmData['ID']
            newRecord['ATTR_CODE'] = parmData['ATTRIBUTE']
            newRecord['ATTR_CLASS'] = parmData['CLASS']
            newRecord['FTYPE_CODE'] = parmData['FEATURE']
            newRecord['FELEM_CODE'] = parmData['ELEMENT']
            newRecord['FELEM_REQ'] = parmData['REQUIRED']
            newRecord['DEFAULT_VALUE'] = parmData['DEFAULT']
            newRecord['ADVANCED'] = 1 if parmData['ADVANCED'].upper() == 'YES' else 0
            newRecord['INTERNAL'] = 1 if parmData['INTERNAL'].upper() == 'YES' else 0
            self.cfgData['G2_CONFIG']['CFG_ATTR'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)

    # -----------------------------
    def getAttributeJson(self, attributeRecord):

        if 'ADVANCED' not in attributeRecord:
            attributeRecord['ADVANCED'] = 0
        if 'INTERNAL' not in attributeRecord:
            attributeRecord['INTERNAL'] = 0
            
        jsonString = '{'
        jsonString += '"id": "%s"' % attributeRecord['ATTR_ID']
        jsonString += ', "attribute": "%s"' % attributeRecord['ATTR_CODE']
        jsonString += ', "class": "%s"' % attributeRecord['ATTR_CLASS']
        jsonString += ', "feature": "%s"' % attributeRecord['FTYPE_CODE']
        jsonString += ', "element": "%s"' % attributeRecord['FELEM_CODE']
        jsonString += ', "required": "%s"' % attributeRecord['FELEM_REQ'].title()
        jsonString += ', "default": "%s"' % attributeRecord['DEFAULT_VALUE']
        jsonString += ', "advanced": "%s"' % ('Yes' if attributeRecord['ADVANCED'] == 1 else 'No') 
        jsonString += ', "internal": "%s"' % ('Yes' if attributeRecord['INTERNAL'] == 1 else 'No')
        jsonString += '}'
        
        return jsonString

# ===== template commands =====

    # -----------------------------
    def do_templateAdd(self,arg):
        '\n\ttemplateAdd {"basic_identifier": "<attribute_name>"}' \
        '\n\ttemplateAdd {"exclusive_identifier": "<attribute_name>"}' \
        '\n\ttemplateAdd {"stable_identifier": "<attribute_name>"}\n'

        validTemplates = ('IDENTIFIER', 'BASIC_IDENTIFIER', 'EXCLUSIVE_IDENTIFIER', 'STABLE_IDENTIFIER')

        if not argCheck('templateAdd', arg, self.do_templateAdd.__doc__):
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            argError(arg, e)
            return

        #--not really expecting a list here, just getting the dictionary key they used
        for templateName in parmData:
            attrName = parmData[templateName].upper()

            if templateName not in validTemplates:
                printWithNewLines( '%s is not a valid template', 'B') 

            #--creates a standard identifier feature and attribute 
            elif 'IDENTIFIER' in templateName:
                if templateName == 'BASIC_IDENTIFIER':
                    behavior = 'F1' 
                elif templateName == 'STABLE_IDENTIFIER':
                    behavior = 'F1ES' 
                else:  #--supports exclusive identifier and the legacy identifier
                    behavior = 'F1E' 
                
                featureParm = '{"feature": "%s", "behavior": "%s", "comparison": "EXACT_COMP", "elementList": [{"compared": "Yes", "element": "ID_NUM"}]}' % (attrName, behavior)
                attributeParm = '{"attribute": "%s", "class": "IDENTIFIER", "feature": "%s", "element": "ID_NUM", "required": "Yes"}' % (attrName, attrName)

                printWithNewLines('addFeature %s' % featureParm, 'S')
                self.do_addFeature(featureParm)

                printWithNewLines('addAttribute %s' % attributeParm, 'S')
                self.do_addAttribute(attributeParm)

# ===== system parameters  =====

    # -----------------------------
    def do_listSystemParameters(self,arg):
        '\n\tlistSystemParameters\n'

        breakRes = 'No'  #--expects all disclosed to be set the same way
        for i in range(len(self.cfgData['G2_CONFIG']['CFG_RTYPE'])):
            if self.cfgData['G2_CONFIG']['CFG_RTYPE'][i]['RCLASS_ID'] == 2:
                breakRes = 'Yes' if self.cfgData['G2_CONFIG']['CFG_RTYPE'][i]['BREAK_RES'] == 1 else 'No'
                break

        print('')
        print('{"relationshipsBreakMatches": "%s"}' % breakRes)
        print('')

    # -----------------------------
    def do_setSystemParameter(self,arg):
        '\n\tsetSystemParameter {"parameter": "<value>"}' 

        validParameters = ('relationshipsBreakMatches')
        if not argCheck('templateAdd', arg, self.do_setSystemParameter.__doc__):
            return
        try:
            parmData = json.loads(arg)  #--don't want these upper
        except (ValueError, KeyError) as e:
            argError(arg, e)
            return
        
        #--not really expecting a list here, just getting the dictionary key they used
        for parameterCode in parmData:
            parameterValue = parmData[parameterCode]
 
            if parameterCode not in validParameters:
                printWithNewLines( '%s is an invalid system parameter' % parameterCode, 'B')

            #--set all disclosed relationship types to break or not break matches 
            elif parameterCode == 'relationshipsBreakMatches':
                if parameterValue.upper() in ('YES', 'Y'):
                    breakRes = 1
                elif parameterValue.upper() in ('NO', 'N'):
                    breakRes = 0
                else:
                    printWithNewLines( '%s is an invalid parameter for %s' % (parameterValue, parameterCode), 'B')
                    return
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_RTYPE'])):
                    if self.cfgData['G2_CONFIG']['CFG_RTYPE'][i]['RCLASS_ID'] == 2:
                        self.cfgData['G2_CONFIG']['CFG_RTYPE'][i]['BREAK_RES'] = breakRes
                        self.configUpdated = True
            
            
# ===== database functions =====

    # -----------------------------
    def do_updateDatabase(self, arg):
        '\n\tWrite changes to DB - for normal use this isn\'t required, just ensure you save!\n'

        if not g2dbUri:
            printWithNewLines('Sorry, no database connection', 'B')
            return
            
        #ans = userInput('\nAre you sure? ')
        if True:   #ans in ['y','Y', 'yes', 'YES']:   <--if they typed the command they are sure.
            print('')

            #try:
            #    g2Dbo.sqlExec('BEGIN TRANSACTION')
            #except G2Exception.G2DBException as err:
            #    printWithNewLines('Database error starting transaction: %s ' % err, 'B')
            #    return

            print('Updating data sources ...')
            insertSql = 'insert into CFG_DSRC (DSRC_ID, DSRC_CODE, DSRC_DESC, DSRC_RELY, RETENTION_LEVEL, CONVERSATIONAL) values (?, ?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_DSRC'), key = lambda k: k['DSRC_ID']):
                record = []
                record.append(jsonRecord['DSRC_ID'])
                record.append(jsonRecord['DSRC_CODE'])
                record.append(jsonRecord['DSRC_DESC'])
                record.append(jsonRecord['DSRC_RELY'])
                record.append(jsonRecord['RETENTION_LEVEL'])
                record.append(jsonRecord['CONVERSATIONAL'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_DSRC')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return
                
            print('Updating entity classes ...')
            insertSql = 'insert into CFG_ECLASS (ECLASS_ID, ECLASS_CODE, ECLASS_DESC, RESOLVE) values (?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_ECLASS'), key = lambda k: k['ECLASS_ID']):
                record = []
                record.append(jsonRecord['ECLASS_ID'])
                record.append(jsonRecord['ECLASS_CODE'])
                record.append(jsonRecord['ECLASS_DESC'])
                record.append(jsonRecord['RESOLVE'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_ECLASS')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return
                
            print('Updating entity types ...')
            insertSql = 'insert into CFG_ETYPE (ETYPE_ID, ETYPE_CODE, ETYPE_DESC, ECLASS_ID) values (?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_ETYPE'), key = lambda k: k['ETYPE_ID']):
                record = []
                record.append(jsonRecord['ETYPE_ID'])
                record.append(jsonRecord['ETYPE_CODE'])
                record.append(jsonRecord['ETYPE_DESC'])
                record.append(jsonRecord['ECLASS_ID'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_ETYPE')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return

            print('Updating features ...')
            insertSql = 'insert into CFG_FTYPE (FTYPE_ID, FTYPE_CODE, FTYPE_DESC, FCLASS_ID, FTYPE_FREQ, FTYPE_STAB, FTYPE_EXCL, ANONYMIZE, DERIVED, USED_FOR_CAND, PERSIST_HISTORY, RTYPE_ID, VERSION) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_FTYPE'), key = lambda k: k['FTYPE_ID']):
                record = []
                record.append(jsonRecord['FTYPE_ID'])
                record.append(jsonRecord['FTYPE_CODE'])
                record.append(jsonRecord['FTYPE_DESC'] if 'FTYPE_DESC' in jsonRecord else jsonRecord['FTYPE_CODE'])
                record.append(jsonRecord['FCLASS_ID'])
                record.append(jsonRecord['FTYPE_FREQ']) 
                record.append(jsonRecord['FTYPE_STAB']) 
                record.append(jsonRecord['FTYPE_EXCL'])
                record.append(jsonRecord['ANONYMIZE']) 
                record.append(jsonRecord['DERIVED']) 
                record.append(jsonRecord['USED_FOR_CAND']) 
                record.append(jsonRecord['PERSIST_HISTORY']) 
                record.append(jsonRecord['RTYPE_ID'])
                record.append(jsonRecord['VERSION']) 
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_FTYPE')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return

            insertSql = 'insert into CFG_FBOM (FTYPE_ID, FELEM_ID, EXEC_ORDER, DISPLAY_DELIM, DISPLAY_LEVEL, DERIVED) values (?, ?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_FBOM'), key = lambda k: k['FTYPE_ID']):
                record = []
                record.append(jsonRecord['FTYPE_ID'])
                record.append(jsonRecord['FELEM_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['DISPLAY_DELIM'])
                record.append(jsonRecord['DISPLAY_LEVEL'])
                record.append(jsonRecord['DERIVED'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_FBOM')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return
                
            insertSql = 'insert into CFG_SFCALL (SFCALL_ID, SFUNC_ID, EXEC_ORDER, FTYPE_ID) values (?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_SFCALL'), key = lambda k: k['SFCALL_ID']):
                record = []
                record.append(jsonRecord['SFCALL_ID'])
                record.append(jsonRecord['SFUNC_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['FTYPE_ID'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_SFCALL')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return

            insertSql = 'insert into CFG_EFCALL (EFCALL_ID, EFUNC_ID, EXEC_ORDER, FTYPE_ID) values (?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_EFCALL'), key = lambda k: k['EFCALL_ID']):
                record = []
                record.append(jsonRecord['EFCALL_ID'])
                record.append(jsonRecord['EFUNC_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['FTYPE_ID'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_EFCALL')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return

            insertSql = 'insert into CFG_EFBOM (EFCALL_ID, EXEC_ORDER, FTYPE_ID, FELEM_ID, FELEM_REQ) values (?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_EFBOM'), key = lambda k: k['EFCALL_ID']):
                record = []
                record.append(jsonRecord['EFCALL_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['FTYPE_ID'])
                record.append(jsonRecord['FELEM_ID'])
                record.append(jsonRecord['FELEM_REQ'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_EFBOM')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return
                
            insertSql = 'insert into CFG_CFCALL (CFCALL_ID, CFUNC_ID, EXEC_ORDER, FTYPE_ID) values (?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_CFCALL'), key = lambda k: k['CFCALL_ID']):
                record = []
                record.append(jsonRecord['CFCALL_ID'])
                record.append(jsonRecord['CFUNC_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['FTYPE_ID'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_CFCALL')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return

            insertSql = 'insert into CFG_CFBOM (CFCALL_ID, EXEC_ORDER, FTYPE_ID, FELEM_ID) values (?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_CFBOM'), key = lambda k: k['CFCALL_ID']):
                record = []
                record.append(jsonRecord['CFCALL_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['FTYPE_ID'])
                record.append(jsonRecord['FELEM_ID'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_CFBOM')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return

            print('Updating attributes ...')
            insertSql = 'insert into CFG_ATTR (ATTR_ID, ATTR_CODE, ATTR_CLASS, FTYPE_CODE, FELEM_CODE, FELEM_REQ, DEFAULT_VALUE, INTERNAL, ADVANCED) values (?, ?, ?, ?, ?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_ATTR'), key = lambda k: k['ATTR_ID']):
                record = []
                record.append(jsonRecord['ATTR_ID'])
                record.append(jsonRecord['ATTR_CODE'])
                record.append(jsonRecord['ATTR_CLASS'])
                record.append(jsonRecord['FTYPE_CODE'])
                record.append(jsonRecord['FELEM_CODE'])
                record.append(jsonRecord['FELEM_REQ'])
                record.append(jsonRecord['DEFAULT_VALUE'])
                record.append(jsonRecord['INTERNAL'])
                record.append(jsonRecord['ADVANCED'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_ATTR')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                insertSql = 'insert into CFG_ATTR (ATTR_ID, ATTR_CODE, ATTR_CLASS, FTYPE_CODE, FELEM_CODE, FELEM_REQ, DEFAULT_VALUE, ADVANCED) values (?, ?, ?, ?, ?, ?, ?, ?)'
                insertRecords = []
                for jsonRecord in sorted(self.getRecordList('CFG_ATTR'), key = lambda k: k['ATTR_ID']):
                    record = []
                    record.append(jsonRecord['ATTR_ID'])
                    record.append(jsonRecord['ATTR_CODE'])
                    record.append(jsonRecord['ATTR_CLASS'])
                    record.append(jsonRecord['FTYPE_CODE'])
                    record.append(jsonRecord['FELEM_CODE'])
                    record.append(jsonRecord['FELEM_REQ'])
                    record.append(jsonRecord['DEFAULT_VALUE'])
                    record.append(jsonRecord['ADVANCED'])
                    insertRecords.append(record)
                try: 
                    g2Dbo.sqlExec('delete from CFG_ATTR')
                    g2Dbo.execMany(insertSql, insertRecords)
                except G2Exception.G2DBException as err:
                    printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                    #--g2Dbo.sqlExec('rollback')
                    return

            print('Updating relationship types ...')
            insertSql = 'insert into CFG_RTYPE (RTYPE_ID, RTYPE_CODE, RCLASS_ID, REL_STRENGTH, BREAK_RES) values (?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_RTYPE'), key = lambda k: k['RTYPE_ID']):
                record = []
                record.append(jsonRecord['RTYPE_ID'])
                record.append(jsonRecord['RTYPE_CODE'])
                record.append(jsonRecord['RCLASS_ID'])
                record.append(jsonRecord['REL_STRENGTH'])
                record.append(jsonRecord['BREAK_RES'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_RTYPE')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return

            print('Updating resolution rules ...')
            insertSql = 'insert into CFG_ERFRAG (ERFRAG_ID, ERFRAG_CODE, ERFRAG_DESC, ERFRAG_SOURCE, ERFRAG_DEPENDS) values (?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_ERFRAG'), key = lambda k: k['ERFRAG_ID']):
                record = []
                record.append(jsonRecord['ERFRAG_ID'])
                record.append(jsonRecord['ERFRAG_CODE'])
                record.append(jsonRecord['ERFRAG_DESC'])
                record.append(jsonRecord['ERFRAG_SOURCE'])
                record.append(jsonRecord['ERFRAG_DEPENDS'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_ERFRAG')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return
                
            insertSql = 'insert into CFG_ERRULE (ERRULE_ID, ERRULE_CODE, ERRULE_DESC, RESOLVE, RELATE, REF_SCORE, RTYPE_ID, QUAL_ERFRAG_CODE, DISQ_ERFRAG_CODE, ERRULE_TIER) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_ERRULE'), key = lambda k: k['ERRULE_ID']):
                record = []
                record.append(jsonRecord['ERRULE_ID'])
                record.append(jsonRecord['ERRULE_CODE'])
                record.append(jsonRecord['ERRULE_DESC'])
                record.append(jsonRecord['RESOLVE'])
                record.append(jsonRecord['RELATE'])
                record.append(jsonRecord['REF_SCORE'])
                record.append(jsonRecord['RTYPE_ID'])
                record.append(jsonRecord['QUAL_ERFRAG_CODE'])
                record.append(jsonRecord['DISQ_ERFRAG_CODE'])
                record.append(jsonRecord['ERRULE_TIER'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_ERRULE')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                return
                
            #--g2Dbo.sqlExec('commit')
            printWithNewLines('Database updated!', 'B')

        
# ===== Utility functions =====

def getFeatureBehavior(feature):

    featureBehavior = feature['FTYPE_FREQ']
    if feature['FTYPE_EXCL'] == 1:
        featureBehavior += 'E'
    if feature['FTYPE_STAB'] == 1:
        featureBehavior += 'S'
    return featureBehavior

def parseFeatureBehavior(behaviorCode):
    behaviorDict = {"EXCLUSIVITY": 0, "STABILITY": 0}
    if 'E' in behaviorCode:
        behaviorDict['EXCLUSIVITY'] = 1
        behaviorCode = behaviorCode.replace('E','')
    if 'S' in behaviorCode:
        behaviorDict['STABILITY'] = 1
        behaviorCode = behaviorCode.replace('S','')
    if behaviorCode in ('F1', 'FF', 'FM', 'FVM', 'NONE'):
        behaviorDict['FREQUENCY'] = behaviorCode
    else:
        behaviorDict = None
    return behaviorDict

def argCheck(func, arg, docstring):

    if len(arg.strip()) == 0:
        print('\nMissing argument(s) for %s, command syntax: %s \n' % (func, '\n\n' + docstring[1:]))
        return False
    else:
        return True

def argError(errorArg, error):

    printWithNewLines('Incorrect argument(s) or error parsing argument: %s' % errorArg, 'S')
    printWithNewLines('Error: %s' % error, 'E')

def printWithNewLines(ln, pos=''):

    pos.upper()
    if pos == 'S' or pos == 'START' :
        print('\n' + ln)
    elif pos == 'E' or pos == 'END' :
        print(ln + '\n')
    elif pos == 'B' or pos == 'BOTH' :
        print('\n' + ln + '\n')
    else:
        print(ln)

def dictKeysUpper(dict):
    return {k.upper():v for k,v in dict.items()}

def showMeTheThings(data, loc=''):
    printWithNewLines('<---- DEBUG')
    printWithNewLines('Func: %s' % sys._getframe(1).f_code.co_name)
    if loc != '': printWithNewLines('Where: %s' % loc) 
    printWithNewLines('Data: %s' % str(data))
    printWithNewLines('---->', 'E')

# ===== The main function =====
if __name__ == '__main__':

    appPath = os.path.dirname(os.path.abspath(sys.argv[0]))
    iniFileName = appPath + os.path.sep + 'G2Project.ini'
    if not os.path.exists(iniFileName):
        print('ERROR: The G2Project.ini file is missing from the application path!')
        sys.exit(1)

    #--get parameters from ini file
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)
    try: g2configFile = iniParser.get('g2', 'G2ConfigFile')
    except: 
        print('ERROR: G2ConfigFile missing from the [g2] section of the G2Project.ini file')
        sys.exit(1)

    #--see if there is database support
    try: g2dbUri = iniParser.get('g2', 'G2Connection')
    except: 
        g2dbUri = None
    else:
        try: g2Dbo = G2Database(g2dbUri)
        except:
            g2dbUri = None

    #--see if there is support for variant file
    g2variantFile = None
    try: g2iniFile = iniParser.get('g2', 'iniPath')
    except: pass 
    else:
        if os.path.exists(g2iniFile): 
            with open(g2iniFile) as fileHandle:
                for line in fileHandle:
                    if line.strip().upper().startswith('SUPPORTPATH') and '=' in line:
                        g2variantFile = line.split('=')[1].strip() + os.sep + 'cfgVariant.json'
    
        
    #--python3 uses input, raw_input was removed
    userInput = input
    if sys.version_info[:2] <= (2,7):
        userInput = raw_input

    #--execute a file of commands or cmdloop()
    if len(sys.argv) > 1:
        G2CmdShell().fileloop(sys.argv[1])
    else:
        G2CmdShell().cmdloop()

    sys.exit()
