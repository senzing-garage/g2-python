#! /usr/bin/env python3

import cmd
import sys
import json
import os
import platform
import G2Paths
import argparse
from shutil import copyfile
from collections import OrderedDict
import traceback

try: import configparser
except: import ConfigParser as configparser

try: 
    from G2IniParams import G2IniParams
    from G2Health import G2Health
    from G2Database import G2Database
    from G2ConfigMgr import G2ConfigMgr
    import G2Exception
except: 
    pass

try:
    import readline
    import atexit
except ImportError:
    readline = None
    
class G2CmdShell(cmd.Cmd):

    def __init__(self):
        cmd.Cmd.__init__(self)

        # set a flag to help us know if this is running an interactive command shell or not
        self.isInteractive = True 

        # this is how you get command history on windows 
        if platform.system() == 'Windows':
            self.use_rawinput = False

        self.intro = '\nWelcome to the G2 Configuration shell. Type help or ? to list commands.\n'
        self.prompt = '(g2) '

        #--define variables for where the config is stored.
        self.g2ConfigFileUsed = False
        self.g2configFile = ''

        #--determine where to get the current configuration from
        iniParamCreator = G2IniParams()
        shouldUseG2ConfigFile = iniParamCreator.hasINIParam(iniFileName,'Sql','G2ConfigFile')
        if shouldUseG2ConfigFile == True:
            #--get the current configuration from a config file.
            self.g2ConfigFileUsed = True
            self.g2configFile = iniParamCreator.getINIParam(iniFileName,'Sql','G2ConfigFile')
            try: self.cfgData = json.load(open(self.g2configFile), encoding="utf-8")
            except ValueError as e:
                print('')
                print('ERROR: %s is broken!' % g2configFile)
                print(e)
                print('')
                sys.exit(1)
        else:
            #--get the current configuration from the database
            iniParams = iniParamCreator.getJsonINIParams(iniFileName)
            g2ConfigMgr = G2ConfigMgr()
            g2ConfigMgr.initV2('pyG2ConfigMgr', iniParams, False)
            defaultConfigID = bytearray() 
            g2ConfigMgr.getDefaultConfigID(defaultConfigID)
            if len(defaultConfigID) == 0:
                print('')
                print('ERROR: No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            defaultConfigDoc = bytearray() 
            g2ConfigMgr.getConfig(defaultConfigID, defaultConfigDoc)
            if len(defaultConfigDoc) == 0:
                print('')
                print('ERROR: No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            self.cfgData = json.loads(defaultConfigDoc.decode())
            g2ConfigMgr.destroy()

        self.configUpdated = False

        self.forceMode = forceMode

        self.attributeClassList = ('NAME', 'ATTRIBUTE', 'IDENTIFIER', 'ADDRESS', 'PHONE', 'RELATIONSHIP', 'OTHER')
        self.lockedFeatureList = ('NAME','ADDRESS', 'PHONE', 'DOB', 'REL_LINK')
        
        self.__hidden_methods = ('do_shell')
        self.doDebug = False
        
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
                printWithNewLines("ERROR: " + str(ex))
                type_, value_, traceback_ = sys.exc_info()
                for item in traceback.format_tb(traceback_):
                    printWithNewLines(item)

    def preloop(self):

        if readline:
            global histfile
            histFileName = '.' + os.path.basename(sys.argv[0].lower().replace('.py','')) + '_history'
            histfile = os.path.join(os.path.expanduser("~"), histFileName)
            if not os.path.isfile(histfile):
                open(histfile, 'a').close()
            hist_size = 2000
            readline.read_history_file(histfile)
            readline.set_history_length(hist_size)

            atexit.register(readline.set_history_length, hist_size)
            atexit.register(readline.write_history_file, histfile)
        else:
            printWithNewLines('INFO: Command history isn\'t available. Try installing python readline module.', 'S')

    def postloop(self):
        # currently do nothing
        pass

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

    def help_History(self):
        print(
              '\nThe commands for managing the session history in the history file.\n'
              '\n\thistClear\n'
              '\t\tClears the current working session history and the history file. This deletes all history, be careful!\n'
              '\n\thistDedupe\n'
              '\t\tThe history can accumulate duplicate entries over time, use this to remove the dupes.\n' 
             )

    def do_shell(self,line):
        '\nRun OS shell commands: !<command>\n'
        output = os.popen(line).read()
        print(output)

    def do_histDedupe(self, arg):

        if readline:
            ans = userInput('\nThis will de-duplicate both this session history and the history file, are you sure?')
            if ans in ['y','Y', 'yes', 'YES']:
    
                with open(histfile) as hf:
                    linesIn = (line.rstrip() for line in hf)
                    uniqLines = OrderedDict.fromkeys( line for line in linesIn if line )
    
                    readline.clear_history()
                    for ul in uniqLines:
                        readline.add_history(ul)
    
                printWithNewLines('Session history and session file both deduplicated.', 'B')
            else:
                printWithNewLines('History session and history file have NOT been deduplicated.', 'B')
        else:
            printWithNewLines('History isn\'t available in this session.', 'B')


    def do_histClear(self, arg):

        if readline:
            ans = userInput('\nThis will clear both this session history and the history file, are you sure?')
            if ans in ['y','Y', 'yes', 'YES']:
                readline.clear_history()
                readline.write_history_file(histfile)
                printWithNewLines('Session history and session file both cleared.', 'B')
            else:
                printWithNewLines('History session and history file have NOT been cleared.', 'B')
        else:
            printWithNewLines('History isn\'t available in this session.', 'B')


    def do_histShow(self, arg):

        if readline:
            print('')
            for i in range(readline.get_current_history_length()):
                printWithNewLines(readline.get_history_item(i + 1))
            print('')
        else:
            printWithNewLines('History isn\'t available in this session.', 'B')

    # -----------------------------
    def fileloop(self, fileName):

        # set a flag to help us know if this is running an interactive command shell or not
        self.isInteractive = False 

        if os.path.exists(fileName): 
            with open(fileName) as data_in:
                for line in data_in:
                    line = line.strip()
                    if len(line) > 0:
                        print('-' * 50)
                        print(line)
                        if line[0:1] not in ('#','-','/'):
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

                            if self.forceMode == False:
                                reply = userInput('Press enter to continue or (Q)uit ... ')
                                if reply and reply.upper().startswith('Q'):
                                    break
                                print('')

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

    # -----------------------------
    def do_configReload(self,arg):
        '\n\tReload configuration and discard all unsaved changes\n'

        #-- check to see if the config has unsaved changes.
        if self.configUpdated:
            ans = userInput('\nYou have unsaved changes, are you sure you want to discard them? ')
            if ans not in ['y','Y', 'yes', 'YES']:
                printWithNewLines('Configuration wasn\'t reloaded.  Your changes remain but are still unsaved.')
                return

        #-- reload the configuration
        iniParamCreator = G2IniParams()
        shouldUseG2ConfigFile = iniParamCreator.hasINIParam(iniFileName,'Sql','G2ConfigFile')
        if self.g2ConfigFileUsed == True:
            #--get the current configuration from a config file.
            try: self.cfgData = json.load(open(self.g2configFile), encoding="utf-8")
            except ValueError as e:
                print('')
                print('ERROR: %s is broken!' % g2configFile)
                print(e)
                print('')
                sys.exit(1)
        else:
            #--get the current configuration from the database
            iniParams = iniParamCreator.getJsonINIParams(iniFileName)
            g2ConfigMgr = G2ConfigMgr()
            g2ConfigMgr.initV2('pyG2ConfigMgr', iniParams, False)
            defaultConfigID = bytearray()
            g2ConfigMgr.getDefaultConfigID(defaultConfigID)
            if len(defaultConfigID) == 0:
                print('')
                print('ERROR: No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            defaultConfigDoc = bytearray()
            g2ConfigMgr.getConfig(defaultConfigID, defaultConfigDoc)
            if len(defaultConfigDoc) == 0:
                print('')
                print('ERROR: No default config stored in database. (see https://senzing.zendesk.com/hc/en-us/articles/360036587313)')
                print('')
                sys.exit(1)
            self.cfgData = json.loads(defaultConfigDoc.decode())
            g2ConfigMgr.destroy()

        self.configUpdated = False
        printWithNewLines('Config has been reloaded.', 'B')


    # -----------------------------
    def do_save(self, args):
        '\n\tSave changes to the config\n'

        if self.configUpdated:

            # Prompt if asked to (do_quit) or if prompts have not been turned off when reading from input file 
            if args == 'ask' or self.forceMode == False: 

                if self.g2ConfigFileUsed == True:
                    ans = userInput('\nSave changes? ')
                else:
                    printWithNewLines('WARNING: This will immediately update the current configuration in the Senzing repository with the current configuration!','B')
                    ans = userInput('Are you certain you wish to proceed and save changes?  ')
            else: 
                ans = 'y'

            if ans in ['y','Y', 'yes', 'YES']:
                
                if self.g2ConfigFileUsed == True:
                    try: copyfile(self.g2configFile, self.g2configFile + '.bk')
                    except:
                        printWithNewLines("Could not create %s" % self.g2configFile + '.bk', 'B')
                        return
                    with open(self.g2configFile, 'w') as fp:
                        json.dump(self.cfgData, fp, indent = 4, sort_keys = True)
                    printWithNewLines('Saved!', 'B')
                    self.configUpdated = False
                else:
                    try:
                        iniParamCreator = G2IniParams()
                        iniParams = iniParamCreator.getJsonINIParams(iniFileName)
                        g2ConfigMgr = G2ConfigMgr()
                        g2ConfigMgr.initV2('pyG2ConfigMgr', iniParams, False)
                        newConfigId = bytearray()
                        retcode = g2ConfigMgr.addConfig(json.dumps(self.cfgData), 'Updated by G2ConfigTool', newConfigId)
                        retcode = g2ConfigMgr.setDefaultConfigID(newConfigId)
                        g2ConfigMgr.destroy()
                    except:
                        printWithNewLines('ERROR: Failed saving config!', 'B')
                    else:
                        printWithNewLines('Saved to Senzing repository!', 'B')
                        self.configUpdated = False

            else:
                printWithNewLines('Current configuration changes have not been saved!', 'B')


    # -----------------------------
    def do_exportToFile(self, arg):
        '\n\tExport the config to a file:  exportToFile <fileName>\n'

        if not argCheck('do_exportToFile', arg, self.do_exportToFile.__doc__):
            return
            
        with open(arg, 'w') as fp:
            json.dump(self.cfgData, fp, indent = 4, sort_keys = True)

        printWithNewLines('Successfully exported!', 'B')

    def do_importFromFile(self,arg):
        '\n\tImport the config from a file:  importToFile <fileName>\n'

        if not argCheck('do_importFromFile', arg, self.do_importFromFile.__doc__):
            return
            
        if self.configUpdated:
            ans = userInput('\nYou have unsaved changes, are you sure you want to discard them? ')
            if ans not in ['y','Y', 'yes', 'YES']:
                printWithNewLines('Configuration wasn\'t imported.  Your changes remain but are still unsaved.')
                return
        
        self.cfgData = json.load(open(arg), encoding="utf-8")
        self.configUpdated = True
        printWithNewLines('%s has been imported.' % arg, 'B')

# ===== Compatibility version commands =====

    # -----------------------------
    def do_verifyCompatibilityVersion(self,arg):
        '\n\tverifyCompatibilityVersion {"expectedVersion": "2"}\n'

        if not argCheck('verifyCompatibilityVersion', arg, self.do_verifyCompatibilityVersion.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if self.cfgData['G2_CONFIG']['CONFIG_BASE_VERSION']['COMPATIBILITY_VERSION']['CONFIG_VERSION'] != parmData['EXPECTEDVERSION']:
                printWithNewLines('Compatibility version does not match specified value [%s]!  Actual version is [%s].' % (parmData['EXPECTEDVERSION'],self.cfgData['G2_CONFIG']['CONFIG_BASE_VERSION']['COMPATIBILITY_VERSION']['CONFIG_VERSION']), 'B')
                if self.isInteractive == False:
                    # throw an exception, so that we abort running scripts
                    raise Exception('Incorrect compatibility version.')
                else:
                    # just finish normally, for the interactive command shell.
                    return
            printWithNewLines('Compatibility version successfully verified!', 'B')


    # -----------------------------
    def do_updateCompatibilityVersion(self,arg):
        '\n\tupdateCompatibilityVersion {"fromVersion": "1", "toVersion": "2"}\n'

        if not argCheck('updateCompatibilityVersion', arg, self.do_updateCompatibilityVersion.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if self.cfgData['G2_CONFIG']['CONFIG_BASE_VERSION']['COMPATIBILITY_VERSION']['CONFIG_VERSION'] != parmData['FROMVERSION']:
                printWithNewLines('Compatibility version to change does not match specified value [%s] !' % parmData['FROMVERSION'], 'B')
                return

            self.cfgData['G2_CONFIG']['CONFIG_BASE_VERSION']['COMPATIBILITY_VERSION']['CONFIG_VERSION'] = parmData['TOVERSION']
            self.configUpdated = True
            printWithNewLines('Compatibility version successfully changed!', 'B')


# ===== Config section commands =====

    # -----------------------------
    def do_addConfigSection(self,arg):
        '\n\taddConfigSection {"name": "<configSection_name>"}'

        if not argCheck('do_addConfigSection', arg, self.do_addConfigSection.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"SECTION": arg}
            parmData['SECTION'] = parmData['SECTION'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if parmData['SECTION'] in self.cfgData['G2_CONFIG']:
                printWithNewLines('Section name %s already exists!' % parmData['SECTION'], 'B')
                return

            self.cfgData['G2_CONFIG'][parmData['SECTION']] = []
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

    # -----------------------------
    def do_addConfigSectionField(self,arg):
        '\n\taddConfigSectionField {"section": "<section_name>","field": "<field_name>","value": "<field_value>"}'

        if not argCheck('do_addConfigSectionField', arg, self.do_addConfigSectionField.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))

            if not 'SECTION' in parmData or len(parmData['SECTION']) == 0:
                raise ValueError('Config section name is required!')
            parmData['SECTION'] = parmData['SECTION'].upper()

            if not 'FIELD' in parmData or len(parmData['FIELD']) == 0:
                raise ValueError('Field name is required!')
            parmData['FIELD'] = parmData['FIELD'].upper()

            if not 'VALUE' in parmData:
                raise ValueError('Field value is required!')
            parmData['VALUE'] = parmData['VALUE']

        except (ValueError, KeyError) as e:
            print('\nError with parsing JSON - %s \n' % e)
        else:

            if not parmData['SECTION'] in self.cfgData['G2_CONFIG']:
                printWithNewLines('Section name %s does not exist!' % parmData['SECTION'], 'B')
                return

            for i in range(len(self.cfgData['G2_CONFIG'][parmData['SECTION']])):
                if parmData['FIELD'] in self.cfgData['G2_CONFIG'][parmData['SECTION']][i]:
                    printWithNewLines('Field name %s already exists!' % parmData['FIELD'], 'B')
                    return

            for i in range(len(self.cfgData['G2_CONFIG'][parmData['SECTION']])):
                self.cfgData['G2_CONFIG'][parmData['SECTION']][i][parmData['FIELD']] = parmData['VALUE']

            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')



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
            newRecord['DSRC_ID'] = int(parmData['ID']) 
            newRecord['DSRC_CODE'] = parmData['DATASOURCE']
            newRecord['DSRC_DESC'] = parmData['DATASOURCE']
            newRecord['DSRC_RELY'] = 1
            newRecord['RETENTION_LEVEL'] = "Remember"
            newRecord['CONVERSATIONAL'] = 'No'
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

            if 'RESOLVE' in parmData and parmData['RESOLVE'].upper() not in ('YES','NO'):
                printWithNewLines('Resolve flag must be Yes or No', 'B')
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
            newRecord['ECLASS_ID'] = int(parmData['ID'])
            newRecord['ECLASS_CODE'] = parmData['ENTITYCLASS']
            newRecord['ECLASS_DESC'] = parmData['ENTITYCLASS']
            newRecord['RESOLVE'] = parmData['RESOLVE'].title() if 'RESOLVE' in parmData else 'Yes'
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
            newRecord['ETYPE_ID'] = int(parmData['ID'])
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
    def do_addFeatureComparison(self,arg):
        '\n\taddFeatureComparison {"feature": "<feature_name>", "comparison": "<comparison_function>", "elementList": ["<element_detail(s)"]}' \
        '\n\n\taddFeatureComparison {"feature":"testFeat", "comparison":"exact_comp", "elementlist": [{"element": "test"}]}' \
        '\n'

        if not argCheck('addFeatureComparison', arg, self.do_addFeatureComparison.__doc__):
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
    
            #--lookup feature and error if it doesn't exist
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return
            ftypeID = ftypeRecord['FTYPE_ID']

            cfuncID = 0  #--comparison function
            if 'COMPARISON' not in parmData or len(parmData['COMPARISON']) == 0:
                printWithNewLines('Comparison function not specified!', 'B')
                return
            parmData['COMPARISON'] = parmData['COMPARISON'].upper()
            cfuncRecord = self.getRecord('CFG_CFUNC', 'CFUNC_CODE', parmData['COMPARISON'])
            if cfuncRecord:
                cfuncID = cfuncRecord['CFUNC_ID']
            else:
                printWithNewLines('Invalid comparison function code: %s' % parmData['COMPARISON'], 'B')
                return

            #--ensure we have elements
            elementCount = 0
            for element in parmData['ELEMENTLIST']:
                elementCount += 1
                elementRecord = dictKeysUpper(element)
                elementRecord['ELEMENT'] = elementRecord['ELEMENT'].upper()
                felemID = 0
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])):
                    if self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_CODE'] == elementRecord['ELEMENT']:
                        felemID = self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']
                        break
                if felemID == 0:
                    printWithNewLines('Invalid element: %s' % elementRecord['ELEMENT'], 'B')
                    return
            if elementCount == 0:
                printWithNewLines('No elements specified for comparison', 'B')
                return

            #--add the comparison call
            cfcallID = 0
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

            #--add elements
            cfbomOrder = 0
            for element in parmData['ELEMENTLIST']:
                cfbomOrder += 1
                elementRecord = dictKeysUpper(element)

                #--lookup 
                elementRecord['ELEMENT'] = elementRecord['ELEMENT'].upper()
                felemID = 0
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])):
                    if self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_CODE'] == elementRecord['ELEMENT']:
                        felemID = self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']
                        break

                #--add to comparison bom if any 
                newRecord = {}
                newRecord['CFCALL_ID'] = cfcallID
                newRecord['EXEC_ORDER'] = cfbomOrder
                newRecord['FTYPE_ID'] = ftypeID
                newRecord['FELEM_ID'] = felemID
                self.cfgData['G2_CONFIG']['CFG_CFBOM'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'CFBOM build')
    
            #--we made it!
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')


    # -----------------------------
    def do_deleteFeatureComparison(self,arg):
        '\n\tdeleteFeatureComparison {"feature": "<feature_name>"}\n'

        if not argCheck('deleteFeatureComparison', arg, self.do_deleteFeatureComparison.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:
    
            #--lookup feature and error if it doesn't exist
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_CODE'] == parmData['FEATURE']:

                    # delete any comparison calls and boms  (must loop through backwards when deleting)
                    for i1 in range(len(self.cfgData['G2_CONFIG']['CFG_CFCALL'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_CFBOM'])-1, -1, -1):
                                if self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]['CFCALL_ID'] == self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]['CFCALL_ID']:
                                    del self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]        
                            del self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]        
                            deleteCnt += 1
                            self.configUpdated = True

            if deleteCnt == 0:
                printWithNewLines('Feature comparator not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

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
            #if parmData['FEATURE'] in self.lockedFeatureList:
            #    printWithNewLines('Feature %s is locked!' % parmData['FEATURE'])
            #    return

            #--lookup feature and error if doesn't exist
            listID = -1
            ftypeID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE'])):
                if self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_CODE'] == parmData['FEATURE']:
                    listID = i
                    ftypeID = self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']
            if listID == -1: 
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
                        self.cfgData['G2_CONFIG']['CFG_FTYPE'][listID]['ANONYMIZE'] = 'Yes' if parmData['ANONYMIZE'].upper() in ('YES', 'Y') else 'No'
                        printWithNewLines('Anonymize setting updated!')
                        self.configUpdated = True
                    else:
                        printWithNewLines('Invalid anonymize setting: %s' % parmData['ANONYMIZE'])
                    
                elif parmCode == 'CANDIDATES':
                    if parmData['CANDIDATES'].upper() in ('YES', 'Y', 'NO','N'):
                        self.cfgData['G2_CONFIG']['CFG_FTYPE'][listID]['USED_FOR_CAND'] = 'Yes' if parmData['CANDIDATES'].upper() in ('YES', 'Y') else 'No'
                        printWithNewLines('Candidates setting updated!')
                        self.configUpdated = True
                    else:
                        printWithNewLines('Invalid candidates setting: %s' % parmData['CANDIDATES'])
                    
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
                ftypeID = int(parmData['ID'])
            else:
                ftypeID = maxID + 1 if maxID >=1000 else 1000
            
            #--default for missing values
            parmData['ID'] = ftypeID
            parmData['BEHAVIOR'] = parmData['BEHAVIOR'].upper() if 'BEHAVIOR' in parmData else 'FM'
            parmData['ANONYMIZE'] = parmData['ANONYMIZE'].upper() if 'ANONYMIZE' in parmData else 'NO'
            parmData['DERIVED'] = parmData['DERIVED'].upper() if 'DERIVED' in parmData else 'NO'
            parmData['DERIVATION'] = parmData['DERIVATION'] if 'DERIVATION' in parmData else ''
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
            newRecord['FTYPE_ID'] = int(ftypeID)
            newRecord['FTYPE_CODE'] = parmData['FEATURE']
            newRecord['FTYPE_DESC'] = parmData['FEATURE']
            newRecord['FCLASS_ID'] = fclassID
            newRecord['FTYPE_FREQ'] = featureBehaviorDict['FREQUENCY'] 
            newRecord['FTYPE_EXCL'] = featureBehaviorDict['EXCLUSIVITY']
            newRecord['FTYPE_STAB'] = featureBehaviorDict['STABILITY']
            newRecord['ANONYMIZE'] = 'No' if parmData['ANONYMIZE'].upper() == 'NO' else 'Yes' 
            newRecord['DERIVED'] = 'No' if parmData['DERIVED'].upper() == 'NO' else 'Yes'
            newRecord['DERIVATION'] = parmData['DERIVATION']
            newRecord['USED_FOR_CAND'] = 'No' if parmData['CANDIDATES'].upper() == 'NO' else 'Yes'
            newRecord['PERSIST_HISTORY'] = 'Yes' 
            newRecord['VERSION'] = 1
            newRecord['RTYPE_ID'] = 0
            self.cfgData['G2_CONFIG']['CFG_FTYPE'].append(newRecord)
            if self.doDebug:
                showMeTheThings(newRecord, 'Feature build')

            #--add the standardization call
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
                newRecord['FELEM_ID'] = -1
                self.cfgData['G2_CONFIG']['CFG_SFCALL'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'SFCALL build')

            #--add the distinct value call (not supported through here yet)
            dfcallID = 0
            dfuncID = 0  #--more efficent to leave it null
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
                newRecord['FELEM_ID'] = -1
                newRecord['EFEAT_FTYPE_ID'] = -1
                newRecord['IS_VIRTUAL'] = 'No'
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
                    newRecord['FELEM_DESC'] = elementRecord['ELEMENT']
                    newRecord['DATA_TYPE'] = 'string'
                    newRecord['TOKENIZE'] = 'No'
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
                newRecord['DERIVED'] = elementRecord['DERIVED'] if 'DERIVED' in elementRecord else 'No'
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
        jsonString += ', "anonymize": "%s"' % ('Yes' if ftypeRecord['ANONYMIZE'].upper() == 'YES' else 'No')
        jsonString += ', "candidates": "%s"' % ('Yes' if ftypeRecord['USED_FOR_CAND'].upper() == 'YES' else 'No')
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
       
 
    # -----------------------------
    def do_addToNamehash(self,arg):
        '\n\taddToNamehash {"feature": "<feature>", "element": "<element>"}'
        if not argCheck('addToNamehash', arg, self.do_addToNamehash.__doc__):
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
            return

        try:
            nameHasher_efuncID = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', 'NAME_HASHER')['EFUNC_ID']
            nameHasher_efcallID = self.getRecord('CFG_EFCALL', 'EFUNC_ID', nameHasher_efuncID)['EFCALL_ID']
        except: 
            nameHasher_efcallID = 0
        if not nameHasher_efcallID:
            printWithNewLines('Name hasher function not found!', 'B')
            return

        ftypeID = -1
        if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0:
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return
            ftypeID = ftypeRecord['FTYPE_ID']

        felemID = -1
        if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Feature element %s not found!' % parmData['ELEMENT'], 'B')
                return
            felemID = felemRecord['FELEM_ID']
        else:
            printWithNewLines('A feature element value is required', 'B')
            return

        if ftypeID != -1:
            if not self.getRecord('CFG_FBOM', ['FTYPE_ID', 'FELEM_ID'], [ftypeID, felemID]):
                printWithNewLines('%s is not an element of feature %s'% (parmData['ELEMENT'], parmData['FEATURE']), 'B')
                return
            
        nameHasher_execOrder = 0
        for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFBOM'])):
            if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EFCALL_ID'] == nameHasher_efcallID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EXEC_ORDER'] > nameHasher_execOrder:
                nameHasher_execOrder = self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EXEC_ORDER']
            if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EFCALL_ID'] == nameHasher_execOrder and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FTYPE_ID'] == ftypeID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FELEM_ID'] == felemID:
                printWithNewLines('Already added to name hash!', 'B')
                return

        #--add record
        newRecord = {}
        newRecord['EFCALL_ID'] = nameHasher_efcallID
        newRecord['EXEC_ORDER'] = nameHasher_execOrder + 1
        newRecord['FTYPE_ID'] = ftypeID
        newRecord['FELEM_ID'] = felemID
        newRecord['FELEM_REQ'] = 'No'
        self.cfgData['G2_CONFIG']['CFG_EFBOM'].append(newRecord)
        if self.doDebug:
            showMeTheThings(newRecord, 'EFBOM build')

        self.configUpdated = True
        printWithNewLines('Successfully added!', 'B')

    # -----------------------------
    def do_deleteFromNamehash(self,arg):
        '\n\tdeleteFromNamehash {"feature": "<feature>", "element": "<element>"}'
        if not argCheck('deleteFromNamehash', arg, self.do_deleteFromNamehash.__doc__):
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
            return

        try:
            nameHasher_efuncID = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', 'NAME_HASHER')['EFUNC_ID']
            nameHasher_efcallID = self.getRecord('CFG_EFCALL', 'EFUNC_ID', nameHasher_efuncID)['EFCALL_ID']
        except: 
            nameHasher_efcallID = 0
        if not nameHasher_efcallID:
            printWithNewLines('Name hasher function not found!', 'B')
            return

        ftypeID = -1
        if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0:
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return
            ftypeID = ftypeRecord['FTYPE_ID']

        felemID = -1
        if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Feature element %s not found!' % parmData['ELEMENT'], 'B')
                return
            felemID = felemRecord['FELEM_ID']
            
        deleteCnt = 0
        for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFBOM'])-1, -1, -1):
            if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EFCALL_ID'] == nameHasher_efcallID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FTYPE_ID'] == ftypeID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FELEM_ID'] == felemID:
                del self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]        
                deleteCnt += 1
                self.configUpdated = True
        if deleteCnt == 0:
            printWithNewLines('Record not found!', 'B')
        printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    # -----------------------------
    def do_addToNameSSNLast4hash(self,arg):
        '\n\taddToNameSSNLast4hash {"feature": "<feature>", "element": "<element>"}'
        if not argCheck('addToNameSSNLast4hash', arg, self.do_addToNameSSNLast4hash.__doc__):
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
            return

        try:
            ssnLast4Hasher_efuncID = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', 'EXPRESS_BOM')['EFUNC_ID']
            ssnLast4Hasher_efcallID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFCALL'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFUNC_ID'] == ssnLast4Hasher_efuncID and self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['FTYPE_ID'] == self.getRecord('CFG_FTYPE', 'FTYPE_CODE', 'SSN_LAST4')['FTYPE_ID']:
                    ssnLast4Hasher_efcallID = self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFCALL_ID']
        except: 
            ssnLast4Hasher_efcallID = 0
        if not ssnLast4Hasher_efcallID:
            printWithNewLines('SSNLast4 hasher function not found!', 'B')
            return

        ftypeID = -1
        if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0:
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return
            ftypeID = ftypeRecord['FTYPE_ID']

        felemID = -1
        if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Feature element %s not found!' % parmData['ELEMENT'], 'B')
                return
            felemID = felemRecord['FELEM_ID']
        else:
            printWithNewLines('A feature element value is required', 'B')
            return

        if ftypeID != -1:
            if not self.getRecord('CFG_FBOM', ['FTYPE_ID', 'FELEM_ID'], [ftypeID, felemID]):
                printWithNewLines('%s is not an element of feature %s'% (parmData['ELEMENT'], parmData['FEATURE']), 'B')
                return

        ssnLast4Hasher_execOrder = 0
        for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFBOM'])):
            if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EFCALL_ID'] == ssnLast4Hasher_efcallID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EXEC_ORDER'] > ssnLast4Hasher_execOrder:
                ssnLast4Hasher_execOrder = self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EXEC_ORDER']
            if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EFCALL_ID'] == ssnLast4Hasher_efcallID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FTYPE_ID'] == ftypeID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FELEM_ID'] == felemID:
                printWithNewLines('Already added to name hash!', 'B')
                return

        #--add record
        newRecord = {}
        newRecord['EFCALL_ID'] = ssnLast4Hasher_efcallID
        newRecord['EXEC_ORDER'] = ssnLast4Hasher_execOrder + 1
        newRecord['FTYPE_ID'] = ftypeID
        newRecord['FELEM_ID'] = felemID
        newRecord['FELEM_REQ'] = 'Yes'
        self.cfgData['G2_CONFIG']['CFG_EFBOM'].append(newRecord)
        if self.doDebug:
            showMeTheThings(newRecord, 'EFBOM build')

        self.configUpdated = True
        printWithNewLines('Successfully added!', 'B')

    # -----------------------------
    def do_deleteFromSSNLast4hash(self,arg):
        '\n\tdeleteFromSSNLast4hash {"feature": "<feature>", "element": "<element>"}'
        if not argCheck('deleteFromSSNLast4hash', arg, self.do_deleteFromSSNLast4hash.__doc__):
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
            return

        try:
            ssnLast4Hasher_efuncID = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', 'EXPRESS_BOM')['EFUNC_ID']
            ssnLast4Hasher_efcallID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFCALL'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFUNC_ID'] == ssnLast4Hasher_efuncID and self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['FTYPE_ID'] == self.getRecord('CFG_FTYPE', 'FTYPE_CODE', 'SSN_LAST4')['FTYPE_ID']:
                    ssnLast4Hasher_efcallID = self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFCALL_ID']
        except: 
            ssnLast4Hasher_efcallID = 0
        if not ssnLast4Hasher_efcallID:
            printWithNewLines('SSNLast4 hasher function not found!', 'B')
            return

        ftypeID = -1
        if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0:
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return
            ftypeID = ftypeRecord['FTYPE_ID']

        felemID = -1
        if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Feature element %s not found!' % parmData['ELEMENT'], 'B')
                return
            felemID = felemRecord['FELEM_ID']
            
        deleteCnt = 0
        for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFBOM'])-1, -1, -1):
            if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['EFCALL_ID'] == ssnLast4Hasher_efcallID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FTYPE_ID'] == ftypeID and self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]['FELEM_ID'] == felemID:
                del self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]        
                deleteCnt += 1
                self.configUpdated = True
        if deleteCnt == 0:
            printWithNewLines('Hash entry not found!', 'B')
        printWithNewLines('%s rows deleted!' % deleteCnt, 'B')


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
                if parmData['ELEMENT'] == '<PREHASHED>':
                    felemRecord = parmData['ELEMENT']
                else:
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
            newRecord['ATTR_ID'] = int(parmData['ID'])
            newRecord['ATTR_CODE'] = parmData['ATTRIBUTE']
            newRecord['ATTR_CLASS'] = parmData['CLASS']
            newRecord['FTYPE_CODE'] = parmData['FEATURE']
            newRecord['FELEM_CODE'] = parmData['ELEMENT']
            newRecord['FELEM_REQ'] = parmData['REQUIRED']
            newRecord['DEFAULT_VALUE'] = parmData['DEFAULT']
            newRecord['ADVANCED'] = 'Yes' if parmData['ADVANCED'].upper() == 'YES' else 'No'
            newRecord['INTERNAL'] = 'Yes' if parmData['INTERNAL'].upper() == 'YES' else 'No'
            self.cfgData['G2_CONFIG']['CFG_ATTR'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)

    # -----------------------------
    def getAttributeJson(self, attributeRecord):

        if 'ADVANCED' not in attributeRecord:
            attributeRecord['ADVANCED'] = 'No'
        if 'INTERNAL' not in attributeRecord:
            attributeRecord['INTERNAL'] = 'No'
            
        jsonString = '{'
        jsonString += '"id": "%s"' % attributeRecord['ATTR_ID']
        jsonString += ', "attribute": "%s"' % attributeRecord['ATTR_CODE']
        jsonString += ', "class": "%s"' % attributeRecord['ATTR_CLASS']
        jsonString += ', "feature": "%s"' % attributeRecord['FTYPE_CODE']
        jsonString += ', "element": "%s"' % attributeRecord['FELEM_CODE']
        jsonString += ', "required": "%s"' % attributeRecord['FELEM_REQ'].title()
        jsonString += ', "default": "%s"' % (attributeRecord['DEFAULT_VALUE'] if attributeRecord['DEFAULT_VALUE'] else "")
        jsonString += ', "advanced": "%s"' % attributeRecord['ADVANCED'] 
        jsonString += ', "internal": "%s"' % attributeRecord['INTERNAL']
        jsonString += '}'
        
        return jsonString

# ===== element commands =====

    # -----------------------------
    def do_listElements(self,arg):
        '\n\tlistElements\n'

        print('')
        for elemRecord in sorted(self.getRecordList('CFG_FELEM'), key = lambda k: k['FELEM_ID']):
            print('{"id": %i, "code": "%s", "tokenize": "%s", "datatype": "%s"}' % (elemRecord['FELEM_ID'], elemRecord['FELEM_CODE'], elemRecord['TOKENIZE'], elemRecord['DATA_TYPE']))
        print('')


    # -----------------------------
    def do_getElement(self,arg):
        '\n\tgetElement {"element": "<element_name>"}\n'

        if not argCheck('getElement', arg, self.do_getElement.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ELEMENT": arg}
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Element %s not found!' % parmData['ELEMENT'], 'B')
            else:
                printWithNewLines('{"id": %s, "code": %s, "datatype": %s, "tokenize": %s}' % (felemRecord['FELEM_ID'], felemRecord['FELEM_CODE'], felemRecord['DATA_TYPE'], felemRecord['TOKENIZE']), 'B')


# -----------------------------

    def do_addStandardizeFunc(self,arg):
        '\n\taddStandardizeFunc {"function":"<function_name>", "connectStr":"<plugin_base_name>"}' \
        '\n\n\taddStandardizeFunc {"function":"STANDARDIZE_COUNTRY", "connectStr":"g2StdCountry"}' \
        '\n'

        if not argCheck('addStandardizeFunc', arg, self.do_addStandardizeFunc.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FUNCTION'] = parmData['FUNCTION'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if self.getRecord('CFG_SFUNC', 'SFUNC_CODE', parmData['FUNCTION']):
                printWithNewLines('Function %s already exists!' % parmData['FUNCTION'], 'B')
                return
            else:

                #--default for missing values

                if 'FUNCLIB' not in parmData or len(parmData['FUNCLIB'].strip()) == 0:
                    parmData['FUNCLIB'] = 'g2func_lib'
                if 'VERSION' not in parmData or len(parmData['VERSION'].strip()) == 0:
                    parmData['VERSION'] = '1'
                if 'CONNECTSTR' not in parmData or len(parmData['CONNECTSTR'].strip()) == 0:
                    printWithNewLines('ConnectStr value not specified.', 'B')
                    return

                maxID = []
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_SFUNC'])) :
                    maxID.append(self.cfgData['G2_CONFIG']['CFG_SFUNC'][i]['SFUNC_ID'])

                sfuncID = 0
                if 'ID' in parmData: 
                    sfuncID = int(parmData['ID'])
                else:
                    sfuncID = max(maxID) + 1 if max(maxID) >=1000 else 1000

                newRecord = {}
                newRecord['SFUNC_ID'] = sfuncID
                newRecord['SFUNC_CODE'] = parmData['FUNCTION']
                newRecord['SFUNC_DESC'] = parmData['FUNCTION']
                newRecord['FUNC_LIB'] = parmData['FUNCLIB']
                newRecord['FUNC_VER'] = parmData['VERSION']
                newRecord['CONNECT_STR'] = parmData['CONNECTSTR']
                self.cfgData['G2_CONFIG']['CFG_SFUNC'].append(newRecord)
                self.configUpdated = True
                printWithNewLines('Successfully added!', 'B')
                if self.doDebug:
                    showMeTheThings(newRecord)


# -----------------------------

    def do_addStandardizeCall(self,arg):
        '\n\taddStandardizeCall {"element":"<element_name>", "function":"<function_name>", "execOrder":<exec_order>}' \
        '\n\n\taddStandardizeCall {"element":"COUNTRY", "function":"STANDARDIZE_COUNTRY", "execOrder":100}' \
        '\n'

        if not argCheck('addStandardizeCall', arg, self.do_addStandardizeCall.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            featureIsSpecified = False;
            ftypeID = -1
            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 :
                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s.' % parmData['FEATURE'], 'B')
                    return
                featureIsSpecified = True;
                ftypeID = ftypeRecord['FTYPE_ID']

            elementIsSpecified = False;
            felemID = -1
            if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0 :
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                if not felemRecord:
                    printWithNewLines('Invalid element: %s.' % parmData['ELEMENT'], 'B')
                    return
                elementIsSpecified = True;
                felemID = felemRecord['FELEM_ID']

            if featureIsSpecified == False and elementIsSpecified == False :
                printWithNewLines('No feature or element specified.', 'B')
                return

            if featureIsSpecified == True and elementIsSpecified == True :
                printWithNewLines('Both feature and element specified.  Must only use one, not both.', 'B')
                return

            sfuncID = -1
            if 'FUNCTION' not in parmData or len(parmData['FUNCTION'].strip()) == 0:
                printWithNewLines('Function not specified.', 'B')
                return
            parmData['FUNCTION'] = parmData['FUNCTION'].upper()
            sfuncRecord = self.getRecord('CFG_SFUNC', 'SFUNC_CODE', parmData['FUNCTION'])
            if not sfuncRecord:
                printWithNewLines('Invalid function: %s.' % parmData['FUNCTION'], 'B')
                return
            sfuncID = sfuncRecord['SFUNC_ID']

            if 'EXECORDER' not in parmData:
                printWithNewLines('Exec order not specified.', 'B')
                return

            maxID = []
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_SFUNC'])) :
                maxID.append(self.cfgData['G2_CONFIG']['CFG_SFUNC'][i]['SFUNC_ID'])

            sfcallID = 0
            if 'ID' in parmData: 
                sfcallID = int(parmData['ID'])
            else:
                sfcallID = max(maxID) + 1 if max(maxID) >=1000 else 1000

            newRecord = {}
            newRecord['SFCALL_ID'] = sfcallID
            newRecord['FTYPE_ID'] = ftypeID
            newRecord['FELEM_ID'] = felemID
            newRecord['SFUNC_ID'] = sfuncID
            newRecord['EXEC_ORDER'] = parmData['EXECORDER']
            self.cfgData['G2_CONFIG']['CFG_SFCALL'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)


# -----------------------------

    def do_addExpressionFunc(self,arg):

        '\n\taddExpressionFunc {"function":"<function_name>", "connectStr":"<plugin_base_name>"}' \
        '\n\n\taddExpressionFunc {"function":"FEAT_BUILDER", "connectStr":"g2FeatBuilder"}' \
        '\n'

        if not argCheck('addExpressionFunc', arg, self.do_addExpressionFunc.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FUNCTION'] = parmData['FUNCTION'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if self.getRecord('CFG_EFUNC', 'EFUNC_CODE', parmData['FUNCTION']):
                printWithNewLines('Function %s already exists!' % parmData['FUNCTION'], 'B')
                return
            else:

                #--default for missing values

                if 'FUNCLIB' not in parmData or len(parmData['FUNCLIB'].strip()) == 0:
                    parmData['FUNCLIB'] = 'g2func_lib'
                if 'VERSION' not in parmData or len(parmData['VERSION'].strip()) == 0:
                    parmData['VERSION'] = '1'
                if 'CONNECTSTR' not in parmData or len(parmData['CONNECTSTR'].strip()) == 0:
                    printWithNewLines('ConnectStr value not specified.', 'B')
                    return

                maxID = []
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFUNC'])) :
                    maxID.append(self.cfgData['G2_CONFIG']['CFG_EFUNC'][i]['EFUNC_ID'])

                efuncID = 0
                if 'ID' in parmData: 
                    efuncID = int(parmData['ID'])
                else:
                    efuncID = max(maxID) + 1 if max(maxID) >=1000 else 1000

                newRecord = {}
                newRecord['EFUNC_ID'] = efuncID
                newRecord['EFUNC_CODE'] = parmData['FUNCTION']
                newRecord['EFUNC_DESC'] = parmData['FUNCTION']
                newRecord['FUNC_LIB'] = parmData['FUNCLIB']
                newRecord['FUNC_VER'] = parmData['VERSION']
                newRecord['CONNECT_STR'] = parmData['CONNECTSTR']
                self.cfgData['G2_CONFIG']['CFG_EFUNC'].append(newRecord)
                self.configUpdated = True
                printWithNewLines('Successfully added!', 'B')
                if self.doDebug:
                    showMeTheThings(newRecord)


# -----------------------------

    def do_addExpressionCall(self,arg):

        '\n\taddExpressionCall {"element":"<element_name>", "function":"<function_name>", "execOrder":<exec_order>, expressionFeature":<feature_name>, "virtual":"No","elementList": ["<element_detail(s)"]}' \
        '\n\n\taddExpressionCall {"element":"COUNTRY_CODE", "function":"FEAT_BUILDER", "execOrder":100, expressionFeature":"COUNTRY_OF_ASSOCIATION", "virtual":"No","elementList": [{"element":"COUNTRY", "featureLink":"parent", "required":"No"}]}' \
        '\n\n\taddExpressionCall {"element":"COUNTRY_CODE", "function":"FEAT_BUILDER", "execOrder":100, expressionFeature":"COUNTRY_OF_ASSOCIATION", "virtual":"No","elementList": [{"element":"COUNTRY", "feature":"ADDRESS", "required":"No"}]}' \
        '\n'

        if not argCheck('addExpressionCall', arg, self.do_addExpressionCall.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            if not 'ELEMENTLIST' in parmData or len(parmData['ELEMENTLIST']) == 0:
                raise ValueError('Element list is required!')
            if type(parmData['ELEMENTLIST']) is not list:
                raise ValueError('Element list should be specified as: "elementlist": ["<values>"]\n\n\tNote the [ and ]')
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            featureIsSpecified = False;
            ftypeID = -1
            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 :
                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s.' % parmData['FEATURE'], 'B')
                    return
                featureIsSpecified = True;
                ftypeID = ftypeRecord['FTYPE_ID']

            elementIsSpecified = False;
            felemID = -1
            if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0 :
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                if not felemRecord:
                    printWithNewLines('Invalid element: %s.' % parmData['ELEMENT'], 'B')
                    return
                elementIsSpecified = True;
                felemID = felemRecord['FELEM_ID']

            if featureIsSpecified == False and elementIsSpecified == False :
                printWithNewLines('No feature or element specified.', 'B')
                return

            if featureIsSpecified == True and elementIsSpecified == True :
                printWithNewLines('Both feature and element specified.  Must only use one, not both.', 'B')
                return

            efuncID = -1
            if 'FUNCTION' not in parmData or len(parmData['FUNCTION'].strip()) == 0:
                printWithNewLines('Function not specified.', 'B')
                return
            parmData['FUNCTION'] = parmData['FUNCTION'].upper()
            efuncRecord = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', parmData['FUNCTION'])
            if not efuncRecord:
                printWithNewLines('Invalid function: %s.' % parmData['FUNCTION'], 'B')
                return
            efuncID = efuncRecord['EFUNC_ID']

            if 'EXECORDER' not in parmData:
                printWithNewLines('An execOrder for the call must be specified.', 'B')
                return

            callExists = False
            efcallID = int(parmData['ID']) if 'ID' in parmData else 0
            maxID = []
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFCALL'])):
                maxID.append(self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFCALL_ID'])
                if self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EFCALL_ID'] == efcallID:
                    printWithNewLines('The supplied ID already exists.', 'B')
                    callExists = True
                    break
                elif self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['FTYPE_ID'] == ftypeID and self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]['EXEC_ORDER'] == parmData['EXECORDER']:
                    printWithNewLines('A call for that feature and execOrder already exists.', 'B')
                    callExists = True
                    break
            if callExists: 
                return

            if 'ID' in parmData: 
                efcallID = int(parmData['ID'])
            else:
                efcallID = max(maxID) + 1 if max(maxID) >=1000 else 1000

            isVirtual = parmData['VIRTUAL'] if 'VIRTUAL' in parmData else 'No'

            efeatFTypeID = -1
            if 'EXPRESSIONFEATURE' in parmData and len(parmData['EXPRESSIONFEATURE']) != 0 :
                parmData['EXPRESSIONFEATURE'] = parmData['EXPRESSIONFEATURE'].upper()
                expressionFTypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['EXPRESSIONFEATURE'])
                if not expressionFTypeRecord:
                    printWithNewLines('Invalid expression feature: %s.' % parmData['EXPRESSIONFEATURE'], 'B')
                    return
                efeatFTypeID = expressionFTypeRecord['FTYPE_ID']

            #--ensure we have valid elements
            elementCount = 0
            for element in parmData['ELEMENTLIST']:
                elementCount += 1
                elementRecord = dictKeysUpper(element)

                bomFTypeIsSpecified = False;
                bomFTypeID = -1
                if 'FEATURE' in elementRecord and len(elementRecord['FEATURE']) != 0 :
                    elementRecord['FEATURE'] = elementRecord['FEATURE'].upper()
                    bomFTypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', elementRecord['FEATURE'])
                    if not bomFTypeRecord:
                        printWithNewLines('Invalid BOM feature: %s.' % elementRecord['FEATURE'], 'B')
                        return
                    bomFTypeIsSpecified = True;
                    bomFTypeID = bomFTypeRecord['FTYPE_ID']

                bomFElemIsSpecified = False;
                bomFElemID = -1
                if 'ELEMENT' in elementRecord and len(elementRecord['ELEMENT']) != 0 :
                    elementRecord['ELEMENT'] = elementRecord['ELEMENT'].upper()
                    bomFElemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', elementRecord['ELEMENT'])
                    if not bomFElemRecord:
                        printWithNewLines('Invalid BOM element: %s.' % elementRecord['ELEMENT'], 'B')
                        return
                    bomFElemIsSpecified = True;
                    bomFElemID = bomFElemRecord['FELEM_ID']

                if bomFElemIsSpecified == False :
                    printWithNewLines('No BOM element specified on BOM entry.', 'B')
                    return

                bomFTypeFeatureLinkIsSpecified = False;
                if 'FEATURELINK' in elementRecord and len(elementRecord['FEATURELINK']) != 0 :
                    elementRecord['FEATURELINK'] = elementRecord['FEATURELINK'].upper()
                    if elementRecord['FEATURELINK'] != 'PARENT':
                        printWithNewLines('Invalid feature link value: %s.  (Must use \'parent\')' % elementRecord['FEATURELINK'], 'B')
                        return
                    bomFTypeFeatureLinkIsSpecified = True;
                    bomFTypeID = 0

                if bomFTypeIsSpecified == True and bomFTypeFeatureLinkIsSpecified == True :
                    printWithNewLines('Cannot specify both ftype and feature-link on single function BOM entry.', 'B')
                    return

            if elementCount == 0:
                printWithNewLines('No elements specified.', 'B')
                return

            #--add the expression call
            newRecord = {}
            newRecord['EFCALL_ID'] = efcallID
            newRecord['FTYPE_ID'] = ftypeID
            newRecord['FELEM_ID'] = felemID
            newRecord['EFUNC_ID'] = efuncID
            newRecord['EXEC_ORDER'] = parmData['EXECORDER']
            newRecord['EFEAT_FTYPE_ID'] = efeatFTypeID
            newRecord['IS_VIRTUAL'] = isVirtual
            self.cfgData['G2_CONFIG']['CFG_EFCALL'].append(newRecord)
            if self.doDebug:
                showMeTheThings(newRecord)

            #--add elements
            efbomOrder = 0
            for element in parmData['ELEMENTLIST']:
                efbomOrder += 1
                elementRecord = dictKeysUpper(element)

                bomFTypeID = -1
                if 'FEATURE' in elementRecord and len(elementRecord['FEATURE']) != 0 :
                    bomFTypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', elementRecord['FEATURE'])
                    bomFTypeID = bomFTypeRecord['FTYPE_ID']

                bomFElemID = -1
                if 'ELEMENT' in elementRecord and len(elementRecord['ELEMENT']) != 0 :
                    bomFElemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', elementRecord['ELEMENT'])
                    bomFElemID = bomFElemRecord['FELEM_ID']

                if 'FEATURELINK' in elementRecord and len(elementRecord['FEATURELINK']) != 0 :
                    elementRecord['FEATURELINK'] = elementRecord['FEATURELINK'].upper()
                    bomFTypeID = 0

                felemRequired = elementRecord['REQUIRED'] if 'REQUIRED' in elementRecord else 'No'

                #--add to expression bom if any 
                newRecord = {}
                newRecord['EFCALL_ID'] = efcallID
                newRecord['EXEC_ORDER'] = efbomOrder
                newRecord['FTYPE_ID'] = bomFTypeID
                newRecord['FELEM_ID'] = bomFElemID
                newRecord['FELEM_REQ'] = felemRequired
                self.cfgData['G2_CONFIG']['CFG_EFBOM'].append(newRecord)
                if self.doDebug:
                    showMeTheThings(newRecord, 'EFBOM build')
    
            #--we made it!
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')


    # -----------------------------
    def do_deleteExpressionCall(self,arg):
        '\n\deleteExpressionCall {"id": "<id>"}' 

        if not argCheck('deleteExpressionCall', arg, self.do_deleteExpressionCall.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ID": arg}
            if 'ID' not in parmData or not parmData['ID'].isnumeric():
                raise ValueError(arg)
            else:
                searchField = 'EFCALL_ID'
                searchValue = int(parmData['ID'])
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFCALL'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_EFCALL'][i][searchField] == searchValue:
                    del self.cfgData['G2_CONFIG']['CFG_EFCALL'][i]        
                    deleteCnt += 1
                    self.configUpdated = True
            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
                return
            printWithNewLines('%s rows deleted!' % deleteCnt, 'B')
    
            #--delete the efboms too
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFBOM'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i][searchField] == searchValue:
                    del self.cfgData['G2_CONFIG']['CFG_EFBOM'][i]        

# -----------------------------

    def do_addElement(self,arg):

        '\n\taddElementToFeature {"element": "<element_name>"}' \
        '\n\n\taddElementToFeature {"element": "<element_name>", "tokenize": "no", "datatype": "no"}' \
        '\n\n\tFor additional example structures, use getFeature or listFeatures\n'

        if not argCheck('addElement', arg, self.do_addElement.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ELEMENT": arg}
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT']):
                printWithNewLines('Element %s already exists!' % parmData['ELEMENT'], 'B')
                return
            else:

                #--default for missing values
                if 'DATATYPE' not in parmData or len(parmData['DATATYPE'].strip()) == 0:
                    parmData['DATATYPE'] = 'string'
                else:
                    if parmData['DATATYPE'].upper() not in ('DATE', 'DATETIME', 'JSON', 'NUMBER', 'STRING'):
                        printWithNewLines('Invalid datatype value: %s  (must be "DATE", "DATETIME", "JSON", "NUMBER", or "STRING")' % parmData['DATATYPE'], 'B')
                        return
                    parmData['DATATYPE'] = parmData['DATATYPE'].lower()

                if 'TOKENIZE' not in parmData or len(parmData['TOKENIZE'].strip()) == 0:
                    parmData['TOKENIZE'] = 'No'
                else:
                    if parmData['TOKENIZE'] not in ('0', '1', 'No', 'Yes'):
                        printWithNewLines('Invalid tokenize value: %s  (must be "0", "1", "No", or "Yes")' % parmData['TOKENIZE'], 'B')
                        return

                maxID = []
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])) :
                    maxID.append(self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID'])

                if 'ID' in parmData: 
                    felemID = int(parmData['ID'])
                else:
                    felemID = max(maxID) + 1 if max(maxID) >=1000 else 1000

                newRecord = {}
                newRecord['FELEM_ID'] = felemID
                newRecord['FELEM_CODE'] = parmData['ELEMENT']
                newRecord['FELEM_DESC'] = parmData['ELEMENT']
                newRecord['TOKENIZE'] = parmData['TOKENIZE']
                newRecord['DATA_TYPE'] = parmData['DATATYPE']
                self.cfgData['G2_CONFIG']['CFG_FELEM'].append(newRecord)
                self.configUpdated = True
                printWithNewLines('Successfully added!', 'B')
                if self.doDebug:
                    showMeTheThings(newRecord)


    # -----------------------------                
    def do_addElementToFeature(self,arg):

        ####
        #deleteElement
        '\n\taddElementToFeature {"feature": "<feature_name>", "element": "<element_name>"}' \
        '\n\n\taddElementToFeature {"feature": "<feature_name>", "element": "<element_name>", "compared": "no", "expressed": "no"}' \
        '\n\n\tFor additional example structures, use getFeature or listFeatures\n'

        if not argCheck('addElementToFeature', arg, self.do_addElementToFeature.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 and 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0 :

                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s. Use listFeatures to see valid features.' % parmData['FEATURE'], 'B')
                    return
    
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])

            else:
                printWithNewLines('Both a feature and element must be specified!', 'B')
                return
            
            #--default for missing values

            if 'COMPARED' not in parmData or len(parmData['COMPARED'].strip()) == 0:
                parmData['COMPARED'] = 'No'
            else:
                if parmData['COMPARED'].upper() not in ('YES', 'NO'):
                    printWithNewLines('Invalid compared value: %s  (must be "Yes", or "No")' % parmData['COMPARED'], 'B')
                    return

            if 'EXPRESSED' not in parmData or len(parmData['EXPRESSED'].strip()) == 0:
                parmData['EXPRESSED'] = 'No'
            else:
                if parmData['EXPRESSED'].upper() not in ('YES', 'NO'):
                    printWithNewLines('Invalid expressed value: %s  (must be "Yes", or "No")' % parmData['EXPRESSED'], 'B')
                    return

            if 'DATATYPE' not in parmData or len(parmData['DATATYPE'].strip()) == 0:
                parmData['DATATYPE'] = 'string'
            else:
                if parmData['DATATYPE'].upper() not in ('DATE', 'DATETIME', 'JSON', 'NUMBER', 'STRING'):
                    printWithNewLines('Invalid datatype value: %s  (must be "DATE", "DATETIME", "JSON", "NUMBER", or "STRING")' % parmData['DATATYPE'], 'B')
                    return
                parmData['DATATYPE'] = parmData['DATATYPE'].lower()

            if 'TOKENIZE' not in parmData or len(parmData['TOKENIZE'].strip()) == 0:
                parmData['TOKENIZE'] = 'No'
            else:
                if parmData['TOKENIZE'] not in ('0', '1', 'No', 'Yes'):
                    printWithNewLines('Invalid tokenize value: %s  (must be "0", "1", "No", or "Yes")' % parmData['TOKENIZE'], 'B')
                    return

            if 'DERIVED' not in parmData:
                parmData['DERIVED'] = 'No'
            else:
                if parmData['DERIVED'] not in ('0', '1', 'No', 'Yes'):
                    printWithNewLines('Invalid derived value: %s  (must be "0", "1", "No", or "Yes")' % parmData['DERIVED'], 'B')
                    return

            if 'DISPLAY_DELIM' not in parmData:
                parmData['DISPLAY_DELIM'] = ''

            if 'DISPLAY_LEVEL' not in parmData:
                parmData['DISPLAY_LEVEL'] = 2 if ftypeRecord['FTYPE_CODE'] =='ADDRESS' else 1

            #--does the element exist already and has conflicting parms to what was requested? 
            if felemRecord:
                felemID = felemRecord['FELEM_ID']
                if ( 
                    ( parmData['DATATYPE'] and len(parmData['DATATYPE'].strip()) > 0 and parmData['DATATYPE'] != felemRecord['DATA_TYPE'] ) or
                    ( parmData['TOKENIZE'] and len(parmData['TOKENIZE'].strip()) > 0 and parmData['TOKENIZE'] != felemRecord['TOKENIZE'] )
                   ) :
                    printWithNewLines('Element %s already exists with conflicting parameters, check with listElement %s' % (parmData['ELEMENT'], parmData['ELEMENT']), 'B')
                    return
            else:
                #If no element already add it first
                if not felemRecord:
                    maxID = 0
                    for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])):
                        if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']) == int(parmData['ID']):
                            printWithNewLines('Element id %s already exists!' % parmData['ID'], 'B')
                            return
                        if self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID'] > maxID:
                            maxID = self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']
        
                    if 'ID' in parmData: 
                        felemID = int(parmData['ID'])
                    else:
                        felemID = maxID + 1 if maxID >=1000 else 1000
    
                    newRecord = {}
                    newRecord['FELEM_ID'] = felemID
                    newRecord['FELEM_CODE'] = parmData['ELEMENT']
                    newRecord['FELEM_DESC'] = parmData['ELEMENT']
                    newRecord['DATA_TYPE'] = parmData['DATATYPE']
                    newRecord['TOKENIZE'] = parmData['TOKENIZE']
                    self.cfgData['G2_CONFIG']['CFG_FELEM'].append(newRecord)
                    self.configUpdated = True
                    printWithNewLines('Successfully added the element!', 'B')
                    if self.doDebug:
                        showMeTheThings(newRecord)

            #--add the fbom, if it does not already exist
            alreadyExists = False
            maxExec = [0]
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FBOM'])):
                if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FTYPE_ID']) == ftypeRecord['FTYPE_ID']:
                    maxExec.append(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['EXEC_ORDER'])
                    if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FELEM_ID']) == felemID:
                        alreadyExists = True
                        break

            if alreadyExists:
                printWithNewLines('Element already exists for feature!', 'B')
            else:
                newRecord = {}
                newRecord['FTYPE_ID'] = ftypeRecord['FTYPE_ID']
                newRecord['FELEM_ID'] = felemID
                newRecord['EXEC_ORDER'] = max(maxExec) + 1
                newRecord['DISPLAY_DELIM'] = parmData['DISPLAY_DELIM']
                newRecord['DISPLAY_LEVEL'] = parmData['DISPLAY_LEVEL']
                newRecord['DERIVED'] = parmData['DERIVED']
                self.cfgData['G2_CONFIG']['CFG_FBOM'].append(newRecord)
                self.configUpdated = True
                printWithNewLines('Successfully added to feature!', 'B')
                if self.doDebug:
                    showMeTheThings(newRecord)

    # -----------------------------                
    def do_setFeatureElementDisplayLevel(self,arg):

        '\n\tsetFeatureElementDisplayLevel {"feature": "<feature_name>", "element": "<element_name>", "display_level": <display_level>}\n'

        if not argCheck('setFeatureElementDisplayLevel', arg, self.do_setFeatureElementDisplayLevel.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 and 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0 :

                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s. Use listFeatures to see valid features.' % parmData['FEATURE'], 'B')
                    return
    
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                if not felemRecord:
                    printWithNewLines('Invalid feature element: %s.' % parmData['ELEMENT'], 'B')
                    return

            else:
                printWithNewLines('Both a feature and element must be specified!', 'B')
                return
            
            
            if 'DISPLAY_LEVEL' in parmData :
                displayLevel = int(parmData['DISPLAY_LEVEL'])
            else:
                printWithNewLines('Display level must be specified!', 'B')
                return

            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FBOM'])):
                if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FTYPE_ID']) == ftypeRecord['FTYPE_ID']:
                    if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FELEM_ID']) == felemRecord['FELEM_ID']:
                        self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['DISPLAY_LEVEL'] = displayLevel
                        self.configUpdated = True
                        printWithNewLines('Feature element display level updated!', 'B')
                        if self.doDebug:
                            showMeTheThings(self.cfgData['G2_CONFIG']['CFG_FBOM'][i])

    # -----------------------------
    def do_deleteElementFromFeature(self,arg):
        '\n\tdeleteElementFromFeature {"feature": "<feature_name>", "element": "<element_name>"}'

        if not argCheck('deleteElementFromFeature', arg, self.do_deleteElementFromFeature.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:
            
            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 and 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0 :
                
                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s. Use listFeatures to see valid features.' % parmData['FEATURE'], 'B')
                    return

                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                if not felemRecord:
                    printWithNewLines('Invalid element: %s. Use listElements to see valid elements.' % parmData['ELEMENT'], 'B')
                    return

            else:
                printWithNewLines('Both a feature and element must be specified!', 'B')
                return

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FBOM'])-1,-1,-1):
                if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FTYPE_ID']) == ftypeRecord['FTYPE_ID'] and int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FELEM_ID']) == felemRecord['FELEM_ID'] :
                    del self.cfgData['G2_CONFIG']['CFG_FBOM'][i]
                    deleteCnt = 1
                    self.configUpdated = True

            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')


    # -----------------------------
    def do_deleteElement(self,arg):
        '\n\tdeleteElement {"feature": "<feature_name>", "element": "<element_name>"}'

        if not argCheck('deleteElement', arg, self.do_deleteElement.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ELEMENT": arg} 
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Invalid element: %s. Use listElements to see valid elements.' % parmData['ELEMENT'], 'B')
                return

            usedIn = []
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FBOM'])):
                if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FELEM_ID']) == felemRecord['FELEM_ID'] :
                    for j in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE'])):
                        if int(self.cfgData['G2_CONFIG']['CFG_FTYPE'][j]['FTYPE_ID']) == self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FTYPE_ID'] :
                            usedIn.append(self.cfgData['G2_CONFIG']['CFG_FTYPE'][j]['FTYPE_CODE'])
            if usedIn:
                printWithNewLines('Can\'t delete %s, it is used in these feature(s): %s' % (parmData['ELEMENT'], usedIn) ,'B')
            else:
                deleteCnt = 0
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])):
                    if int(self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']) == felemRecord['FELEM_ID'] :
                        del self.cfgData['G2_CONFIG']['CFG_FELEM'][i]
                        deleteCnt = 1
                        self.configUpdated = True

                if deleteCnt == 0:
                    printWithNewLines('Record not found!', 'B')
                else:
                    printWithNewLines('%s rows deleted!' % deleteCnt, 'B')
                    

    def do_listExpressionCalls(self,arg):
        '\nVerifies expression call configurations' 
        efcallList = []
        for efcallRecord in sorted(self.cfgData['G2_CONFIG']['CFG_EFCALL'], key = lambda k: (k['FTYPE_ID'], k['EXEC_ORDER'])):
            efuncRecord = self.getRecord('CFG_EFUNC', 'EFUNC_ID', efcallRecord['EFUNC_ID'])
            ftypeRecord1 = self.getRecord('CFG_FTYPE', 'FTYPE_ID', efcallRecord['FTYPE_ID'])
            ftypeRecord2 = self.getRecord('CFG_FTYPE', 'FTYPE_ID', efcallRecord['EFEAT_FTYPE_ID'])
            felemRecord2 = self.getRecord('CFG_FELEM', 'FELEM_ID', efcallRecord['FELEM_ID'])

            efcallDict = {}
            efcallDict['id'] = efcallRecord['EFCALL_ID']
            if ftypeRecord1:
                efcallDict['feature'] = ftypeRecord1['FTYPE_CODE']
            if felemRecord2:
                efcallDict['element'] = felemRecord2['FELEM_CODE']
            efcallDict['execOrder'] = efcallRecord['EXEC_ORDER']
            efcallDict['function'] = efuncRecord['EFUNC_CODE']
            efcallDict['is_virtual'] = efcallRecord['IS_VIRTUAL']
            if ftypeRecord2:
                efcallDict['new_feature'] = ftypeRecord2['FTYPE_CODE']
  
            efbomList = []
            for efbomRecord in [record for record in self.cfgData['G2_CONFIG']['CFG_EFBOM'] if record['EFCALL_ID'] == efcallRecord['EFCALL_ID']]:
                ftypeRecord3 = self.getRecord('CFG_FTYPE', 'FTYPE_ID', efbomRecord['FTYPE_ID'])
                felemRecord3 = self.getRecord('CFG_FELEM', 'FELEM_ID', efbomRecord['FELEM_ID'])

                if efbomRecord['FTYPE_ID'] == 0:
                    fromFeature = 'parent'
                elif efbomRecord['FTYPE_ID'] == -1:
                    fromFeature = '*'
                elif ftypeRecord3: 
                    fromFeature = ftypeRecord3['FTYPE_CODE']
                else:
                    fromFeature = '!error!'

                efbomDict = {}
                efbomDict['feature'] = fromFeature
                efbomDict['element'] = felemRecord3['FELEM_CODE'] if felemRecord3 else str(efbomRecord['FELEM_ID'])
                efbomDict['required'] = efbomRecord['FELEM_REQ']
                efbomList.append(efbomDict)
            efcallDict['elementList'] = efbomList

            efcallList.append(efcallDict)


        for efcallDict in efcallList:
            print(json.dumps(efcallDict))

# ===== misc commands =====
    def do_setDistinct(self,arg):
        '\nDistinct processing only compares the most complete feature values for an entity. You may want to turn this off for watch list checking.' \
        '\n\nSyntax:' \
        '\n\tsetDistinct on ' \
        '\n\tsetDistinct off ' \

        if not arg:
            printWithNewLines('Distinct is currently %s' % ('ON' if len(self.cfgData['G2_CONFIG']['CFG_DFCALL']) != 0 else 'OFF'), 'B')
            return

        if arg.upper() not in ('ON', 'OFF'):
            printWithNewLines('invalid distinct setting %s' % arg, 'B')
            return

        newSetting = arg.upper()

        if len(self.cfgData['G2_CONFIG']['CFG_DFCALL']) == 0 and newSetting == 'OFF':
            printWithNewLines('distinct is already off!', 'B')
            return

        if len(self.cfgData['G2_CONFIG']['CFG_DFCALL']) != 0 and newSetting == 'ON':
            printWithNewLines('distinct is already on!', 'B')
            return

        if newSetting == 'OFF':
            self.cfgData['G2_CONFIG']['XXX_DFCALL'] = self.cfgData['G2_CONFIG']['CFG_DFCALL']
            self.cfgData['G2_CONFIG']['XXX_DFBOM'] = self.cfgData['G2_CONFIG']['CFG_DFBOM']
            self.cfgData['G2_CONFIG']['CFG_DFCALL'] = []
            self.cfgData['G2_CONFIG']['CFG_DFBOM'] = []
        else:
            if 'XXX_DFCALL' not in self.cfgData['G2_CONFIG']:
                printWithNewLines('distinct settings cannot be restored, backup could not be found!', 'B')
                return

            self.cfgData['G2_CONFIG']['CFG_DFCALL'] = self.cfgData['G2_CONFIG']['XXX_DFCALL']
            self.cfgData['G2_CONFIG']['CFG_DFBOM'] = self.cfgData['G2_CONFIG']['XXX_DFBOM']
            del(self.cfgData['G2_CONFIG']['XXX_DFCALL'])
            del(self.cfgData['G2_CONFIG']['XXX_DFBOM'])

        printWithNewLines('distinct is now %s!' % newSetting, 'B')

        self.configUpdated = True

        return


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

# ===== fragment commands =====

    # -----------------------------
    def getFragmentJson(self, thisRecord):
        jsonString = '{'
        jsonString += '"id": "%s"' % thisRecord['ERFRAG_ID']
        jsonString += ', "fragment": "%s"' % thisRecord['ERFRAG_CODE']
        jsonString += ', "source": "%s"' % thisRecord['ERFRAG_SOURCE']
        jsonString += ', "depends": "%s"' % thisRecord['ERFRAG_DEPENDS']
        jsonString += '}'
        return jsonString

    # -----------------------------
    def do_listFragments(self,arg):
        '\n\tlistFragments\n'

        print('')
        for thisRecord in sorted(self.getRecordList('CFG_ERFRAG'), key = lambda k: k['ERFRAG_ID']):
            print(self.getFragmentJson(thisRecord))
        print('')

    # -----------------------------
    def do_getFragment(self,arg):
        '\n\tgetFragment {"id": "<fragment_id>"}' \
        '\n\tgetFragment {"fragment": "<fragment_code>"}'

        if not argCheck('getFragment', arg, self.do_getFragment.__doc__):
            return

        try:
            if arg.startswith('{'):
                parmData = dictKeysUpper(json.loads(arg)) 
            elif arg.isdigit():
                parmData = {"ID": arg}
            else:
                parmData = {"FRAGMENT": arg}
            if 'FRAGMENT' in parmData and len(parmData['FRAGMENT'].strip()) != 0:
                searchField = 'ERFRAG_CODE'
                searchValue = parmData['FRAGMENT'].upper()
            elif 'ID' in parmData and len(parmData['ID'].strip()) != 0:
                searchField = 'ERFRAG_ID'
                searchValue = int(parmData['ID'])
            else:
                raise ValueError(arg)
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            foundRecords = self.getRecordList('CFG_ERFRAG', searchField, searchValue)
            if not foundRecords:
                printWithNewLines('Record not found!', 'B')
            else:
                print('')
                for thisRecord in sorted(foundRecords, key = lambda k: k['ERFRAG_ID']):
                    print(self.getFragmentJson(thisRecord))
                print('')

    # -----------------------------
    def do_deleteFragment(self,arg):
        '\n\tdeleteFragment {"id": "<fragment_id>"}' \
        '\n\tdeleteFragment {"fragment": "<fragment_code>"}'

        if not argCheck('deleteFragment', arg, self.do_getFragment.__doc__):
            return

        try:
            if arg.startswith('{'):
                parmData = dictKeysUpper(json.loads(arg)) 
            elif arg.isdigit():
                parmData = {"ID": arg}
            else:
                parmData = {"FRAGMENT": arg}
            if 'FRAGMENT' in parmData and len(parmData['FRAGMENT'].strip()) != 0:
                searchField = 'ERFRAG_CODE'
                searchValue = parmData['FRAGMENT'].upper()
            elif 'ID' in parmData and len(parmData['ID'].strip()) != 0:
                searchField = 'ERFRAG_ID'
                searchValue = int(parmData['ID'])
            else:
                raise ValueError(arg)
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERFRAG'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i][searchField] == searchValue:
                    del self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]        
                    deleteCnt += 1
                    self.configUpdated = True
            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            printWithNewLines('%s rows deleted!' % deleteCnt, 'B')
        
    # -----------------------------
    def do_addFragment(self,arg):
        '\n\taddFragment {"id": "<fragment_id>", "fragment": "<fragment_code>", "source": "<fragment_source>"}' \
        '\n\n\tFor additional example structures, use getFragment or listFragments\n'

        if not argCheck('addFragment', arg, self.do_addFragment.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FRAGMENT'] = parmData['FRAGMENT'].upper()
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERFRAG'])):
                if self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_CODE'] == parmData['FRAGMENT']:
                    printWithNewLines('Fragment %s already exists!' % parmData['FRAGMENT'], 'B')
                    return
                if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_ID']) == int(parmData['ID']):
                    printWithNewLines('Fragment ID %s already exists!' % parmData['ID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_ID']

            if 'ID' not in parmData: 
                parmData['ID'] = maxID + 1 if maxID >= 1000 else 1000

            #--must have a source field
            if 'SOURCE' not in parmData: 
                printWithNewLines( 'A fragment source field is required!', 'B')
                return

            #--compute dependencies from source
            #--example: './FRAGMENT[./SAME_NAME>0 and ./SAME_STAB>0] or ./FRAGMENT[./SAME_NAME1>0 and ./SAME_STAB1>0]'
            dependencyList = []
            sourceString = parmData['SOURCE']
            startPos = sourceString.find('FRAGMENT[')
            while startPos > 0:
                fragmentString = sourceString[startPos:sourceString.find(']', startPos) + 1]
                sourceString = sourceString.replace(fragmentString, '')
                #--parse the fragment string
                currentFrag = 'eof'
                fragmentChars = list(fragmentString)
                potentialErrorString = ''
                for thisChar in fragmentChars:
                    potentialErrorString += thisChar
                    if thisChar == '/':
                        currentFrag = ''
                    elif currentFrag != 'eof':
                        if thisChar in ' =><)':
                            #--lookup the fragment code
                            fragRecord = self.getRecord('CFG_ERFRAG', 'ERFRAG_CODE', currentFrag)
                            if not fragRecord:
                                printWithNewLines('Invalid fragment reference: %s' % currentFrag, 'B')
                                return
                            else:
                                dependencyList.append(str(fragRecord['ERFRAG_ID']))
                            currentFrag = 'eof'
                        else:
                            currentFrag += thisChar
                #--next list of fragments
                startPos = sourceString.find('FRAGMENT[')

            newRecord = {}
            newRecord['ERFRAG_ID'] = int(parmData['ID'])
            newRecord['ERFRAG_CODE'] = parmData['FRAGMENT']
            newRecord['ERFRAG_DESC'] = parmData['FRAGMENT']
            newRecord['ERFRAG_SOURCE'] = parmData['SOURCE']
            newRecord['ERFRAG_DEPENDS'] = ','.join(dependencyList)
            self.cfgData['G2_CONFIG']['CFG_ERFRAG'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)

# ===== rule commands =====

    # -----------------------------
    def getRuleJson(self, thisRecord):
        jsonString = '{'
        jsonString += '"id": %s' % thisRecord['ERRULE_ID']
        jsonString += ', "rule": "%s"' % thisRecord['ERRULE_CODE']
        jsonString += ', "tier": %s' % showNullableJsonNumeric(thisRecord['ERRULE_TIER'])
        jsonString += ', "resolve": "%s"' % thisRecord['RESOLVE']
        jsonString += ', "relate": "%s"' % ('Yes' if thisRecord['RELATE'] == 1 else "No")
        jsonString += ', "ref_score": %s' % thisRecord['REF_SCORE']
        jsonString += ', "fragment": "%s"' % thisRecord['QUAL_ERFRAG_CODE']
        jsonString += ', "disqualifier": %s' % showNullableJsonString(thisRecord['DISQ_ERFRAG_CODE'])
        jsonString += ', "rtype_id": %s' % showNullableJsonNumeric(thisRecord['RTYPE_ID'])
        jsonString += '}'
        return jsonString

    # -----------------------------
    def do_listRules(self,arg):
        '\n\tlistRules\n'

        print('')
        for thisRecord in sorted(self.getRecordList('CFG_ERRULE'), key = lambda k: k['ERRULE_ID']):
            print(self.getRuleJson(thisRecord))
        print('')

    # -----------------------------
    def do_getRule(self,arg):
        '\n\tgetRule {"id": "<rule_id>"}' 

        if not argCheck('getRule', arg, self.do_getRule.__doc__):
            return

        try:
            if arg.startswith('{'):
                parmData = dictKeysUpper(json.loads(arg)) 
            elif arg.isdigit():
                parmData = {"ID": arg}
            else:
                parmData = {"RULE": arg}
            if 'RULE' in parmData and len(parmData['RULE'].strip()) != 0:
                searchField = 'ERRULE_CODE'
                searchValue = parmData['RULE'].upper()
            elif 'ID' in parmData and len(parmData['ID'].strip()) != 0:
                searchField = 'ERRULE_ID'
                searchValue = int(parmData['ID'])
            else:
                raise ValueError(arg)
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            foundRecords = self.getRecordList('CFG_ERRULE', searchField, searchValue)
            if not foundRecords:
                printWithNewLines('Record not found!', 'B')
            else:
                print('')
                for thisRecord in sorted(foundRecords, key = lambda k: k['ERRULE_ID']):
                    print(self.getRuleJson(thisRecord))
                print('')

    # -----------------------------
    def do_deleteRule(self,arg):
        '\n\tdeleteRule {"id": "<rule_id>"}'

        if not argCheck('deleteRule', arg, self.do_getRule.__doc__):
            return

        try:
            if arg.startswith('{'):
                parmData = dictKeysUpper(json.loads(arg)) 
            elif arg.isdigit():
                parmData = {"ID": arg}
            else:
                parmData = {"RULE": arg}
            if 'RULE' in parmData and len(parmData['RULE'].strip()) != 0:
                searchField = 'ERRULE_CODE'
                searchValue = parmData['FRAGMENT'].upper()
            elif 'ID' in parmData and int(parmData['ID']) != 0:
                searchField = 'ERRULE_ID'
                searchValue = int(parmData['ID'])
            else:
                raise ValueError(arg)
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERRULE'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_ERRULE'][i][searchField] == searchValue:
                    del self.cfgData['G2_CONFIG']['CFG_ERRULE'][i]        
                    deleteCnt += 1
                    self.configUpdated = True
            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    # -----------------------------
    def do_addRule(self,arg):
        '\n\taddRule {"id": 130, "rule": "SF1_CNAME", "tier": 30, "resolve": "Yes", "relate": "No", "ref_score": 8, "fragment": "SF1_CNAME", "disqualifier": "DIFF_EXCL", "rtype_id": 1}' \
        '\n\n\tFor additional example structures, use getRule or listRules\n'

        if not argCheck('addRule', arg, self.do_addRule.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['ID'] = int(parmData['ID'])
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERRULE'])):
                if self.cfgData['G2_CONFIG']['CFG_ERRULE'][i]['ERRULE_CODE'] == parmData['RULE']:
                    printWithNewLines('Rule %s already exists!' % parmData['FRAGMENT'], 'B')
                    return
                if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_ERRULE'][i]['ERRULE_ID']) == int(parmData['ID']):
                    printWithNewLines('Rule ID %s already exists!' % parmData['ID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_ERRULE'][i]['ERRULE_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_ERRULE'][i]['ERRULE_ID']

            if 'ID' not in parmData: 
                parmData['ID'] = maxID + 1 if maxID >= 1000 else 1000


            #--must have a valid fragment field
            if 'FRAGMENT' not in parmData: 
                printWithNewLines( 'A fragment source field is required!', 'B')
                return
            else:
                #--lookup the fragment code
                fragRecord = self.getRecord('CFG_ERFRAG', 'ERFRAG_CODE', parmData['FRAGMENT'])
                if not fragRecord:
                    printWithNewLines('Invalid fragment reference: %s' % parmData['FRAGMENT'], 'B')
                    return

            #--if no rule code, replace with fragment
            if 'CODE' not in parmData:
                parmData['CODE'] = parmData['FRAGMENT']

            #--default or validate the disqualifier
            if 'DISQUALIFIER' not in parmData or not parmData['DISQUALIFIER']: 
                parmData['DISQUALIFIER'] = None
            else:
                #--lookup the disqualifier code
                fragRecord = self.getRecord('CFG_ERFRAG', 'ERFRAG_CODE', parmData['DISQUALIFIER'])
                if not fragRecord:
                    printWithNewLines('Invalid disqualifer reference: %s' % parmData['DISQUALIFIER'], 'B')
                    return

            newRecord = {}
            newRecord['ERRULE_ID'] = int(parmData['ID'])
            newRecord['ERRULE_CODE'] = parmData['RULE']
            newRecord['ERRULE_DESC'] = parmData['RULE']
            newRecord['RESOLVE'] = parmData['RESOLVE']
            newRecord['RELATE'] = parmData['RELATE']
            newRecord['REF_SCORE'] = int(parmData['REF_SCORE'])
            newRecord['RTYPE_ID'] = int(parmData['RTYPE_ID'])
            newRecord['QUAL_ERFRAG_CODE'] = parmData['FRAGMENT']
            newRecord['DISQ_ERFRAG_CODE'] = storeNullableJsonString(parmData['DISQUALIFIER'])
            newRecord['ERRULE_TIER'] = storeNullableJsonNumeric(parmData['TIER'])


            self.cfgData['G2_CONFIG']['CFG_ERRULE'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                showMeTheThings(newRecord)

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
            
    # -----------------------------
    def do_touch(self,arg):
        '\n\touch' 

        # this is a no-op.  It just marks the database as modified, without doing anything to it.
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
                #--return
                
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
                #--return
                
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
                #--return

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
                #--return

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
                #--return

            insertSql = 'insert into CFG_FELEM (FELEM_ID, FELEM_CODE, TOKENIZE, DATA_TYPE) values (?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_FELEM'), key = lambda k: k['FELEM_ID']):
                record = []
                record.append(jsonRecord['FELEM_ID'])
                record.append(jsonRecord['FELEM_CODE'])
                record.append(jsonRecord['TOKENIZE'])
                record.append(jsonRecord['DATA_TYPE'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_FELEM')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                #--return
                
            insertSql = 'insert into CFG_SFCALL (SFCALL_ID, SFUNC_ID, EXEC_ORDER, FTYPE_ID, FELEM_ID) values (?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_SFCALL'), key = lambda k: k['SFCALL_ID']):
                record = []
                record.append(jsonRecord['SFCALL_ID'])
                record.append(jsonRecord['SFUNC_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['FTYPE_ID'])
                record.append(jsonRecord['FELEM_ID'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_SFCALL')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                #--return

            insertSql = 'insert into CFG_EFCALL (EFCALL_ID, EFUNC_ID, EXEC_ORDER, FTYPE_ID, FELEM_ID, EFEAT_FTYPE_ID, IS_VIRTUAL) values (?, ?, ?, ?, ?, ?, ?)'
            insertRecords = []
            for jsonRecord in sorted(self.getRecordList('CFG_EFCALL'), key = lambda k: k['EFCALL_ID']):
                record = []
                record.append(jsonRecord['EFCALL_ID'])
                record.append(jsonRecord['EFUNC_ID'])
                record.append(jsonRecord['EXEC_ORDER'])
                record.append(jsonRecord['FTYPE_ID'])
                record.append(jsonRecord['FELEM_ID'])
                record.append(jsonRecord['EFEAT_FTYPE_ID'])
                record.append(jsonRecord['IS_VIRTUAL'])
                insertRecords.append(record)
            try: 
                g2Dbo.sqlExec('delete from CFG_EFCALL')
                g2Dbo.execMany(insertSql, insertRecords)
            except G2Exception.G2DBException as err:
                printWithNewLines('Database error, database hasn\'t been updated: %s ' % err, 'B')
                #--g2Dbo.sqlExec('rollback')
                #--return

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
                #--return
                
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
                #--return

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
                #--return

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
                    #--return

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
                #--return

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
                #--return
                
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
                #--return
                
            #--g2Dbo.sqlExec('commit')
            printWithNewLines('Database updated!', 'B')

        
# ===== Utility functions =====

def getFeatureBehavior(feature):

    featureBehavior = feature['FTYPE_FREQ']
    if str(feature['FTYPE_EXCL']).upper() in ('1', 'Y', 'YES'):
        featureBehavior += 'E'
    if str(feature['FTYPE_STAB']).upper() in ('1', 'Y', 'YES'):
        featureBehavior += 'S'
    return featureBehavior

def parseFeatureBehavior(behaviorCode):
    behaviorDict = {"EXCLUSIVITY": 'No', "STABILITY": 'No'}
    if 'E' in behaviorCode:
        behaviorDict['EXCLUSIVITY'] = 'Yes'
        behaviorCode = behaviorCode.replace('E','')
    if 'S' in behaviorCode:
        behaviorDict['STABILITY'] = 'Yes'
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

def showNullableJsonString(val):
    if not val:
        return 'null'
    else:
        return '"%s"' % val

def showNullableJsonNumeric(val):
    if not val:
        return 'null'
    else:
        return '%s' % val

def storeNullableJsonString(val):
    if not val or val == 'null':
        return None
    else:
        return val

def storeNullableJsonNumeric(val):
    if not val or val == 'null':
        return None
    else:
        return val

def showMeTheThings(data, loc=''):
    printWithNewLines('<---- DEBUG')
    printWithNewLines('Func: %s' % sys._getframe(1).f_code.co_name)
    if loc != '': printWithNewLines('Where: %s' % loc) 
    printWithNewLines('Data: %s' % str(data))
    printWithNewLines('---->', 'E')

# ===== The main function =====
if __name__ == '__main__':

    #--capture the command line arguments
    fileToProcess = None
    argParser = argparse.ArgumentParser()
    argParser.add_argument("fileToProcess", nargs='?')
    argParser.add_argument('-c', '--ini-file-name', dest='ini_file_name', default=None, help='name of the g2.ini file')
    argParser.add_argument('-f', '--force', dest='forceMode', default=False, action='store_true', help='when reading from a file, execute each command without prompts')
    args = argParser.parse_args()
    if args.fileToProcess:
        fileToProcess = args.fileToProcess
    iniFileName = args.ini_file_name
    forceMode = args.forceMode
    
    if not iniFileName:    
        iniFileName = G2Paths.get_G2Module_ini_path()

    if not os.path.exists(iniFileName):
        printWithNewLines('ERROR: %s not found' % iniFileName, 'B')
        sys.exit(1)

    g2health = G2Health()
    g2health.checkIniParams(iniFileName)

    #--get parameters from ini file
    iniParser = configparser.ConfigParser()
    iniParser.read(iniFileName)
    try: g2dbUri = iniParser.get('SQL', 'CONNECTION')
    except: 
        print('')
        print('CONNECTION parameter not found in [SQL] section of the ini file')
        print('')
        sys.exit(1)

    #--see if there is database support
    try: g2dbUri = iniParser.get('SQL', 'CONNECTION')
    except: 
        g2dbUri = None
    else:
        try: g2Dbo = G2Database(g2dbUri)
        except:
            g2dbUri = None

    #--python3 uses input, raw_input was removed
    userInput = input
    if sys.version_info[:2] <= (2,7):
        userInput = raw_input

    #--execute a file of commands or cmdloop()
    if fileToProcess:
        G2CmdShell().fileloop(fileToProcess)
    else:
        G2CmdShell().cmdloop()

    sys.exit()

