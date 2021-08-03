#! /usr/bin/env python3

import argparse
import cmd
import glob
import json
import os
import pathlib
import platform
import re
import shlex
import sys
import textwrap
import traceback
from collections import OrderedDict
from contextlib import suppress
from datetime import datetime
from shutil import copyfile

import G2Paths

try:
    from G2IniParams import G2IniParams
    from G2Engine import G2Engine
    from G2Health import G2Health
    from G2ConfigMgr import G2ConfigMgr
    from G2Config import G2Config
    import G2Exception
except Exception:
    pass

try:
    import readline
    import atexit
except ImportError:
    readline = None

try:
    from pygments import highlight, lexers, formatters
except ImportError:
    pass


class G2CmdShell(cmd.Cmd, object):

    #Override function from cmd module to make command completion case insensitive
    def completenames(self, text, *ignored):
        dotext = 'do_' + text
        return [a[3:] for a in self.get_names() if a.lower().startswith(dotext.lower())]

    def __init__(self, g2_cfg_file, hist_disable, force_mode, file_to_process, ini_file):
        cmd.Cmd.__init__(self)

        # Cmd Module settings
        self.intro = ''
        self.prompt = '(g2cfg) '
        self.ruler = '-'
        self.doc_header = 'Configuration Commands'
        self.misc_header = 'Help Topics (help <topic>)'
        self.undoc_header = 'Misc Commands'
        self.__hidden_methods = ('do_shell', 'do_EOF', 'do_help')

        self.g2_module = G2Engine()
        self.g2_configmgr = G2ConfigMgr()
        self.g2_config = G2Config()

        # Set flag to know if running an interactive command shell or reading from file
        self.isInteractive = True

        # Windows command history
        if platform.system() == 'Windows':
            self.use_rawinput = False

        # Config variables and setup
        self.g2ConfigFileUsed = False
        self.configUpdated = False
        self.g2configFile = g2_cfg_file
        self.ini_file_name = ini_file

        # Processing input file
        self.forceMode = force_mode
        self.fileToProcess = file_to_process

        self.attributeClassList = ('NAME', 'ATTRIBUTE', 'IDENTIFIER', 'ADDRESS', 'PHONE', 'RELATIONSHIP', 'OTHER')
        self.lockedFeatureList = ('NAME', 'ADDRESS', 'PHONE', 'DOB', 'REL_LINK', 'REL_ANCHOR', 'REL_POINTER')

        self.doDebug = False

        # Setup for pretty printing
        self.prettyPrint = True
        self.pygmentsInstalled = True if 'pygments' in sys.modules else False

        # Readline and history
        self.readlineAvail = True if 'readline' in sys.modules else False
        self.histDisable = hist_disable
        self.histCheck()

        self.parser = argparse.ArgumentParser(prog='', add_help=False)
        self.subparsers = self.parser.add_subparsers()

        getConfig_parser = self.subparsers.add_parser('getConfig', usage=argparse.SUPPRESS)
        getConfig_parser.add_argument('configID', type=int)

    def getConfig(self):
        ''' Get configutation from database or set default one if not found '''

        # Older Senzing versions can use G2CONFIGFILE in G2Module.ini - deprecated
        if self.g2configFile:

            # Get the current configuration from a config file.
            self.g2ConfigFileUsed = True

            try:
                self.cfgData = json.load(open(self.g2configFile), encoding="utf-8")
            except ValueError as e:
                print(f'\nERROR: {self.g2configFile} doesn\'t appear to be valid JSON!')
                print(f'       {e}\n')
                sys.exit(1)

        else:

            # Get the current configuration from the Senzing database
            defaultConfigID = bytearray()
            self.g2_configmgr.getDefaultConfigID(defaultConfigID)

            # If a default config isn't found, create a new default configuration
            if not defaultConfigID:

                print('\nWARNING: No default config stored in the database, see: https://senzing.zendesk.com/hc/en-us/articles/360036587313')
                print('\nINFO: Adding a new default configuration to the database...')

                config_handle = self.g2_config.create()

                config_default = bytearray()
                self.g2_config.save(config_handle, config_default)
                config_string = config_default.decode()

                # Persist new default config to Senzing Repository
                try:
                    addconfig_id = bytearray()
                    self.g2ConfigMgr.addConfig(config_string, 'New default configuration added by G2ConfigTool.', addconfig_id)
                    self.g2ConfigMgr.setDefaultConfigID(addconfig_id)
                except G2Exception.G2ModuleGenericException:
                    raise

                self.destroyEngines()
                sys.exit(1)
            else:
                config_current = bytearray()
                self.g2_configmgr.getConfig(defaultConfigID, config_current)
                config_string = config_current.decode()

            self.cfgData = json.loads(config_string)

            self.configUpdated = False

    def do_quit(self, arg):

        if self.configUpdated and input('\nThere are unsaved changes, would you like to save first? (y/n)  ') in ['y', 'Y', 'yes', 'YES']:
            self.do_save(self)

        return True

    def do_exit(self, arg):
        self.do_quit(self)

        return True

    def do_EOF(self, line):
        return True

    def emptyline(self):
        return

    def default(self, line):
        printWithNewLines('ERROR: Unknown command, type help to list available commands.', 'B')
        return

    def cmdloop(self):

        while True:
            try:
                cmd.Cmd.cmdloop(self)
                break
            except KeyboardInterrupt:
                if self.configUpdated:
                    if input('\n\nThere are unsaved changes, would you like to save first? (y/n)  ') in ['y', 'Y', 'yes', 'YES']:
                        self.do_save(self)
                        break

                if input('\nAre you sure you want to exit? (y/n)  ') in ['y', 'Y', 'yes', 'YES']:
                    break
                else:
                    print()

            except TypeError as ex:
                printWithNewLines("ERROR: " + str(ex))
                type_, value_, traceback_ = sys.exc_info()
                for item in traceback.format_tb(traceback_):
                    printWithNewLines(item)

    def initEngines(self, init_msg=True):

        if init_msg:
            printWithNewLines('Initializing Senzing engines...', 'B')

        try:
            self.g2_module.initV2('pyG2E', g2module_params, False)
            self.g2_configmgr.initV2('pyG2ConfigMgr', g2module_params, False)
            self.g2_config.initV2('pyG2Config', g2module_params, False)
        except G2Exception.G2Exception as ex:
            printWithNewLines(f'ERROR: {ex}', 'B')
            # Clean up before exiting
            self.destroyEngines()
            sys.exit(1)

        # Re-read config after a save
        if self.configUpdated:
            self.getConfig()

    def destroyEngines(self):

        with suppress(Exception):
            self.g2_module.destroy()
            self.g2_configmgr.destroy()
            self.g2_config.destroy()

    def preloop(self):

        self.initEngines()
        self.getConfig()

        printWithNewLines('Welcome to G2Config Tool. Type help or ? to list commands.', 'E')

    def postloop(self):

        self.destroyEngines()

    def do_prettyPrint(self, arg):

        if self.prettyPrint:
            self.prettyPrint = False
            printWithNewLines('JSON pretty print is now off', 'B')
        else:
            self.prettyPrint = True
            printWithNewLines('JSON pretty print is now on', 'B')

    # Hide functions from available list of Commands. Seperate help sections for some
    def get_names(self):
        return [n for n in dir(self.__class__) if n not in self.__hidden_methods]

    def help_KnowledgeCenter(self):
        printWithNewLines(textwrap.dedent('''\
            - Senzing Knowledge Center: https://senzing.zendesk.com/hc/en-us
            '''), 'S')

    def help_Support(self):
        printWithNewLines(textwrap.dedent('''\
            - Senzing Support Request: https://senzing.zendesk.com/hc/en-us/requests/new
            '''), 'S')

    def help_Arguments(self):
        printWithNewLines(textwrap.dedent('''\
            - Argument values to specify are surrounded with < >, replace with your value

            - Example:

                addAttribute {"attribute": "<attribute_name>"}

                addAttribute {"attribute": "myNewAttribute"}
            '''), 'S')

    def help_Shell(self):
        printWithNewLines(textwrap.dedent('''\
            - Run basic OS shell commands: ! <command>
            '''), 'S')

    def help_History(self):
        printWithNewLines(textwrap.dedent(f'''\
            - Use shell like history, requires Python readline module.

            - Tries to create a history file in the users home directory for use across instances of G2ConfigTool.

            - If a history file can't be created in the users home, /tmp is tried for temporary session history.

            - Ctrl-r can be used to search history when history is available

            - Commands to manage history

                - histClear = Clears the current working session history and the history file. This deletes all history, be careful!
                - histDedupe = The history can accumulate duplicate entries over time, use this to remove them
                - histShow = Display all history

            - History Status:
                - Readline available: {self.readlineAvail}
                - History available: {self.histAvail}
                - History file: {self.histFileName}
                - History file error: {self.histFileError}
            '''), 'S')

    def help_PrettyPrint(self):
        printWithNewLines(textwrap.dedent(f'''\
            - Toggles pretty printing on/off for JSON output

            - Pretty print enabled: {self.prettyPrint}
            '''), 'S')

    def help_PrettyPrintColor(self):
        printWithNewLines(textwrap.dedent(f'''\
            - To colorize pretty printed JSON output install python Pygments if not installed: https://pypi.org/project/Pygments/

            - Python Pygments installed: {self.pygmentsInstalled}
            '''), 'S')

    def do_shell(self, line):
        '\nRun OS shell commands: !<command>\n'

        output = os.popen(line).read()
        print(output)

    def histCheck(self):
        '''  '''

        self.histFileName = None
        self.histFileError = None
        self.histAvail = False

        if not self.histDisable:

            if readline:
                tmpHist = '.' + os.path.basename(sys.argv[0].lower().replace('.py','_history'))
                self.histFileName = os.path.join(os.path.expanduser('~'), tmpHist)

                #Try and open history in users home first for longevity
                try:
                    open(self.histFileName, 'a').close()
                except IOError as e:
                    self.histFileError = f'{e} - Couldn\'t use home, trying /tmp/...'

                #Can't use users home, try using /tmp/ for history useful at least in the session
                if self.histFileError:

                    self.histFileName = f'/tmp/{tmpHist}'
                    try:
                        open(self.histFileName, 'a').close()
                    except IOError as e:
                        self.histFileError = f'{e} - User home dir and /tmp/ failed!'
                        return

                hist_size = 2000
                readline.read_history_file(self.histFileName)
                readline.set_history_length(hist_size)
                atexit.register(readline.set_history_length, hist_size)
                atexit.register(readline.write_history_file, self.histFileName)

                self.histFileName = self.histFileName
                self.histFileError = None
                self.histAvail = True

    def do_histDedupe(self, arg):

        if self.histAvail:
            if input('\nThis will de-duplicate both this session history and the history file, are you sure? (y/n)  ') in ['y', 'Y', 'yes', 'YES']:

                with open(self.histFileName) as hf:
                    linesIn = (line.rstrip() for line in hf)
                    uniqLines = OrderedDict.fromkeys(line for line in linesIn if line)

                    readline.clear_history()
                    for ul in uniqLines:
                        readline.add_history(ul)

                printWithNewLines('Session history and history file both deduplicated.', 'B')
            else:
                print()
        else:
            printWithNewLines('History isn\'t available in this session.', 'B')

    def do_histClear(self, arg):

        if self.histAvail:
            if input('\nThis will clear both this session history and the history file, are you sure? (y/n)  ') in ['y', 'Y', 'yes', 'YES']:
                readline.clear_history()
                readline.write_history_file(self.histFileName)
                printWithNewLines('Session history and history file both cleared.', 'B')
            else:
                print()
        else:
            printWithNewLines('History isn\'t available in this session.', 'B')

    def do_histShow(self, arg):

        if self.histAvail:
            print()
            for i in range(readline.get_current_history_length()):
                printWithNewLines(readline.get_history_item(i + 1))
            print()
        else:
            printWithNewLines('History isn\'t available in this session.', 'B')

    def fileloop(self):

        # Get initial config
        self.initEngines(init_msg=False)
        self.getConfig()

        # Set flag to know running an interactive command shell or not
        self.isInteractive = False

        save_detected = False

        with open(self.fileToProcess) as data_in:
            for line in data_in:
                line = line.strip()
                if len(line) > 0 and line[0:1] not in ('#', '-', '/'):
                    #*args allows for empty list if there are no args
                    (read_cmd, *args) = line.split()
                    process_cmd = f'do_{read_cmd}'
                    printWithNewLines(f'----- {read_cmd} -----', 'S')
                    printWithNewLines(f'{line}', 'S')

                    if process_cmd == 'do_save' and not save_detected:
                        save_detected = True

                    if process_cmd not in dir(self):
                        printWithNewLines(f'ERROR: Command {read_cmd} not found', 'E')
                    else:
                        exec_cmd = f"self.{process_cmd}('{' '.join(args)}')"
                        exec(exec_cmd)

                    if not self.forceMode:
                        if input('\nPress enter to continue or (Q)uit... ') in ['q', 'Q']:
                            break
                            print()

        if not save_detected and self.configUpdated:
            if not self.forceMode:
                if input('\nWARNING: No save command was issued would you like to save now? ') in ['y', 'Y', 'yes', 'YES']:
                    self.do_save(self)
                    print()
                    return

            printWithNewLines('WARNING: Configuration changes were made but have not been saved!', 'B')

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

    def getRecordList(self, table, field=None, value=None):

        recordList = []
        for i in range(len(self.cfgData['G2_CONFIG'][table])):
            if field and value:
                if self.cfgData['G2_CONFIG'][table][i][field] == value:
                    recordList.append(self.cfgData['G2_CONFIG'][table][i])
            else:
                recordList.append(self.cfgData['G2_CONFIG'][table][i])
        return recordList

# ===== General config =====

    def do_getActiveConfigID(self, arg):
        '\nGet the config identifier:  getActiveConfigID\n'

        response = bytearray()

        try:
            self.g2_module.getActiveConfigID(response)
            printResponse(response)
        except G2Exception.G2Exception as err:
            printWithNewLines(err, 'B')

    def do_getConfig(self, arg):
        '\nGet the config:  getConfig <configID> \n'

        try:
            args = self.parser.parse_args(['getConfig'] + parse(arg))
        except SystemExit:
            print(self.do_getConfig.__doc__)
            return

        response = bytearray()

        try:
            self.g2_configmgr.getConfig(args.configID, response)
            self.printJsonResponse(response)
        except G2Exception.G2Exception as err:
            print(err)

    def do_getConfigList(self, arg):
        '\nGet a list of known configs:  getConfigList \n'

        response = bytearray()

        try:
            self.g2_configmgr.getConfigList(response)
            self.printJsonResponse(response)
        except G2Exception.G2Exception as err:
            print(err)

    def do_getConfigSection(self, arg):
        '\n\tGet a single section from the current config: getConfigSection <section>\n'

        if not argCheck('do_getConfigSection', arg, self.do_getConfigSection.__doc__):
            return

        try:
            section = json.dumps(self.cfgData["G2_CONFIG"][arg])
        except KeyError:
            printWithNewLines(f'{arg} is an unknown section in the configuration!', 'B')
            return

        self.printJsonResponse(section)


# ===== global commands =====

    def do_configReload(self, arg):
        '\n\tReload configuration and discard all unsaved changes\n'

        # Check if config has unsaved changes
        if self.configUpdated:
            if input('\nYou have unsaved changes, are you sure you want to discard them? (y/n)  ') not in ['y', 'Y', 'yes', 'YES']:
                printWithNewLines('\nConfiguration wasn\'t reloaded. Your changes remain but are still unsaved.\n')
                return

        self.getConfig()

        self.configUpdated = False

        printWithNewLines('Config has been reloaded.', 'B')

    def do_save(self, args):
        '\n\tSave changes to the config\n'

        if self.configUpdated:

            # If not accepting file commands without prompts and not using older style config file
            if not self.forceMode and not self.g2ConfigFileUsed:
                printWithNewLines('WARNING: This will immediately update the current configuration in the Senzing repository with the current configuration!', 'B')
                if input('Are you certain you wish to proceed and save changes? (y/n)  ') not in ['y', 'Y', 'yes', 'YES']:
                    printWithNewLines('Current configuration changes have not been saved!', 'B')
                    return

            if self.g2ConfigFileUsed:

                try:
                    config_file_bkup = '_'.join([self.g2configFile, datetime.now().isoformat(timespec='seconds')])
                    copyfile(self.g2configFile, config_file_bkup)
                except OSError as ex:
                    printWithNewLines(textwrap.dedent(f'''\
                        ERROR: Couldn\'t backup configuration file to {config_file_bkup}. Configuration not saved.
                               {ex}
                    '''), 'S')
                    return
                else:
                    with open(self.g2configFile, 'w') as fp:
                        json.dump(self.cfgData, fp, indent=4, sort_keys=True)
                    printWithNewLines(f'Configuration saved to {self.g2configFile}.', 'B')
                    self.configUpdated = False

            else:

                try:
                    newConfigId = bytearray()
                    self.g2_configmgr.addConfig(json.dumps(self.cfgData), 'Updated by G2ConfigTool', newConfigId)
                    self.g2_configmgr.setDefaultConfigID(newConfigId)

                except G2Exception.G2ModuleGenericException as err:
                    printWithNewLines('ERROR: Failed to save configuration to Senzing repository!', 'S')
                    printWithNewLines(f'       {err}', 'E')
                else:
                    printWithNewLines('Configuration saved to Senzing repository.', 'B')

                    # Reinit engines to pick up changes. This is good practice and will be needed when rewritten to fully use cfg APIs
                    # Don't disply init msg if not interactive (fileloop)
                    self.destroyEngines()
                    self.initEngines(init_msg=(True if self.isInteractive else False))
                    self.configUpdated = False

# ===== Autocompleters =====

    def complete_exportToFile(self, text, line, begidx, endidx):
        if re.match("exportToFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'exportToFile')

    def complete_importFromFile(self, text, line, begidx, endidx):
        if re.match("importFromFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'importFromFile')

    def pathCompletes(self, text, line, begidx, endidx, callingcmd):
        ''' Auto complete paths for commands that have a complete_ function '''

        completes = []

        pathComp = line[len(callingcmd) + 1:endidx]
        fixed = line[len(callingcmd) + 1:begidx]

        for path in glob.glob(f'{pathComp}*'):
            path = path + os.sep if path and os.path.isdir(path) and path[-1] != os.sep else path
            completes.append(path.replace(fixed, '', 1))

        return completes

    # Auto completers for approproate commands
    def complete_getAttribute(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_ATTR', 'ATTR_CODE', text)

    def complete_getFeature(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_FTYPE', 'FTYPE_CODE', text)

    def complete_getElement(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_FELEM', 'FELEM_CODE', text)

    def complete_getFragment(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_ERFRAG', 'ERFRAG_CODE', text)

    def complete_getRule(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_ERRULE', 'ERRULE_CODE', text)

    def complete_deleteAttribute(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_ATTR', 'ATTR_CODE', text)

    def complete_deleteDataSource(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_DSRC', 'DSRC_CODE', text)

    def complete_deleteElement(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_FELEM', 'FELEM_CODE', text)

    def complete_deleteEntityType(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_ETYPE', 'ETYPE_CODE', text)

    def complete_deleteFeature(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_FTYPE', 'FTYPE_CODE', text)

    def complete_deleteFeatureComparison(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_FTYPE', 'FTYPE_CODE', text)

    def complete_deleteFeatureDistinctCall(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_FTYPE', 'FTYPE_CODE', text)

    def complete_deleteFragment(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_ERFRAG', 'ERFRAG_CODE', text)

    def complete_deleteRule(self, text, line, begidx, endidx):
        return self.codes_completes('CFG_ERRULE', 'ERRULE_CODE', text)

    def codes_completes(self, table, field, arg):
        ''' Return codes for auto complete on get* delete*  '''

        # Build list each time to have latest even after an add*, delete*
        codes_list = self.getRecordCodes(table, field)

        return [code for code in codes_list if code.lower().startswith(arg.lower())]

    def getRecordCodes(self, table, field):
        ''' Return list of codes from config '''

        code_list = []
        for i in range(len(self.cfgData['G2_CONFIG'][table])):
            code_list.append(self.cfgData["G2_CONFIG"][table][i][field])

        return code_list

    def complete_getConfigSection(self, text, line, begidx, endidx):
        '''  '''
        return [section for section in self.cfgData["G2_CONFIG"].keys() if section.lower().startswith(text.lower())]

# ===== Export / Import =====

    def do_exportToFile(self, arg):
        '\n\tExport the config to a file:  exportToFile <fileName>\n'

        if not argCheck('do_exportToFile', arg, self.do_exportToFile.__doc__):
            return

        try:
            with open(arg, 'w') as fp:
                json.dump(self.cfgData, fp, indent=4, sort_keys=True)
        except OSError as ex:
            printWithNewLines(textwrap.dedent(f'''\
                ERROR: Couldn\'t export to {arg}.
                       {ex}
                '''), 'S')
            return
        else:
            printWithNewLines('Successfully exported!', 'B')

    def do_importFromFile(self, arg):
        '\n\tImport a config from a file:  importFromFile <fileName>\n'

        if not argCheck('do_importFromFile', arg, self.do_importFromFile.__doc__):
            return

        if self.configUpdated:
            if input('\nYou have unsaved changes, are you sure you want to discard them? (y/n)  ') not in ['y', 'Y', 'yes', 'YES']:
                printWithNewLines('Configuration wasn\'t imported.  Your changes remain but are still unsaved.')
                return

        try:
            self.cfgData = json.load(open(arg), encoding="utf-8")
        except ValueError as e:
            print(f'\nERROR: {arg} doesn\'t appear to be valid JSON, configuration not imported!')
            print(f'ERROR: {e}\n')
            return
        else:
            self.configUpdated = True
            printWithNewLines('Successfully imported!', 'B')


# ===== Compatibility version commands =====

    def do_verifyCompatibilityVersion(self, arg):
        '\n\tverifyCompatibilityVersion {"expectedVersion": "2"}\n'

        if not argCheck('verifyCompatibilityVersion', arg, self.do_verifyCompatibilityVersion.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if self.cfgData['G2_CONFIG']['CONFIG_BASE_VERSION']['COMPATIBILITY_VERSION']['CONFIG_VERSION'] != parmData['EXPECTEDVERSION']:
                printWithNewLines('Compatibility version does not match specified value [%s]!  Actual version is [%s].' % (parmData['EXPECTEDVERSION'], self.cfgData['G2_CONFIG']['CONFIG_BASE_VERSION']['COMPATIBILITY_VERSION']['CONFIG_VERSION']), 'B')

                if self.isInteractive is False:
                    # throw an exception, so that we abort running scripts
                    raise Exception('Incorrect compatibility version.')
                else:
                    # just finish normally, for the interactive command shell.
                    return
            printWithNewLines('Compatibility version successfully verified!', 'B')

    def do_updateCompatibilityVersion(self, arg):
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

    def do_getCompatibilityVersion(self, arg):
        '\n\tgetCompatibilityVersion\n'

        try:
            compat_version = self.cfgData['G2_CONFIG']['CONFIG_BASE_VERSION']['COMPATIBILITY_VERSION']['CONFIG_VERSION']
            printWithNewLines(f'Compatibility version: {compat_version}', 'B')
        except KeyError:
            printWithNewLines('ERROR: Couldn\'t retrieve compatibility version', 'B')


# ===== Config section commands =====

    def do_addConfigSection(self, arg):
        '\n\taddConfigSection {"name": "<configSection_name>"}\n'

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

    def do_addConfigSectionField(self, arg):
        '\n\taddConfigSectionField {"section": "<section_name>","field": "<field_name>","value": "<field_value>"}\n'

        if not argCheck('do_addConfigSectionField', arg, self.do_addConfigSectionField.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))

            if 'SECTION' not in parmData or len(parmData['SECTION']) == 0:
                raise ValueError('Config section name is required!')
            parmData['SECTION'] = parmData['SECTION'].upper()

            if 'FIELD' not in parmData or len(parmData['FIELD']) == 0:
                raise ValueError('Field name is required!')
            parmData['FIELD'] = parmData['FIELD'].upper()

            if 'VALUE' not in parmData:
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

    def do_listDataSources(self, arg):
        '\n\tlistDataSources [search_value]\n'

        print()
        for dsrcRecord in sorted(self.getRecordList('CFG_DSRC'), key=lambda k: k['DSRC_ID']):
            if arg and arg.lower() not in str(dsrcRecord).lower():
                continue
            print('{"id": %i, "dataSource": "%s"}' % (dsrcRecord['DSRC_ID'], dsrcRecord['DSRC_CODE']))
        print()

    def do_addDataSource(self, arg):
        '\n\taddDataSource {"dataSource": "<dataSource_name>"}\n'

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
                debug(newRecord)

    def do_deleteDataSource(self, arg):
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
                printWithNewLines('Can\'t delete the SEARCH data source!', 'B')
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

    def do_listEntityClasses(self, arg):
        '\n\tlistEntityClasses [search_value]\n'

        print()
        for eclassRecord in sorted(self.getRecordList('CFG_ECLASS'), key=lambda k: k['ECLASS_ID']):
            if arg and arg.lower() not in str(eclassRecord).lower():
                continue
            print('{"id": %i, "entityClass": "%s"}' % (eclassRecord['ECLASS_ID'], eclassRecord['ECLASS_CODE']))
        print()

    ## ----------------------------
    #def do_addEntityClass(self ,arg):
    #    '\n\taddEntityClass {"entityClass": "<entityClass_value>"}\n'
    #
    #    if not argCheck('addEntityClass', arg, self.do_addEntityClass.__doc__):
    #        return
    #
    #    try:
    #        parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ENTITYCLASS": arg}
    #        parmData['ENTITYCLASS'] = parmData['ENTITYCLASS'].upper()
    #    except (ValueError, KeyError) as e:
    #        argError(arg, e)
    #    else:
    #
    #        if 'RESOLVE' in parmData and parmData['RESOLVE'].upper() not in ('YES','NO'):
    #            printWithNewLines('Resolve flag must be Yes or No', 'B')
    #            return
    #        if 'ID' in parmData and type(parmData['ID']) is not int:
    #            parmData['ID'] = int(parmData['ID'])
    #
    #        maxID = 0
    #        for i in range(len(self.cfgData['G2_CONFIG']['CFG_ECLASS'])):
    #            if self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_CODE'] == parmData['ENTITYCLASS']:
    #                printWithNewLines('Entity class %s already exists!' % parmData['ENTITYCLASS'], 'B')
    #                return
    #            if 'ID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_ID']) == parmData['ID']:
    #                printWithNewLines('Entity class id %s already exists!' % parmData['ID'], 'B')
    #                return
    #            if self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_ID'] > maxID:
    #                maxID = self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_ID']
    #        if 'ID' not in parmData:
    #            parmData['ID'] = maxID + 1 if maxID >=1000 else 1000
    #
    #        newRecord = {}
    #        newRecord['ECLASS_ID'] = int(parmData['ID'])
    #        newRecord['ECLASS_CODE'] = parmData['ENTITYCLASS']
    #        newRecord['ECLASS_DESC'] = parmData['ENTITYCLASS']
    #        newRecord['RESOLVE'] = parmData['RESOLVE'].title() if 'RESOLVE' in parmData else 'Yes'
    #        self.cfgData['G2_CONFIG']['CFG_ECLASS'].append(newRecord)
    #        self.configUpdated = True
    #        printWithNewLines('Successfully added!', 'B')
    #        if self.doDebug:
    #            debug(newRecord)

    ## -----------------------------
    #def do_deleteEntityClass(self ,arg):
    #    '\n\tdeleteEntityClass {"entityClass": "<entityClass_value>"}\n'
    #
    #    if not argCheck('deleteEntityClass', arg, self.do_deleteEntityClass.__doc__):
    #        return
    #
    #    try:
    #        parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"ENTITYCLASS": arg}
    #        parmData['ENTITYCLASS'] = parmData['ENTITYCLASS'].upper()
    #    except (ValueError, KeyError) as e:
    #        argError(arg, e)
    #    else:
    #
    #        deleteCnt = 0
    #        for i in range(len(self.cfgData['G2_CONFIG']['CFG_ECLASS'])-1, -1, -1):
    #            if self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]['ECLASS_CODE'] == parmData['ENTITYCLASS']:
    #                del self.cfgData['G2_CONFIG']['CFG_ECLASS'][i]
    #                deleteCnt += 1
    #                self.configUpdated = True
    #        if deleteCnt == 0:
    #            printWithNewLines('Record not found!', 'B')
    #        else:
    #            printWithNewLines('%s rows deleted!' % deleteCnt, 'B')


# ===== entity type commands =====

    def do_listEntityTypes(self, arg):
        '\n\tlistEntityTypes [search_value]\n'

        print()
        for etypeRecord in sorted(self.getRecordList('CFG_ETYPE'), key=lambda k: k['ETYPE_ID']):
            if arg and arg.lower() not in str(etypeRecord).lower():
                continue
            eclassRecord = self.getRecord('CFG_ECLASS', 'ECLASS_ID', etypeRecord['ECLASS_ID'])
            print('{"id": %i, "entityType":"%s", "class": "%s"}' % (etypeRecord['ETYPE_ID'], etypeRecord['ETYPE_CODE'], ('unknown' if not eclassRecord else eclassRecord['ECLASS_CODE'])))
        print()

    def do_addEntityType(self, arg):
        '\n\taddEntityType {"entityType": "<entityType_value>"}\n'

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
                debug(newRecord)

    def do_deleteEntityType(self, arg):
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

    def do_listFunctions(self, arg):
        '\n\tlistFunctions [search_value]\n'

        print()
        for funcRecord in sorted(self.getRecordList('CFG_SFUNC'), key = lambda k: k['SFUNC_ID']):
            if arg and arg.lower() not in str(funcRecord).lower():
                continue
            print(f'{{"id": "{funcRecord["SFUNC_ID"]}", type": "Standardization", "function": "{funcRecord["SFUNC_CODE"]}"}}')

        print()
        for funcRecord in sorted(self.getRecordList('CFG_EFUNC'), key = lambda k: k['EFUNC_ID']):
            if arg and arg.lower() not in str(funcRecord).lower():
                continue
            print(f'{{"id": "{funcRecord["EFUNC_ID"]}", type": "Expression", "function": "{funcRecord["EFUNC_CODE"]}"}}')

        print()
        for funcRecord in sorted(self.getRecordList('CFG_CFUNC'), key = lambda k: k['CFUNC_ID']):
            if arg and arg.lower() not in str(funcRecord).lower():
                continue
            print(f'{{"id": "{funcRecord["CFUNC_ID"]}", type": "Comparison", "function": "{funcRecord["CFUNC_CODE"]}"}}')

        print()

    def do_listFeatureClasses(self, arg):
        '\n\tlistFeatureClasses [search_value]\n'

        print()
        for fclassRecord in sorted(self.getRecordList('CFG_FCLASS'), key=lambda k: k['FCLASS_ID']):
            if arg and arg.lower() not in str(fclassRecord).lower():
                continue
            print('{"id": %i, "class":"%s"}' % (fclassRecord['FCLASS_ID'], fclassRecord['FCLASS_CODE']))
        print()

    def do_listFeatures(self, arg):
        '\n\tlistFeatures [search_value]\n'
        #'\n\tlistFeatures\t\t(displays all features)\n' #\
        #'listFeatures -n\t\t(to display new features only)\n'

        print()
        for ftypeRecord in sorted(self.getRecordList('CFG_FTYPE'), key=lambda k: k['FTYPE_ID']):
            if arg and arg.lower() not in str(ftypeRecord).lower():
                continue
            printWithNewLines(self.getFeatureJson(ftypeRecord), 'E')
        print()

    def do_getFeature(self, arg):
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
                self.printJsonResponse(self.getFeatureJson(ftypeRecord))

    def do_addFeatureComparisonElement(self, arg):
        '\n\taddFeatureComparisonElement {"feature": "<feature_name>", "element": "<element_name>"}\n'

        if not argCheck('addFeatureComparisonElement', arg, self.do_addFeatureComparisonElement.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            #--lookup feature and error if it doesn't exist
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return
            ftypeID = ftypeRecord['FTYPE_ID']

            #--lookup element and error if it doesn't exist
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Element %s not found!' % parmData['ELEMENT'], 'B')
                return
            felemID = felemRecord['FELEM_ID']

            #--find the comparison function call
            cfcallRecord = self.getRecord('CFG_CFCALL', 'FTYPE_ID', ftypeID)
            if not cfcallRecord:
                printWithNewLines('Comparison function for feature %s not found!' % parmData['FEATURE'], 'B')
                return
            cfcallID = cfcallRecord['CFCALL_ID']

            #--check to see if the element is already in the feature
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFBOM'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_CFBOM'][i]['CFCALL_ID'] == cfcallID and self.cfgData['G2_CONFIG']['CFG_CFBOM'][i]['FELEM_ID'] == felemID:
                    printWithNewLines('Comparison function for feature %s aleady contains element %s!' % (parmData['FEATURE'],parmData['ELEMENT']), 'B')
                    return

            #--add the feature element
            cfbomExecOrder = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFBOM'])):
                if self.cfgData['G2_CONFIG']['CFG_CFBOM'][i]['CFCALL_ID'] == cfcallID:
                    if self.cfgData['G2_CONFIG']['CFG_CFBOM'][i]['EXEC_ORDER'] > cfbomExecOrder:
                        cfbomExecOrder = self.cfgData['G2_CONFIG']['CFG_CFBOM'][i]['EXEC_ORDER']
            cfbomExecOrder = cfbomExecOrder + 1
            newRecord = {}
            newRecord['CFCALL_ID'] = cfcallID
            newRecord['EXEC_ORDER'] = cfbomExecOrder
            newRecord['FTYPE_ID'] = ftypeID
            newRecord['FELEM_ID'] = felemID
            self.cfgData['G2_CONFIG']['CFG_CFBOM'].append(newRecord)
            if self.doDebug:
                debug(newRecord, 'CFBOM build')

            #--we made it!
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

    def do_addFeatureDistinctCallElement(self, arg):
        '\n\taddFeatureDistinctCallElement {"feature": "<feature_name>", "element": "<element_name>"}\n'

        if not argCheck('addFeatureDistinctCallElement', arg, self.do_addFeatureDistinctCallElement.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            #--lookup feature and error if it doesn't exist
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return
            ftypeID = ftypeRecord['FTYPE_ID']

            #--lookup element and error if it doesn't exist
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Element %s not found!' % parmData['ELEMENT'], 'B')
                return
            felemID = felemRecord['FELEM_ID']

            #--find the distinct function call
            dfcallRecord = self.getRecord('CFG_DFCALL', 'FTYPE_ID', ftypeID)
            if not dfcallRecord:
                printWithNewLines('Distinct function for feature %s not found!' % parmData['FEATURE'], 'B')
                return
            dfcallID = dfcallRecord['DFCALL_ID']

            #--check to see if the element is already in the feature
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_DFBOM'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_DFBOM'][i]['DFCALL_ID'] == dfcallID and self.cfgData['G2_CONFIG']['CFG_DFBOM'][i]['FELEM_ID'] == felemID:
                    printWithNewLines('Distinct function for feature %s aleady contains element %s!' % (parmData['FEATURE'],parmData['ELEMENT']), 'B')
                    return

            #--add the feature element
            dfbomExecOrder = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_DFBOM'])):
                if self.cfgData['G2_CONFIG']['CFG_DFBOM'][i]['DFCALL_ID'] == dfcallID:
                    if self.cfgData['G2_CONFIG']['CFG_DFBOM'][i]['EXEC_ORDER'] > dfbomExecOrder:
                        dfbomExecOrder = self.cfgData['G2_CONFIG']['CFG_DFBOM'][i]['EXEC_ORDER']
            dfbomExecOrder = dfbomExecOrder + 1
            newRecord = {}
            newRecord['DFCALL_ID'] = dfcallID
            newRecord['EXEC_ORDER'] = dfbomExecOrder
            newRecord['FTYPE_ID'] = ftypeID
            newRecord['FELEM_ID'] = felemID
            self.cfgData['G2_CONFIG']['CFG_DFBOM'].append(newRecord)
            if self.doDebug:
                debug(newRecord, 'DFBOM build')

            #--we made it!
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

    def do_addFeatureComparison(self, arg):
        '\n\taddFeatureComparison {"feature": "<feature_name>", "comparison": "<comparison_function>", "elementList": ["<element_detail(s)"]}' \
        '\n\n\taddFeatureComparison {"feature":"testFeat", "comparison":"exact_comp", "elementlist": [{"element": "test"}]}\n'

        if not argCheck('addFeatureComparison', arg, self.do_addFeatureComparison.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            if 'ELEMENTLIST' not in parmData or len(parmData['ELEMENTLIST']) == 0:
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

            cfuncID = 0  # --comparison function
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
                debug(newRecord, 'CFCALL build')

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
                    debug(newRecord, 'CFBOM build')

            #--we made it!
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

    def do_setFeatureComparison(self, arg):
        '\n\tsetFeatureComparison {"feature": "<feature_name>", "comparison": "<comparison_function>"}\n'

        if not argCheck('setFeatureComparison', arg, self.do_setFeatureComparison.__doc__):
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
            ftypeID = ftypeRecord['FTYPE_ID']

            cfuncID = 0  # --comparison function
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

            #--set the comparison call
            modificationMade = False
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFCALL'])):
                if self.cfgData['G2_CONFIG']['CFG_CFCALL'][i]['FTYPE_ID'] == ftypeID:
                    self.cfgData['G2_CONFIG']['CFG_CFCALL'][i]['CFUNC_ID'] = cfuncID
                    modificationMade = True
            if modificationMade == False:
                printWithNewLines('No previous comparison method for \'%s\' exists.  Use \'addFeatureComparison\' instead.' % parmData['FEATURE'], 'B')
                return

            #--we made it!
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

    def do_deleteFeatureComparisonElement(self, arg):
        '\n\tdeleteFeatureComparisonElement {"feature": "<feature_name>", "element": "<element_name>"}\n'

        if not argCheck('deleteFeatureComparisonElement', arg, self.do_deleteFeatureComparisonElement.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            parmData['ELEMENT'] = parmData['ELEMENT'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            #--lookup feature and error if it doesn't exist
            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s not found!' % parmData['FEATURE'], 'B')
                return

            #--lookup element and error if it doesn't exist
            felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
            if not felemRecord:
                printWithNewLines('Element %s not found!' % parmData['ELEMENT'], 'B')
                return

            deleteCnt = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE']) -1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_CODE'] == parmData['FEATURE']:
                    for i1 in range(len(self.cfgData['G2_CONFIG']['CFG_CFCALL'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]['FTYPE_ID'] == ftypeRecord['FTYPE_ID']:
                            for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_CFBOM'])-1, -1, -1):
                                if self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]['CFCALL_ID'] == self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]['CFCALL_ID'] and self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]['FTYPE_ID'] == ftypeRecord['FTYPE_ID'] and self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]['FELEM_ID'] == felemRecord['FELEM_ID']:
                                    del self.cfgData['G2_CONFIG']['CFG_CFBOM'][i2]
                                    deleteCnt += 1
                                    self.configUpdated = True

            if deleteCnt == 0:
                printWithNewLines('Feature comparator element not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    def do_deleteFeatureComparison(self, arg):
        '\n\tdeleteFeatureComparison {"feature": "<feature_name>"}\n'

        if not argCheck('deleteFeatureComparison', arg, self.do_deleteFeatureComparison.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"FEATURE": arg}
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
                                    deleteCnt += 1
                            del self.cfgData['G2_CONFIG']['CFG_CFCALL'][i1]
                            deleteCnt += 1
                            self.configUpdated = True

            if deleteCnt == 0:
                printWithNewLines('Feature comparator not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    def do_deleteFeatureDistinctCall(self, arg):
        '\n\tdeleteFeatureDistinctCall {"feature": "<feature_name>"}\n'

        if not argCheck('deleteFeatureDistinctCall', arg, self.do_deleteFeatureDistinctCall.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg)) if arg.startswith('{') else {"FEATURE": arg}
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

                    # delete any distinct-func calls and boms  (must loop through backwards when deleting)
                    for i1 in range(len(self.cfgData['G2_CONFIG']['CFG_DFCALL'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_DFCALL'][i1]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_DFBOM'])-1, -1, -1):
                                if self.cfgData['G2_CONFIG']['CFG_DFBOM'][i2]['DFCALL_ID'] == self.cfgData['G2_CONFIG']['CFG_DFCALL'][i1]['DFCALL_ID']:
                                    del self.cfgData['G2_CONFIG']['CFG_DFBOM'][i2]
                                    deleteCnt += 1
                            del self.cfgData['G2_CONFIG']['CFG_DFCALL'][i1]
                            deleteCnt += 1
                            self.configUpdated = True

            if deleteCnt == 0:
                printWithNewLines('Feature distinct call not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    def do_deleteFeature(self, arg):
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
                printWithNewLines('Can\'t delete feature %s!' % parmData['FEATURE'], 'B')
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

                    # delete the expression calls builder felems (must loop through backwards when deleting)
                    for i2 in range(len(self.cfgData['G2_CONFIG']['CFG_EFBOM'])-1, -1, -1):
                        if self.cfgData['G2_CONFIG']['CFG_EFBOM'][i2]['FTYPE_ID'] == self.cfgData['G2_CONFIG']['CFG_FTYPE'][i]['FTYPE_ID']:
                            del self.cfgData['G2_CONFIG']['CFG_EFBOM'][i2]

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

    def do_setFeature(self, arg):
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
            print()

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
                    if parmData['ANONYMIZE'].upper() in ('YES', 'Y', 'NO', 'N'):
                        self.cfgData['G2_CONFIG']['CFG_FTYPE'][listID]['ANONYMIZE'] = 'Yes' if parmData['ANONYMIZE'].upper() in ('YES', 'Y') else 'No'
                        printWithNewLines('Anonymize setting updated!')
                        self.configUpdated = True
                    else:
                        printWithNewLines('Invalid anonymize setting: %s' % parmData['ANONYMIZE'])

                elif parmCode == 'CANDIDATES':
                    if parmData['CANDIDATES'].upper() in ('YES', 'Y', 'NO', 'N'):
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

            print()

    def do_addFeature(self, arg):
        '\n\taddFeature {"feature": "<feature_name>", "behavior": "<behavior_code>", "elementList": ["<element_detail(s)"]}' \
        '\n\n\taddFeature {"feature":"testFeat", "behavior":"FM", "comparison":"exact_comp", "elementlist": [{"compared": "Yes", "expressed": "No", "element": "test"}]}' \
        '\n\n\tFor additional example structures, use getFeature or listFeatures\n'

        if not argCheck('addFeature', arg, self.do_addFeature.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FEATURE'] = parmData['FEATURE'].upper()
            if 'ELEMENTLIST' not in parmData or len(parmData['ELEMENTLIST']) == 0:
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
                ftypeID = maxID + 1 if maxID >= 1000 else 1000

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

            sfuncID = 0  # --standardization function
            if 'STANDARDIZE' in parmData and len(parmData['STANDARDIZE']) != 0:
                parmData['STANDARDIZE'] = parmData['STANDARDIZE'].upper()
                sfuncRecord = self.getRecord('CFG_SFUNC', 'SFUNC_CODE', parmData['STANDARDIZE'])
                if sfuncRecord:
                    sfuncID = sfuncRecord['SFUNC_ID']
                else:
                    printWithNewLines('Invalid standardization code: %s' % parmData['STANDARDIZE'], 'B')
                    return

            efuncID = 0  # --expression function
            if 'EXPRESSION' in parmData and len(parmData['EXPRESSION']) != 0:
                parmData['EXPRESSION'] = parmData['EXPRESSION'].upper()
                efuncRecord = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', parmData['EXPRESSION'])
                if efuncRecord:
                    efuncID = efuncRecord['EFUNC_ID']
                else:
                    printWithNewLines('Invalid expression code: %s' % parmData['EXPRESSION'], 'B')
                    return

            cfuncID = 0  # --comparison function
            if 'COMPARISON' in parmData and len(parmData['COMPARISON']) != 0:
                parmData['COMPARISON'] = parmData['COMPARISON'].upper()
                cfuncRecord = self.getRecord('CFG_CFUNC', 'CFUNC_CODE', parmData['COMPARISON'])
                if cfuncRecord:
                    cfuncID = cfuncRecord['CFUNC_ID']
                else:
                    printWithNewLines('Invalid comparison code: %s' % parmData['COMPARISON'], 'B')
                    return

            #--ensure elements going to express or compare routines
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
            newRecord['PERSIST_HISTORY'] = 'No' if 'HISTORY' in parmData and parmData['HISTORY'].upper() == 'NO' else 'Yes'
            newRecord['VERSION'] = 1
            newRecord['RTYPE_ID'] = int(parmData['RTYPE_ID']) if 'RTYPE_ID' in parmData else 0
            self.cfgData['G2_CONFIG']['CFG_FTYPE'].append(newRecord)
            if self.doDebug:
                debug(newRecord, 'Feature build')

            #--add the standardization call
            sfcallID = 0
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
                    debug(newRecord, 'SFCALL build')

            #--add the distinct value call (not supported through here yet)
            dfcallID = 0
            dfuncID = 0  # --more efficent to leave it null
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
                    debug(newRecord, 'DFCALL build')

            #--add the expression call
            efcallID = 0
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
                    debug(newRecord, 'EFCALL build')

            #--add the comparison call
            cfcallID = 0
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
                    debug(newRecord, 'CFCALL build')

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
                    felemID = maxID + 1 if maxID >= 1000 else 1000
                    newRecord = {}
                    newRecord['FELEM_ID'] = felemID
                    newRecord['FELEM_CODE'] = elementRecord['ELEMENT']
                    newRecord['FELEM_DESC'] = elementRecord['ELEMENT']
                    newRecord['DATA_TYPE'] = 'string'
                    newRecord['TOKENIZE'] = 'No'
                    self.cfgData['G2_CONFIG']['CFG_FELEM'].append(newRecord)
                    if self.doDebug:
                        debug(newRecord, 'FELEM build')

                #--add to distinct value  bom if any
                if dfcallID > 0:
                    newRecord = {}
                    newRecord['DFCALL_ID'] = dfcallID
                    newRecord['EXEC_ORDER'] = fbomOrder
                    newRecord['FTYPE_ID'] = ftypeID
                    newRecord['FELEM_ID'] = felemID
                    self.cfgData['G2_CONFIG']['CFG_DFBOM'].append(newRecord)
                    if self.doDebug:
                        debug(newRecord, 'DFBOM build')

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
                        debug(newRecord, 'EFBOM build')

                #--add to comparison bom if any
                if cfcallID > 0 and elementRecord['COMPARED'].upper() == 'YES':
                    newRecord = {}
                    newRecord['CFCALL_ID'] = cfcallID
                    newRecord['EXEC_ORDER'] = fbomOrder
                    newRecord['FTYPE_ID'] = ftypeID
                    newRecord['FELEM_ID'] = felemID
                    self.cfgData['G2_CONFIG']['CFG_CFBOM'].append(newRecord)
                    if self.doDebug:
                        debug(newRecord, 'CFBOM build')

                #--standardize display_level to just display while maintainin backwards compatibility
                #-- also note that display_delem has been deprecated and does nothing
                if 'DISPLAY' in elementRecord:
                    elementRecord['DISPLAY_LEVEL'] = 1 if elementRecord['DISPLAY'].upper() == 'YES' else 0

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
                    debug(newRecord, 'FBOM build')

            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

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
                    elementRecord['display'] = 'No' if fbomRecord['DISPLAY_LEVEL'] == 0 else 'Yes'
                    elementList.append(elementRecord)
                else:
                    elementList.append(felemRecord['FELEM_CODE'])

        jsonString += ', "elementList": %s' % json.dumps(elementList)
        jsonString += '}'

        return jsonString

    def do_addToNamehash(self, arg):
        '\n\taddToNamehash {"feature": "<feature>", "element": "<element>"}\n'

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
        except Exception:
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
                printWithNewLines('%s is not an element of feature %s' % (parmData['ELEMENT'], parmData['FEATURE']), 'B')
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
            debug(newRecord, 'EFBOM build')

        self.configUpdated = True
        printWithNewLines('Successfully added!', 'B')

    def do_deleteFromNamehash(self, arg):
        '\n\tdeleteFromNamehash {"feature": "<feature>", "element": "<element>"}\n'

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
        except Exception:
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

    def do_addToNameSSNLast4hash(self, arg):
        '\n\taddToNameSSNLast4hash {"feature": "<feature>", "element": "<element>"}\n'

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
        except Exception:
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
                printWithNewLines('%s is not an element of feature %s' % (parmData['ELEMENT'], parmData['FEATURE']), 'B')
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
            debug(newRecord, 'EFBOM build')

        self.configUpdated = True
        printWithNewLines('Successfully added!', 'B')

    def do_deleteFromSSNLast4hash(self, arg):
        '\n\tdeleteFromSSNLast4hash {"feature": "<feature>", "element": "<element>"}\n'

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
        except Exception:
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

    def do_listAttributes(self, arg):
        '\n\tlistAttributes [search_value]\n'

        print()
        for attrRecord in sorted(self.getRecordList('CFG_ATTR'), key=lambda k: k['ATTR_ID']):
            if arg and arg.lower() not in str(attrRecord).lower():
                continue
            printWithNewLines(self.getAttributeJson(attrRecord), 'E')
        print()

    def do_listAttributeClasses(self, arg):
        '\n\tlistAttributeClasses [search_value]\n'

        print()
        for attrClass in self.attributeClassList:
            if arg and arg.lower() not in str(attrClass).lower():
                continue
            print('{"attributeClass": "%s"}' % attrClass)
        print()

    def do_getAttribute(self, arg):
        '\n\tgetAttribute {"attribute": "<attribute_name>"}' \
        '\n\tgetAttribute {"feature": "<feature_name>"}\t\tList all the attributes for a feature\n'

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
                for attrRecord in sorted(attrRecords, key=lambda k: k['ATTR_ID']):
                    self.printJsonResponse(self.getAttributeJson(attrRecord))

    def do_deleteAttribute(self, arg):
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

    def do_addEntityScore(self, arg):
        '\n\taddEntityScore {"behavior": "<behavior code>", "grouperFeat": "<yes/no>", "richnessScore": "<richness score>", "exclusivityScore": "<exclusivity score>"}\n'

        if not argCheck('addEntityScore', arg, self.do_addEntityScore.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['BEHAVIOR'] = parmData['BEHAVIOR'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            #--lookup behavior and error if it doesn't exist
            if self.getRecord('CFG_ESCORE', 'BEHAVIOR_CODE', parmData['BEHAVIOR']):
                printWithNewLines('Entity score entry %s already exists!' % parmData['BEHAVIOR'], 'B')
                return

            if 'GROUPERFEAT' not in parmData:
                parmData['GROUPERFEAT'] = 'No'

            newRecord = {}
            newRecord['BEHAVIOR_CODE'] = parmData['BEHAVIOR']
            newRecord['GROUPER_FEAT'] = 'Yes' if parmData['GROUPERFEAT'].upper() == 'YES' else 'No'
            newRecord['RICHNESS_SCORE'] = int(parmData['RICHNESSSCORE'])
            newRecord['EXCLUSIVITY_SCORE'] = int(parmData['EXCLUSIVITYSCORE'])
            self.cfgData['G2_CONFIG']['CFG_ESCORE'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                debug(newRecord)

    def do_addAttribute(self, arg):
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
                parmData['FEATURE'] = None
                ftypeRecord = None

            if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                if parmData['ELEMENT'] in ('<PREHASHED>', 'USED_FROM_DT', 'USED_THRU_DT', 'USAGE_TYPE'):
                    felemRecord = parmData['ELEMENT']
                else:
                    felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                    if not felemRecord:
                        printWithNewLines('Invalid element: %s' % parmData['ELEMENT'], 'B')
                        return
                    else:
                        if not self.getRecord('CFG_FBOM', ['FTYPE_ID', 'FELEM_ID'], [ftypeRecord['FTYPE_ID'], felemRecord['FELEM_ID']]):
                            printWithNewLines('%s is not an element of feature %s' % (parmData['ELEMENT'], parmData['FEATURE']), 'B')
                            return
            else:
                parmData['ELEMENT'] = None
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
                parmData['DEFAULT'] = None
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
                debug(newRecord)

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

    def do_listElements(self, arg):
        '\n\tlistElements [search_value]\n'

        print()
        for elemRecord in sorted(self.getRecordList('CFG_FELEM'), key=lambda k: k['FELEM_ID']):
            if arg and arg.lower() not in str(elemRecord).lower():
                continue
            print('{"id": %i, "code": "%s", "tokenize": "%s", "datatype": "%s"}' % (elemRecord['FELEM_ID'], elemRecord['FELEM_CODE'], elemRecord['TOKENIZE'], elemRecord['DATA_TYPE']))
        print()

    def do_getElement(self, arg):
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
                self.printJsonResponse('{"id": %s, "code": "%s", "datatype": "%s", "tokenize": "%s"}' % (felemRecord['FELEM_ID'], felemRecord['FELEM_CODE'], felemRecord['DATA_TYPE'], felemRecord['TOKENIZE']))

    def do_addStandardizeFunc(self, arg):
        '\n\taddStandardizeFunc {"function":"<function_name>", "connectStr":"<plugin_base_name>"}' \
        '\n\n\taddStandardizeFunc {"function":"STANDARDIZE_COUNTRY", "connectStr":"g2StdCountry"}\n'

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
                    sfuncID = max(maxID) + 1 if max(maxID) >= 1000 else 1000

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
                    debug(newRecord)

    def do_addStandardizeCall(self, arg):
        '\n\taddStandardizeCall {"element":"<element_name>", "function":"<function_name>", "execOrder":<exec_order>}' \
        '\n\n\taddStandardizeCall {"element":"COUNTRY", "function":"STANDARDIZE_COUNTRY", "execOrder":100}\n'

        if not argCheck('addStandardizeCall', arg, self.do_addStandardizeCall.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            featureIsSpecified = False
            ftypeID = -1
            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0:
                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s.' % parmData['FEATURE'], 'B')
                    return
                featureIsSpecified = True
                ftypeID = ftypeRecord['FTYPE_ID']

            elementIsSpecified = False
            felemID = -1
            if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                if not felemRecord:
                    printWithNewLines('Invalid element: %s.' % parmData['ELEMENT'], 'B')
                    return
                elementIsSpecified = True
                felemID = felemRecord['FELEM_ID']

            if featureIsSpecified is False and elementIsSpecified is False:
                printWithNewLines('No feature or element specified.', 'B')
                return

            if featureIsSpecified is True and elementIsSpecified is True:
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
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_SFCALL'])):
                maxID.append(self.cfgData['G2_CONFIG']['CFG_SFCALL'][i]['SFCALL_ID'])

            sfcallID = 0
            if 'ID' in parmData:
                sfcallID = int(parmData['ID'])
            else:
                sfcallID = max(maxID) + 1 if max(maxID) >= 1000 else 1000

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
                debug(newRecord)

    def do_addExpressionFunc(self, arg):

        '\n\taddExpressionFunc {"function":"<function_name>", "connectStr":"<plugin_base_name>"}' \
        '\n\n\taddExpressionFunc {"function":"FEAT_BUILDER", "connectStr":"g2FeatBuilder"}\n'

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
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_EFUNC'])):
                    maxID.append(self.cfgData['G2_CONFIG']['CFG_EFUNC'][i]['EFUNC_ID'])

                efuncID = 0
                if 'ID' in parmData:
                    efuncID = int(parmData['ID'])
                else:
                    efuncID = max(maxID) + 1 if max(maxID) >= 1000 else 1000

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
                    debug(newRecord)

    def do_updateFeatureVersion(self, arg):

        '\n\tupdateFeatureVersion {"feature":"<feature_name>", "version":<version_number>}\n'

        if not argCheck('updateFeatureVersion', arg, self.do_updateFeatureVersion.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            if 'FEATURE' not in parmData or len(parmData['FEATURE']) == 0:
                raise ValueError('Feature name is required!')
            if 'VERSION' not in parmData:
                raise ValueError('Version is required!')
            parmData['FEATURE'] = parmData['FEATURE'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
            if not ftypeRecord:
                printWithNewLines('Feature %s does not exist!' % parmData['FEATURE'], 'B')
                return
            else:
                ftypeRecord['VERSION'] = parmData['VERSION']
                self.configUpdated = True
                printWithNewLines('Successfully updated!', 'B')
                if self.doDebug:
                    debug(ftypeRecord)

    def do_updateAttributeAdvanced(self, arg):

        '\n\tupdateAttributeAdvanced {"attribute":"<attribute_name>", "advanced":"Yes"}\n'

        if not argCheck('updateAttributeAdvanced', arg, self.do_updateAttributeAdvanced.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            if 'ATTRIBUTE' not in parmData or len(parmData['ATTRIBUTE']) == 0:
                raise ValueError('Attribute name is required!')
            if 'ADVANCED' not in parmData:
                raise ValueError('Advanced value is required!')
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            attrRecord = self.getRecord('CFG_ATTR', 'ATTR_CODE', parmData['ATTRIBUTE'])
            if not attrRecord:
                printWithNewLines('Attribute %s does not exist!' % parmData['ATTRIBUTE'], 'B')
                return
            else:
                attrRecord['ADVANCED'] = parmData['ADVANCED']
                self.configUpdated = True
                printWithNewLines('Successfully updated!', 'B')
                if self.doDebug:
                    debug(attrRecord)

    def do_updateExpressionFuncVersion(self, arg):

        '\n\tupdateExpressionFuncVersion {"function":"<function_name>", "version":"<version_number>"}\n'

        if not argCheck('updateExpressionFuncVersion', arg, self.do_updateExpressionFuncVersion.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            if 'FUNCTION' not in parmData or len(parmData['FUNCTION']) == 0:
                raise ValueError('Function is required!')
            if 'VERSION' not in parmData or len(parmData['VERSION']) == 0:
                raise ValueError('Version is required!')
            parmData['FUNCTION'] = parmData['FUNCTION'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            funcRecord = self.getRecord('CFG_EFUNC', 'EFUNC_CODE', parmData['FUNCTION'])
            if not funcRecord:
                printWithNewLines('Function %s does not exist!' % parmData['FUNCTION'], 'B')
                return
            else:
                funcRecord['FUNC_VER'] = parmData['VERSION']
                self.configUpdated = True
                printWithNewLines('Successfully updated!', 'B')

                if self.doDebug:
                    debug(funcRecord)

    def do_addComparisonFuncReturnCode(self, arg):

        '\n\taddComparisonFuncReturnCode {"function":"<function_name>", "scoreName":"<score_name>"}' \
        '\n\n\taddComparisonFuncReturnCode {"function":"EMAIL_COMP", "scoreName":"FULL_SCORE"}\n'

        if not argCheck('addComparisonFuncReturnCode', arg, self.do_addComparisonFuncReturnCode.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FUNCTION'] = parmData['FUNCTION'].upper()
            parmData['SCORENAME'] = parmData['SCORENAME'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            cfuncRecord = self.getRecord('CFG_CFUNC', 'CFUNC_CODE', parmData['FUNCTION'])
            if not cfuncRecord:
                printWithNewLines('Function %s does not exist!' % parmData['FUNCTION'], 'B')
                return

            cfuncID = cfuncRecord['CFUNC_ID']

            #-- check for duplicated return codes
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFRTN'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_CFRTN'][i]['CFUNC_ID'] == cfuncID and self.cfgData['G2_CONFIG']['CFG_CFRTN'][i]['CFUNC_RTNVAL'] == parmData['SCORENAME']:
                    printWithNewLines('Comparison function aleady contains return code %s!' % (parmData['SCORENAME']), 'B')
                    return

            maxID = []
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFRTN'])) :
                maxID.append(self.cfgData['G2_CONFIG']['CFG_CFRTN'][i]['CFRTN_ID'])

            cfrtnID = 0
            if 'ID' in parmData:
                cfrtnID = int(parmData['ID'])
            else:
                cfrtnID = max(maxID) + 1 if max(maxID) >= 1000 else 1000

            execOrder = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFRTN'])):
                if self.cfgData['G2_CONFIG']['CFG_CFRTN'][i]['CFUNC_ID'] == cfuncID:
                    if self.cfgData['G2_CONFIG']['CFG_CFRTN'][i]['EXEC_ORDER'] > execOrder:
                        execOrder = self.cfgData['G2_CONFIG']['CFG_CFRTN'][i]['EXEC_ORDER']
            execOrder = execOrder + 1

            sameScore=100
            closeScore=90
            likelyScore=80
            plausibleScore=70
            unlikelyScore=60

            if 'SAMESCORE' in parmData:
                sameScore = int(parmData['SAMESCORE'])
            if 'CLOSESCORE' in parmData:
                closeScore = int(parmData['CLOSESCORE'])
            if 'LIKELYSCORE' in parmData:
                likelyScore = int(parmData['LIKELYSCORE'])
            if 'PLAUSIBLESCORE' in parmData:
                plausibleScore = int(parmData['PLAUSIBLESCORE'])
            if 'UNLIKELYSCORE' in parmData:
                unlikelyScore = int(parmData['UNLIKELYSCORE'])

            newRecord = {}
            newRecord['CFRTN_ID'] = cfrtnID
            newRecord['CFUNC_ID'] = cfuncID
            newRecord['CFUNC_RTNVAL'] = parmData['SCORENAME']
            newRecord['EXEC_ORDER'] = execOrder
            newRecord['SAME_SCORE'] = sameScore
            newRecord['CLOSE_SCORE'] = closeScore
            newRecord['LIKELY_SCORE'] = likelyScore
            newRecord['PLAUSIBLE_SCORE'] = plausibleScore
            newRecord['UN_LIKELY_SCORE'] = unlikelyScore
            self.cfgData['G2_CONFIG']['CFG_CFRTN'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                debug(newRecord)

    def do_addComparisonFunc(self, arg):

        '\n\taddComparisonFunc {"function":"<function_name>", "connectStr":"<plugin_base_name>"}' \
        '\n\n\taddComparisonFunc {"function":"EMAIL_COMP", "connectStr":"g2EmailComp"}\n'

        if not argCheck('addComparisonFunc', arg, self.do_addComparisonFunc.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['FUNCTION'] = parmData['FUNCTION'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            if self.getRecord('CFG_CFUNC', 'CFUNC_CODE', parmData['FUNCTION']):
                printWithNewLines('Function %s already exists!' % parmData['FUNCTION'], 'B')
                return
            else:

                #--default for missing values

                if 'FUNCLIB' not in parmData or len(parmData['FUNCLIB'].strip()) == 0:
                    parmData['FUNCLIB'] = 'INT_LIB'
                if 'VERSION' not in parmData or len(parmData['VERSION'].strip()) == 0:
                    parmData['VERSION'] = '1'
                if 'CONNECTSTR' not in parmData or len(parmData['CONNECTSTR'].strip()) == 0:
                    printWithNewLines('ConnectStr value not specified.', 'B')
                    return

                maxID = []
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_CFUNC'])):
                    maxID.append(self.cfgData['G2_CONFIG']['CFG_CFUNC'][i]['CFUNC_ID'])

                cfuncID = 0
                if 'ID' in parmData:
                    cfuncID = int(parmData['ID'])
                else:
                    cfuncID = max(maxID) + 1 if max(maxID) >= 1000 else 1000

                newRecord = {}
                newRecord['CFUNC_ID'] = cfuncID
                newRecord['CFUNC_CODE'] = parmData['FUNCTION']
                newRecord['CFUNC_DESC'] = parmData['FUNCTION']
                newRecord['FUNC_LIB'] = parmData['FUNCLIB']
                newRecord['FUNC_VER'] = parmData['VERSION']
                newRecord['CONNECT_STR'] = parmData['CONNECTSTR']
                newRecord['ANON_SUPPORT'] = 'Yes'
                newRecord['LANGUAGE'] = parmData['LANGUAGE']
                newRecord['JAVA_CLASS_NAME'] = parmData['JAVACLASSNAME']
                self.cfgData['G2_CONFIG']['CFG_CFUNC'].append(newRecord)
                self.configUpdated = True
                printWithNewLines('Successfully added!', 'B')
                if self.doDebug:
                    debug(newRecord)

    def do_addExpressionCall(self, arg):

        '\n\taddExpressionCall {"element":"<element_name>", "function":"<function_name>", "execOrder":<exec_order>, expressionFeature":<feature_name>, "virtual":"No","elementList": ["<element_detail(s)"]}' \
        '\n\n\taddExpressionCall {"element":"COUNTRY_CODE", "function":"FEAT_BUILDER", "execOrder":100, expressionFeature":"COUNTRY_OF_ASSOCIATION", "virtual":"No","elementList": [{"element":"COUNTRY", "featureLink":"parent", "required":"No"}]}' \
        '\n\n\taddExpressionCall {"element":"COUNTRY_CODE", "function":"FEAT_BUILDER", "execOrder":100, expressionFeature":"COUNTRY_OF_ASSOCIATION", "virtual":"No","elementList": [{"element":"COUNTRY", "feature":"ADDRESS", "required":"No"}]}\n'

        if not argCheck('addExpressionCall', arg, self.do_addExpressionCall.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            if 'ELEMENTLIST' not in parmData or len(parmData['ELEMENTLIST']) == 0:
                raise ValueError('Element list is required!')
            if type(parmData['ELEMENTLIST']) is not list:
                raise ValueError('Element list should be specified as: "elementlist": ["<values>"]\n\n\tNote the [ and ]')
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:

            featureIsSpecified = False
            ftypeID = -1
            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0:
                parmData['FEATURE'] = parmData['FEATURE'].upper()
                ftypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', parmData['FEATURE'])
                if not ftypeRecord:
                    printWithNewLines('Invalid feature: %s.' % parmData['FEATURE'], 'B')
                    return
                featureIsSpecified = True
                ftypeID = ftypeRecord['FTYPE_ID']

            elementIsSpecified = False
            felemID = -1
            if 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:
                parmData['ELEMENT'] = parmData['ELEMENT'].upper()
                felemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', parmData['ELEMENT'])
                if not felemRecord:
                    printWithNewLines('Invalid element: %s.' % parmData['ELEMENT'], 'B')
                    return
                elementIsSpecified = True
                felemID = felemRecord['FELEM_ID']

            if featureIsSpecified is False and elementIsSpecified is False:
                printWithNewLines('No feature or element specified.', 'B')
                return

            if featureIsSpecified is True and elementIsSpecified is True:
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
                efcallID = max(maxID) + 1 if max(maxID) >= 1000 else 1000

            isVirtual = parmData['VIRTUAL'] if 'VIRTUAL' in parmData else 'No'

            efeatFTypeID = -1
            if 'EXPRESSIONFEATURE' in parmData and len(parmData['EXPRESSIONFEATURE']) != 0:
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

                bomFTypeIsSpecified = False
                bomFTypeID = -1
                if 'FEATURE' in elementRecord and len(elementRecord['FEATURE']) != 0:
                    elementRecord['FEATURE'] = elementRecord['FEATURE'].upper()
                    bomFTypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', elementRecord['FEATURE'])
                    if not bomFTypeRecord:
                        printWithNewLines('Invalid BOM feature: %s.' % elementRecord['FEATURE'], 'B')
                        return
                    bomFTypeIsSpecified = True
                    bomFTypeID = bomFTypeRecord['FTYPE_ID']

                bomFElemIsSpecified = False
                bomFElemID = -1
                if 'ELEMENT' in elementRecord and len(elementRecord['ELEMENT']) != 0:
                    elementRecord['ELEMENT'] = elementRecord['ELEMENT'].upper()
                    bomFElemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', elementRecord['ELEMENT'])
                    if not bomFElemRecord:
                        printWithNewLines('Invalid BOM element: %s.' % elementRecord['ELEMENT'], 'B')
                        return
                    bomFElemIsSpecified = True
                    bomFElemID = bomFElemRecord['FELEM_ID']

                if bomFElemIsSpecified is False:
                    printWithNewLines('No BOM element specified on BOM entry.', 'B')
                    return

                bomFTypeFeatureLinkIsSpecified = False
                if 'FEATURELINK' in elementRecord and len(elementRecord['FEATURELINK']) != 0:
                    elementRecord['FEATURELINK'] = elementRecord['FEATURELINK'].upper()
                    if elementRecord['FEATURELINK'] != 'PARENT':
                        printWithNewLines('Invalid feature link value: %s.  (Must use \'parent\')' % elementRecord['FEATURELINK'], 'B')
                        return
                    bomFTypeFeatureLinkIsSpecified = True
                    bomFTypeID = 0

                if bomFTypeIsSpecified is True and bomFTypeFeatureLinkIsSpecified is True:
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
                debug(newRecord)

            #--add elements
            efbomOrder = 0
            for element in parmData['ELEMENTLIST']:
                efbomOrder += 1
                elementRecord = dictKeysUpper(element)

                bomFTypeID = -1
                if 'FEATURE' in elementRecord and len(elementRecord['FEATURE']) != 0:
                    bomFTypeRecord = self.getRecord('CFG_FTYPE', 'FTYPE_CODE', elementRecord['FEATURE'])
                    bomFTypeID = bomFTypeRecord['FTYPE_ID']

                bomFElemID = -1
                if 'ELEMENT' in elementRecord and len(elementRecord['ELEMENT']) != 0:
                    bomFElemRecord = self.getRecord('CFG_FELEM', 'FELEM_CODE', elementRecord['ELEMENT'])
                    bomFElemID = bomFElemRecord['FELEM_ID']

                if 'FEATURELINK' in elementRecord and len(elementRecord['FEATURELINK']) != 0:
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
                    debug(newRecord, 'EFBOM build')

            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')

    def do_deleteExpressionCall(self, arg):
        '\n\tdeleteExpressionCall {"id": "<id>"}\n'

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

    def do_addElement(self, arg):

        '\n\taddElement {"element": "<element_name>"}' \
        '\n\n\taddElement {"element": "<element_name>", "tokenize": "no", "datatype": "no"}' \
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
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])):
                    maxID.append(self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID'])

                if 'ID' in parmData:
                    felemID = int(parmData['ID'])
                else:
                    felemID = max(maxID) + 1 if max(maxID) >= 1000 else 1000

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
                    debug(newRecord)

    def do_addElementToFeature(self, arg):

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
                parmData['DISPLAY_DELIM'] = None

            if 'DISPLAY_LEVEL' not in parmData:
                parmData['DISPLAY_LEVEL'] = 0

            #--does the element exist already and has conflicting parms to what was requested?
            if felemRecord:
                felemID = felemRecord['FELEM_ID']
                if (
                    ( parmData['DATATYPE'] and len(parmData['DATATYPE'].strip()) > 0 and parmData['DATATYPE'] != felemRecord['DATA_TYPE'] ) or
                    ( parmData['TOKENIZE'] and len(parmData['TOKENIZE'].strip()) > 0 and parmData['TOKENIZE'] != felemRecord['TOKENIZE'] )
                ):
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
                        felemID = maxID + 1 if maxID >= 1000 else 1000

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
                        debug(newRecord)

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
                    debug(newRecord)

    def do_setFeatureElementDisplayLevel(self, arg):

        '\n\tsetFeatureElementDisplayLevel {"feature": "<feature_name>", "element": "<element_name>", "display_level": <display_level>}\n'

        if not argCheck('setFeatureElementDisplayLevel', arg, self.do_setFeatureElementDisplayLevel.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 and 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:

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

            if 'DISPLAY_LEVEL' in parmData:
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
                            debug(self.cfgData['G2_CONFIG']['CFG_FBOM'][i])

    def do_setFeatureElementDerived(self, arg):

        '\n\tsetFeatureElementDerived {"feature": "<feature_name>", "element": "<element_name>", "derived": <display_level>}\n'

        if not argCheck('setFeatureElementDerived', arg, self.do_setFeatureElementDerived.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 and 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:

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

            if 'DERIVED' in parmData:
                derived = parmData['DERIVED']
            else:
                printWithNewLines('Derived status must be specified!', 'B')
                return

            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FBOM'])):
                if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FTYPE_ID']) == ftypeRecord['FTYPE_ID']:
                    if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FELEM_ID']) == felemRecord['FELEM_ID']:
                        self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['DERIVED'] = derived
                        self.configUpdated = True
                        printWithNewLines('Feature element derived status updated!', 'B')
                        if self.doDebug:
                            debug(self.cfgData['G2_CONFIG']['CFG_FBOM'][i])

    def do_deleteElementFromFeature(self, arg):
        '\n\tdeleteElementFromFeature {"feature": "<feature_name>", "element": "<element_name>"}\n'

        if not argCheck('deleteElementFromFeature', arg, self.do_deleteElementFromFeature.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            if 'FEATURE' in parmData and len(parmData['FEATURE']) != 0 and 'ELEMENT' in parmData and len(parmData['ELEMENT']) != 0:

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
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_FBOM'])-1, -1, -1):
                if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FTYPE_ID']) == ftypeRecord['FTYPE_ID'] and int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FELEM_ID']) == felemRecord['FELEM_ID'] :
                    del self.cfgData['G2_CONFIG']['CFG_FBOM'][i]
                    deleteCnt = 1
                    self.configUpdated = True

            if deleteCnt == 0:
                printWithNewLines('Record not found!', 'B')
            else:
                printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    def do_deleteElement(self, arg):
        '\n\tdeleteElement {"feature": "<feature_name>", "element": "<element_name>"}\n'

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
                if int(self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FELEM_ID']) == felemRecord['FELEM_ID']:
                    for j in range(len(self.cfgData['G2_CONFIG']['CFG_FTYPE'])):
                        if int(self.cfgData['G2_CONFIG']['CFG_FTYPE'][j]['FTYPE_ID']) == self.cfgData['G2_CONFIG']['CFG_FBOM'][i]['FTYPE_ID']:
                            usedIn.append(self.cfgData['G2_CONFIG']['CFG_FTYPE'][j]['FTYPE_CODE'])
            if usedIn:
                printWithNewLines('Can\'t delete %s, it is used in these feature(s): %s' % (parmData['ELEMENT'], usedIn), 'B')
                return
            else:
                deleteCnt = 0
                for i in range(len(self.cfgData['G2_CONFIG']['CFG_FELEM'])):
                    if int(self.cfgData['G2_CONFIG']['CFG_FELEM'][i]['FELEM_ID']) == felemRecord['FELEM_ID']:
                        del self.cfgData['G2_CONFIG']['CFG_FELEM'][i]
                        deleteCnt = 1
                        self.configUpdated = True
                        break

                if deleteCnt == 0:
                    printWithNewLines('Record not found!', 'B')
                else:
                    printWithNewLines('%s rows deleted!' % deleteCnt, 'B')

    def do_listExpressionCalls(self, arg):
        '\n\tlistExpressionCalls [search_value]\n'

        efcallList = []
        for efcallRecord in sorted(self.cfgData['G2_CONFIG']['CFG_EFCALL'], key=lambda k: (k['FTYPE_ID'], k['EXEC_ORDER'])):
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
                efcallDict['expressionFeature'] = ftypeRecord2['FTYPE_CODE']

            efbomList = []
            for efbomRecord in [record for record in self.cfgData['G2_CONFIG']['CFG_EFBOM'] if record['EFCALL_ID'] == efcallRecord['EFCALL_ID']]:
                ftypeRecord3 = self.getRecord('CFG_FTYPE', 'FTYPE_ID', efbomRecord['FTYPE_ID'])
                felemRecord3 = self.getRecord('CFG_FELEM', 'FELEM_ID', efbomRecord['FELEM_ID'])

                efbomDict = {}
                if efbomRecord['FTYPE_ID'] == 0:
                    efbomDict['featureLink'] = 'parent'
                elif ftypeRecord3:
                    efbomDict['feature'] = ftypeRecord3['FTYPE_CODE']
                if felemRecord3:
                    efbomDict['element'] = felemRecord3['FELEM_CODE']
                else:
                    efbomDict['element'] = str(efbomRecord['FELEM_ID'])
                efbomDict['required'] = efbomRecord['FELEM_REQ']
                efbomList.append(efbomDict)
            efcallDict['elementList'] = efbomList

            efcallList.append(efcallDict)

        print()
        for efcallDict in efcallList:
            if arg and arg.lower() not in str(efcallDict).lower():
                continue
            printWithNewLines(json.dumps(efcallDict), 'E')
        print()

# ===== misc commands =====

    def do_setDistinct(self, arg):
        '\n\tDistinct processing only compares the most complete feature values for an entity. You may want to turn this off for watch list checking.' \
        '\n\n\tSyntax:' \
        '\n\t\tsetDistinct on ' \
        '\n\t\tsetDistinct off\n'

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

    def do_listGenericThresholds(self, arg):
        '\n\tlistGenericThresholds [search_value]\n'

        planCode = {}
        planCode[1] = 'load'
        planCode[2] = 'search'
        print()
        for thisRecord in sorted(self.getRecordList('CFG_GENERIC_THRESHOLD'), key=lambda k: k['GPLAN_ID']):
            if arg and arg.lower() not in str(thisRecord).lower():
                continue
            print('{"plan": "%s", "behavior": "%s", "candidateCap": %s, "scoringCap": %s}' % (planCode[thisRecord['GPLAN_ID']], thisRecord['BEHAVIOR'], thisRecord['CANDIDATE_CAP'], thisRecord['SCORING_CAP']))
            #{
            #    "BEHAVIOR": "NAME",
            #    "CANDIDATE_CAP": 10,
            #    "FTYPE_ID": 0,
            #    "GPLAN_ID": 1,
            #    "SCORING_CAP": -1,
            #    "SEND_TO_REDO": "No"
            #},
        print()

    def do_setGenericThreshold(self, arg):
        '\n\tsetGenericThreshold {"plan": "load", "behavior": "<behavior_type>", "scoringCap": 99}' \
        '\n\tsetGenericThreshold {"plan": "search", "behavior": "<behavior_type>", "candidateCap": 99}\n'

        if not argCheck('setGenericThreshold', arg, self.do_setGenericThreshold.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['PLAN'] = {'LOAD': 1, 'SEARCH': 2}[parmData['PLAN'].upper()]
            parmData['BEHAVIOR'] = parmData['BEHAVIOR'].upper()
        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:
            print()

            #--lookup threshold and error if doesn't exist
            listID = -1
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_GENERIC_THRESHOLD'])):
                if self.cfgData['G2_CONFIG']['CFG_GENERIC_THRESHOLD'][i]['GPLAN_ID'] == parmData['PLAN'] and self.cfgData['G2_CONFIG']['CFG_GENERIC_THRESHOLD'][i]['BEHAVIOR'] == parmData['BEHAVIOR']:
                    listID = i
            if listID == -1:
                printWithNewLines('Threshold does not exist!')
                return

            #--make the updates
            if 'CANDIDATECAP' in parmData:
                self.cfgData['G2_CONFIG']['CFG_GENERIC_THRESHOLD'][listID]['CANDIDATE_CAP'] = int(parmData['CANDIDATECAP'])
                printWithNewLines('Candidate cap updated!')
                self.configUpdated = True
            if 'SCORINGCAP' in parmData:
                self.cfgData['G2_CONFIG']['CFG_GENERIC_THRESHOLD'][listID]['SCORING_CAP'] = int(parmData['SCORINGCAP'])
                printWithNewLines('Scoring cap updated!')
                self.configUpdated = True

            print()


# ===== template commands =====

    def do_templateAdd(self, arg):
        '\nFull syntax:' \
        '\n\ttemplateAdd {"feature": "<name>", "template": "<template>", "behavior": "<optional-overide>", "comparison": "<optional-overide>}' \
        '\n\nTypical use: (behavior and comparison are optional)' \
        '\n\ttemplateAdd {"feature": "customer_number", "template": "global_id"}' \
        '\n\ttemplateAdd {"feature": "customer_number", "template": "global_id", "behavior": "F1E"}' \
        '\n\ttemplateAdd {"feature": "customer_number", "template": "global_id", "behavior": "F1E", "comparison": "exact_comp"}' \
        '\n\nType "templateAdd List" to get a list of valid templates.\n'

        validTemplates = {}

        validTemplates['GLOBAL_ID'] = {'DESCRIPTION': 'globally unique identifer (like an ssn, a credit card, or a medicare_id)',
                                       'BEHAVIOR': ['F1', 'F1E', 'F1ES'],
                                       'CANDIDATES': ['No'],
                                       'STANDARDIZE': ['PARSE_ID'],
                                       'EXPRESSION': ['EXPRESS_ID'],
                                       'COMPARISON': ['ID_COMP', 'EXACT_COMP'],
                                       'FEATURE_CLASS': 'ISSUED_ID',
                                       'ATTRIBUTE_CLASS': 'IDENTIFIER',
                                       'ELEMENTS': [{'element': 'ID_NUM', 'expressed': 'No', 'compared': 'no', 'display': 'Yes'},
                                                    {'element': 'ID_NUM_STD', 'expressed': 'Yes', 'compared': 'yes', 'display': 'No'}],
                                       'ATTRIBUTES': [{'attribute': '<feature>', 'element': 'ID_NUM', 'required': 'Yes'}]}

        validTemplates['STATE_ID'] = {'DESCRIPTION': 'state issued identifier (like a drivers license)',
                                      'BEHAVIOR': ['F1', 'F1E', 'F1ES'],
                                      'CANDIDATES': ['No'],
                                      'STANDARDIZE': ['PARSE_ID'],
                                      'EXPRESSION': ['EXPRESS_ID'],
                                      'COMPARISON': ['ID_COMP'],
                                      'FEATURE_CLASS': 'ISSUED_ID',
                                      'ATTRIBUTE_CLASS': 'IDENTIFIER',
                                      'ELEMENTS': [{'element': 'ID_NUM', 'expressed': 'No', 'compared': 'no', 'display': 'Yes'},
                                                   {'element': 'STATE', 'expressed': 'No', 'compared': 'yes', 'display': 'Yes'},
                                                   {'element': 'ID_NUM_STD', 'expressed': 'Yes', 'compared': 'yes', 'display': 'No'}],
                                      'ATTRIBUTES': [{'attribute': '<feature>_NUMBER', 'element': 'ID_NUM', 'required': 'Yes'},
                                                     {'attribute': '<feature>_STATE', 'element': 'STATE', 'required': 'No'}]}

        validTemplates['COUNTRY_ID'] = {'DESCRIPTION': 'country issued identifier (like a passport)',
                                        'BEHAVIOR': ['F1', 'F1E', 'F1ES'],
                                        'CANDIDATES': ['No'],
                                        'STANDARDIZE': ['PARSE_ID'],
                                        'EXPRESSION': ['EXPRESS_ID'],
                                        'COMPARISON': ['ID_COMP'],
                                        'FEATURE_CLASS': 'ISSUED_ID',
                                        'ATTRIBUTE_CLASS': 'IDENTIFIER',
                                        'ELEMENTS': [{'element': 'ID_NUM', 'expressed': 'No', 'compared': 'no', 'display': 'Yes'},
                                                     {'element': 'COUNTRY', 'expressed': 'No', 'compared': 'yes', 'display': 'Yes'},
                                                     {'element': 'ID_NUM_STD', 'expressed': 'Yes', 'compared': 'yes', 'display': 'No'}],
                                        'ATTRIBUTES': [{'attribute': '<feature>_NUMBER', 'element': 'ID_NUM', 'required': 'Yes'},
                                                       {'attribute': '<feature>_COUNTRY', 'element': 'COUNTRY', 'required': 'No'}]}

        if arg and arg.upper() == 'LIST':
            print()
            for template in validTemplates:
                print('\t', template, '-', validTemplates[template]['DESCRIPTION'])
                print('\t\tbehaviors:', validTemplates[template]['BEHAVIOR'])
                print('\t\tcomparisons:', validTemplates[template]['COMPARISON'])
                print()
            return

        if not argCheck('templateAdd', arg, self.do_templateAdd.__doc__):
            return
        try:
            parmData = dictKeysUpper(json.loads(arg))
        except (ValueError, KeyError) as e:
            argError(arg, e)
            return

        feature = parmData['FEATURE'].upper() if 'FEATURE' in parmData else None
        template = parmData['TEMPLATE'].upper() if 'TEMPLATE' in parmData else None
        behavior = parmData['BEHAVIOR'].upper() if 'BEHAVIOR' in parmData else None
        comparison = parmData['COMPARISON'].upper() if 'COMPARISON' in parmData else None

        standardize = parmData['STANDARDIZE'].upper() if 'STANDARDIZE' in parmData else None
        expression = parmData['EXPRESSION'].upper() if 'EXPRESSION' in parmData else None
        candidates = parmData['CANDIDATES'].upper() if 'Candidates' in parmData else None

        if not feature:
            printWithNewLines('A new feature name is required', 'B')
            return
        if self.getRecord('CFG_FTYPE', 'FTYPE_CODE', feature):
            printWithNewLines('Feature already exists!', 'B')
            return

        if not template:
            printWithNewLines('A valid template name is required', 'B')
            return
        if template not in validTemplates:
            printWithNewLines('template name supplied is not valid', 'B')
            return

        if not behavior:
            behavior = validTemplates[template]['BEHAVIOR'][0]
        if behavior not in validTemplates[template]['BEHAVIOR']:
            printWithNewLines('behavior code supplied is not valid for template', 'B')
            return

        if not comparison:
            comparison = validTemplates[template]['COMPARISON'][0]
        if comparison not in validTemplates[template]['COMPARISON']:
            printWithNewLines('comparison code supplied is not valid for template', 'B')
            return

        if not standardize:
            standardize = validTemplates[template]['STANDARDIZE'][0]
        if standardize not in validTemplates[template]['STANDARDIZE']:
            printWithNewLines('standarize code supplied is not valid for template', 'B')
            return

        if not expression:
            expression = validTemplates[template]['EXPRESSION'][0]
        if expression not in validTemplates[template]['EXPRESSION']:
            printWithNewLines('expression code supplied is not valid for template', 'B')
            return

        if not candidates:
            candidates = validTemplates[template]['CANDIDATES'][0]
        if candidates not in validTemplates[template]['CANDIDATES']:
            printWithNewLines('candidates setting supplied is not valid for template', 'B')
            return

        #--values that can't be overridden
        featureClass = validTemplates[template]['FEATURE_CLASS']
        attributeClass = validTemplates[template]['ATTRIBUTE_CLASS']

        #--exact comp corrections
        if comparison == 'EXACT_COMP':
            standardize = ''
            expression = ''
            candidates = 'Yes'

        #--build the feature
        featureData = {'feature': feature,
                       'behavior': behavior,
                       'class': featureClass,
                       'candidates': candidates,
                       'standardize': standardize,
                       'expression': expression,
                       'comparison': comparison,
                       'elementList': []}
        for elementDict in validTemplates[template]['ELEMENTS']:
            if not expression:
                elementDict['expressed'] = 'No'
            if not standardize:
                if elementDict['display'] == 'Yes':
                    elementDict['compared'] = 'Yes'
                else:
                    elementDict['compared'] = 'No'
            featureData['elementList'].append(elementDict)

        featureParm = json.dumps(featureData)
        printWithNewLines('addFeature %s' % featureParm, 'S')
        self.do_addFeature(featureParm)

        #--build the attributes
        for attributeDict in validTemplates[template]['ATTRIBUTES']:
            attributeDict['attribute'] = attributeDict['attribute'].replace('<feature>', feature)

            attributeData = {'attribute': attributeDict['attribute'].upper(),
                             'class': attributeClass,
                             'feature': feature,
                             'element': attributeDict['element'].upper(),
                             'required': attributeDict['required']}

            attributeParm = json.dumps(attributeData)
            printWithNewLines('addAttribute %s' % attributeParm, 'S')
            self.do_addAttribute(attributeParm)

        return


# ===== fragment commands =====

    def getFragmentJson(self, record):

        return f'{{' \
               f'"id": "{record["ERFRAG_ID"]}", ' \
               f'"fragment": "{record["ERFRAG_CODE"]}", ' \
               f'"source": "{record["ERFRAG_SOURCE"]}", ' \
               f'"depends": "{record["ERFRAG_DEPENDS"]}"' \
               f'}}'

    def do_listFragments(self, arg):
        '\n\tlistFragments [search_value]\n'

        print()
        for thisRecord in sorted(self.getRecordList('CFG_ERFRAG'), key=lambda k: k['ERFRAG_ID']):
            if arg and arg.lower() not in str(thisRecord).lower():
                continue
            printWithNewLines(self.getFragmentJson(thisRecord), 'E')
        print()

    def do_getFragment(self, arg):
        '\n\tgetFragment {"id": "<fragment_id>"}' \
        '\n\tgetFragment {"fragment": "<fragment_code>"}\n'

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
                print()
                for thisRecord in sorted(foundRecords, key=lambda k: k['ERFRAG_ID']):
                    self.printJsonResponse(self.getFragmentJson(thisRecord))
                print()

    def do_deleteFragment(self, arg):
        '\n\tdeleteFragment {"id": "<fragment_id>"}' \
        '\n\tdeleteFragment {"fragment": "<fragment_code>"}\n'

        if not argCheck('deleteFragment', arg, self.do_deleteFragment.__doc__):
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

    def do_setFragment(self, arg):
        '\n\tsetFragment {"id": "<fragment_id>", "fragment": "<fragment_code>", "source": "<fragment_source>"}\n'

        if not argCheck('setFragment', arg, self.do_setFragment.__doc__):
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
            print()

            #--lookup fragment and error if doesn't exist
            listID = -1
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERFRAG'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i][searchField] == searchValue:
                    listID = i
            if listID == -1:
                printWithNewLines('Fragment does not exist!')
                return

            #--make the updates
            for parmCode in parmData:
                if parmCode == 'ID':
                    pass

                elif parmCode == 'SOURCE':
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
                                if thisChar in '| =><)':
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

            self.cfgData['G2_CONFIG']['CFG_ERFRAG'][listID]['ERFRAG_SOURCE'] = parmData['SOURCE']
            self.cfgData['G2_CONFIG']['CFG_ERFRAG'][listID]['ERFRAG_DEPENDS'] = ','.join(dependencyList)
            printWithNewLines('Fragment source updated!')
            self.configUpdated = True

            print()

    def do_addFragment(self, arg):
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
                printWithNewLines('A fragment source field is required!', 'B')
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
                        if thisChar in '| =><)':
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
                debug(newRecord)


    def do_copyFragment(self, arg):
        '\n\tcopyFragment {"currentFragment": "<fragment_code>", "newID": "<fragment_id>", "newFragment": "<fragment_code>"}\n'

        if not argCheck('copyFragment', arg, self.do_copyFragment.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            parmData['CURRENTFRAGMENT'] = parmData['CURRENTFRAGMENT'].upper()
            parmData['NEWFRAGMENT'] = parmData['NEWFRAGMENT'].upper()
        except (ValueError, KeyError) as e:
            print('\nError with argument(s) or parsing JSON - %s \n' % e)
        else:

            #--lookup fragment and error if doesn't exist
            currentFragmentID = -1
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERFRAG'])-1, -1, -1):
                if self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_CODE'] == parmData['CURRENTFRAGMENT']:
                    currentFragmentID = i
            if currentFragmentID == -1:
                printWithNewLines('Fragment %s does not exist!' % parmData['CURRENTFRAGMENT'], 'B')
                return

            maxID = 0
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERFRAG'])):
                if self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_CODE'] == parmData['NEWFRAGMENT']:
                    printWithNewLines('Fragment %s already exists!' % parmData['NEWFRAGMENT'], 'B')
                    return
                if 'NEWID' in parmData and int(self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_ID']) == int(parmData['NEWID']):
                    printWithNewLines('Fragment ID %s already exists!' % parmData['NEWID'], 'B')
                    return
                if self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_ID'] > maxID:
                    maxID = self.cfgData['G2_CONFIG']['CFG_ERFRAG'][i]['ERFRAG_ID']

            if 'NEWID' not in parmData:
                parmData['NEWID'] = maxID + 1 if maxID >= 1000 else 1000

            fragmentCopy = self.cfgData['G2_CONFIG']['CFG_ERFRAG'][currentFragmentID].copy()
            fragmentCopy['ERFRAG_ID'] = int(parmData['NEWID'])
            fragmentCopy['ERFRAG_CODE'] = parmData['NEWFRAGMENT']
            self.cfgData['G2_CONFIG']['CFG_ERFRAG'].append(fragmentCopy)
            self.configUpdated = True
            printWithNewLines('Successfully copied!', 'B')
            if self.doDebug:
                debug(fragmentCopy)


# ===== rule commands =====

    def getRuleJson(self, record):

        return (
            f'{{'
            f'"id": {record["ERRULE_ID"]}, '
            f'"rule": "{record["ERRULE_CODE"]}", '
            f'"desc": "{record["ERRULE_DESC"]}", '
            f'"resolve": "{record["RESOLVE"]}", '
            f'"relate": "{record["RELATE"]}", '
            f'"ref_score": {record["REF_SCORE"]}, '
            f'"fragment": "{record["QUAL_ERFRAG_CODE"]}", '
            f'"disqualifier": "{showNullableJsonNumeric(record["DISQ_ERFRAG_CODE"])}", '
            f'"rtype_id": {showNullableJsonNumeric(record["RTYPE_ID"])}, '
            f'"tier": {showNullableJsonNumeric(record["ERRULE_TIER"])}'
            f'}}'
        )

    def do_listRules(self, arg):
        '\n\tlistRules [search_value]\n'

        print()
        for ruleRecord in sorted(self.getRecordList('CFG_ERRULE'), key=lambda k: k['ERRULE_ID']):
            if arg and arg.lower() not in str(ruleRecord).lower():
                continue
            printWithNewLines(self.getRuleJson(ruleRecord), 'E')

    def do_getRule(self, arg):
        '\n\tgetRule {"id": "<rule_id>"}\n'

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
                print()
                for thisRecord in sorted(foundRecords, key=lambda k: k['ERRULE_ID']):
                    self.printJsonResponse(self.getRuleJson(thisRecord))
                print()

    def do_deleteRule(self, arg):
        '\n\tdeleteRule {"id": "<rule_id>"}\n'

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
                searchValue = arg.upper()
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
                printWithNewLines('Record not found!', 'S')
            printWithNewLines('%s rows deleted!' % deleteCnt, 'B')


    def do_setRule(self, arg):
        '\n\tsetRule {"id": <rule_id>, "rule": "<rule_name>", "desc": "<description>", "fragment": "<fragment_name>", "disqualifier": "<disqualifier_name>"}\n'

        if not argCheck('setRule', arg, self.do_setRule.__doc__):
            return

        try:
            parmData = dictKeysUpper(json.loads(arg))
            if not isinstance(parmData['ID'], int):
                parmData['ID'] = int(parmData['ID'])
            if parmData['RESOLVE'] and parmData['RESOLVE'].upper() not in ('YES', 'NO'):
                printWithNewLines('ERROR: Resolve should be Yes or No', 'B')
                return
            if parmData['RELATE'] and parmData['RELATE'].upper() not in ('YES', 'NO'):
                printWithNewLines('ERROR: Relate should be Yes or No', 'B')
                return

        except (ValueError, KeyError) as e:
            argError(arg, e)
        else:
            print()

            #--lookup rule and error if doesn't exist
            listID = -1
            for i in range(len(self.cfgData['G2_CONFIG']['CFG_ERRULE'])):
                if self.cfgData['G2_CONFIG']['CFG_ERRULE'][i]['ERRULE_ID'] == parmData['ID']:
                    listID = i
            if listID == -1:
                printWithNewLines('Rule %s does not exist!' % parmData['ID'])
                return

            #--make the updates
            for parmCode in parmData:
                if parmCode == 'ID':
                    pass

                elif parmCode == 'RULE':
                    self.cfgData['G2_CONFIG']['CFG_ERRULE'][listID]['ERRULE_CODE'] = parmData['RULE']
                    printWithNewLines('Rule code updated!')
                    self.configUpdated = True

                elif parmCode == 'DESC':
                    self.cfgData['G2_CONFIG']['CFG_ERRULE'][listID]['ERRULE_DESC'] = parmData['DESC']
                    printWithNewLines('Rule description updated!')
                    self.configUpdated = True

                elif parmCode == 'FRAGMENT':
                    self.cfgData['G2_CONFIG']['CFG_ERRULE'][listID]['QUAL_ERFRAG_CODE'] = parmData['FRAGMENT']
                    printWithNewLines('Rule fragment updated!')
                    self.configUpdated = True

                elif parmCode == 'DISQUALIFIER':
                    self.cfgData['G2_CONFIG']['CFG_ERRULE'][listID]['DISQ_ERFRAG_CODE'] = parmData['DISQUALIFIER']
                    printWithNewLines('Rule disqualifier updated!')
                    self.configUpdated = True

                elif parmCode == 'RESOLVE':
                    self.cfgData['G2_CONFIG']['CFG_ERRULE'][listID]['RESOLVE'] = parmData['RESOLVE'].capitalize()
                    printWithNewLines('Rule resolve updated!')
                    self.configUpdated = True

                elif parmCode == 'RELATE':
                    self.cfgData['G2_CONFIG']['CFG_ERRULE'][listID]['RELATE'] = parmData['RELATE'].capitalize()
                    printWithNewLines('Rule relate updated!')
                    self.configUpdated = True

            print()

    def do_addRule(self, arg):
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
                printWithNewLines('A fragment source field is required!', 'B')
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

            if 'TIER' not in parmData or not parmData['TIER']:
                parmData['TIER'] = None
            else:
                if not isinstance(parmData['TIER'], int) and parmData['TIER'].lower() != 'null':
                    try:
                        parmData['TIER'] = int(parmData['TIER'])
                    except ValueError:
                        printWithNewLines(f'Invalid TIER value ({parmData["TIER"]}), should be an integer', 'B')
                        return

            newRecord = {}
            newRecord['ERRULE_ID'] = int(parmData['ID'])
            newRecord['ERRULE_CODE'] = parmData['RULE']
            newRecord['ERRULE_DESC'] = parmData['DESC'] if 'DESC' in parmData and parmData['DESC'] else parmData['RULE']
            newRecord['RESOLVE'] = parmData['RESOLVE']
            newRecord['RELATE'] = parmData['RELATE']
            newRecord['REF_SCORE'] = int(parmData['REF_SCORE'])
            newRecord['RTYPE_ID'] = int(parmData['RTYPE_ID'])
            newRecord['QUAL_ERFRAG_CODE'] = parmData['FRAGMENT']
            newRecord['DISQ_ERFRAG_CODE'] = storeNullableJsonString(parmData['DISQUALIFIER'])
            newRecord['ERRULE_TIER'] = parmData['TIER']

            self.cfgData['G2_CONFIG']['CFG_ERRULE'].append(newRecord)
            self.configUpdated = True
            printWithNewLines('Successfully added!', 'B')
            if self.doDebug:
                debug(newRecord)

# ===== system parameters  =====

    def do_listSystemParameters(self, arg):
        '\n\tlistSystemParameters\n'

        for i in self.cfgData['G2_CONFIG']['CFG_RTYPE']:
            if i["RCLASS_ID"] == 2:
                print(f'\n{{"relationshipsBreakMatches": "{i["BREAK_RES"]}"}}\n')
                break

    def do_setSystemParameter(self, arg):
        '\n\tsetSystemParameter {"parameter": "<value>"}\n'

        validParameters = ('relationshipsBreakMatches')
        if not argCheck('templateAdd', arg, self.do_setSystemParameter.__doc__):
            return
        try:
            parmData = json.loads(arg)  # --don't want these upper
        except (ValueError, KeyError) as e:
            argError(arg, e)
            return

        #--not really expecting a list here, getting the dictionary key they used
        for parameterCode in parmData:
            parameterValue = parmData[parameterCode]

            if parameterCode not in validParameters:
                printWithNewLines('%s is an invalid system parameter' % parameterCode, 'B')

            #--set all disclosed relationship types to break or not break matches
            elif parameterCode == 'relationshipsBreakMatches':
                if parameterValue.upper() in ('YES', 'Y'):
                    breakRes = 1
                elif parameterValue.upper() in ('NO', 'N'):
                    breakRes = 0
                else:
                    printWithNewLines('%s is an invalid parameter for %s' % (parameterValue, parameterCode), 'B')
                    return

                for i in range(len(self.cfgData['G2_CONFIG']['CFG_RTYPE'])):
                    if self.cfgData['G2_CONFIG']['CFG_RTYPE'][i]['RCLASS_ID'] == 2:
                        self.cfgData['G2_CONFIG']['CFG_RTYPE'][i]['BREAK_RES'] = breakRes
                        self.configUpdated = True

    def do_touch(self, arg):
        '\n\tMarks configuration object as modified when no configuration changes have been applied yet.\n'

        # This is a no-op. It marks the configuration as modified, without doing anything to it.
        self.configUpdated = True
        print()

# ===== match levels  =====

    def do_listMatchLevels(self, arg):
        '\n\tlistMatchLevels [search_value]\n'

        print()
        for rtypeRecord in sorted(self.getRecordList('CFG_RTYPE'), key=lambda k: k['RTYPE_ID']):
            if arg and arg.lower() not in str(rtypeRecord).lower():
                continue
            print(f'{{"level": {rtypeRecord["RTYPE_ID"]}, "code": "{rtypeRecord["RTYPE_CODE"]}", "class": "{self.getRecord("CFG_RCLASS", "RCLASS_ID", rtypeRecord["RCLASS_ID"])["RCLASS_DESC"]}"}}')
        print()


# ===== Class Utils =====

    def printResponse(self, response):

        if response:
            self.jsonOutput(response.decode().rstrip())
        else:
            printWithNewLines('Empty response!', 'B')

    def printJsonResponse(self, response):
        '''  '''

        if response:
            response = response.decode() if isinstance(response, bytearray) else response

            if self.pygmentsInstalled:
                if self.prettyPrint:
                    print('\n' + highlight(json.dumps(json.loads(response), indent=2), lexers.JsonLexer(), formatters.TerminalFormatter()))
                    return
            else:
                if self.prettyPrint:
                    print(f'\n {json.dumps(json.loads(response), indent=2)} \n')
                    return

            printWithNewLines(f'{response}', 'B')
            return

        printWithNewLines('Empty response!', 'B')


# ===== Utility functions =====

def parse(argumentString):
    'Parses an argument list into a logical set of argument strings'

    return shlex.split(argumentString)


def getFeatureBehavior(feature):

    featureBehavior = feature['FTYPE_FREQ']
    if str(feature['FTYPE_EXCL']).upper() in ('1', 'Y', 'YES'):
        featureBehavior += 'E'
    if str(feature['FTYPE_STAB']).upper() in ('1', 'Y', 'YES'):
        featureBehavior += 'S'
    return featureBehavior


def parseFeatureBehavior(behaviorCode):

    behaviorDict = {"EXCLUSIVITY": 'No', "STABILITY": 'No'}
    if behaviorCode not in ('NAME','NONE'):
        if 'E' in behaviorCode:
            behaviorDict['EXCLUSIVITY'] = 'Yes'
            behaviorCode = behaviorCode.replace('E','')
        if 'S' in behaviorCode:
            behaviorDict['STABILITY'] = 'Yes'
            behaviorCode = behaviorCode.replace('S','')
    if behaviorCode in ('A1', 'F1', 'FF', 'FM', 'FVM', 'NONE', 'NAME'):
        behaviorDict['FREQUENCY'] = behaviorCode
    else:
        behaviorDict = None
    return behaviorDict


def argCheck(func, arg, docstring):

    if len(arg.strip()) == 0:
        printWithNewLines(f'Missing argument(s), syntax: \n\n{docstring[1:]}', 'B')
        return False

    return True


def argError(errorArg, error):

    printWithNewLines(f'Incorrect argument(s) or error parsing argument: {errorArg}', 'S')
    printWithNewLines(f'Error: {error}', 'E')


def printWithNewLines(ln, pos=''):

    pos = pos.upper()
    if pos == 'S' or pos == 'START':
        print(f'\n{ln}')
    elif pos == 'E' or pos == 'END':
        print(f'{ln}\n')
    elif pos == 'B' or pos == 'BOTH':
        print(f'\n{ln}\n')
    else:
        print(f'{ln}')


def printResponse(response):

    if response:
        response = response.decode() if isinstance(response, bytearray) else response
        print(f'\n{response}\n')
        return

    printWithNewLines('Empty response!', 'B')


def dictKeysUpper(dictionary):
    if isinstance(dictionary, list):
        return [v.upper() for v in dictionary]
    elif isinstance(dictionary, dict):
        return {k.upper(): v for k, v in dictionary.items()}
    else:
        return dictionary


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


def debug(data, loc=''):

    printWithNewLines(textwrap.dedent(f'''\
    <--- DEBUG
    Func: {sys._getframe(1).f_code.co_name}
    Loc: {loc}
    Data: {data}
    --->
    '''), 'E')


if __name__ == '__main__':

    argParser = argparse.ArgumentParser()
    argParser.add_argument("fileToProcess", default=None, nargs='?')
    argParser.add_argument('-c', '--ini-file-name', dest='ini_file_name', default=None, help='name of a G2Module.ini file to use')
    argParser.add_argument('-f', '--force', dest='forceMode', default=False, action='store_true', help='when reading from a file, execute each command without prompts')
    argParser.add_argument('-H', '--histDisable', dest='histDisable', action='store_true', default=False, help='disable history file usage')
    args = argParser.parse_args()

    # If ini file isn't specified try and locate it with G2Paths
    ini_file_name = pathlib.Path(G2Paths.get_G2Module_ini_path()) if not args.ini_file_name else pathlib.Path(args.ini_file_name).resolve()
    G2Paths.check_file_exists_and_readable(ini_file_name)

    # Warn if using out dated parms
    g2health = G2Health()
    g2health.checkIniParams(ini_file_name)

    # Older Senzing using G2CONFIGFILE, e.g, G2CONFIGFILE=/opt/senzing/g2/python/g2config.json
    iniParamCreator = G2IniParams()
    g2ConfigFile = iniParamCreator.getINIParam(ini_file_name, 'SQL', 'G2CONFIGFILE')

    # Get the INI paramaters to use
    g2module_params = iniParamCreator.getJsonINIParams(ini_file_name)

    cmd_obj = G2CmdShell(g2ConfigFile, args.histDisable, args.forceMode, args.fileToProcess, ini_file_name)

    if args.fileToProcess:
        cmd_obj.fileloop()
    else:
        cmd_obj.cmdloop()

    sys.exit()
