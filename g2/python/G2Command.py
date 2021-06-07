#! /usr/bin/env python3

import argparse
import cmd
import csv
import inspect
import json
import os
import pathlib
import shlex
import sys
import textwrap
from collections import OrderedDict
from timeit import default_timer as timer

import G2Exception
import G2Paths
from G2Config import G2Config
from G2ConfigMgr import G2ConfigMgr
from G2Diagnostic import G2Diagnostic
from G2Engine import G2Engine
from G2Hasher import G2Hasher
from G2Health import G2Health
from G2IniParams import G2IniParams
from G2Product import G2Product

try:
    import readline
    import atexit
except ImportError:
    readline = None


class G2CmdShell(cmd.Cmd, object):

    #Override function from cmd module to make command completion case insensitive 
    def completenames(self, text, *ignored):
        dotext = 'do_'+text                                                                                                                                                              
        return  [a[3:] for a in self.get_names() if a.lower().startswith(dotext.lower())]

    def __init__(self, debug_trace, hist_disable, ini_file = None):

        cmd.Cmd.__init__(self)

        # Cmd Module settings
        self.intro = ''
        self.prompt = '(g2cmd) '
        self.ruler = '-'
        self.doc_header = 'Senzing APIs'
        self.misc_header  = 'Help Topics (help <topic>)'
        self.undoc_header = 'Misc Commands'
        self.__hidden_methods = ('do_shell', 'do_EOF', 'do_help')

        self.g2_module = G2Engine()
        self.g2_hasher_module = G2Hasher()
        self.g2_product_module = G2Product()
        self.g2_diagnostic_module = G2Diagnostic()
        self.g2_config_module = G2Config()
        self.g2_configmgr_module = G2ConfigMgr()

        self.initialized = False
        self.restart = False
        self.restart_debug = False
        self.debug_trace = debug_trace
        self.quit = False
        self.timerOn = False
        self.timerStart = self.timerEnd = None
        
        # Readline and history 
        self.readlineAvail = True if 'readline' in sys.modules else False
        self.histDisable = hist_disable
        self.histCheck()

        self.parser = argparse.ArgumentParser(prog='', add_help=False)    
        self.subparsers = self.parser.add_subparsers()

        jsonOnly_parser = self.subparsers.add_parser('jsonOnly', usage=argparse.SUPPRESS)
        jsonOnly_parser.add_argument('jsonData')
        
        jsonWithInfo_parser = self.subparsers.add_parser('jsonWithInfo', usage=argparse.SUPPRESS)
        jsonWithInfo_parser.add_argument('jsonData')
        jsonWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        addConfigFile_parser = self.subparsers.add_parser('addConfigFile', usage=argparse.SUPPRESS)
        addConfigFile_parser.add_argument('configJsonFile')
        addConfigFile_parser.add_argument('configComments')

        getConfig_parser = self.subparsers.add_parser('getConfig', usage=argparse.SUPPRESS)
        getConfig_parser.add_argument('configID', type=int)

        setDefaultConfigID_parser = self.subparsers.add_parser('setDefaultConfigID', usage=argparse.SUPPRESS)
        setDefaultConfigID_parser.add_argument('configID', type=int)

        replaceDefaultConfigID_parser = self.subparsers.add_parser('replaceDefaultConfigID', usage=argparse.SUPPRESS)
        replaceDefaultConfigID_parser.add_argument('oldConfigID', type=int)
        replaceDefaultConfigID_parser.add_argument('newConfigID', type=int)

        interfaceName_parser = self.subparsers.add_parser('interfaceName', usage=argparse.SUPPRESS)  
        interfaceName_parser.add_argument('interfaceName')

        searchByAttributesV2_parser = self.subparsers.add_parser('searchByAttributesV2', usage=argparse.SUPPRESS)
        searchByAttributesV2_parser.add_argument('jsonData')
        searchByAttributesV2_parser.add_argument('flags', type=int)

        processFile_parser = self.subparsers.add_parser('processFile', usage=argparse.SUPPRESS)
        processFile_parser.add_argument('inputFile')

        validateLicenseFile_parser = self.subparsers.add_parser('validateLicenseFile', usage=argparse.SUPPRESS)
        validateLicenseFile_parser.add_argument('licenseFilePath')

        inputFile_parser = self.subparsers.add_parser('inputFile', usage=argparse.SUPPRESS)
        inputFile_parser.add_argument('inputFile')
        inputFile_parser.add_argument('-o', '--outputFile', required=False)

        processWithResponse_parser = self.subparsers.add_parser('processWithResponse',  usage=argparse.SUPPRESS)
        processWithResponse_parser.add_argument('jsonData')
        processWithResponse_parser.add_argument('-o', '--outputFile', required=False)

        exportEntityReport_parser = self.subparsers.add_parser('exportEntityReport', usage=argparse.SUPPRESS)
        exportEntityReport_parser.add_argument('-f', '--flags', required=True, default=0, type=int)
        exportEntityReport_parser.add_argument('-o', '--outputFile', required=False)

        exportEntityCsvV2_parser = self.subparsers.add_parser('exportEntityCsvV2', usage=argparse.SUPPRESS)
        exportEntityCsvV2_parser.add_argument('-t', '--headersForCSV', required=False)
        exportEntityCsvV2_parser.add_argument('-f', '--flags', required=True, default=0, type=int)
        exportEntityCsvV2_parser.add_argument('-o', '--outputFile', required=False)

        exportEntityCsvV3_parser = self.subparsers.add_parser('exportEntityCsvV3', usage=argparse.SUPPRESS)
        exportEntityCsvV3_parser.add_argument('-t', '--headersForCSV', required=False)
        exportEntityCsvV3_parser.add_argument('-f', '--flags', required=True, default=0, type=int)
        exportEntityCsvV3_parser.add_argument('-o', '--outputFile', required=False)
        recordModify_parser = self.subparsers.add_parser('recordModify', usage=argparse.SUPPRESS)
        recordModify_parser.add_argument('dataSourceCode')
        recordModify_parser.add_argument('recordID')
        recordModify_parser.add_argument('jsonData')
        recordModify_parser.add_argument('-l', '--loadID', required=False)

        recordModifyWithInfo_parser = self.subparsers.add_parser('recordModifyWithInfo', usage=argparse.SUPPRESS)
        recordModifyWithInfo_parser.add_argument('dataSourceCode')
        recordModifyWithInfo_parser.add_argument('recordID')
        recordModifyWithInfo_parser.add_argument('jsonData')
        recordModifyWithInfo_parser.add_argument('-l', '--loadID', required=False)
        recordModifyWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        addRecordWithReturnedRecordID_parser = self.subparsers.add_parser('addRecordWithReturnedRecordID', usage=argparse.SUPPRESS)
        addRecordWithReturnedRecordID_parser.add_argument('dataSourceCode')
        addRecordWithReturnedRecordID_parser.add_argument('jsonData')
        addRecordWithReturnedRecordID_parser.add_argument('-l', '--loadID', required=False)

        addRecordWithInfoWithReturnedRecordID_parser = self.subparsers.add_parser('addRecordWithInfoWithReturnedRecordID', usage=argparse.SUPPRESS)
        addRecordWithInfoWithReturnedRecordID_parser.add_argument('dataSourceCode')
        addRecordWithInfoWithReturnedRecordID_parser.add_argument('jsonData')
        addRecordWithInfoWithReturnedRecordID_parser.add_argument('-l', '--loadID', required=False)
        addRecordWithInfoWithReturnedRecordID_parser.add_argument('-f', '--flags', required=False, default=0, type=int)

        recordDelete_parser = self.subparsers.add_parser('recordDelete', usage=argparse.SUPPRESS)
        recordDelete_parser.add_argument('dataSourceCode')
        recordDelete_parser.add_argument('recordID')
        recordDelete_parser.add_argument('-l', '--loadID', required=False)

        recordDeleteWithInfo_parser = self.subparsers.add_parser('recordDeleteWithInfo', usage=argparse.SUPPRESS)
        recordDeleteWithInfo_parser.add_argument('dataSourceCode')
        recordDeleteWithInfo_parser.add_argument('recordID')
        recordDeleteWithInfo_parser.add_argument('-l', '--loadID', required=False)
        recordDeleteWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        getEntityByEntityID_parser = self.subparsers.add_parser('getEntityByEntityID', usage=argparse.SUPPRESS)
        getEntityByEntityID_parser.add_argument('entityID', type=int)

        getEntityByEntityIDV2_parser = self.subparsers.add_parser('getEntityByEntityIDV2', usage=argparse.SUPPRESS)
        getEntityByEntityIDV2_parser.add_argument('entityID', type=int)
        getEntityByEntityIDV2_parser.add_argument('flags', type=int)

        findPathByEntityID_parser = self.subparsers.add_parser('findPathByEntityID', usage=argparse.SUPPRESS)
        findPathByEntityID_parser.add_argument('startEntityID', type=int)
        findPathByEntityID_parser.add_argument('endEntityID', type=int)
        findPathByEntityID_parser.add_argument('maxDegree', type=int)

        findPathByEntityIDV2_parser = self.subparsers.add_parser('findPathByEntityIDV2', usage=argparse.SUPPRESS)
        findPathByEntityIDV2_parser.add_argument('startEntityID', type=int)
        findPathByEntityIDV2_parser.add_argument('endEntityID', type=int)
        findPathByEntityIDV2_parser.add_argument('maxDegree', type=int)
        findPathByEntityIDV2_parser.add_argument('flags', type=int)

        findPathExcludingByEntityID_parser = self.subparsers.add_parser('findPathExcludingByEntityID', usage=argparse.SUPPRESS)
        findPathExcludingByEntityID_parser.add_argument('startEntityID', type=int)
        findPathExcludingByEntityID_parser.add_argument('endEntityID', type=int)
        findPathExcludingByEntityID_parser.add_argument('maxDegree', type=int)
        findPathExcludingByEntityID_parser.add_argument('excludedEntities')
        findPathExcludingByEntityID_parser.add_argument('flags', type=int)

        findPathIncludingSourceByEntityID_parser = self.subparsers.add_parser('findPathIncludingSourceByEntityID', usage=argparse.SUPPRESS)
        findPathIncludingSourceByEntityID_parser.add_argument('startEntityID', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('endEntityID', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('maxDegree', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('excludedEntities')
        findPathIncludingSourceByEntityID_parser.add_argument('requiredDsrcs')
        findPathIncludingSourceByEntityID_parser.add_argument('flags', type=int)

        findNetworkByEntityID_parser = self.subparsers.add_parser('findNetworkByEntityID', usage=argparse.SUPPRESS)
        findNetworkByEntityID_parser.add_argument('entityList')
        findNetworkByEntityID_parser.add_argument('maxDegree', type=int)
        findNetworkByEntityID_parser.add_argument('buildOutDegree', type=int)
        findNetworkByEntityID_parser.add_argument('maxEntities', type=int)

        findNetworkByEntityIDV2_parser = self.subparsers.add_parser('findNetworkByEntityIDV2', usage=argparse.SUPPRESS)
        findNetworkByEntityIDV2_parser.add_argument('entityList')
        findNetworkByEntityIDV2_parser.add_argument('maxDegree', type=int)
        findNetworkByEntityIDV2_parser.add_argument('buildOutDegree', type=int)
        findNetworkByEntityIDV2_parser.add_argument('maxEntities', type=int)
        findNetworkByEntityIDV2_parser.add_argument('flags', type=int)

        getEntityDetails_parser = self.subparsers.add_parser('getEntityDetails', usage=argparse.SUPPRESS)
        getEntityDetails_parser.add_argument('-e', '--entityID', required=True, type=int, default=0)
        getEntityDetails_parser.add_argument('-d', '--includeInternalFeatures', action='store_true', required=False, default=False)

        getRelationshipDetails_parser = self.subparsers.add_parser('getRelationshipDetails', usage=argparse.SUPPRESS)
        getRelationshipDetails_parser.add_argument('-r', '--relationshipID', required=True, type=int, default=0)
        getRelationshipDetails_parser.add_argument('-d', '--includeInternalFeatures', action='store_true', required=False, default=False)

        getMappingStatistics_parser = self.subparsers.add_parser('getMappingStatistics', usage=argparse.SUPPRESS)
        getMappingStatistics_parser.add_argument('-d', '--includeInternalFeatures', action='store_true', required=False, default=False)

        checkDBPerf_parser = self.subparsers.add_parser('checkDBPerf', usage=argparse.SUPPRESS)
        checkDBPerf_parser.add_argument('-s', '--secondsToRun', required=True, type=int)

        getGenericFeatures_parser = self.subparsers.add_parser('getGenericFeatures', usage=argparse.SUPPRESS)
        getGenericFeatures_parser.add_argument('-t', '--featureType', required=True)
        getGenericFeatures_parser.add_argument('-m', '--maximumEstimatedCount', required=False, type=int, default=1000)

        getEntitySizeBreakdown_parser = self.subparsers.add_parser('getEntitySizeBreakdown', usage=argparse.SUPPRESS)
        getEntitySizeBreakdown_parser.add_argument('-m', '--minimumEntitySize', required=True, type=int)
        getEntitySizeBreakdown_parser.add_argument('-d', '--includeInternalFeatures', action='store_true', required=False, default=False)

        getEntityResume_parser = self.subparsers.add_parser('getEntityResume', usage=argparse.SUPPRESS)
        getEntityResume_parser.add_argument('entityID', type=int)

        getEntityListBySize_parser = self.subparsers.add_parser('getEntityListBySize', usage=argparse.SUPPRESS)
        getEntityListBySize_parser.add_argument('-s', '--entitySize', required=True, type=int)
        getEntityListBySize_parser.add_argument('-o', '--outputFile', required=False)

        getEntityByRecordID_parser = self.subparsers.add_parser('getEntityByRecordID', usage=argparse.SUPPRESS)
        getEntityByRecordID_parser.add_argument('dataSourceCode')
        getEntityByRecordID_parser.add_argument('recordID')

        getRecord_parser = self.subparsers.add_parser('getRecord', usage=argparse.SUPPRESS)
        getRecord_parser.add_argument('dataSourceCode')
        getRecord_parser.add_argument('recordID')

        getRecordV2_parser = self.subparsers.add_parser('getRecordV2', usage=argparse.SUPPRESS)
        getRecordV2_parser.add_argument('dataSourceCode')
        getRecordV2_parser.add_argument('recordID')
        getRecordV2_parser.add_argument('flags', type=int)

        reevaluateRecord_parser = self.subparsers.add_parser('reevaluateRecord', usage=argparse.SUPPRESS)
        reevaluateRecord_parser.add_argument('dataSourceCode')
        reevaluateRecord_parser.add_argument('recordID')
        reevaluateRecord_parser.add_argument('flags', type=int)
        
        reevaluateRecordWithInfo_parser = self.subparsers.add_parser('reevaluateRecordWithInfo', usage=argparse.SUPPRESS)
        reevaluateRecordWithInfo_parser.add_argument('dataSourceCode')
        reevaluateRecordWithInfo_parser.add_argument('recordID')
        reevaluateRecordWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        reevaluateEntity_parser = self.subparsers.add_parser('reevaluateEntity', usage=argparse.SUPPRESS)
        reevaluateEntity_parser.add_argument('entityID', type=int)
        reevaluateEntity_parser.add_argument('flags', type=int)
        
        reevaluateEntityWithInfo_parser = self.subparsers.add_parser('reevaluateEntityWithInfo', usage=argparse.SUPPRESS)
        reevaluateEntityWithInfo_parser.add_argument('entityID', type=int)
        reevaluateEntityWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        getEntityByRecordIDV2_parser = self.subparsers.add_parser('getEntityByRecordIDV2', usage=argparse.SUPPRESS)
        getEntityByRecordIDV2_parser.add_argument('dataSourceCode')
        getEntityByRecordIDV2_parser.add_argument('recordID')
        getEntityByRecordIDV2_parser.add_argument('flags', type=int)

        findPathByRecordID_parser = self.subparsers.add_parser('findPathByRecordID', usage=argparse.SUPPRESS)
        findPathByRecordID_parser.add_argument('startDataSourceCode')
        findPathByRecordID_parser.add_argument('startRecordID')
        findPathByRecordID_parser.add_argument('endDataSourceCode')
        findPathByRecordID_parser.add_argument('endRecordID')
        findPathByRecordID_parser.add_argument('maxDegree', type=int)

        findPathByRecordIDV2_parser = self.subparsers.add_parser('findPathByRecordIDV2', usage=argparse.SUPPRESS)
        findPathByRecordIDV2_parser.add_argument('startDataSourceCode')
        findPathByRecordIDV2_parser.add_argument('startRecordID')
        findPathByRecordIDV2_parser.add_argument('endDataSourceCode')
        findPathByRecordIDV2_parser.add_argument('endRecordID')
        findPathByRecordIDV2_parser.add_argument('maxDegree', type=int)
        findPathByRecordIDV2_parser.add_argument('flags', type=int)

        findPathExcludingByRecordID_parser = self.subparsers.add_parser('findPathExcludingByRecordID', usage=argparse.SUPPRESS)
        findPathExcludingByRecordID_parser.add_argument('startDataSourceCode')
        findPathExcludingByRecordID_parser.add_argument('startRecordID')
        findPathExcludingByRecordID_parser.add_argument('endDataSourceCode')
        findPathExcludingByRecordID_parser.add_argument('endRecordID')
        findPathExcludingByRecordID_parser.add_argument('maxDegree', type=int)
        findPathExcludingByRecordID_parser.add_argument('excludedEntities')
        findPathExcludingByRecordID_parser.add_argument('flags', type=int)

        findPathIncludingSourceByRecordID_parser = self.subparsers.add_parser('findPathIncludingSourceByRecordID', usage=argparse.SUPPRESS)
        findPathIncludingSourceByRecordID_parser.add_argument('startDataSourceCode')
        findPathIncludingSourceByRecordID_parser.add_argument('startRecordID')
        findPathIncludingSourceByRecordID_parser.add_argument('endDataSourceCode')
        findPathIncludingSourceByRecordID_parser.add_argument('endRecordID')
        findPathIncludingSourceByRecordID_parser.add_argument('maxDegree', type=int)
        findPathIncludingSourceByRecordID_parser.add_argument('excludedEntities')
        findPathIncludingSourceByRecordID_parser.add_argument('requiredDsrcs')
        findPathIncludingSourceByRecordID_parser.add_argument('flags', type=int)

        findNetworkByRecordID_parser = self.subparsers.add_parser('findNetworkByRecordID', usage=argparse.SUPPRESS)
        findNetworkByRecordID_parser.add_argument('recordList')
        findNetworkByRecordID_parser.add_argument('maxDegree', type=int)
        findNetworkByRecordID_parser.add_argument('buildOutDegree', type=int)
        findNetworkByRecordID_parser.add_argument('maxEntities', type=int)

        findNetworkByRecordIDV2_parser = self.subparsers.add_parser('findNetworkByRecordIDV2', usage=argparse.SUPPRESS)
        findNetworkByRecordIDV2_parser.add_argument('recordList')
        findNetworkByRecordIDV2_parser.add_argument('maxDegree', type=int)
        findNetworkByRecordIDV2_parser.add_argument('buildOutDegree', type=int)
        findNetworkByRecordIDV2_parser.add_argument('maxEntities', type=int)
        findNetworkByRecordIDV2_parser.add_argument('flags', type=int)

        whyEntityByRecordID_parser = self.subparsers.add_parser('whyEntityByRecordID', usage=argparse.SUPPRESS)
        whyEntityByRecordID_parser.add_argument('dataSourceCode')
        whyEntityByRecordID_parser.add_argument('recordID')

        whyEntityByRecordIDV2_parser = self.subparsers.add_parser('whyEntityByRecordIDV2', usage=argparse.SUPPRESS)
        whyEntityByRecordIDV2_parser.add_argument('dataSourceCode')
        whyEntityByRecordIDV2_parser.add_argument('recordID')
        whyEntityByRecordIDV2_parser.add_argument('flags', type=int)

        whyEntityByEntityID_parser = self.subparsers.add_parser('whyEntityByEntityID', usage=argparse.SUPPRESS)
        whyEntityByEntityID_parser.add_argument('entityID', type=int)

        whyEntityByEntityIDV2_parser = self.subparsers.add_parser('whyEntityByEntityIDV2', usage=argparse.SUPPRESS)
        whyEntityByEntityIDV2_parser.add_argument('entityID', type=int)
        whyEntityByEntityIDV2_parser.add_argument('flags', type=int)

        whyEntities_parser = self.subparsers.add_parser('whyEntities', usage=argparse.SUPPRESS)
        whyEntities_parser.add_argument('entityID1', type=int)
        whyEntities_parser.add_argument('entityID2', type=int)

        whyEntitiesV2_parser = self.subparsers.add_parser('whyEntitiesV2', usage=argparse.SUPPRESS)
        whyEntitiesV2_parser.add_argument('entityID1', type=int)
        whyEntitiesV2_parser.add_argument('entityID2', type=int)
        whyEntitiesV2_parser.add_argument('flags', type=int)

        whyRecords_parser = self.subparsers.add_parser('whyRecords', usage=argparse.SUPPRESS)
        whyRecords_parser.add_argument('dataSourceCode1')
        whyRecords_parser.add_argument('recordID1')
        whyRecords_parser.add_argument('dataSourceCode2')
        whyRecords_parser.add_argument('recordID2')

        whyRecordsV2_parser = self.subparsers.add_parser('whyRecordsV2', usage=argparse.SUPPRESS)
        whyRecordsV2_parser.add_argument('dataSourceCode1')
        whyRecordsV2_parser.add_argument('recordID1')
        whyRecordsV2_parser.add_argument('dataSourceCode2')
        whyRecordsV2_parser.add_argument('recordID2')
        whyRecordsV2_parser.add_argument('flags', type=int)

        outputOptional_parser = self.subparsers.add_parser('outputOptional',  usage=argparse.SUPPRESS)
        outputOptional_parser.add_argument('-o', '--outputFile', required=False)

        findEntitiesByFeatureIDs_parser = self.subparsers.add_parser('findEntitiesByFeatureIDs',  usage=argparse.SUPPRESS)
        findEntitiesByFeatureIDs_parser.add_argument('jsonData')

        purgeRepository_parser = self.subparsers.add_parser('purgeRepository',  usage=argparse.SUPPRESS)
        purgeRepository_parser.add_argument('-n', '--noReset', required=False, nargs='?', const=1, type=int)
        purgeRepository_parser.add_argument('-FORCEPURGE', '--FORCEPURGE', dest='forcePurge', action='store_true', default=False)

        processRedoRecordWithInfo_parser = self.subparsers.add_parser('processRedoRecordWithInfo', usage=argparse.SUPPRESS)
        processRedoRecordWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)


    def preloop(self):
        
        if (self.initialized):
            return

        printWithNewLines('Initializing Senzing engine...', 'S')

        try:
            self.g2_module.initV2('pyG2E', g2module_params, self.debug_trace)
            self.g2_product_module.initV2('pyG2Product', g2module_params, self.debug_trace)
            self.g2_diagnostic_module.initV2('pyG2Diagnostic', g2module_params, self.debug_trace)
            self.g2_config_module.initV2('pyG2Config', g2module_params, self.debug_trace)
            self.g2_configmgr_module.initV2('pyG2ConfigMgr', g2module_params, self.debug_trace)
        except G2Exception.G2Exception as ex:
            printWithNewLines(f'G2Exception: {ex}', 'B')
            raise 

        exportedConfig = bytearray() 
        exportedConfigID = bytearray() 
        self.g2_module.exportConfig(exportedConfig, exportedConfigID)
        self.g2_hasher_module.initWithConfigV2('pyG2Hasher', g2module_params, exportedConfig, self.debug_trace)

        self.initialized = True
        printWithNewLines('Welcome to G2Command. Type help or ? to list commands.', 'B')


    def postloop(self):

        if (self.initialized):
            self.g2_module.destroy()
            self.g2_product_module.destroy()
            self.g2_diagnostic_module.destroy()
            self.g2_config_module.destroy()
            self.g2_configmgr_module.destroy()
            self.g2_hasher_module.destroy()

        self.initialized = False


    def precmd(self, line):
        
        if self.timerOn: 
            #Reset timer for every command
            self.timerStart = self.timerEnd = None
            self.timerStart = timer() 

        return cmd.Cmd.precmd(self, line)
    

    def postcmd(self, stop, line):
        
        if self.timerOn and self.timerStart:
            self.timerEnd = timer() 
            self.execTime = (self.timerEnd - self.timerStart)
            printWithNewLines(f'Approximate execution time (s): {self.execTime:.5f}\n', 'N')

        return cmd.Cmd.postcmd(self, stop, line)


    def do_quit(self, arg):
        return True


    def do_exit(self, arg):

        self.do_quit(self)

        return True


    def ret_quit(self):
        return self.quit


    def do_EOF(self, line):
        return True


    def emptyline(self):
        return


    def default(self, line):

        printWithNewLines(f'ERROR: Unknown command, type help or ? to list available commands and help.', 'B')

        return


    def cmdloop(self, intro=None):

        while True:
            try:
                super(G2CmdShell,self).cmdloop(intro=self.intro)
                self.postloop()
                break
            except KeyboardInterrupt:
                if input('\n\nAre you sure you want to exit? (y/n)  ') in ['y','Y', 'yes', 'YES']:
                    break
                else:
                    print()            
            except TypeError as ex:
                printWithNewLines(f'ERROR: {ex}', 'B')


    def fileloop(self, fileName):

        self.preloop()

        with open(fileName) as data_in:
            for line in data_in:
                line = line.strip()
                # Ignore comments
                if len(line) > 0 and line[0:1] not in ('#','-','/'):
                    # *args allows for empty list if there are no args
                    (read_cmd, *args) = line.split()
                    process_cmd = f'do_{read_cmd}'
                    printWithNewLines(f'----- {read_cmd} -----', 'S')
                    printWithNewLines(f'{line}', 'S')

                    if process_cmd not in dir(self):
                        printWithNewLines(f'ERROR: Command {read_cmd} not found', 'B')
                    else:
                        # Join the args into a printable string, format into the command + args to call
                        try:
                            exec_cmd = f'self.{process_cmd}({repr(" ".join(args))})'
                            exec(exec_cmd)
                        except (ValueError, TypeError) as ex: 
                            printWithNewLines('ERROR: Command could not be run!', 'S')
                            printWithNewLines(f'       {ex}', 'E')


    def get_names(self):
        ''' Hide do_shell from list of APIs. Seperate help section for it  '''

        return [n for n in dir(self.__class__) if n not in self.__hidden_methods]

    # ----- Misc Help -----

    def help_Arguments(self):
        printWithNewLines(textwrap.dedent('''\
            - Optional arguments are surrounded with [ ]

            - Argument values to specify are surrounded with < >, replace with your value 
            
            - Example:
            
                [-o <output_file>]

                - -o = an optional argument
                - <output_file> = replace with path and/or filename to output to
            '''), 'S')


    def help_InterfaceName(self):
        printWithNewLines(textwrap.dedent('''\
            - The name of a G2 interface (engine, product, diagnostic, hasher, config, configmgr).
            '''), 'S')

    def help_KnowledgeCenter(self):
        printWithNewLines(textwrap.dedent('''\
            - Senzing Knowledge Center: https://senzing.zendesk.com/hc/en-us
            '''), 'S')


    def help_Support(self):
        printWithNewLines(textwrap.dedent('''\
            - Senzing Support Request: https://senzing.zendesk.com/hc/en-us/requests/new
            '''), 'S')

    def help_Shell(self):
        printWithNewLines(textwrap.dedent('''\
            - Run basic OS shell commands: ! <command>
            '''), 'S')

    def help_History(self):
        printWithNewLines(textwrap.dedent(f'''\
            - Use shell like history, requires Python readline module.

            - Tries to create a history file in the users home directory for use across instances of G2Command. 

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


    def help_Restart(self):
        printWithNewLines(textwrap.dedent('''\
            - restartDebug - Restart G2Command and enable engine debug

            - restart - Restart G2Command, if restartDebug has previouisly been issued, disable engine debug
            '''), 'S')


    def do_shell(self,line):

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


     # ----- exception commands -----

    def do_clearLastException(self,arg):
        '\nClear the last exception:  clearLastException <interfaceName>\n'
        try:
            args = self.parser.parse_args(['interfaceName'] + parse(arg))
        except SystemExit:
            print(self.do_clearLastException.__doc__)
            return

        try: 
            if args.interfaceName == 'engine':
                self.g2_module.clearLastException()
            elif args.interfaceName == 'hasher':
                self.g2_hasher_module.clearLastException()
            elif args.interfaceName == 'product':
                self.g2_product_module.clearLastException()
            elif args.interfaceName == 'diagnostic':
                self.g2_diagnostic_module.clearLastException()
            elif args.interfaceName == 'config':
                self.g2_config_module.clearLastException()
            elif args.interfaceName == 'configmgr':
                self.g2_configmgr_module.clearLastException()
            else:
                raise G2Exception.G2ModuleGenericException("ERROR: Unknown interface name '" + args.interfaceName + "'")
        except G2Exception.G2Exception as err:
            print(err)


    def do_getLastException(self,arg):
        '\nGet the last exception:  getLastException <interfaceName>\n'

        try:
            args = self.parser.parse_args(['interfaceName'] + parse(arg))
        except SystemExit:
            print(self.do_getLastException.__doc__)
            return

        try: 
            resultString = ''
            if args.interfaceName == 'engine':
                resultString = self.g2_module.getLastException()
            elif args.interfaceName == 'hasher':
                resultString = self.g2_hasher_module.getLastException()
            elif args.interfaceName == 'product':
                resultString = self.g2_product_module.getLastException()
            elif args.interfaceName == 'diagnostic':
                resultString = self.g2_diagnostic_module.getLastException()
            elif args.interfaceName == 'config':
                resultString = self.g2_config_module.getLastException()
            elif args.interfaceName == 'configmgr':
                resultString = self.g2_configmgr_module.getLastException()
            else:
                raise G2Exception.G2ModuleGenericException("ERROR: Unknown interface name '" + args.interfaceName + "'")
            printWithNewLine('Last exception: "%s"' % (resultString))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getLastExceptionCode(self,arg):
        '\nGet the last exception:  getLastExceptionCode <interfaceName>\n'
        try:
            args = self.parser.parse_args(['interfaceName'] + parse(arg))
        except SystemExit:
            print(self.do_getLastExceptionCode.__doc__)
            return
        
        try: 
            resultInt = 0
            if args.interfaceName == 'engine':
                resultInt = self.g2_module.getLastExceptionCode()
            elif args.interfaceName == 'hasher':
                resultInt = self.g2_hasher_module.getLastExceptionCode()
            elif args.interfaceName == 'product':
                resultInt = self.g2_product_module.getLastExceptionCode()
            elif args.interfaceName == 'diagnostic':
                resultInt = self.g2_diagnostic_module.getLastExceptionCode()
            elif args.interfaceName == 'config':
                resultInt = self.g2_config_module.getLastExceptionCode()
            elif args.interfaceName == 'configmgr':
                resultInt = self.g2_configmgr_module.getLastExceptionCode()
            else:
                raise G2Exception.G2ModuleGenericException("ERROR: Unknown interface name '" + args.interfaceName + "'")
            printWithNewLine('Last exception code: %d' % (resultInt))
        except G2Exception.G2Exception as err:
            print(err)


    # ----- basic G2 commands -----


    def do_primeEngine(self,arg):
        '\nPrime the G2 engine:  primeEngine\n'

        try: 
            self.g2_module.primeEngine()
        except G2Exception.G2Exception as err:
            print(err)


    def do_process(self, arg):
        '\nProcess a generic record:  process <json_data>\n'

        try:
            args = self.parser.parse_args(['jsonOnly'] + parse(arg))
        except SystemExit:
            print(self.do_process.__doc__)
            return

        try: 
            self.g2_module.process(args.jsonData)
            print()
        except G2Exception.G2Exception as err:
            print(err)


    def do_processWithInfo(self, arg):
        '\nProcess a generic record with returned info:  process <json_data> [-f flags]\n'

        try:
            args = self.parser.parse_args(['jsonWithInfo'] + parse(arg))
        except SystemExit:
            print(self.do_processWithInfo.__doc__)
            return

        try:
            flags = inspect.signature(self.g2_module.processWithInfo).parameters['flags'].default
            if args.flags:
                flags = int(args.flags)

            response = bytearray()    
            self.g2_module.processWithInfo(args.jsonData,response,flags=flags)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_processFile(self, arg):
        '\nProcess a file of entity records:  processFile <input_file>\n'

        try:
            args = self.parser.parse_args(['processFile'] + parse(arg))
        except SystemExit:
            print(self.do_processFile.__doc__)
            return

        try: 
            dataSourceParm = None
            if '/?' in args.inputFile:
                fileName, dataSourceParm = args.inputFile.split("/?")
                if dataSourceParm.upper().startswith('DATA_SOURCE='):
                    dataSourceParm = dataSourceParm[12:]
                dataSourceParm = dataSourceParm.upper()
            else:
                fileName = args.inputFile
            dummy, fileExtension = os.path.splitext(fileName)
            fileExtension = fileExtension[1:].upper()
            
            with open(fileName) as data_in:
                if fileExtension != 'CSV':
                    fileReader = data_in
                else:
                    fileReader = csv.reader(data_in)
                    csvHeaders = [x.upper() for x in next(fileReader)]
                cnt = 0
                for line in fileReader:
                    if fileExtension != 'CSV':
                        jsonStr = line.strip()
                    else:
                        jsonObj = dict(list(zip(csvHeaders, line)))
                        if dataSourceParm:
                            jsonObj['DATA_SOURCE'] = dataSourceParm
                            jsonObj['ENTITY_TYPE'] = dataSourceParm
                        jsonStr = json.dumps(jsonObj)
                  
                    self.g2_module.process(jsonStr)
                    cnt += 1
                    if cnt % 1000 ==0:
                        print('%s rows processed' % cnt)
                print('%s rows processed, done!' % cnt)
                        
                print()
        except G2Exception.G2Exception as err:
            print(err)
    

    def do_processWithResponse(self, arg):
        '\nProcess a generic record, and print response:  processWithResponse <json_data> [-o <output_file>]\n'

        try:
            args = self.parser.parse_args(['processWithResponse'] + parse(arg))
        except SystemExit:
            print(self.do_processWithResponse.__doc__)
            return

        try:
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    processedData = bytearray()
                    self.g2_module.processWithResponse(args.jsonData,processedData)
                    data_out.write(processedData.decode())
                    data_out.write('\n')
                print()
            else:
                    processedData = bytearray()
                    self.g2_module.processWithResponse(args.jsonData,processedData)
                    printResponse(processedData.decode())
        except G2Exception.G2Exception as err:
            print(err)


    def do_processFileWithResponse(self, arg):
        '\nProcess a file of entity records:  processFileWithResponse <input_file> [-o <output_file>]\n'

        try:
            args = self.parser.parse_args(['inputFile'] + parse(arg))
        except SystemExit:
            print(self.do_processFileWithResponse.__doc__)
            return

        try: 
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    with open(args.inputFile.split("?")[0]) as data_in:
                        for line in data_in:
                            processedData = bytearray()
                            self.g2_module.processWithResponse(line.strip(),processedData)
                            data_out.write(processedData.decode())
                            data_out.write('\n')
                print()
            else:
                with open(args.inputFile.split("?")[0]) as data_in :
                    for line in data_in:
                        processedData = bytearray()
                        self.g2_module.processWithResponse(line.strip(),processedData)
                        printResponse(processedData.decode())
        except G2Exception.G2Exception as err:
            print(err)


    def do_exportCSVEntityReportV2(self, arg):
        '\nExport repository contents as CSV:  exportCSVEntityReportV2 -t <csvColumnList> -f <flags> [-o <output_file>]\n' 

        try:
            args = self.parser.parse_args(['exportEntityCsvV2'] + parse(arg))
        except SystemExit:
            print(self.do_exportCSVEntityReportV2.__doc__)
            return

        try: 
            exportHandle = self.g2_module.exportCSVEntityReportV2(args.headersForCSV, args.flags)
            response = bytearray() 
            rowData = self.g2_module.fetchNext(exportHandle,response)
            recCnt = 0
            resultString = b""
            while rowData:
                resultString += response
                recCnt = recCnt + 1
                response = bytearray()
                rowData = self.g2_module.fetchNext(exportHandle,response)
            self.g2_module.closeExport(exportHandle)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(resultString.decode())
            else:
                print('{}'.format(resultString.decode()))
        except G2Exception.G2Exception as err:
            print(err)
        else:
            #Remove 1 for the header on CSV
            print('Number of exported records = %s\n' % (recCnt-1) )


    def do_exportJSONEntityReport(self, arg):
        '\nExport repository contents as JSON:  exportJSONEntityReport -f <flags> [-o <output_file>]\n' 

        try:
            args = self.parser.parse_args(['exportEntityReport'] + parse(arg))
        except SystemExit:
            print(self.do_exportJSONEntityReport.__doc__)
            return

        try: 
            exportHandle = self.g2_module.exportJSONEntityReport(args.flags)
            response = bytearray() 
            rowData = self.g2_module.fetchNext(exportHandle,response)
            recCnt = 0
            resultString = b""
            while rowData:
                resultString += response
                recCnt = recCnt + 1
                response = bytearray()
                rowData = self.g2_module.fetchNext(exportHandle,response)
            self.g2_module.closeExport(exportHandle)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(resultString.decode())
            else:
                print('{}'.format(resultString.decode()))
        except G2Exception.G2Exception as err:
            print(err)
        else:
            print('Number of exported records = %s\n' % (recCnt) )


    def do_exportCSVEntityReportV3(self, arg):
        '\nExport repository contents as CSV:  exportCSVEntityReportV3 -t <csvColumnList> -f <flags> [-o <output_file>]\n' 
        try:
            args = self.parser.parse_args(['exportEntityCsvV3'] + parse(arg))
        except SystemExit:
            print(self.do_exportCSVEntityReportV3.__doc__)
            return
        try: 
            exportHandle = self.g2_module.exportCSVEntityReportV3(args.headersForCSV, args.flags)
            response = bytearray() 
            rowData = self.g2_module.fetchNextV3(exportHandle,response)
            recCnt = 0
            resultString = b""
            while rowData:
                resultString += response
                recCnt = recCnt + 1
                response = bytearray()
                rowData = self.g2_module.fetchNextV3(exportHandle,response)
            self.g2_module.closeExportV3(exportHandle)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(resultString.decode())
            else:
                print('{}'.format(resultString.decode()))
        except G2Exception.G2Exception as err:
            print(err)
        else:
            print('Number of exported records = %s\n' % (recCnt-1) )
    def do_exportJSONEntityReportV3(self, arg):
        '\nExport repository contents as JSON:  exportJSONEntityReportV3 -f <flags> [-o <output_file>]\n' 
        try:
            args = self.parser.parse_args(['exportEntityReport'] + parse(arg))
        except SystemExit:
            print(self.do_exportJSONEntityReportV3.__doc__)
            return
        try: 
            exportHandle = self.g2_module.exportJSONEntityReportV3(args.flags)
            response = bytearray() 
            rowData = self.g2_module.fetchNextV3(exportHandle,response)
            recCnt = 0
            resultString = b""
            while rowData:
                resultString += response
                recCnt = recCnt + 1
                response = bytearray()
                rowData = self.g2_module.fetchNextV3(exportHandle,response)
            self.g2_module.closeExportV3(exportHandle)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(resultString.decode())
            else:
                print('{}'.format(resultString.decode()))
        except G2Exception.G2Exception as err:
            print(err)
        else:
            print('Number of exported records = %s\n' % (recCnt) )
    def do_getTemplateConfig(self, arg):
        '\nGet a template config:  getTemplateConfig \n'

        try:
            configHandle = self.g2_config_module.create()
            response = bytearray() 
            self.g2_config_module.save(configHandle,response)
            self.g2_config_module.close(configHandle)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getConfig(self, arg):
        '\nGet the config:  getConfig <configID> \n'

        try:
            args = self.parser.parse_args(['getConfig'] + parse(arg))
        except SystemExit:
            print(self.do_getConfig.__doc__)
            return

        try:
            response = bytearray() 
            self.g2_configmgr_module.getConfig(args.configID,response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getConfigList(self, arg):
        '\nGet a list of known configs:  getConfigList \n'

        try:
            response = bytearray() 
            self.g2_configmgr_module.getConfigList(response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_addConfigFile(self, arg):
        '\nAdd config from a file:  addConfigFile <configJsonFile> <configComments>\n'

        try:
            args = self.parser.parse_args(['addConfigFile'] + parse(arg))
        except SystemExit:
            print(self.do_addConfigFile.__doc__)
            return

        try:
            configStr = ''
            with open(args.configJsonFile.split("?")[0]) as data_in:
                for line in data_in:
                    configStr += line.strip()
            configID = bytearray() 
            self.g2_configmgr_module.addConfig(configStr,args.configComments,configID)
            printWithNewLine('Config added.  [ID = %s]' % configID.decode())
        except G2Exception.G2Exception as err:
            print(err)


    def do_getDefaultConfigID(self, arg):
        '\nGet the default config ID:  getDefaultConfigID \n'

        try:
            configID = bytearray() 
            self.g2_configmgr_module.getDefaultConfigID(configID)
            printWithNewLine('Default Config ID: \'%s\'' % (configID.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_setDefaultConfigID(self, arg):
        '\nSet the default config ID:  setDefaultConfigID <configID>\n'

        try:
            args = self.parser.parse_args(['setDefaultConfigID'] + parse(arg))
        except SystemExit:
            print(self.do_setDefaultConfigID.__doc__)
            return

        try:
            self.g2_configmgr_module.setDefaultConfigID(str(args.configID).encode())
            printWithNewLine('Default config set')
        except G2Exception.G2Exception as err:
            print(err)


    def do_replaceDefaultConfigID(self, arg):
        '\nReplace the default config ID:  replaceDefaultConfigID <oldConfigID> <newConfigID>\n'

        try:
            args = self.parser.parse_args(['replaceDefaultConfigID'] + parse(arg))
        except SystemExit:
            print(self.do_replaceDefaultConfigID.__doc__)
            return

        try:
            self.g2_configmgr_module.replaceDefaultConfigID(args.oldConfigID,args.newConfigID)
            printWithNewLine('New default config set')
        except G2Exception.G2Exception as err:
            print(err)


    def do_addRecord(self, arg):
        '\nAdd record:  addRecord <dataSourceCode> <recordID> <jsonData> [-l <loadID>]\n'

        try:
            args = self.parser.parse_args(['recordModify'] + parse(arg))
        except SystemExit:
            print(self.do_addRecord.__doc__)
            return

        try: 
            if args.loadID:
                self.g2_module.addRecord(args.dataSourceCode, args.recordID, args.jsonData, args.loadID)
            else:
                self.g2_module.addRecord(args.dataSourceCode, args.recordID, args.jsonData)
            printWithNewLine('Record added.')
        except G2Exception.G2Exception as err:
            print(err)
    

    def do_addRecordWithInfo(self, arg):
        '\nAdd record with returned info:  addRecordWithInfo <dataSourceCode> <recordID> <jsonData> [-l <loadID> -f <flags>]\n'

        try:
            args = self.parser.parse_args(['recordModifyWithInfo'] + parse(arg))
        except SystemExit:
            print(self.do_addRecordWithInfo.__doc__)
            return

        try:
            loadID = inspect.signature(self.g2_module.addRecordWithInfo).parameters['loadId'].default
            flags = inspect.signature(self.g2_module.addRecordWithInfo).parameters['flags'].default
            if args.loadID:
                loadID = args.loadID
            if args.flags:
                flags = int(args.flags)

            response = bytearray() 
            self.g2_module.addRecordWithInfo(args.dataSourceCode, args.recordID, args.jsonData,response,loadId=loadID,flags=flags)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_addRecordWithReturnedRecordID(self, arg):
        '\nAdd record with returned record ID:  addRecordWithReturnedRecordID <dataSourceCode> <jsonData> [-l <loadID>]\n'

        try:
            args = self.parser.parse_args(['addRecordWithReturnedRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_addRecordWithReturnedRecordID.__doc__)
            return

        try: 
            recordID = bytearray() 
            if args.loadID:
                self.g2_module.addRecordWithReturnedRecordID(args.dataSourceCode, recordID, args.jsonData, args.loadID)
            else:
                self.g2_module.addRecordWithReturnedRecordID(args.dataSourceCode, recordID, args.jsonData)
            if recordID:
                print('{}'.format(recordID.decode()))
            else:
                print('\nNo record ID!\n')
        except G2Exception.G2Exception as err:
            print(err)
    

    def do_addRecordWithInfoWithReturnedRecordID(self, arg):
        '\nAdd record with returned record ID:  addRecordWithInfoWithReturnedRecordID <dataSourceCode> <jsonData> [-l <loadID>] [-f <flags>]\n'

        try:
            args = self.parser.parse_args(['addRecordWithInfoWithReturnedRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_addRecordWithInfoWithReturnedRecordID.__doc__)
            return

        try: 
            recordID = bytearray() 
            info = bytearray() 
            if args.loadID:
                self.g2_module.addRecordWithInfoWithReturnedRecordID(args.dataSourceCode, args.jsonData, args.flags, recordID, info, args.loadID)
            else:
                self.g2_module.addRecordWithInfoWithReturnedRecordID(args.dataSourceCode, args.jsonData, args.flags, recordID, info)
            if recordID:
                print('Record ID: {}'.format(recordID.decode()))
            else:
                print('\nNo record ID!\n')
            if info:
                print('Info: {}'.format(info.decode()))
            else:
                print('\nNo info response!\n')
            printWithNewLine('Record added.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_reevaluateRecord(self, arg):
        '\n Reevaluate record:  reevaluateRecord <dataSourceCode> <recordID> <flags>\n'

        try:
            args = self.parser.parse_args(['reevaluateRecord'] + parse(arg))
        except SystemExit:
            print(self.do_reevaluateRecord.__doc__)
            return

        try: 
            self.g2_module.reevaluateRecord(args.dataSourceCode, args.recordID, args.flags)
            printWithNewLine('Record reevaluated.')
        except G2Exception.G2Exception as err:
            print(err)
    

    def do_reevaluateRecordWithInfo(self, arg):
        '\n Reevaluate record with returned info:  reevaluateRecordWithInfo <dataSourceCode> <recordID> [-f flags]\n'

        try:
            args = self.parser.parse_args(['reevaluateRecordWithInfo'] + parse(arg))
        except SystemExit:
            print(self.do_reevaluateRecordWithInfo.__doc__)
            return

        try:
            flags = inspect.signature(self.g2_module.reevaluateRecordWithInfo).parameters['flags'].default
            if args.flags:
                flags = int(args.flags)

            response = bytearray() 
            self.g2_module.reevaluateRecordWithInfo(args.dataSourceCode,args.recordID,response,flags=flags)            
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            printWithNewLine('Record reevaluated.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_reevaluateEntity(self, arg):
        '\n Reevaluate entity:  reevaluateEntity <entityID> <flags>\n'

        try:
            args = self.parser.parse_args(['reevaluateEntity'] + parse(arg))
        except SystemExit:
            print(self.do_reevaluateEntity.__doc__)
            return

        try: 
            self.g2_module.reevaluateEntity(args.entityID, args.flags)
            printWithNewLine('Entity reevaluated.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_reevaluateEntityWithInfo(self, arg):
        '\n Reevaluate entity with returned info:  reevaluateEntityWithInfo <entityID> [-f flags]\n'

        try:
            args = self.parser.parse_args(['reevaluateEntityWithInfo'] + parse(arg))
        except SystemExit:
            print(self.do_reevaluateEntityWithInfo.__doc__)
            return

        try:
            flags = inspect.signature(self.g2_module.reevaluateEntityWithInfo).parameters['flags'].default
            if args.flags:
                flags = int(args.flags)

            response = bytearray()
            self.g2_module.reevaluateEntityWithInfo(args.entityID,response,flags=flags)            
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            printWithNewLine('Entity reevaluated.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_replaceRecord(self, arg):
        '\nReplace record:  replaceRecord <dataSourceCode> <recordID> <jsonData> [-l loadID]\n'

        try:
            args = self.parser.parse_args(['recordModify'] + parse(arg))
        except SystemExit:
            print(self.do_replaceRecord.__doc__)
            return

        try: 
            if args.loadID:
                self.g2_module.replaceRecord(args.dataSourceCode, args.recordID, args.jsonData, args.loadID)
            else:
                self.g2_module.replaceRecord(args.dataSourceCode, args.recordID, args.jsonData)
            printWithNewLine('Record replaced.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_replaceRecordWithInfo(self, arg):
        '\nReplace record with returned info:  replaceRecordWithInfo <dataSourceCode> <recordID> <jsonData> [-l loadID -f flags]\n'

        try:
            args = self.parser.parse_args(['recordModifyWithInfo'] + parse(arg))
        except SystemExit:
            print(self.do_replaceRecordWithInfo.__doc__)
            return

        try:
            loadID = inspect.signature(self.g2_module.replaceRecordWithInfo).parameters['loadId'].default
            flags = inspect.signature(self.g2_module.replaceRecordWithInfo).parameters['flags'].default
            if args.loadID:
                loadID = args.loadID
            if args.flags:
                flags = int(args.flags)

            response = bytearray()
            self.g2_module.replaceRecordWithInfo(args.dataSourceCode,args.recordID,args.jsonData,response,loadId=loadID,flags=flags)
            
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            printWithNewLine('Record replaced.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_deleteRecord(self, arg):
        '\nDelete record:  deleteRecord <dataSourceCode> <recordID> [-l loadID]\n' 

        try:
            args = self.parser.parse_args(['recordDelete'] + parse(arg))
        except SystemExit:
            print(self.do_deleteRecord.__doc__)
            return

        try: 
            if args.loadID:
                self.g2_module.deleteRecord(args.dataSourceCode, args.recordID, args.loadID)
            else:
                self.g2_module.deleteRecord(args.dataSourceCode, args.recordID)
            printWithNewLine('Record deleted.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_deleteRecordWithInfo(self, arg):
        '\nDelete record with returned info:  deleteRecord <dataSourceCode> <recordID> [-l loadID -f flags]\n' 

        try:
            args = self.parser.parse_args(['recordDeleteWithInfo'] + parse(arg))
        except SystemExit:
            print(self.do_deleteRecordWithInfo.__doc__)
            return

        try:
            loadID = inspect.signature(self.g2_module.deleteRecordWithInfo).parameters['loadId'].default
            flags = inspect.signature(self.g2_module.deleteRecordWithInfo).parameters['flags'].default
            if args.loadID:
                loadID = args.loadID
            if args.flags:
                flags = int(args.flags)
                
            response = bytearray()
            self.g2_module.deleteRecordWithInfo(args.dataSourceCode,args.recordID,response,loadId=args.loadID,flags=flags)

            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            printWithNewLine('Record deleted.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_searchByAttributes(self, arg):
        '\nSearch by attributes:  searchByAttributes <jsonData>\n'

        try:
            args = self.parser.parse_args(['jsonOnly'] + parse(arg))
        except SystemExit:
            print(self.do_searchByAttributes.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.searchByAttributes(args.jsonData,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_searchByAttributesV2(self, arg):
        '\nSearch by attributes:  searchByAttributesV2 <jsonData> <flags>\n'

        try:
            args = self.parser.parse_args(['searchByAttributesV2'] + parse(arg))
        except SystemExit:
            print(self.do_searchByAttributesV2.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.searchByAttributesV2(args.jsonData,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityByEntityID(self, arg):
        '\nGet entity by resolved entity ID:  getEntityByEntityID <entityID>\n'

        try:
            args = self.parser.parse_args(['getEntityByEntityID'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityByEntityID.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.getEntityByEntityID(args.entityID, response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityByEntityIDV2(self, arg):
        '\nGet entity by resolved entity ID:  getEntityByEntityIDV2 <entityID> <flags>\n'

        try:
            args = self.parser.parse_args(['getEntityByEntityIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityByEntityIDV2.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.getEntityByEntityIDV2(args.entityID,args.flags,response)

            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathByEntityID(self, arg):
        '\nFind path between two entities:  findPathByEntityID <startEntityID> <endEntityID> <maxDegree>\n'

        try:
            args = self.parser.parse_args(['findPathByEntityID'] + parse(arg))
        except SystemExit:
            print(self.do_findPathByEntityID.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.findPathByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathByEntityIDV2(self, arg):
        '\nFind path between two entities:  findPathByEntityIDV2 <startEntityID> <endEntityID> <maxDegree> <flags>\n'

        try:
            args = self.parser.parse_args(['findPathByEntityIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_findPathByEntityIDV2.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.findPathByEntityIDV2(args.startEntityID,args.endEntityID,args.maxDegree,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findNetworkByEntityID(self, arg):
        '\nFind network between entities:  findNetworkByEntityID <entityList> <maxDegree> <buildOutDegree> <maxEntities>\n'

        try:
            args = self.parser.parse_args(['findNetworkByEntityID'] + parse(arg))
        except SystemExit:
            print(self.do_findNetworkByEntityID.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.findNetworkByEntityID(args.entityList,args.maxDegree,args.buildOutDegree,args.maxEntities,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findNetworkByEntityIDV2(self, arg):
        '\nFind network between entities:  findNetworkByEntityIDV2 <entityList> <maxDegree> <buildOutDegree> <maxEntities> <flags>\n'

        try:
            args = self.parser.parse_args(['findNetworkByEntityIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_findNetworkByEntityIDV2.__doc__)
            return

        try: 
            response = bytearray() 
            self.g2_module.findNetworkByEntityIDV2(args.entityList,args.maxDegree,args.buildOutDegree,args.maxEntities,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathExcludingByEntityID(self, arg):
        '\nFind path between two entities, with exclusions:  findPathExcludingByEntityID <startEntityID> <endEntityID> <maxDegree> <excludedEntities> <flags>\n'
        
        try:
            args = self.parser.parse_args(['findPathExcludingByEntityID'] + parse(arg))
        except SystemExit:
            print(self.do_findPathExcludingByEntityID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findPathExcludingByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,args.excludedEntities,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathIncludingSourceByEntityID(self, arg):
        '\nFind path between two entities that includes a watched dsrc list, with exclusions:  findPathIncludingSourceByEntityID <startEntityID> <endEntityID> <maxDegree> <excludedEntities> <requiredDsrcs> <flags>\n'
        
        try:
            args = self.parser.parse_args(['findPathIncludingSourceByEntityID'] + parse(arg))
        except SystemExit:
            print(self.do_findPathIncludingSourceByEntityID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findPathIncludingSourceByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,args.excludedEntities,args.requiredDsrcs,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityByRecordID(self, arg):
        '\nGet entity by record ID:  getEntityByRecordID <dataSourceCode> <recordID>\n'

        try:
            args = self.parser.parse_args(['getEntityByRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityByRecordID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.getEntityByRecordID(args.dataSourceCode, args.recordID,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityByRecordIDV2(self, arg):
        '\nGet entity by record ID:  getEntityByRecordIDV2 <dataSourceCode> <recordID> <flags>\n'
        
        try:
            args = self.parser.parse_args(['getEntityByRecordIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityByRecordIDV2.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.getEntityByRecordIDV2(args.dataSourceCode, args.recordID, args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathByRecordID(self, arg):
        '\nFind path between two records:  findPathByRecordID <startDataSourceCode> <startRecordID> <endDataSourceCode> <endRecordID> <maxDegree>\n'
        
        try:
            args = self.parser.parse_args(['findPathByRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_findPathByRecordID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findPathByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathByRecordIDV2(self, arg):
        '\nFind path between two records:  findPathByRecordIDV2 <startDataSourceCode> <startRecordID> <endDataSourceCode> <endRecordID> <maxDegree> <flags>\n'
        
        try:
            args = self.parser.parse_args(['findPathByRecordIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_findPathByRecordIDV2.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findPathByRecordIDV2(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findNetworkByRecordID(self, arg):
        '\nFind network between records:  findNetworkByRecordID <recordList> <maxDegree> <buildOutDegree> <maxEntities>\n'
        
        try:
            args = self.parser.parse_args(['findNetworkByRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_findNetworkByRecordID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findNetworkByRecordID(args.recordList,args.maxDegree,args.buildOutDegree,args.maxEntities,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findNetworkByRecordIDV2(self, arg):
        '\nFind network between records:  findNetworkByRecordIDV2 <recordList> <maxDegree> <buildOutDegree> <maxEntities> <flags>\n'
        
        try:
            args = self.parser.parse_args(['findNetworkByRecordIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_findNetworkByRecordIDV2.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findNetworkByRecordIDV2(args.recordList,args.maxDegree,args.buildOutDegree,args.maxEntities,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyEntityByRecordID(self, arg):
        '\nDetermine why a record is inside its entity:  whyEntityByRecordID <dataSourceCode> <recordID>\n'
        
        try:
            args = self.parser.parse_args(['whyEntityByRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_whyEntityByRecordID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.whyEntityByRecordID(args.dataSourceCode,args.recordID,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyEntityByRecordIDV2(self, arg):
        '\nDetermine why a record is inside its entity:  whyEntityByRecordIDV2 <dataSourceCode> <recordID> <flags>\n'
        
        try:
            args = self.parser.parse_args(['whyEntityByRecordIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_whyEntityByRecordIDV2.__doc__)
            return
       
        try: 
            response = bytearray() 
            self.g2_module.whyEntityByRecordIDV2(args.dataSourceCode,args.recordID,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyEntityByEntityID(self, arg):
        '\nDetermine why records are inside an entity:  whyEntityByEntityID <entityID>\n'
        
        try:
            args = self.parser.parse_args(['whyEntityByEntityID'] + parse(arg))
        except SystemExit:
            print(self.do_whyEntityByEntityID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.whyEntityByEntityID(args.entityID,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyEntityByEntityIDV2(self, arg):
        '\nDetermine why records are inside an entity:  whyEntityByEntityIDV2 <entityID> <flags>\n'
        
        try:
            args = self.parser.parse_args(['whyEntityByEntityIDV2'] + parse(arg))
        except SystemExit:
            print(self.do_whyEntityByEntityIDV2.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.whyEntityByEntityIDV2(args.entityID,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyEntities(self, arg):
        '\nDetermine how entities relate to each other:  whyEntities <entityID1> <entityID2>\n'
        
        try:
            args = self.parser.parse_args(['whyEntities'] + parse(arg))
        except SystemExit:
            print(self.do_whyEntities.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.whyEntities(args.entityID1,args.entityID2,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyEntitiesV2(self, arg):
        '\nDetermine how entities relate to each other:  whyEntitiesV2 <entityID1> <entityID2> <flags>\n'
        
        try:
            args = self.parser.parse_args(['whyEntitiesV2'] + parse(arg))
        except SystemExit:
            print(self.do_whyEntitiesV2.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.whyEntitiesV2(args.entityID1,args.entityID2,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyRecords(self, arg):
        '\nDetermine how two records relate to each other:  whyRecords <dataSourceCode1> <recordID1> <dataSourceCode2> <recordID2>\n'
        
        try:
            args = self.parser.parse_args(['whyRecords'] + parse(arg))
        except SystemExit:
            print(self.do_whyRecords.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.whyRecords(args.dataSourceCode1,args.recordID1,args.dataSourceCode2,args.recordID2,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_whyRecordsV2(self, arg):
        '\nDetermine how two records relate to each other:  whyRecordsV2 <dataSourceCode1> <recordID1> <dataSourceCode2> <recordID2> <flags>\n'
        
        try:
            args = self.parser.parse_args(['whyRecordsV2'] + parse(arg))
        except SystemExit:
            print(self.do_whyRecordsV2.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.whyRecordsV2(args.dataSourceCode1,args.recordID1,args.dataSourceCode2,args.recordID2,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathExcludingByRecordID(self, arg):
        '\nFind path between two records, with exclusions:  findPathExcludingByRecordID <startDataSourceCode> <startRecordID> <endDataSourceCode> <endRecordID> <maxDegree> <excludedEntities> <flags>\n'

        try:
            args = self.parser.parse_args(['findPathExcludingByRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_findPathExcludingByRecordID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findPathExcludingByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.excludedEntities,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_findPathIncludingSourceByRecordID(self, arg):
        '\nFind path between two records that includes a watched dsrc list, with exclusions:  findPathIncludingSourceByRecordID <startDataSourceCode> <startRecordID> <endDataSourceCode> <endRecordID> <maxDegree> <excludedEntities> <requiredDsrcs> <flags>\n'
        
        try:
            args = self.parser.parse_args(['findPathIncludingSourceByRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_findPathIncludingSourceByRecordID.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.findPathIncludingSourceByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.excludedEntities,args.requiredDsrcs,args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getRecord(self, arg):
        '\nGet record for record ID :  getRecord <dataSourceCode> <recordID>\n'
        
        try:
            args = self.parser.parse_args(['getRecord'] + parse(arg))
        except SystemExit:
            print(self.do_getRecord.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.getRecord(args.dataSourceCode, args.recordID,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getRecordV2(self, arg):
        '\nGet record for record ID :  getRecordV2 <dataSourceCode> <recordID> <flags>\n'
        
        try:
            args = self.parser.parse_args(['getRecordV2'] + parse(arg))
        except SystemExit:
            print(self.do_getRecordV2.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_module.getRecordV2(args.dataSourceCode, args.recordID, args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_countRedoRecords(self,arg):
        '\nCounts the number of records in the redo queue:  countRedoRecords\n'

        try: 
            recordCount = self.g2_module.countRedoRecords()
            print('Record Count: %d' % recordCount)
        except G2Exception.G2Exception as err:
            print(err)


    def do_processRedoRecord(self, arg):
        '\nProcess a redo record:  processRedoRecord <recordID>\n'
        
        try:
            response = bytearray()
            self.g2_module.processRedoRecord(response)
            if response:
                print('response: {}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_processRedoRecordWithInfo(self, arg):
        '\nProcess a redo record with returned info:  processRedoRecordWithInfo <recordID> [-f <flags>]\n'
        
        try:
            args = self.parser.parse_args(['processRedoRecordWithInfo'] + parse(arg))
        except SystemExit:
            print(self.do_processRedoRecordWithInfo.__doc__)
            return
        
        try:
            flags = int(inspect.signature(self.g2_module.processRedoRecordWithInfo).parameters['flags'].default)
            if args.flags:
                flags = int(args.flags)

            response = bytearray()
            info = bytearray()
            self.g2_module.processRedoRecordWithInfo(response, info, flags=flags)
            if response:
                print('response: {}'.format(response.decode()))
            else:
                print('\nNo response!\n')
            if info:
                print('info: {}'.format(info.decode()))
            else:
                print('\nNo info!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityDetails(self, arg):
        '\nGet the profile of a resolved entity:  getEntityDetails -e <entityID> [-d]\n'
        
        try:
            args = self.parser.parse_args(['getEntityDetails'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityDetails.__doc__)
            return
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getEntityDetails(args.entityID,args.includeInternalFeatures,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getRelationshipDetails(self, arg):
        '\nGet the profile of a relationship:  getRelationshipDetails -r <relationshipID> [-d]\n'
        
        try:
            args = self.parser.parse_args(['getRelationshipDetails'] + parse(arg))
        except SystemExit:
            print(self.do_getRelationshipDetails.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getRelationshipDetails(args.relationshipID,args.includeInternalFeatures,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityResume(self, arg):
        '\nGet the related records for a resolved entity:  getEntityResume <entityID>\n'
        
        try:
            args = self.parser.parse_args(['getEntityResume'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityResume.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getEntityResume(args.entityID,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityListBySize(self, arg):
        '\nGet list of resolved entities of specified size:  getEntityListBySize -s <entitySize> [-o <output_file>]\n'
        
        try:
            args = self.parser.parse_args(['getEntityListBySize'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityListBySize.__doc__)
            return
        
        try: 
            sizedEntityHandle = self.g2_diagnostic_module.getEntityListBySize(args.entitySize)
            response = bytearray() 
            rowData = self.g2_diagnostic_module.fetchNextEntityBySize(sizedEntityHandle,response)
            resultString = b""
            while rowData:
                resultString += response
                response = bytearray()
                rowData = self.g2_diagnostic_module.fetchNextEntityBySize(sizedEntityHandle,response)
            self.g2_diagnostic_module.closeEntityListBySize(sizedEntityHandle)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(resultString.decode())
            else:
                print('{}'.format(resultString.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntityListBySizeV2(self, arg):
        '\nGet list of resolved entities of specified size:  getEntityListBySizeV2 -s <entitySize> [-o <output_file>]\n'
        try:
            args = self.parser.parse_args(['getEntityListBySize'] + parse(arg))
        except SystemExit:
            print(self.do_getEntityListBySizeV2.__doc__)
            return
        try: 
            sizedEntityHandle = self.g2_diagnostic_module.getEntityListBySizeV2(args.entitySize)
            response = bytearray() 
            rowData = self.g2_diagnostic_module.fetchNextEntityBySizeV2(sizedEntityHandle,response)
            resultString = b""
            while rowData:
                resultString += response
                response = bytearray()
                rowData = self.g2_diagnostic_module.fetchNextEntityBySizeV2(sizedEntityHandle,response)
            self.g2_diagnostic_module.closeEntityListBySizeV2(sizedEntityHandle)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(resultString.decode())
            else:
                print('{}'.format(resultString.decode()))
        except G2Exception.G2Exception as err:
            print(err)
    def do_checkDBPerf(self,arg):
        '\nRun a check on the DB performance:  checkDBPerf\n'
        
        try:
            args = self.parser.parse_args(['checkDBPerf'] + parse(arg))
        except SystemExit:
            print(self.do_checkDBPerf.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.checkDBPerf(args.secondsToRun,response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getDataSourceCounts(self,arg):
        '\nGet record counts by data source and entity type:  getDataSourceCounts\n'
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getDataSourceCounts(response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getMappingStatistics(self,arg):
        '\nGet data source mapping statistics:  getMappingStatistics [-d]\n'
        
        try:
            args = self.parser.parse_args(['getMappingStatistics'] + parse(arg))
        except SystemExit:
            print(self.do_getMappingStatistics.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getMappingStatistics(args.includeInternalFeatures,response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getGenericFeatures(self,arg):
        '\nGet a list of generic values for a feature type:  getGenericFeatures [-t <featureType>] [-m <maximumEstimatedCount>]\n'
        
        try:
            args = self.parser.parse_args(['getGenericFeatures'] + parse(arg))
        except SystemExit:
            print(self.do_getGenericFeatures.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getGenericFeatures(args.featureType,args.maximumEstimatedCount,response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getEntitySizeBreakdown(self,arg):
        '\nGet the number of entities of each entity size:  getEntitySizeBreakdown [-m <minimumEntitySize>] [-d]\n'
        
        try:
            args = self.parser.parse_args(['getEntitySizeBreakdown'] + parse(arg))
        except SystemExit:
            print(self.do_getEntitySizeBreakdown.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getEntitySizeBreakdown(args.minimumEntitySize,args.includeInternalFeatures,response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getResolutionStatistics(self,arg):
        '\nGet resolution statistics:  getResolutionStatistics\n'
        
        try: 
            response = bytearray() 
            self.g2_diagnostic_module.getResolutionStatistics(response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_findEntitiesByFeatureIDs(self, arg):
        '\nGet the entities for a list of features:  findEntitiesByFeatureIDs <json_data>\n'
        
        try:
            args = self.parser.parse_args(['findEntitiesByFeatureIDs'] + parse(arg))
        except SystemExit:
            print(self.do_findEntitiesByFeatureIDs.__doc__)
            return
        
        try:
            response = bytearray()
            self.g2_diagnostic_module.findEntitiesByFeatureIDs(args.features,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_stats(self,arg):
        '\nGet engine workload statistics for last process:  stats\n'
        
        try: 
            response = bytearray() 
            self.g2_module.stats(response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)


    def do_exportConfig(self,arg):
        '\nExport the config:  exportConfig [-o <output_file>]\n'    
        
        try:
            args = self.parser.parse_args(['outputOptional'] + parse(arg))
        except SystemExit:
            print(self.do_exportConfig.__doc__)
            return
        
        try: 
            response = bytearray() 
            configID = bytearray() 
            self.g2_module.exportConfig(response,configID)
            responseMsg = json.loads(response)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    json.dump(responseMsg,data_out)
            else:
                printResponse(json.dumps(responseMsg))
        except G2Exception.G2Exception as err:
            print(err)


    def do_getActiveConfigID(self,arg):
        '\nGet the config identifier:  getActiveConfigID\n'    
        
        try: 
            response = bytearray() 
            self.g2_module.getActiveConfigID(response)
            printResponse(response.decode())
        except G2Exception.G2Exception as err:
            print(err)


    def do_getRepositoryLastModifiedTime(self,arg):
        '\nGet the last modified time of the datastore:  getRepositoryLastModifiedTime\n'    
        
        try: 
            response = bytearray() 
            self.g2_module.getRepositoryLastModifiedTime(response)
            printResponse(response.decode())
        except G2Exception.G2Exception as err:
            print(err)


    def do_exportTokenLibrary(self,arg):
        '\nExport the token library:  exportTokenLibrary [-o <output_file>]\n'
        
        try:
            args = self.parser.parse_args(['outputOptional'] + parse(arg))
        except SystemExit:
            print(self.do_exportTokenLibrary.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_hasher_module.exportTokenLibrary(response)
            responseMsg = json.loads(response)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    json.dump(responseMsg,data_out)
            else:
                printResponse(json.dumps(responseMsg))
        except G2Exception.G2Exception as err:
            print(err)


    def do_purgeRepository(self, arg):

        '''\nPurge G2 repository:  purgeRepository [-n] [--purgeWithoutPrompt]

  Where:
    -n = Skip resetting the resolver

    --FORCEPURGE = Don't prompt before purging. USE WITH CAUTION!\n'''

        try:
            args = self.parser.parse_args(['purgeRepository'] + parse(arg))
            if not args.forcePurge:
                if input('\n*** This will purge all currently loaded data from the Senzing repository! ***\n\nAre you sure? (y/n)  ') not in ['y','Y', 'yes', 'YES']:
                    print()
                    return
            else:
                printWithNewLines(f'INFO: Purging without prompting, --FORCEPURGE was specified!', 'S')
            
        except SystemExit:
            print(self.do_purgeRepository.__doc__)
            return

        if args.noReset:
            reset_resolver = False
            resolver_txt = '(without resetting resolver)'
        else:
            reset_resolver=True
            resolver_txt = '(and resetting resolver)'
        
        printWithNewLines(f'Purging the repository {resolver_txt}', 'B')     

        try:
            self.g2_module.purgeRepository(reset_resolver) 
        except G2Exception.G2Exception as err:
            printWithNewLines(f'G2Exception: {err}', 'B')


    def do_hashRecord(self, arg):
        '\nHash an entity record:  hashRecord <json_data>\n'
        
        try:
            args = self.parser.parse_args(['jsonOnly'] + parse(arg))
        except SystemExit:
            print(self.do_hashRecord.__doc__)
            return
        
        try: 
            response = bytearray() 
            self.g2_hasher_module.process(args.jsonData,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)


    def do_hashFile(self, arg):
        '\nHash a file of entity records:  hashFile <input_file> [-o <output_file>]\n'
        
        try:
            args = self.parser.parse_args(['inputFile'] + parse(arg))
        except SystemExit:
            print(self.do_hashFile.__doc__)
            return
        
        try: 
            print()
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    with open(args.inputFile.split("?")[0]) as data_in:
                        for line in data_in:
                            response = bytearray() 
                            self.g2_hasher_module.process(line.strip(),response)
                            hashedData = response.decode()
                            data_out.write(hashedData)
                            data_out.write('\n')
            else:
                with open(args.inputFile.split("?")[0]) as data_in :
                    for line in data_in:
                        response = bytearray() 
                        self.g2_hasher_module.process(line.strip(),response)
                        hashedData = response.decode()
                        printWithNewLine(hashedData)
        except G2Exception.G2Exception as err:
            print(err)


    def do_license(self,arg):
        '\nGet the license information:  license\n'

        try: 
            response = self.g2_product_module.license()
            printWithNewLines(response, 'B')
        except G2Exception.G2Exception as err:
            printWithNewLines(f'G2Exception: {err}', 'B')


    def do_validateLicenseFile(self,arg):
        '\nValidate a license file:  validateLicenseFile <licenseFilePath>\n'
        
        try:
            args = self.parser.parse_args(['validateLicenseFile'] + parse(arg))
        except SystemExit:
            print(self.do_validateLicenseFile.__doc__)
            return
        
        try: 
            returnCode = self.g2_product_module.validateLicenseFile(args.licenseFilePath)
            if returnCode == 0:
                printWithNewLine('License validated')
            else:
                printWithNewLine('License is not valid.')
        except G2Exception.G2Exception as err:
            print(err)


    def do_version(self,arg):
        '\nGet the version information:  version\n'

        try: 
            response = json.dumps(json.loads(self.g2_product_module.version()))
            print('\nG2 version:')
            printWithNewLine(response)
        except G2Exception.G2Exception as err:
            print(err)


    def do_getPhysicalCores(self,arg):
        '\nGet the number of physical cores:  getPhysicalCores\n'
        
        try: 
            numCores = self.g2_diagnostic_module.getPhysicalCores()
            printWithNewLine('\nPhysical Cores: %d' % numCores)
        except G2Exception.G2Exception as err:
            print(err)


    def do_getLogicalCores(self,arg):
        '\nGet the number of logical cores:  getLogicalCores\n'
        
        try: 
            numCores = self.g2_diagnostic_module.getLogicalCores()
            printWithNewLine('\nLogical Cores: %d' % numCores)
        except G2Exception.G2Exception as err:
            print(err)


    def do_getTotalSystemMemory(self,arg):
        '\nGet the total system memory:  getTotalSystemMemory\n'
        
        try: 
            memory = self.g2_diagnostic_module.getTotalSystemMemory()
            printWithNewLine('\nTotal System Memory: %d' % memory)
        except G2Exception.G2Exception as err:
            print(err)


    def do_getAvailableMemory(self,arg):
        '\nGet the available memrory:  getAvailableMemory\n'

        try: 
            memory = self.g2_diagnostic_module.getAvailableMemory()
            printWithNewLine('\nAvailable Memory: %d' % memory)
        except G2Exception.G2Exception as err:
            print(err)


# ----- Non API call commands -----
# ---- DO NOT docstring these! ----

    def do_histDedupe(self, arg):

        if self.histAvail:
            if input('\nThis will de-duplicate both this session history and the history file, are you sure? (y/n)  ') in ['y','Y', 'yes', 'YES']:
    
                with open(self.histFileName) as hf:
                    linesIn = (line.rstrip() for line in hf)
                    uniqLines = OrderedDict.fromkeys( line for line in linesIn if line )
    
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
            if input('\nThis will clear both this session history and the history file, are you sure? (y/n)  ') in ['y','Y', 'yes', 'YES']:
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


    def do_restart(self, arg):
        self.restart = True
        return True


    def do_restartDebug(self, arg):
        self.restart_debug = True
        return True


    def ret_restart(self):
        return self.restart


    def ret_restart_debug(self):
        return self.restart_debug


    def do_timer(self, arg):

        if self.timerOn:
            self.timerOn = False
            printWithNewLines('Timer is now off', 'B')
        else:
            self.timerOn = True
            printWithNewLines('Timer is now on', 'B')


# ----- Utility functions -----

def parse(argumentString):
    'Parses an argument list into a logical set of argument strings'
    return shlex.split(argumentString)


def printWithNewLine(arg):
    print(arg + '\n')


def printWithNewLines(ln, pos=''):

    pos = pos.upper()
    if pos == 'S' or pos == 'START' :
        print(f'\n{ln}')
    elif pos == 'E' or pos == 'END' :
        print(f'{ln}\n')
    elif pos == 'B' or pos == 'BOTH' :
        print(f'\n{ln}\n')
    else:
        print(f'{ln}') 


def printResponse(response):
    print('\n' + response + '\n')



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('fileToProcess', default=None, nargs='?')
    parser.add_argument('-c', '--iniFile', dest='iniFile', default='', help='name of a G2Module.ini file to use', nargs=1)
    parser.add_argument('-t', '--debugTrace', dest='debugTrace', action='store_true', default=False, help='output debug trace information')
    parser.add_argument('-H', '--histDisable', dest='histDisable', action='store_true', default=False, help='disable history file usage')
    args = parser.parse_args()

    first_loop = True
    restart = False
    
    # If ini file isn't specified try and locate it with G2Paths
    ini_file_name = pathlib.Path(G2Paths.get_G2Module_ini_path()) if not args.iniFile else pathlib.Path(args.iniFile[0]).resolve()
    G2Paths.check_file_exists_and_readable(ini_file_name)

    # Warn if using out dated parms
    g2health = G2Health()
    g2health.checkIniParams(ini_file_name)

    # Get the INI paramaters to use
    iniParamCreator = G2IniParams()
    g2module_params = iniParamCreator.getJsonINIParams(ini_file_name)

    # Execute a file of commands
    if args.fileToProcess:

        cmd_obj = G2CmdShell(args.debugTrace, args.histDisable, ini_file_name)
        cmd_obj.fileloop(args.fileToProcess)

    # Start command shell 
    else:
        # Don't use args.debugTrace here we may need to restart
        debug_trace = args.debugTrace

        while first_loop or restart:

            # Have we been in the command shell already and are trying to quit? Used for restarting
            if 'cmd_obj' in locals() and cmd_obj.ret_quit():
                    break

            cmd_obj = G2CmdShell(debug_trace, args.histDisable, ini_file_name)
            cmd_obj.cmdloop()

            restart = True if cmd_obj.ret_restart() or cmd_obj.ret_restart_debug() else False
            debug_trace = True if cmd_obj.ret_restart_debug() else False
            first_loop = False

    sys.exit()
