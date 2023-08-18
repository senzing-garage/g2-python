#! /usr/bin/env python3

import argparse
import cmd
import csv
import glob
import json
import os
import pathlib
import re
import shlex
import sys
import textwrap
import time
from contextlib import suppress
from functools import wraps


import G2Paths
from G2IniParams import G2IniParams
from senzing import G2Config, G2ConfigMgr, G2Diagnostic, G2Engine, G2EngineFlags, G2Exception, G2Hasher, G2Product

try:
    import readline
    import atexit
    readline_avail = True
except ImportError:
    readline_avail = False

try:
    import pyperclip
    pyperclip_avail = True
except ImportError:
    pyperclip_avail = False


class Colors:
    @classmethod
    def apply(cls, in_string, color_list=None):
        """Apply list of colors to a string"""
        if color_list:
            prefix = ''.join([getattr(cls, i.strip().upper()) for i in color_list.split(',')])
            suffix = cls.RESET
            return f'{prefix}{in_string}{suffix}'
        return in_string

    @classmethod
    def set_theme(cls, theme):
        if theme.upper() == 'DEFAULT':
            cls.GOOD = cls.FG_GREEN
            cls.BAD = cls.FG_RED
            cls.CAUTION = cls.FG_YELLOW
            cls.HIGHLIGHT1 = cls.FG_MAGENTA
            cls.HIGHLIGHT2 = cls.FG_BLUE
        elif theme.upper() == 'LIGHT':
            cls.GOOD = cls.FG_LIGHTGREEN
            cls.BAD = cls.FG_LIGHTRED
            cls.CAUTION = cls.FG_LIGHTYELLOW
            cls.HIGHLIGHT1 = cls.FG_LIGHTMAGENTA
            cls.HIGHLIGHT2 = cls.FG_LIGHTBLUE

    # styles
    RESET = '\033[0m'
    BOLD = '\033[01m'
    DIM = '\033[02m'
    ITALICS = '\033[03m'
    UNDERLINE = '\033[04m'
    BLINK = '\033[05m'
    REVERSE = '\033[07m'
    STRIKETHROUGH = '\033[09m'
    INVISIBLE = '\033[08m'
    # foregrounds
    FG_BLACK = '\033[30m'
    FG_WHITE = '\033[97m'
    FG_BLUE = '\033[34m'
    FG_MAGENTA = '\033[35m'
    FG_CYAN = '\033[36m'
    FG_YELLOW = '\033[33m'
    FG_GREEN = '\033[32m'
    FG_RED = '\033[31m'
    FG_LIGHTBLACK = '\033[90m'
    FG_LIGHTWHITE = '\033[37m'
    FG_LIGHTBLUE = '\033[94m'
    FG_LIGHTMAGENTA = '\033[95m'
    FG_LIGHTCYAN = '\033[96m'
    FG_LIGHTYELLOW = '\033[93m'
    FG_LIGHTGREEN = '\033[92m'
    FG_LIGHTRED = '\033[91m'


# Override argparse error to format message
class G2CommandArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.exit(2, colorize_msg(f'\nERROR: {self.prog} - {message}\n', 'error'))


def cmd_decorator(cmd_has_args=True):
    """Decorator for do_* commands to parse args, display help, set response variables etc."""
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            cmd_args = args[0]
            # Check for and set JSON response output type
            # Don't check for JSON format if the end of the string looks like a json file name
            # being written to or from on a command
            if cmd_args[-6:] != '.jsonl' and cmd_args[-5:] != '.json':
                if cmd_args[-5:].lower() == 'jsonl':
                    self.json_output_format = 'jsonl'
                    cmd_args = cmd_args.rstrip('jsonl')
                elif cmd_args[-4:].lower() == 'json':
                    self.json_output_format = 'json'
                    cmd_args = cmd_args.rstrip('json')

            if cmd_has_args:
                try:
                    # Parse args for a command
                    kwargs['parsed_args'] = self.parser.parse_args(
                        [f'{func.__name__[3:]}'] + self.parse(cmd_args))

                    if hasattr(kwargs['parsed_args'], 'flags'):
                        kwargs["flags_int"] = (
                            {"flags": get_engine_flags(kwargs["parsed_args"].flags)}
                            if kwargs["parsed_args"].flags
                            else {}
                        )

                    if func.__name__ in (
                            "do_addRecordWithInfoWithReturnedRecordID",
                            "do_addRecordWithReturnedRecordID"
                    ):
                        kwargs["recordID"] = bytearray()

                    if func.__name__ == 'do_processRedoRecordWithInfo':
                        kwargs['withInfo'] = bytearray()

                    if func.__name__ == 'do_exportConfig':
                        kwargs['configID'] = bytearray()
                # Catch argument errors from parser and display the commands help
                except SystemExit:
                    self.do_help(func.__name__)
                    return
                # Catch parsing errors such as missing single quote around JSON etc.
                # Error is displayed in parse()
                except ValueError:
                    return
                except KeyError as err:
                    self.printError(err)
                    return

            # Reset response for each do_ command, not all commands use it
            kwargs['response'] = bytearray()

            # Run the decorated function passing back args
            try:
                func(self, **kwargs)
            except (G2Exception, IOError) as err:
                self.printError(err)

        return wrapper
    return decorator


class G2CmdShell(cmd.Cmd, object):

    def __init__(self, debug, hist_disable):
        cmd.Cmd.__init__(self)

        self.intro = ''
        self.prompt = '(g2cmd) '
        self.ruler = '-'
        self.doc_header = 'Senzing APIs'
        self.misc_header = 'Help Topics (help <topic>)'
        self.undoc_header = 'Misc Commands'
        # Hide methods - could be deprecated, undocumented, not supported
        self.__hidden_methods = (
            'do_shell', 'do_EOF', 'do_help', 'do_getRedoRecord', 'do_getEntityDetails',
            'do_getRelationshipDetails', 'do_getEntityResume', 'do_getEntityListBySize',
            'do_getDataSourceCounts', 'do_getMappingStatistics', 'do_getGenericFeatures',
            'do_getEntitySizeBreakdown', 'do_getFeature', 'do_getResolutionStatistics',
            'do_findEntitiesByFeatureIDs', 'do_getPhysicalCores', 'do_getLogicalCores',
            'do_getTotalSystemMemory', 'do_getAvailableMemory', 'do_exportTokenLibrary',
            'do_hashRecord', 'do_hashFile', 'do_process', 'do_processWithInfo', 'do_hidden')

        self.g2_engine = G2Engine()
        self.g2_hasher = G2Hasher()
        self.g2_product = G2Product()
        self.g2_diagnostic = G2Diagnostic()
        self.g2_config = G2Config()
        self.g2_configmgr = G2ConfigMgr()

        self.initialized = self.restart = self.restart_debug = self.quit = self.timerOn = False
        self.debug_trace = debug
        self.timerStart = self.timerEnd = None
        self.last_response = ''

        # Readline and history
        self.histDisable = hist_disable
        self.histFileName = self.histFileError = None
        self.histAvail = False
        self.histCheck()

        # Setup for pretty printing
        self.json_output_format = 'json'
        Colors.set_theme('DEFAULT')

        self.parser = G2CommandArgumentParser(add_help=False,
                                              prog='G2Command',
                                              usage=argparse.SUPPRESS)
        self.subparsers = self.parser.add_subparsers()

        hashRecord_parser = self.subparsers.add_parser('hashRecord', usage=argparse.SUPPRESS)
        hashRecord_parser.add_argument('jsonData')

        process_parser = self.subparsers.add_parser('process', usage=argparse.SUPPRESS)
        process_parser.add_argument('jsonData')

        processWithInfo_parser = self.subparsers.add_parser('processWithInfo', usage=argparse.SUPPRESS)
        processWithInfo_parser.add_argument('jsonData')
        processWithInfo_parser.add_argument('-f', '--flags', required=False, nargs='+')

        addConfigFile_parser = self.subparsers.add_parser('addConfigFile', usage=argparse.SUPPRESS)
        addConfigFile_parser.add_argument('configJsonFile')
        addConfigFile_parser.add_argument('configComments')

        getConfig_parser = self.subparsers.add_parser('getConfig', usage=argparse.SUPPRESS)
        getConfig_parser.add_argument('configID', type=int)

        setDefaultConfigID_parser = self.subparsers.add_parser('setDefaultConfigID', usage=argparse.SUPPRESS)
        setDefaultConfigID_parser.add_argument('configID', type=str)

        replaceDefaultConfigID_parser = self.subparsers.add_parser('replaceDefaultConfigID', usage=argparse.SUPPRESS)
        replaceDefaultConfigID_parser.add_argument('oldConfigID', type=int)
        replaceDefaultConfigID_parser.add_argument('newConfigID', type=int)

        searchByAttributes_parser = self.subparsers.add_parser('searchByAttributes', usage=argparse.SUPPRESS)
        searchByAttributes_parser.add_argument('jsonData')
        searchByAttributes_parser.add_argument('-f', '--flags', required=False, nargs='+')

        searchByAttributesV3_parser = self.subparsers.add_parser('searchByAttributesV3', usage=argparse.SUPPRESS)
        searchByAttributesV3_parser.add_argument('jsonData')
        searchByAttributesV3_parser.add_argument('searchProfile')
        searchByAttributesV3_parser.add_argument('-f', '--flags', required=False, nargs='+')

        processFile_parser = self.subparsers.add_parser('processFile', usage=argparse.SUPPRESS)
        processFile_parser.add_argument('inputFile')

        validateLicenseFile_parser = self.subparsers.add_parser('validateLicenseFile', usage=argparse.SUPPRESS)
        validateLicenseFile_parser.add_argument('licenseFilePath')

        validateLicenseStringBase64_parser = self.subparsers.add_parser('validateLicenseStringBase64', usage=argparse.SUPPRESS)
        validateLicenseStringBase64_parser.add_argument('licenseString')

        hashFile_parser = self.subparsers.add_parser('hashFile', usage=argparse.SUPPRESS)
        hashFile_parser.add_argument('inputFile')
        hashFile_parser.add_argument('-o', '--outputFile', required=False)

        exportEntityJson_parser = self.subparsers.add_parser('exportJSONEntityReport', usage=argparse.SUPPRESS)
        exportEntityJson_parser.add_argument('outputFile')
        exportEntityJson_parser.add_argument('-f', '--flags', required=False, nargs='+')

        exportEntityCsv_parser = self.subparsers.add_parser('exportCSVEntityReport', usage=argparse.SUPPRESS)
        exportEntityCsv_parser.add_argument('outputFile')
        exportEntityCsv_parser.add_argument('-f', '--flags', required=False, nargs='+')
        exportEntityCsv_parser.add_argument('-t', '--headersForCSV', required=False, type=str)

        recordModify_parser = self.subparsers.add_parser('recordModify', usage=argparse.SUPPRESS)
        recordModify_parser.add_argument('dataSourceCode')
        recordModify_parser.add_argument('recordID')
        recordModify_parser.add_argument('jsonData')

        replaceRecord_parser = self.subparsers.add_parser('replaceRecord', usage=argparse.SUPPRESS)
        replaceRecord_parser.add_argument('dataSourceCode')
        replaceRecord_parser.add_argument('recordID')
        replaceRecord_parser.add_argument('jsonData')

        replaceRecordWithInfo_parser = self.subparsers.add_parser('replaceRecordWithInfo', usage=argparse.SUPPRESS)
        replaceRecordWithInfo_parser.add_argument('dataSourceCode')
        replaceRecordWithInfo_parser.add_argument('recordID')
        replaceRecordWithInfo_parser.add_argument('jsonData')
        replaceRecordWithInfo_parser.add_argument('-f', '--flags', required=False, nargs='*')

        addRecord_parser = self.subparsers.add_parser('addRecord', usage=argparse.SUPPRESS)
        addRecord_parser.add_argument('dataSourceCode')
        addRecord_parser.add_argument('recordID')
        addRecord_parser.add_argument('jsonData')

        addRecordWithInfo_parser = self.subparsers.add_parser('addRecordWithInfo', usage=argparse.SUPPRESS)
        addRecordWithInfo_parser.add_argument('dataSourceCode')
        addRecordWithInfo_parser.add_argument('recordID')
        addRecordWithInfo_parser.add_argument('jsonData')
        addRecordWithInfo_parser.add_argument('-f', '--flags', required=False, nargs='+')

        replaceRecordWithInfo_parser = self.subparsers.add_parser('replaceRecordWithInfo', usage=argparse.SUPPRESS)
        replaceRecordWithInfo_parser.add_argument('dataSourceCode')
        replaceRecordWithInfo_parser.add_argument('recordID')
        replaceRecordWithInfo_parser.add_argument('jsonData')
        replaceRecordWithInfo_parser.add_argument('-f', '--flags', required=False, nargs='+')

        addRecordWithReturnedRecordID_parser = self.subparsers.add_parser('addRecordWithReturnedRecordID', usage=argparse.SUPPRESS)
        addRecordWithReturnedRecordID_parser.add_argument('dataSourceCode')
        addRecordWithReturnedRecordID_parser.add_argument('jsonData')

        addRecordWithInfoWithReturnedRecordID_parser = self.subparsers.add_parser('addRecordWithInfoWithReturnedRecordID', usage=argparse.SUPPRESS)
        addRecordWithInfoWithReturnedRecordID_parser.add_argument('dataSourceCode')
        addRecordWithInfoWithReturnedRecordID_parser.add_argument('jsonData')
        addRecordWithInfoWithReturnedRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        deleteRecord_parser = self.subparsers.add_parser('deleteRecord', usage=argparse.SUPPRESS)
        deleteRecord_parser.add_argument('dataSourceCode')
        deleteRecord_parser.add_argument('recordID')

        deleteRecordWithInfo_parser = self.subparsers.add_parser('deleteRecordWithInfo', usage=argparse.SUPPRESS)
        deleteRecordWithInfo_parser.add_argument('dataSourceCode')
        deleteRecordWithInfo_parser.add_argument('recordID')
        deleteRecordWithInfo_parser.add_argument('-f', '--flags', required=False, nargs='+')

        getEntityByEntityID_parser = self.subparsers.add_parser('getEntityByEntityID', usage=argparse.SUPPRESS)
        getEntityByEntityID_parser.add_argument('entityID', type=int)
        getEntityByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findInterestingEntitiesByEntityID_parser = self.subparsers.add_parser('findInterestingEntitiesByEntityID', usage=argparse.SUPPRESS)
        findInterestingEntitiesByEntityID_parser.add_argument('entityID', type=int)
        findInterestingEntitiesByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findInterestingEntitiesByRecordID_parser = self.subparsers.add_parser('findInterestingEntitiesByRecordID', usage=argparse.SUPPRESS)
        findInterestingEntitiesByRecordID_parser.add_argument('dataSourceCode')
        findInterestingEntitiesByRecordID_parser.add_argument('recordID')
        findInterestingEntitiesByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findPathByEntityID_parser = self.subparsers.add_parser('findPathByEntityID', usage=argparse.SUPPRESS)
        findPathByEntityID_parser.add_argument('startEntityID', type=int)
        findPathByEntityID_parser.add_argument('endEntityID', type=int)
        findPathByEntityID_parser.add_argument('maxDegree', type=int)
        findPathByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findPathExcludingByEntityID_parser = self.subparsers.add_parser('findPathExcludingByEntityID', usage=argparse.SUPPRESS)
        findPathExcludingByEntityID_parser.add_argument('startEntityID', type=int)
        findPathExcludingByEntityID_parser.add_argument('endEntityID', type=int)
        findPathExcludingByEntityID_parser.add_argument('maxDegree', type=int)
        findPathExcludingByEntityID_parser.add_argument('excludedEntities')
        findPathExcludingByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findPathIncludingSourceByEntityID_parser = self.subparsers.add_parser('findPathIncludingSourceByEntityID', usage=argparse.SUPPRESS)
        findPathIncludingSourceByEntityID_parser.add_argument('startEntityID', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('endEntityID', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('maxDegree', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('excludedEntities')
        findPathIncludingSourceByEntityID_parser.add_argument('requiredDsrcs')
        findPathIncludingSourceByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findNetworkByEntityID_parser = self.subparsers.add_parser('findNetworkByEntityID', usage=argparse.SUPPRESS)
        findNetworkByEntityID_parser.add_argument('entityList')
        findNetworkByEntityID_parser.add_argument('maxDegree', type=int)
        findNetworkByEntityID_parser.add_argument('buildOutDegree', type=int)
        findNetworkByEntityID_parser.add_argument('maxEntities', type=int)
        findNetworkByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        getEntityDetails_parser = self.subparsers.add_parser('getEntityDetails', usage=argparse.SUPPRESS)
        getEntityDetails_parser.add_argument('entityID', type=int)
        getEntityDetails_parser.add_argument('-i', '--includeInternalFeatures', action='store_true', required=False, default=False)

        getRelationshipDetails_parser = self.subparsers.add_parser('getRelationshipDetails', usage=argparse.SUPPRESS)
        getRelationshipDetails_parser.add_argument('relationshipID', type=int)
        getRelationshipDetails_parser.add_argument('-i', '--includeInternalFeatures', action='store_true', required=False, default=False)

        getMappingStatistics_parser = self.subparsers.add_parser('getMappingStatistics', usage=argparse.SUPPRESS)
        getMappingStatistics_parser.add_argument('-i', '--includeInternalFeatures', action='store_true', required=False, default=False)

        checkDBPerf_parser = self.subparsers.add_parser('checkDBPerf', usage=argparse.SUPPRESS)
        checkDBPerf_parser.add_argument('secondsToRun', type=int, nargs='?', default=3)

        getGenericFeatures_parser = self.subparsers.add_parser('getGenericFeatures', usage=argparse.SUPPRESS)
        getGenericFeatures_parser.add_argument('featureType')
        getGenericFeatures_parser.add_argument('-m', '--maximumEstimatedCount', required=False, type=int, default=1000)

        getEntitySizeBreakdown_parser = self.subparsers.add_parser('getEntitySizeBreakdown', usage=argparse.SUPPRESS)
        getEntitySizeBreakdown_parser.add_argument('minimumEntitySize', type=int)
        getEntitySizeBreakdown_parser.add_argument('-i', '--includeInternalFeatures', action='store_true', required=False, default=False)

        getFeature_parser = self.subparsers.add_parser('getFeature', usage=argparse.SUPPRESS)
        getFeature_parser.add_argument('featureID', type=int)

        getEntityResume_parser = self.subparsers.add_parser('getEntityResume', usage=argparse.SUPPRESS)
        getEntityResume_parser.add_argument('entityID', type=int)

        getEntityListBySize_parser = self.subparsers.add_parser('getEntityListBySize', usage=argparse.SUPPRESS)
        getEntityListBySize_parser.add_argument('entitySize', type=int)

        getEntityByRecordID_parser = self.subparsers.add_parser('getEntityByRecordID', usage=argparse.SUPPRESS)
        getEntityByRecordID_parser.add_argument('dataSourceCode')
        getEntityByRecordID_parser.add_argument('recordID')
        getEntityByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        getRecord_parser = self.subparsers.add_parser('getRecord', usage=argparse.SUPPRESS)
        getRecord_parser.add_argument('dataSourceCode')
        getRecord_parser.add_argument('recordID')
        getRecord_parser.add_argument('-f', '--flags', required=False, nargs='+')

        reevaluateRecord_parser = self.subparsers.add_parser('reevaluateRecord', usage=argparse.SUPPRESS)
        reevaluateRecord_parser.add_argument('dataSourceCode')
        reevaluateRecord_parser.add_argument('recordID')
        reevaluateRecord_parser.add_argument('-f', '--flags', required=False, type=int)

        reevaluateRecordWithInfo_parser = self.subparsers.add_parser('reevaluateRecordWithInfo', usage=argparse.SUPPRESS)
        reevaluateRecordWithInfo_parser.add_argument('dataSourceCode')
        reevaluateRecordWithInfo_parser.add_argument('recordID')
        reevaluateRecordWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        reevaluateEntity_parser = self.subparsers.add_parser('reevaluateEntity', usage=argparse.SUPPRESS)
        reevaluateEntity_parser.add_argument('entityID', type=int)
        reevaluateEntity_parser.add_argument('-f', '--flags', required=False, type=int)

        reevaluateEntityWithInfo_parser = self.subparsers.add_parser('reevaluateEntityWithInfo', usage=argparse.SUPPRESS)
        reevaluateEntityWithInfo_parser.add_argument('entityID', type=int)
        reevaluateEntityWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        findPathByRecordID_parser = self.subparsers.add_parser('findPathByRecordID', usage=argparse.SUPPRESS)
        findPathByRecordID_parser.add_argument('startDataSourceCode')
        findPathByRecordID_parser.add_argument('startRecordID')
        findPathByRecordID_parser.add_argument('endDataSourceCode')
        findPathByRecordID_parser.add_argument('endRecordID')
        findPathByRecordID_parser.add_argument('maxDegree', type=int)
        findPathByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findPathExcludingByRecordID_parser = self.subparsers.add_parser('findPathExcludingByRecordID', usage=argparse.SUPPRESS)
        findPathExcludingByRecordID_parser.add_argument('startDataSourceCode')
        findPathExcludingByRecordID_parser.add_argument('startRecordID')
        findPathExcludingByRecordID_parser.add_argument('endDataSourceCode')
        findPathExcludingByRecordID_parser.add_argument('endRecordID')
        findPathExcludingByRecordID_parser.add_argument('maxDegree', type=int)
        findPathExcludingByRecordID_parser.add_argument('excludedEntities')
        findPathExcludingByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findPathIncludingSourceByRecordID_parser = self.subparsers.add_parser('findPathIncludingSourceByRecordID', usage=argparse.SUPPRESS)
        findPathIncludingSourceByRecordID_parser.add_argument('startDataSourceCode')
        findPathIncludingSourceByRecordID_parser.add_argument('startRecordID')
        findPathIncludingSourceByRecordID_parser.add_argument('endDataSourceCode')
        findPathIncludingSourceByRecordID_parser.add_argument('endRecordID')
        findPathIncludingSourceByRecordID_parser.add_argument('maxDegree', type=int)
        findPathIncludingSourceByRecordID_parser.add_argument('excludedEntities')
        findPathIncludingSourceByRecordID_parser.add_argument('requiredDsrcs')
        findPathIncludingSourceByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        findNetworkByRecordID_parser = self.subparsers.add_parser('findNetworkByRecordID', usage=argparse.SUPPRESS)
        findNetworkByRecordID_parser.add_argument('recordList')
        findNetworkByRecordID_parser.add_argument('maxDegree', type=int)
        findNetworkByRecordID_parser.add_argument('buildOutDegree', type=int)
        findNetworkByRecordID_parser.add_argument('maxEntities', type=int)
        findNetworkByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        whyEntityByRecordID_parser = self.subparsers.add_parser('whyEntityByRecordID', usage=argparse.SUPPRESS)
        whyEntityByRecordID_parser.add_argument('dataSourceCode')
        whyEntityByRecordID_parser.add_argument('recordID')
        whyEntityByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        whyEntityByEntityID_parser = self.subparsers.add_parser('whyEntityByEntityID', usage=argparse.SUPPRESS)
        whyEntityByEntityID_parser.add_argument('entityID', type=int)
        whyEntityByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        howEntityByEntityID_parser = self.subparsers.add_parser('howEntityByEntityID', usage=argparse.SUPPRESS)
        howEntityByEntityID_parser.add_argument('entityID', type=int)
        howEntityByEntityID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        getVirtualEntityByRecordID_parser = self.subparsers.add_parser('getVirtualEntityByRecordID', usage=argparse.SUPPRESS)
        getVirtualEntityByRecordID_parser.add_argument('recordList')
        getVirtualEntityByRecordID_parser.add_argument('-f', '--flags', required=False, nargs='+')

        whyEntities_parser = self.subparsers.add_parser('whyEntities', usage=argparse.SUPPRESS)
        whyEntities_parser.add_argument('entityID1', type=int)
        whyEntities_parser.add_argument('entityID2', type=int)
        whyEntities_parser.add_argument('-f', '--flags', required=False, nargs='+')

        whyRecords_parser = self.subparsers.add_parser('whyRecords', usage=argparse.SUPPRESS)
        whyRecords_parser.add_argument('dataSourceCode1')
        whyRecords_parser.add_argument('recordID1')
        whyRecords_parser.add_argument('dataSourceCode2')
        whyRecords_parser.add_argument('recordID2')
        whyRecords_parser.add_argument('-f', '--flags', required=False, nargs='+')

        exportConfig_parser = self.subparsers.add_parser('exportConfig', usage=argparse.SUPPRESS)
        exportConfig_parser.add_argument('-o', '--outputFile', required=False)

        exportTokenLibrary_parser = self.subparsers.add_parser('exportTokenLibrary', usage=argparse.SUPPRESS)
        exportTokenLibrary_parser.add_argument('-o', '--outputFile', required=False)

        findEntitiesByFeatureIDs_parser = self.subparsers.add_parser('findEntitiesByFeatureIDs', usage=argparse.SUPPRESS)
        findEntitiesByFeatureIDs_parser.add_argument('features')

        purgeRepository_parser = self.subparsers.add_parser('purgeRepository', usage=argparse.SUPPRESS)
        purgeRepository_parser.add_argument('-n', '--noReset', required=False, action='store_true', default=False)
        purgeRepository_parser.add_argument('-FORCEPURGE', '--FORCEPURGE', required=False, dest='forcePurge', action='store_true', default=False)

        processRedoRecordWithInfo_parser = self.subparsers.add_parser('processRedoRecordWithInfo', usage=argparse.SUPPRESS)
        processRedoRecordWithInfo_parser.add_argument('-f', '--flags', required=False, nargs='+')

        responseToFile_parser = self.subparsers.add_parser('responseToFile', usage=argparse.SUPPRESS)
        responseToFile_parser.add_argument('filePath')

        setOutputFormat_parser = self.subparsers.add_parser('setOutputFormat', usage=argparse.SUPPRESS)
        setOutputFormat_parser.add_argument('outputFormat', nargs='?')

        setTheme_parser = self.subparsers.add_parser('setTheme', usage=argparse.SUPPRESS)
        setTheme_parser.add_argument('theme', choices=['light', 'default'], nargs=1)

    # Override function from cmd module to make command completion case-insensitive
    def completenames(self, text, *ignored):
        do_text = 'do_' + text
        return [a[3:] for a in self.get_names() if a.lower().startswith(do_text.lower())]

    # Override base method in cmd module to return methods for autocomplete and help
    def get_names(self, include_hidden=False):
        if not include_hidden:
            return [n for n in dir(self.__class__) if n not in self.__hidden_methods]

        ####return [n for n in dir(self.__class__)]
        return list(dir(self.__class__))

    def preloop(self):
        if self.initialized:
            return

        exportedConfig = bytearray()
        exportedConfigID = bytearray()

        try:
            self.g2_engine.init('pyG2E', g2module_params, self.debug_trace)
            self.g2_engine.exportConfig(exportedConfig, exportedConfigID)
            self.g2_product.init('pyG2Product', g2module_params, self.debug_trace)
            self.g2_diagnostic.init('pyG2Diagnostic', g2module_params, self.debug_trace)
            self.g2_config.init('pyG2Config', g2module_params, self.debug_trace)
            self.g2_configmgr.init('pyG2ConfigMgr', g2module_params, self.debug_trace)
            self.g2_hasher.initWithConfig('pyG2Hasher', g2module_params, exportedConfig, self.debug_trace)
        except G2Exception as ex:
            self.printError(ex)
            self.postloop()
            sys.exit(1)

        self.initialized = True
        self.printResponse(
            colorize_msg('Welcome to G2Command. Type help or ? for help.', 'highlight2')
        )

    def postloop(self):
        with suppress(Exception):
            self.g2_hasher.destroy()
            self.g2_configmgr.destroy()
            self.g2_config.destroy()
            self.g2_diagnostic.destroy()
            self.g2_product.destroy()
            self.g2_engine.destroy()

        self.initialized = False

    def precmd(self, line):
        if self.timerOn:
            self.timerStart = time.perf_counter()

        return cmd.Cmd.precmd(self, line)

    def postcmd(self, stop, line):
        self.timerEnd = time.perf_counter()
        if self.timerOn and self.timerStart:
            execTime = (self.timerEnd - self.timerStart)
            # Don't use printResponse for this, don't want this message as the last response
            print(colorize_msg(f'\nApproximate execution time (s): {execTime:.5f}\n', 'info'))

        # If restart has been requested, set stop value to True to restart engines in main loop
        if self.restart:
            return cmd.Cmd.postcmd(self, True, line)

        return cmd.Cmd.postcmd(self, stop, line)

    @staticmethod
    def do_quit(_):
        return True

    def do_exit(self, _):
        self.do_quit(self)
        return True

    def ret_quit(self):
        return self.quit

    @staticmethod
    def do_EOF(_):
        return True

    def emptyline(self):
        return

    def default(self, line):
        self.printError('Unknown command, type help or ?')
        return

    def cmdloop(self, intro=None):
        while True:
            try:
                super(G2CmdShell, self).cmdloop(intro=self.intro)
                break
            except KeyboardInterrupt:
                if input(
                        colorize_msg('\n\nAre you sure you want to exit? (y/n) ', 'caution')
                ) in ['y', 'Y', 'yes', 'YES']:
                    break
                else:
                    print()
            except TypeError as ex:
                self.printError(ex)

    def fileloop(self, file_name):
        self.preloop()

        with open(file_name) as data_in:
            for line in data_in:
                line = line.strip()
                # Ignore comments and blank lines
                if len(line) > 0 and line[0:1] not in ('#', '-', '/'):
                    # *args allows for empty list if there are no args
                    (read_cmd, *args) = line.split()
                    process_cmd = f'do_{read_cmd}'
                    self.printWithNewLines(f'----- {read_cmd} -----', 'S')
                    self.printWithNewLines(f'{line}', 'S')

                    if process_cmd not in dir(self):
                        self.printError(f'Command {read_cmd} not found')
                    else:
                        # Join the args into a printable string, format into the command + args to call
                        try:
                            exec_cmd = f'self.{process_cmd}({repr(" ".join(args))})'
                            exec(exec_cmd)
                        except (ValueError, TypeError) as ex:
                            self.printError('Command could not be run!')
                            self.printError(ex)

    def do_hidden(self, _):
        self.printResponse(self.__hidden_methods)

    # ----- Help -----
    # ===== custom help section =====
    def do_help(self, help_topic):
        if not help_topic:
            self.help_overview()
            return

        if help_topic not in self.get_names(include_hidden=True):
            help_topic = 'do_' + help_topic
            if help_topic not in self.get_names(include_hidden=True):
                cmd.Cmd.do_help(self, help_topic[3:])
                return

        topic_docstring = getattr(self, help_topic).__doc__
        if not topic_docstring:
            self.printResponse(colorize_msg(f'No help found for {help_topic[3:]}', 'warning'))
            return

        help_text = current_section = ''
        headers = ['Syntax:', 'Examples:', 'Example:', 'Notes:', 'Caution:', 'Arguments:']

        if cli_args.colorDisable:
            print(textwrap.dedent(topic_docstring))
            return

        help_lines = textwrap.dedent(topic_docstring).split('\n')

        for line in help_lines:
            line_color = ''
            if line:
                if line in headers:
                    line_color = 'highlight2'
                    current_section = line

                if current_section == 'Caution:':
                    line_color = 'caution, italics'
                elif current_section not in ('Syntax:', 'Examples:', 'Example:', 'Notes:', 'Arguments:'):
                    line_color = ''

            if re.match(fr'^\s*{help_topic[3:]}', line) and not line_color:
                sep_column = line.find(help_topic[3:]) + len(help_topic[3:])
                help_text += line[0:sep_column] + colorize(line[sep_column:], 'dim') + '\n'
            else:
                help_text += colorize(line, line_color) + '\n'

        print(help_text)

    def help_all(self):
        cmd.Cmd.do_help(self, '')

    @staticmethod
    def help_overview():
        print(textwrap.dedent(f'''
        {colorize('This utility allows you to interact with the Senzing APIs.', 'dim')}

        {colorize('Help', 'highlight2')}
            {colorize('- View help for a command:', 'dim')} help COMMAND
            {colorize('- View all commands:', 'dim')} help all

        {colorize('Tab Completion', 'highlight2')}
            {colorize('- Tab completion is available for commands, files and engine flags', 'dim')}
            {colorize('- Hit tab on a blank line to see all commands', 'dim')}

        {colorize('JSON Formatting', 'highlight2')}
            {colorize('- Change JSON formatting by adding "json" or "jsonl" to the end of a command', 'dim')}
                - getEntityByEntityID 1001 jsonl
                
            {colorize('- Set the JSON format', 'dim')}
                - setOutputFormat json|jsonl
                
            {colorize('- Convert last response output between json and jsonl', 'dim')}
                - responseReformatJson

        {colorize('Capturing Output', 'highlight2')}
            {colorize('- Capture the last response output to a file or the clipboard', 'dim')}
                - responseToClipboard
                - responseToFile /tmp/myoutput.json
            {colorize('- responseToClipboard does not work in containers or SSH sessions', 'dim')}

        {colorize('History', 'highlight2')}
            {colorize('- Arrow keys to cycle through history of commands', 'dim')}
            {colorize('- Ctrl-r can be used to search history', 'dim')}
            {colorize('- Display history:', 'dim')} history

        {colorize('Timer', 'highlight2')}
            {colorize('- Toggle on/off approximate time a command takes to complete', 'dim')}
                - timer
                
        {colorize('Shell', 'highlight2')}
            {colorize('- Run basic OS shell commands', 'dim')}
                - ! ls
                
        {colorize('Support', 'highlight2')}
            {colorize('- Senzing Support:', 'dim')} {colorize('https://senzing.zendesk.com/hc/en-us/requests/new', 'highlight1,underline')}
            {colorize('- Senzing Knowledge Center:', 'dim')} {colorize('https://senzing.zendesk.com/hc/en-us', 'highlight1,underline')}
            {colorize('- API Docs:', 'dim')} {colorize('https://docs.senzing.com', 'highlight1,underline')}

        '''))

    def do_shell(self, line):
        self.printResponse(os.popen(line).read())

    def histCheck(self):
        """Attempt to set up history"""

        if not self.histDisable:
            if readline_avail:
                tmpHist = '.' + os.path.basename(sys.argv[0].lower().replace('.py', '_history'))
                self.histFileName = os.path.join(os.path.expanduser('~'), tmpHist)

                # Try and open history in users home first for longevity
                try:
                    open(self.histFileName, 'a').close()
                except IOError as err:
                    self.histFileError = f'{err} - Couldn\'t use home, trying /tmp/...'
                    # Can't use users home, try using /tmp/ for history useful at least in the session
                    self.histFileName = f'/tmp/{tmpHist}'
                    try:
                        open(self.histFileName, 'a').close()
                    except IOError as err:
                        self.histFileError = f'{err} - User home dir and /tmp/ failed!'
                        return

                hist_size = 2000
                readline.read_history_file(self.histFileName)
                readline.set_history_length(hist_size)
                atexit.register(readline.set_history_length, hist_size)
                atexit.register(readline.write_history_file, self.histFileName)

                self.histFileError = None
                self.histAvail = True

    # ----- Senzing Commands -----

    @cmd_decorator(cmd_has_args=False)
    def do_primeEngine(self, **kwargs):
        """
        Prime the Senzing engine

        Syntax:
            primeEngine"""

        self.g2_engine.primeEngine()
        self.printResponse('Engine primed.', 'success')

    @cmd_decorator()
    def do_process(self, **kwargs):
        """
        Process a record (typically a redo record)

        Syntax:
            process JSON_DATA

        Example:
            process '{"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "2192", "ENTITY_TYPE": "GENERIC", "DSRC_ACTION": "X"...'

        Arguments:
            JSON_DATA = Senzing mapped JSON representation of a record"""

        self.g2_engine.process(kwargs['parsed_args'].jsonData)
        self.printResponse('Record processed.', 'success')

    @cmd_decorator()
    def do_processWithInfo(self, **kwargs):
        """
        Process a record (typically a redo record) with returned info

        Syntax:
            processWithInfo JSON_DATA [-f FLAG ...]

        Example:
            processWithInfo '{"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "2192", "ENTITY_TYPE": "GENERIC", "DSRC_ACTION": "X"...'

        Arguments:
            JSON_DATA = Senzing mapped JSON representation of a record
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.processWithInfo(
            kwargs['parsed_args'].jsonData,
            kwargs['response'],
            **kwargs['flags_int']
        )
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_processFile(self, **kwargs):
        """
        Process a file of Senzing mapped records

        Syntax:
            processFile FILE

        Example:
            processFile demo/truth/customers.json

        Arguments:
            FILE = Input file containing Senzing mapped data records"""

        input_file = fileName = kwargs['parsed_args'].inputFile
        dataSourceParm = None
        cnt = 0

        if '/?' in input_file:
            fileName, dataSourceParm = input_file.split("/?")
            if dataSourceParm.upper().startswith('DATA_SOURCE='):
                dataSourceParm = dataSourceParm[12:].upper()

        _, fileExtension = os.path.splitext(fileName)
        fileExtension = fileExtension[1:].upper()

        with open(fileName) as data_in:
            if fileExtension != 'CSV':
                fileReader = data_in
            else:
                fileReader = csv.reader(data_in)
                csvHeaders = [x.upper() for x in next(fileReader)]

            for line in fileReader:
                if fileExtension != 'CSV':
                    jsonStr = line.strip()
                else:
                    jsonObj = dict(list(zip(csvHeaders, line)))
                    if dataSourceParm:
                        jsonObj['DATA_SOURCE'] = dataSourceParm
                    jsonStr = json.dumps(jsonObj)

                try:
                    self.g2_engine.process(jsonStr)
                except G2Exception as err:
                    self.printError(err, f'At record {cnt + 1}')
                cnt += 1
                if cnt % 1000 == 0:
                    self.printResponse(f'{cnt} records processed')
            self.printResponse(f'{cnt} records processed', 'success')

    @cmd_decorator()
    def do_exportCSVEntityReport(self, **kwargs):
        """
        Export repository contents as CSV

        Syntax:
            exportCSVEntityReport OUTPUT_FILE [-t CSV_COLUMN,...] [-f FLAG ...]

        Examples:
            exportCSVEntityReport export.csv
            exportCSVEntityReport export.csv -t RESOLVED_ENTITY_ID,RELATED_ENTITY_ID,MATCH_LEVEL,MATCH_KEY,DATA_SOURCE,RECORD_ID
            exportCSVEntityReport export.csv -f G2_EXPORT_INCLUDE_RESOLVED G2_EXPORT_INCLUDE_POSSIBLY_SAME

        Arguments:
            OUTPUT_FILE = File to save export to
            CSV_COLUMN = Comma separated list of output columns (don't specify for defaults)
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Available CSV_COLUMNs
                - RESOLVED_ENTITY_ID,RELATED_ENTITY_ID,MATCH_LEVEL,MATCH_KEY,DATA_SOURCE,RECORD_ID,RESOLVED_ENTITY_NAME,JSON_DATA,ERRULE_CODE

            - Engine flag details https://docs.senzing.com/flags/index.html

        Caution:
            - Export isn't intended for exporting large numbers of entities and associated data source record information.
              Beyond 100M+ data source records isn't suggested. For exporting overview entity and relationship data for
              analytical purposes outside of Senzing please review the following article.

              https://senzing.zendesk.com/hc/en-us/articles/360010716274--Advanced-Replicating-the-Senzing-results-to-a-Data-Warehouse"""

        response = bytearray()

        try:
            with open(kwargs['parsed_args'].outputFile, 'w') as data_out:
                exportHandle = self.g2_engine.exportCSVEntityReport(kwargs['parsed_args'].headersForCSV, **kwargs['flags_int'])
                rowData = self.g2_engine.fetchNext(exportHandle, response)
                recCnt = 0

                while rowData:
                    data_out.write(response.decode())
                    response = bytearray()
                    rowData = self.g2_engine.fetchNext(exportHandle, response)
                    recCnt += 1
                    if recCnt % 1000 == 0:
                        print(f'Exported {recCnt} records...', flush=True)

                self.g2_engine.closeExport(exportHandle)
        except (G2Exception, IOError) as err:
            self.printError(err)
        else:
            self.printResponse(f'Total exported records: {recCnt}', 'success')

    @cmd_decorator()
    def do_exportJSONEntityReport(self, **kwargs):
        """
        Export repository contents as JSON

        Syntax:
            exportJSONEntityReport OUTPUT_FILE [-f FLAG ...]

        Examples:
            exportJSONEntityReport export.json
            exportJSONEntityReport export.json -f G2_EXPORT_INCLUDE_RESOLVED G2_EXPORT_INCLUDE_POSSIBLY_SAME

        Arguments:
            OUTPUT_FILE = File to save export to
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html

        Caution:
            - Export isn't intended for exporting large numbers of entities and associated data source record information.
              Beyond 100M+ data source records isn't suggested. For exporting overview entity and relationship data for
              analytical purposes outside of Senzing please review the following article.

              https://senzing.zendesk.com/hc/en-us/articles/360010716274--Advanced-Replicating-the-Senzing-results-to-a-Data-Warehouse"""

        response = bytearray()

        try:
            with open(kwargs['parsed_args'].outputFile, 'w') as data_out:
                exportHandle = self.g2_engine.exportJSONEntityReport(**kwargs['flags_int'])
                rowData = self.g2_engine.fetchNext(exportHandle, response)
                recCnt = 0

                while rowData:
                    data_out.write(response.decode())
                    response = bytearray()
                    rowData = self.g2_engine.fetchNext(exportHandle, response)
                    recCnt += 1
                    if recCnt % 1000 == 0:
                        print(f'Exported {recCnt} records...', flush=True)

                self.g2_engine.closeExport(exportHandle)
        except (G2Exception, IOError) as err:
            self.printError(err)
        else:
            self.printResponse(f'Total exported records: {recCnt}', 'success')

    @cmd_decorator(cmd_has_args=False)
    def do_getTemplateConfig(self, **kwargs):
        """
        Get a template configuration

        Syntax:
            getTemplateConfig"""

        configHandle = self.g2_config.create()
        self.g2_config.save(configHandle, kwargs['response'])
        self.g2_config.close(configHandle)
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_getConfig(self, **kwargs):
        """
        Get a configuration

        Syntax:
            getConfig CONFIG_ID

        Example:
            getConfig 4180061352

        Arguments:
            CONFIG_ID = Configuration identifier

        Notes:
            - Retrieve the active configuration identifier with getActiveConfigID

            - Retrieve a list of configurations and identifiers with getConfigList"""

        self.g2_configmgr.getConfig(
            kwargs['parsed_args'].configID,
            kwargs['response']
        )
        self.printResponse(kwargs['response'])

    @cmd_decorator(cmd_has_args=False)
    def do_getConfigList(self, **kwargs):
        """
        Get a list of current configurations

        Syntax:
            getConfigList"""

        self.g2_configmgr.getConfigList(kwargs['response'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_addConfigFile(self, **kwargs):
        """
        Add a configuration from a file

        Syntax:
            addConfigFile CONFIG_FILE 'COMMENTS'

        Example:
            addConfigFile config.json 'Added new features'

        Arguments:
            CONFIG_FILE = File containing configuration to add
            COMMENTS = Comments for the configuration"""

        config = pathlib.Path(kwargs['parsed_args'].configJsonFile).read_text()
        config = config.replace('\n', '')
        self.g2_configmgr.addConfig(
            config,
            kwargs['parsed_args'].configComments,
            kwargs['response']
        )
        self.printResponse(f'Configuration added, ID = {kwargs["response"].decode()}', 'success')

    @cmd_decorator(cmd_has_args=False)
    def do_getDefaultConfigID(self, **kwargs):
        """
        Get the default configuration ID

        Syntax:
            getDefaultConfigID"""

        self.g2_configmgr.getDefaultConfigID(kwargs['response'])
        self.printResponse(kwargs['response'], 'success')

    @cmd_decorator()
    def do_setDefaultConfigID(self, **kwargs):
        """
        Set the default configuration ID

        Syntax:
            setDefaultConfigID CONFIG_ID

        Example:
            setDefaultConfigID 4180061352

        Arguments:
            CONFIG_ID = Configuration identifier

        Notes:
            - Retrieve a list of configurations and identifiers with getConfigList"""

        self.g2_configmgr.setDefaultConfigID(kwargs['parsed_args'].configID)
        self.printResponse('Default config set, restarting engines...', 'success')
        self.do_restart(None) if not self.debug_trace else self.do_restartDebug(None)

    @cmd_decorator()
    def do_replaceDefaultConfigID(self, **kwargs):
        """
        Replace the default configuration ID

        Syntax:
            replaceDefaultConfigID OLD_CONFIG_ID NEW_CONFIG_ID

        Example:
            replaceDefaultConfigID 4180061352 2787925967

        Arguments:
            OLD_CONFIG_ID = Configuration identifier
            NEW_CONFIG_ID = Configuration identifier

        Notes:
            - Retrieve a list of configurations and identifiers with getConfigList"""

        self.g2_configmgr.replaceDefaultConfigID(
            kwargs['parsed_args'].oldConfigID,
            kwargs['parsed_args'].newConfigID)
        self.printResponse('New default config set, restarting engines...', 'success')
        self.do_restart(None) if not self.debug_trace else self.do_restartDebug(None)

    @cmd_decorator()
    def do_addRecord(self, **kwargs):
        """
        Add a record

        Syntax:
            addRecord DSRC_CODE RECORD_ID JSON_DATA

        Example:
            addRecord test 1 '{"NAME_FULL":"Robert Smith", "DATE_OF_BIRTH":"7/4/1976", "PHONE_NUMBER":"787-767-2088"}'

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            JSON_DATA = Senzing mapped JSON representation of a record"""

        self.g2_engine.addRecord(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            kwargs['parsed_args'].jsonData)
        self.printResponse('Record added.', 'success')

    @cmd_decorator()
    def do_addRecordWithInfo(self, **kwargs):
        """
        Add a record with returned info

        Syntax:
            addRecordWithInfo DSRC_CODE RECORD_ID JSON_DATA [-f FLAG ...]

        Example:
            addRecordWithInfo test 1 '{"NAME_FULL":"Robert Smith", "DATE_OF_BIRTH":"7/4/1976", "PHONE_NUMBER":"787-767-2088"}'

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            JSON_DATA = Senzing mapped JSON representation of a record
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.addRecordWithInfo(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            kwargs['parsed_args'].jsonData,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_addRecordWithReturnedRecordID(self, **kwargs):
        """
        Add a record with returned record identifier

        Syntax:
            addRecordWithReturnedRecordID DSRC_CODE JSON_DATA

        Example:
            addRecordWithReturnedRecordID test '{"NAME_FULL":"Robert Smith", "DATE_OF_BIRTH":"7/4/1976", "PHONE_NUMBER":"787-767-2088"}'

        Arguments:
            DSRC_CODE = Data source code
            JSON_DATA = Senzing mapped JSON representation of a record"""

        self.g2_engine.addRecordWithReturnedRecordID(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['recordID'],
            kwargs['parsed_args'].jsonData)
        self.printResponse(kwargs['recordID'], 'success')

    @cmd_decorator()
    def do_addRecordWithInfoWithReturnedRecordID(self, **kwargs):
        """
        Add a record with returned info and record identifier

        Syntax:
            addRecordWithInfoWithReturnedRecordID DSRC_CODE JSON_DATA [-f FLAG ...]

        Example:
            addRecordWithInfoWithReturnedRecordID test '{"NAME_FULL":"Robert Smith", "DATE_OF_BIRTH":"7/4/1976", "PHONE_NUMBER":"787-767-2088"}'

        Arguments:
            DSRC_CODE = Data source code
            JSON_DATA = Senzing mapped JSON representation of a record
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.addRecordWithInfoWithReturnedRecordID(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].jsonData,
            kwargs['recordID'],
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])
        print(colorize_msg(f'Record ID: {kwargs["recordID"].decode()}\n', 'success'))

    @cmd_decorator()
    def do_reevaluateRecord(self, **kwargs):
        """
        Reevaluate a record

        Syntax:
            reevaluateRecord DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            reevaluateRecord customers 1001

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.reevaluateRecord(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            **kwargs['flags_int'])
        self.printResponse('Record reevaluated.', 'success')

    @cmd_decorator()
    def do_reevaluateRecordWithInfo(self, **kwargs):
        """
        Reevaluate a record with returned info

        Syntax:
            reevaluateRecordWithInfo DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            reevaluateRecordWithInfo customers 1001

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.reevaluateRecordWithInfo(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_reevaluateEntity(self, **kwargs):
        """
        Reevaluate an entity

        Syntax:
            reevaluateEntity ENTITY_ID [-f FLAG ...]

        Example:
            reevaluateEntity 1

        Arguments:
            ENTITY_ID = Entity identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.reevaluateEntity(
            kwargs['parsed_args'].entityID,
            **kwargs['flags_int'])
        self.printResponse('Entity reevaluated.', 'success')

    @cmd_decorator()
    def do_reevaluateEntityWithInfo(self, **kwargs):
        """
        Reevaluate an entity with returned info

        Syntax:
            reevaluateEntityWithInfo ENTITY_ID [-f FLAG ...]

        Example:
            reevaluateEntityWithInfo 1

        Arguments:
            ENTITY_ID = Entity identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.reevaluateEntityWithInfo(
            kwargs['parsed_args'].entityID,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_replaceRecord(self, **kwargs):
        """
        Replace a record

        Syntax:
            replaceRecord DSRC_CODE RECORD_ID JSON_DATA

        Example:
            replaceRecord test 1 '{"NAME_FULL":"John Smith", "DATE_OF_BIRTH":"7/4/1976", "PHONE_NUMBER":"787-767-2088"}'

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            JSON_DATA = Senzing mapped JSON representation of a record"""

        self.g2_engine.replaceRecord(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            kwargs['parsed_args'].jsonData)
        self.printResponse('Record replaced.', 'success')

    @cmd_decorator()
    def do_replaceRecordWithInfo(self, **kwargs):
        """
        Replace a record with returned info

        Syntax:
            replaceRecordWithInfo DSRC_CODE RECORD_ID JSON_DATA [-f FLAG ...]

        Example:
            replaceRecordWithInfo test 1 '{"NAME_FULL":"Robert Smith", "DATE_OF_BIRTH":"7/4/1976", "PHONE_NUMBER":"787-767-2088"}'

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            JSON_DATA = Senzing mapped JSON representation of a record
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.replaceRecordWithInfo(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            kwargs['parsed_args'].jsonData,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_deleteRecord(self, **kwargs):
        """
        Delete a record

        Syntax:
            deleteRecord DSRC_CODE RECORD_ID

        Example:
            deleteRecord test 1

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier"""

        self.g2_engine.deleteRecord(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID)
        self.printResponse('Record deleted.', 'success')

    @cmd_decorator()
    def do_deleteRecordWithInfo(self, **kwargs):
        """
        Delete a record with returned info

        Syntax:
            deleteRecordWithInfo DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            deleteRecordWithInfo test 1

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.deleteRecordWithInfo(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_searchByAttributes(self, **kwargs):
        """
        Search for entities

        Syntax:
            searchByAttributes JSON_DATA [-f FLAG ...]

        Example:
            searchByAttributes '{"name_full":"Robert Smith", "date_of_birth":"11/12/1978"}'
            searchByAttributes '{"name_full":"Robert Smith", "date_of_birth":"11/12/1978"}' -f G2_SEARCH_BY_ATTRIBUTES_MINIMAL_ALL

        Arguments:
            JSON_DATA = Senzing mapped JSON containing the attributes to search on
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.searchByAttributes(
            kwargs['parsed_args'].jsonData,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_searchByAttributesV3(self, **kwargs):
        """
        Search for entities

        Syntax:
            searchByAttributesV3 JSON_DATA SEARCH_PROFILE [-f FLAG ...]

        Example:
            searchByAttributesV3 '{"name_full":"Robert Smith", "date_of_birth":"11/12/1978"}' SEARCH
            searchByAttributesV3 '{"name_full":"Robert Smith", "date_of_birth":"11/12/1978"}' SEARCH -f G2_SEARCH_BY_ATTRIBUTES_MINIMAL_ALL

        Arguments:
            JSON_DATA = Senzing mapped JSON containing the attributes to search on
            SEARCH_PROFILE = Search profile to use
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.searchByAttributesV3(
            kwargs['parsed_args'].jsonData,
            kwargs['parsed_args'].searchProfile,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_getEntityByEntityID(self, **kwargs):
        """
        Get entity by resolved entity identifier

        Syntax:
            getEntityByEntityID ENTITY_ID [-f FLAG ...]

        Example:
            getEntityByEntityID 1
            getEntityByEntityID 1 -f G2_ENTITY_BRIEF_DEFAULT_FLAGS G2_ENTITY_INCLUDE_RECORD_SUMMARY

        Arguments:
            ENTITY_ID = Identifier for an entity
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.getEntityByEntityID(
            kwargs['parsed_args'].entityID,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_getEntityByRecordID(self, **kwargs):
        """
        Get entity by record identifier

        Syntax:
            getEntityByRecordID DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            getEntityByRecordID customers 1001
            getEntityByRecordID customers 1001 -f G2_ENTITY_BRIEF_DEFAULT_FLAGS G2_ENTITY_INCLUDE_RECORD_SUMMARY

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.getEntityByRecordID(
            kwargs["parsed_args"].dataSourceCode,
            kwargs["parsed_args"].recordID,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findInterestingEntitiesByEntityID(self, **kwargs):
        """
        Find interesting entities close to an entity by resolved entity identifier

        Syntax:
            findInterestingEntitiesByEntityID ENTITY_ID [-f FLAG ...]

        Example:
            findInterestingEntitiesByEntityID 1

        Arguments:
            ENTITY_ID = Identifier for an entity
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html

            - Experimental feature requires additional configuration, contact support@senzing.com"""

        self.g2_engine.findInterestingEntitiesByEntityID(
            kwargs["parsed_args"].entityID,
            kwargs["response"],
            **kwargs["flags_int"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findInterestingEntitiesByRecordID(self, **kwargs):
        """
        Find interesting entities close to an entity by record identifier

        Syntax:
            findInterestingEntitiesByRecordID DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            findInterestingEntitiesByRecordID customers 1001

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html

            - Experimental feature requires additional configuration, contact support@senzing.com"""

        self.g2_engine.findInterestingEntitiesByRecordID(
            kwargs['parsed_args'].dataSourceCode,
            kwargs['parsed_args'].recordID,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_findPathByEntityID(self, **kwargs):
        """
        Find a path between two entities

        Syntax:
            findPathByEntityID START_ENTITY_ID END_ENTITY_ID MAX_DEGREE [-f FLAG ...]

        Example:
            findPathByEntityID 100002 5 3

        Arguments:
            START_ENTITY_ID = Identifier for an entity
            END_ENTITY_ID = Identifier for an entity
            MAX_DEGREE = Maximum number of relationships to search for a path
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findPathByEntityID(
            kwargs['parsed_args'].startEntityID,
            kwargs['parsed_args'].endEntityID,
            kwargs['parsed_args'].maxDegree,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs['response'])

    @cmd_decorator()
    def do_findNetworkByEntityID(self, **kwargs):
        """
        Find network between entities

        Syntax:
            findNetworkByEntityID ENTITIES MAX_DEGREE BUILD_OUT_DEGREE MAX_ENTITIES [-f FLAG ...]

        Example:
            findNetworkByEntityID '{"ENTITIES":[{"ENTITY_ID":"6"},{"ENTITY_ID":"11"},{"ENTITY_ID":"9"}]}' 4 3 20

        Arguments:
            ENTITIES = JSON document listing entities to find paths between and networks around
            MAX_DEGREE = Maximum number of relationships to search for a path
            BUILD_OUT_DEGREE = Maximum degree of relationships to include around each entity
            MAX_ENTITIES = Maximum number of entities to return
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findNetworkByEntityID(
            kwargs["parsed_args"].entityList,
            kwargs["parsed_args"].maxDegree,
            kwargs["parsed_args"].buildOutDegree,
            kwargs["parsed_args"].maxEntities,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findPathExcludingByEntityID(self, **kwargs):
        """
        Find path between two entities, with exclusions

        Syntax:
            findPathExcludingByEntityID START_ENTITY_ID END_ENTITY_ID MAX_DEGREE EXCLUDED_ENTITIES [-f FLAG ...]

        Example:
            findPathExcludingByEntityID 6 13 5 '{"ENTITIES": [{"ENTITY_ID": "6"},{"ENTITY_ID":"11"}]}'

        Arguments:
            START_ENTITY_ID = Identifier for an entity
            END_ENTITY_ID = Identifier for an entity
            MAX_DEGREE = Maximum number of relationships to search for a path
            EXCLUDED_ENTITIES = JSON document listing entities to exclude from the path
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findPathExcludingByEntityID(
            kwargs["parsed_args"].startEntityID,
            kwargs["parsed_args"].endEntityID,
            kwargs["parsed_args"].maxDegree,
            kwargs["parsed_args"].excludedEntities,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findPathIncludingSourceByEntityID(self, **kwargs):
        """
        Find path between two entities that includes a data source, with exclusions

        Syntax:
            findPathIncludingSourceByEntityID START_ENTITY_ID END_ENTITY_ID MAX_DEGREE EXCLUDED_ENTITIES REQUIRED_DSRC [-f FLAG ...]

        Example:
            findPathIncludingSourceByEntityID 98 200011 4 '{"ENTITIES": [{"ENTITY_ID":"200017"}]}' '{"SOURCES": [{"DATA_SOURCE": "customers"}]}'
            findPathIncludingSourceByEntityID 98 200011 4 '{"ENTITIES": []}' '{"SOURCES": [{"DATA_SOURCE": "customers"}, {"DATA_SOURCE":"reference"}]}'

        Arguments:
            START_ENTITY_ID = Identifier for an entity
            END_ENTITY_ID = Identifier for an entity
            MAX_DEGREE = Maximum number of relationships to search for a path
            EXCLUDED_ENTITIES = JSON document listing entities to exclude from the path
            REQUIRED_DSRC = JSON document listing data sources in the path
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findPathIncludingSourceByEntityID(
            kwargs["parsed_args"].startEntityID,
            kwargs["parsed_args"].endEntityID,
            kwargs["parsed_args"].maxDegree,
            kwargs["parsed_args"].excludedEntities,
            kwargs["parsed_args"].requiredDsrcs,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findPathByRecordID(self, **kwargs):
        """
        Find a path between two records

        Syntax:
            findPathByRecordID START_DSRC_CODE START_RECORD_ID END_DSRC_CODE END_RECORD_ID MAX_DEGREE [-f FLAG ...]

        Example:
            findPathByRecordID reference 2141 reference 2121 6

        Arguments:
            START_DSRC_CODE = Data source code
            START_RECORD_ID = Record identifier
            END_DSRC_CODE = Data source code
            END_RECORD_ID = Record identifier
            MAX_DEGREE = Maximum number of relationships to search for a path
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findPathByRecordID(
            kwargs["parsed_args"].startDataSourceCode,
            kwargs["parsed_args"].startRecordID,
            kwargs["parsed_args"].endDataSourceCode,
            kwargs["parsed_args"].endRecordID,
            kwargs["parsed_args"].maxDegree,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findNetworkByRecordID(self, **kwargs):
        """
        Find network between records

        Syntax:
            findNetworkByRecordID RECORDS MAX_DEGREE BUILD_OUT_DEGREE MAX_ENTITIES [-f FLAG ...]

        Example:
            findNetworkByRecordID '{"RECORDS":[{"DATA_SOURCE":"REFERENCE","RECORD_ID":"2071"},{"DATA_SOURCE":"CUSTOMERS","RECORD_ID":"1069"}]}' 6 4 15

        Arguments:
            RECORDS = JSON document listing records to find paths between and networks around
            MAX_DEGREE = Maximum number of relationships to search for a path
            BUILD_OUT_DEGREE = Maximum degree of relationships to include around each entity
            MAX_ENTITIES = Maximum number of entities to return
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findNetworkByRecordID(
            kwargs["parsed_args"].recordList,
            kwargs["parsed_args"].maxDegree,
            kwargs["parsed_args"].buildOutDegree,
            kwargs["parsed_args"].maxEntities,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findPathExcludingByRecordID(self, **kwargs):
        """
        Find a path between two records, with exclusions

        Syntax:
            findPathExcludingByRecordID START_DSRC_CODE START_RECORD_ID END_DSRC_CODE END_RECORD_ID MAX_DEGREE EXCLUDED_ENTITIES [-f FLAG ...]

        Example:
            findPathExcludingByRecordID reference 2121 watchlist 2092 4 '{"ENTITIES": [{"ENTITY_ID": "200017"}, {"ENTITY_ID": "200013"}]}'

        Arguments:
            START_DSRC_CODE = Data source code
            START_RECORD_ID = Record identifier
            END_DSRC_CODE = Data source code
            END_RECORD_ID = Record identifier
            MAX_DEGREE = Maximum number of relationships to search for a path
            EXCLUDED_ENTITIES = JSON document listing entities to exclude from the path
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findPathExcludingByRecordID(
            kwargs["parsed_args"].startDataSourceCode,
            kwargs["parsed_args"].startRecordID,
            kwargs["parsed_args"].endDataSourceCode,
            kwargs["parsed_args"].endRecordID,
            kwargs["parsed_args"].maxDegree,
            kwargs["parsed_args"].excludedEntities,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findPathIncludingSourceByRecordID(self, **kwargs):
        """
        Find path between two records that includes a data source, with exclusions

        Syntax:
            findPathIncludingSourceByRecordID START_DSRC_CODE START_RECORD_ID END_DSRC_CODE END_RECORD_ID MAX_DEGREE EXCLUDED_ENTITIES REQUIRED_DSRC [-f FLAG ...]

        Example:
            findPathIncludingSourceByRecordID reference 2121 watchlist 2092 4 '{"ENTITIES": [{"ENTITY_ID":"200013"}]}' '{"SOURCES": [{"DATA_SOURCE": "customers"}]}'
            findPathIncludingSourceByRecordID reference 2121 watchlist 2092 4 '{"ENTITIES": []}' '{"SOURCES": [{"DATA_SOURCE": "customers"}]}'

        Arguments:
            START_DSRC_CODE = Data source code
            START_RECORD_ID = Record identifier
            END_DSRC_CODE = Data source code
            END_RECORD_ID = Record identifier
            MAX_DEGREE = Maximum number of relationships to search for a path
            EXCLUDED_ENTITIES = JSON document listing entities to exclude from the path
            REQUIRED_DSRC = JSON document listing data sources in the path
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.findPathIncludingSourceByRecordID(
            kwargs["parsed_args"].startDataSourceCode,
            kwargs["parsed_args"].startRecordID,
            kwargs["parsed_args"].endDataSourceCode,
            kwargs["parsed_args"].endRecordID,
            kwargs["parsed_args"].maxDegree,
            kwargs["parsed_args"].excludedEntities,
            kwargs["parsed_args"].requiredDsrcs,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_whyEntityByRecordID(self, **kwargs):
        """
        Determine why a record resolved to an entity

        Syntax:
            whyEntityByRecordID DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            whyEntityByRecordID reference 2121
            whyEntityByRecordID reference 2121 -f G2_WHY_ENTITY_DEFAULT_FLAGS G2_ENTITY_INCLUDE_RECORD_JSON_DATA

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.whyEntityByRecordID(
            kwargs["parsed_args"].dataSourceCode,
            kwargs["parsed_args"].recordID,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_whyEntityByEntityID(self, **kwargs):
        """
        Determine why records resolved to an entity

        Syntax:
            whyEntityByEntityID ENTITY_ID [-f FLAG ...]

        Example:
            whyEntityByEntityID 98
            whyEntityByEntityID 98 -f G2_WHY_ENTITY_DEFAULT_FLAGS G2_ENTITY_INCLUDE_RECORD_JSON_DATA

        Arguments:
            ENTITY_ID = Identifier for an entity
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.whyEntityByEntityID(
            kwargs['parsed_args'].entityID,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_whyEntities(self, **kwargs):
        """
        Determine how entities relate to each other

        Syntax:
            whyEntities ENTITY_ID ENTITY_ID [-f FLAG ...]

        Example:
            whyEntities 96 200011
            whyEntities 96 200011 -f G2_WHY_ENTITY_DEFAULT_FLAGS G2_ENTITY_INCLUDE_RECORD_JSON_DATA

        Arguments:
            ENTITY_ID = Identifier for an entity
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.whyEntities(
            kwargs["parsed_args"].entityID1,
            kwargs["parsed_args"].entityID2,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_whyRecords(self, **kwargs):
        """
        Determine how two records relate to each other

        Syntax:
            whyRecords DSRC_CODE RECORD_ID DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            whyRecords reference 2121 watchlist 2092
            whyRecords reference 2121 watchlist 2092 -f G2_WHY_ENTITY_DEFAULT_FLAGS G2_ENTITY_INCLUDE_RECORD_JSON_DATA

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.whyRecords(
            kwargs["parsed_args"].dataSourceCode1,
            kwargs["parsed_args"].recordID1,
            kwargs["parsed_args"].dataSourceCode2,
            kwargs["parsed_args"].recordID2,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_howEntityByEntityID(self, **kwargs):
        """
        Retrieve information on how entities are constructed from their records

        Syntax:
            howEntityByEntityID ENTITY_ID [-f FLAG ...]

        Example:
            howEntityByEntityID 96

        Arguments:
            ENTITY_ID = Identifier for an entity
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.howEntityByEntityID(
            kwargs['parsed_args'].entityID,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getVirtualEntityByRecordID(self, **kwargs):
        """
        Determine how an entity composed of a given set of records would look

        Syntax:
            getVirtualEntityByRecordID RECORDS [-f FLAG ...]

        Example:
            getVirtualEntityByRecordID '{"RECORDS": [{"DATA_SOURCE": "REFERENCE","RECORD_ID": "2071"},{"DATA_SOURCE": "CUSTOMERS","RECORD_ID": "1069"}]}'

        Arguments:
            RECORDS = JSON document listing data sources and records
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.getVirtualEntityByRecordID(
            kwargs["parsed_args"].recordList,
            kwargs['response'],
            **kwargs['flags_int'])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getRecord(self, **kwargs):
        """
        Get a record

        Syntax:
            getRecord DSRC_CODE RECORD_ID [-f FLAG ...]

        Example:
            getRecord watchlist 2092
            getRecord watchlist 2092 -f G2_RECORD_DEFAULT_FLAGS G2_ENTITY_INCLUDE_RECORD_FORMATTED_DATA

        Arguments:
            DSRC_CODE = Data source code
            RECORD_ID = Record identifier
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.getRecord(
            kwargs["parsed_args"].dataSourceCode,
            kwargs["parsed_args"].recordID,
            kwargs["response"],
            **kwargs["flags_int"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator(cmd_has_args=False)
    def do_countRedoRecords(self, **kwargs):
        """
        Counts the number of records in the redo queue

        Syntax:
            countRedoRecords"""

        recordCount = self.g2_engine.countRedoRecords()
        if not recordCount:
            self.printResponse('No redo records.', 'info')
        else:
            self.printResponse(recordCount, 'success')

    @cmd_decorator(cmd_has_args=False)
    def do_getRedoRecord(self, **kwargs):
        """
        Get a redo record from the redo queue

        Syntax:
            getRedoRecord"""

        self.g2_engine.getRedoRecord(kwargs["response"])
        if not kwargs["response"]:
            self.printResponse('No redo records.', 'info')
        else:
            self.printResponse(kwargs["response"])

    @cmd_decorator(cmd_has_args=False)
    def do_processRedoRecord(self, **kwargs):
        """
        Process a redo record from the redo queue

        Syntax:
            processRedoRecord"""

        self.g2_engine.processRedoRecord(kwargs["response"])
        if not kwargs["response"]:
            self.printResponse('No redo records.', 'info')
        else:
            self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_processRedoRecordWithInfo(self, **kwargs):
        """
        Process a redo record from the redo queue with returned info

        Syntax:
            processRedoRecordWithInfo [-f FLAG ...]

        Example:
            processRedoRecordWithInfo

        Arguments:
            FLAG = Space separated list of engine flag(s) to determine output (don't specify for defaults)

        Notes:
            - Engine flag details https://docs.senzing.com/flags/index.html"""

        self.g2_engine.processRedoRecordWithInfo(
            kwargs["response"],
            kwargs["withInfo"],
            **kwargs["flags_int"])

        if kwargs["response"]:
            self.printResponse(kwargs['response'])
            print(colorize_msg('Info:', 'info'), end='')
            self.printResponse(kwargs["withInfo"].decode())
        else:
            self.printResponse('No redo records.', 'info')

    @cmd_decorator()
    def do_getEntityDetails(self, **kwargs):
        """
        Get the profile of a resolved entity

        Syntax:
            getEntityDetails ENTITY_ID [-i]

        Example:
            getEntityDetails 96

        Arguments:
            ENTITY_ID = Identifier for an entity
            -i = Include internal features"""

        self.g2_diagnostic.getEntityDetails(
            kwargs["parsed_args"].entityID,
            kwargs["parsed_args"].includeInternalFeatures,
            kwargs["response"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getRelationshipDetails(self, **kwargs):
        """
        Get the profile of a relationship

        Syntax:
            getRelationshipDetails RELATIONSHIP_ID [-i]

        Example:
            getRelationshipDetails 30

        Arguments:
            RELATIONSHIP_ID = Identifier for a relationship
            -i = Include internal features"""

        self.g2_diagnostic.getRelationshipDetails(
            kwargs["parsed_args"].relationshipID,
            kwargs["parsed_args"].includeInternalFeatures,
            kwargs["response"]
            )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getEntityResume(self, **kwargs):
        """
        Get the related records for a resolved entity

        Syntax:
            getEntityResume ENTITY_ID

        Example:
            getEntityResume 96

        Arguments:
            ENTITY_ID = Identifier for an entity"""

        self.g2_diagnostic.getEntityResume(
            kwargs["parsed_args"].entityID,
            kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getEntityListBySize(self, **kwargs):
        """
        Get list of resolved entities of specified size

        Syntax:
            getEntityListBySize ENTITY_SIZE

        Example:
            getEntityListBySize 6

        Arguments:
            ENTITY_SIZE = Entity size to retrieve"""

        sizedEntityHandle = self.g2_diagnostic.getEntityListBySize(kwargs["parsed_args"].entitySize)
        response = bytearray()
        rowData = self.g2_diagnostic.fetchNextEntityBySize(sizedEntityHandle, response)
        resultString = bytearray()

        while rowData:
            resultString += response
            response = bytearray()
            rowData = self.g2_diagnostic.fetchNextEntityBySize(sizedEntityHandle, response)
        self.g2_diagnostic.closeEntityListBySize(sizedEntityHandle)

        self.printResponse(resultString)

    @cmd_decorator()
    def do_checkDBPerf(self, **kwargs):
        """
        Run a performance check on the database

        Syntax:
            checkDBPerf [SECONDS]

        Example:
            checkDBPerf

        Arguments:
            SECONDS = Time in seconds to run check, default is 3"""

        self.g2_diagnostic.checkDBPerf(
            kwargs["parsed_args"].secondsToRun,
            kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator(cmd_has_args=False)
    def do_getDataSourceCounts(self, **kwargs):
        """
        Get record counts by data source and entity type

        Syntax:
            getDataSourceCounts"""

        self.g2_diagnostic.getDataSourceCounts(kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getMappingStatistics(self, **kwargs):
        """
        Get data source mapping statistics

        Syntax:
            getMappingStatistics [-i]

        Arguments:
            -i = Include internal features"""

        self.g2_diagnostic.getMappingStatistics(
            kwargs["parsed_args"].includeInternalFeatures,
            kwargs["response"]
        )
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getGenericFeatures(self, **kwargs):
        """
        Get a list of generic values for a feature type

        Syntax:
            getGenericFeatures FEATURE_TYPE [-m MAX_ESTIMATED_COUNT]

        Example:
            getGenericFeatures email

        Arguments:
            FEATURE_TYPE = Type of feature
            -m = The maximum estimated count to stop on"""

        self.g2_diagnostic.getGenericFeatures(
            kwargs["parsed_args"].featureType,
            kwargs["parsed_args"].maximumEstimatedCount,
            kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getEntitySizeBreakdown(self, **kwargs):
        """
        Get the number of entities of each entity size

        Syntax:
            getEntitySizeBreakdown MIN_ENTITY_SIZE [-i]

        Example:
            getEntitySizeBreakdown 5

        Arguments:
            MIN_ENTITY_SIZE = Minimum entity size to return
            -i = Include internal features"""

        self.g2_diagnostic.getEntitySizeBreakdown(
            kwargs["parsed_args"].minimumEntitySize,
            kwargs["parsed_args"].includeInternalFeatures,
            kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_getFeature(self, **kwargs):
        """
        Get information for a feature

        Syntax:
            getFeature FEATURE_ID

        Example:
            getFeature 18

        Arguments:
            FEATURE_ID = Feature identifier"""

        self.g2_diagnostic.getFeature(
            kwargs["parsed_args"].featureID,
            kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator(cmd_has_args=False)
    def do_getResolutionStatistics(self, **kwargs):
        """
        Get resolution statistics

        Syntax:
            getResolutionStatistics"""

        self.g2_diagnostic.getResolutionStatistics(kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_findEntitiesByFeatureIDs(self, **kwargs):
        """
        Get the entities for a list of features

        Syntax:
            findEntitiesByFeatureIDs FEATURES

        Example:
            findEntitiesByFeatureIDs '{"LIB_FEAT_IDS":[1,2,3]}'

        Arguments:
            FEATURES = JSON document listing data the feature identifiers"""

        self.g2_diagnostic.findEntitiesByFeatureIDs(
            kwargs["parsed_args"].features,
            kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator(cmd_has_args=False)
    def do_stats(self, **kwargs):
        """
        Get engine workload statistics for last process

        Syntax:
            stats"""

        self.g2_engine.stats(kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_exportConfig(self, **kwargs):
        """
        Export the current configuration

        Syntax:
            exportConfig [-o FILE_NAME]

        Example:
            exportConfig
            exportConfig -o /tmp/config.json

        Arguments:
            FILE_NAME = Path and name of file to write configuration to"""

        self.g2_engine.exportConfig(kwargs["response"], kwargs["configID"])
        responseMsg = json.loads(kwargs["response"])

        if kwargs['parsed_args'].outputFile:
            with open(kwargs['parsed_args'].outputFile, 'w') as data_out:
                json.dump(responseMsg, data_out)
        else:
            self.printResponse(json.dumps(responseMsg))

    @cmd_decorator(cmd_has_args=False)
    def do_getActiveConfigID(self, **kwargs):
        """
        Get the config identifier

        Syntax:
            getActiveConfigID"""

        self.g2_engine.getActiveConfigID(kwargs["response"])
        self.printResponse(kwargs["response"], 'success')

    @cmd_decorator(cmd_has_args=False)
    def do_getRepositoryLastModifiedTime(self, **kwargs):
        """
        Get the last modified time of the database

        Syntax:
            getRepositoryLastModifiedTime"""

        self.g2_engine.getRepositoryLastModifiedTime(kwargs["response"])
        self.printResponse(kwargs["response"], 'success')

    @cmd_decorator()
    def do_exportTokenLibrary(self, **kwargs):
        """
        Export the token library

        Syntax:
            exportTokenLibrary [-o FILE_NAME]

        Example:
            exportTokenLibrary
            exportTokenLibrary -o /tmp/tokens.json

        Arguments:
            FILE_NAME = Path and name of file to write configuration to"""

        self.g2_hasher.exportTokenLibrary(kwargs["response"])
        responseMsg = json.loads(kwargs["response"])
        if kwargs['parsed_args'].outputFile:
            with open(kwargs['parsed_args'].outputFile, 'w') as data_out:
                json.dump(responseMsg, data_out)
        else:
            self.printResponse(json.dumps(responseMsg))

    @cmd_decorator()
    def do_purgeRepository(self, **kwargs):
        """
        Purge Senzing database of all data

        Syntax:
            purgeRepository [-n] [--FORCEPURGE]

        Example:
            purgeRepository

        Arguments:
            -n = Skip resetting the resolver
            --FORCEPURGE = Don't prompt before purging. USE WITH CAUTION!

        Caution:
            - This deletes all data in the Senzing database!"""

        purge_msg = colorize_msg(
            textwrap.dedent('''
            
                ********** WARNING **********
                
                This will purge all currently loaded data from the senzing database!
                Before proceeding, all instances of senzing (custom code, rest api, redoer, etc.) must be shut down.
                
                ********** WARNING **********
                
                Are you sure you want to purge the senzing database? (y/n) '''
                            ),
            'warning'
        )

        if not kwargs['parsed_args'].forcePurge:
            if input(purge_msg) not in ['y', 'Y', 'yes', 'YES']:
                print()
                return

        if kwargs['parsed_args'].noReset:
            reset_resolver = False
            resolver_txt = '(without resetting resolver)'
        else:
            reset_resolver = True
            resolver_txt = '(and resetting resolver)'

        self.printResponse(f'Purging the Senzing database {resolver_txt}...', 'info')
        self.g2_engine.purgeRepository(reset_resolver)

    @cmd_decorator()
    def do_hashRecord(self, **kwargs):
        """
        Hash a record

        Syntax:
            hashRecord JSON_DATA

        Example:
            hashRecord '{"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "2192", "NAME_FULL": "John Edwards"...'

        Arguments:
            JSON_DATA = Senzing mapped JSON representation of a record"""

        self.g2_hasher.process(kwargs['parsed_args'].jsonData, kwargs["response"])
        self.printResponse(kwargs["response"])

    @cmd_decorator()
    def do_hashFile(self, **kwargs):
        """
        Hash a file of records

        Syntax:
            hashFile INPUT_FILE [-o OUTPUT_FILE]

        Example:
            hashFile records.json

        Arguments:
            INPUT_FILE = File to read records to hash from
            OUTPUT_FILE = File to optionally write hashed records to"""

        with open(kwargs['parsed_args'].inputFile.split("?")[0]) as data_in:
            if kwargs['parsed_args'].outputFile:
                with open(kwargs['parsed_args'].outputFile, 'w') as data_out:
                    for line in data_in:
                        response = bytearray()
                        self.g2_hasher.process(line.strip(), response)
                        data_out.write(response.decode())
                        data_out.write('\n')
            else:
                for line in data_in:
                    response = bytearray()
                    self.g2_hasher.process(line.strip(), response)
                    self.printResponse(response.decode())

    @cmd_decorator(cmd_has_args=False)
    def do_license(self, **kwargs):
        """
        Get the license information

        Syntax:
            license"""

        self.printResponse(self.g2_product.license())

    @cmd_decorator()
    def do_validateLicenseFile(self, **kwargs):
        """
        Validate a license file

        Syntax:
            validateLicenseFile LIC_FILE

        Example:
            validateLicenseFile g2.lic

        Arguments:
            LIC_FILE = License file to validate"""

        returnCode = self.g2_product.validateLicenseFile(kwargs['parsed_args'].licenseFilePath)
        if returnCode == 0:
            self.printResponse('License validated', 'success')
        else:
            self.printError('License is not valid.')

    @cmd_decorator()
    def do_validateLicenseStringBase64(self, **kwargs):
        """
        Validate a license string

        Syntax:
            validateLicenseStringBase64 'LIC_STRING'

        Example:
            validateLicenseStringBase64 'AQAAADgCAAAAAAAAU2VuemluZyBJbnRlcm5hbAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA...'

        Arguments:
            LIC_STRING = License string to validate"""

        returnCode = self.g2_product.validateLicenseStringBase64(kwargs['parsed_args'].licenseString)
        if returnCode == 0:
            self.printResponse('License validated.', 'success')
        else:
            self.printError('License is not valid.')

    @cmd_decorator(cmd_has_args=False)
    def do_version(self, **kwargs):
        """
        Get the version information

        Syntax:
            version"""

        self.printResponse(json.dumps(json.loads(str(self.g2_product.version()))))

    def do_getPhysicalCores(self, _):
        """
        Get the number of physical cores

        Syntax:
            getPhysicalCores"""

        self.printResponse(self.g2_diagnostic.getPhysicalCores())

    def do_getLogicalCores(self, _):
        """
        Get the number of logical cores

        Syntax:
            getLogicalCores"""

        self.printResponse(self.g2_diagnostic.getLogicalCores())

    def do_getTotalSystemMemory(self, _):
        """
        Get the total system memory

        Syntax:
            getTotalSystemMemory"""

        self.printResponse(
            f'{self.g2_diagnostic.getTotalSystemMemory()/(1024 * 1024 * 1024):.1f} GB'
        )

    def do_getAvailableMemory(self, _):
        """
        Get the available memory

        Syntax:
            getAvailableMemory"""

        self.printResponse(
            f'{self.g2_diagnostic.getAvailableMemory()/(1024 * 1024 * 1024):.1f} GB'
        )

    @cmd_decorator(cmd_has_args=False)
    def do_getDBInfo(self, **kwargs):
        """
        Get database information

        Syntax:
            getDBInfo"""

        self.g2_diagnostic.getDBInfo(kwargs['response'])
        self.printResponse(kwargs['response'])

# ---- Non API call commands ----
# ---- DO NOT docstring these! ----

    def do_history(self, arg):

        if self.histAvail:
            print()
            for i in range(readline.get_current_history_length()):
                self.printWithNewLines(readline.get_history_item(i + 1))
            print()
        else:
            self.printResponse('History isn\'t available in this session.', 'caution')

    @cmd_decorator()
    def do_responseToFile(self, **kwargs):
        with open(kwargs["parsed_args"].filePath, 'w') as data_out:
            data_out.write(self.last_response)
            data_out.write('\n')

    def do_responseToClipboard(self, arg):
        def pyperclip_clip_msg():
            self.printWithNewLines(
                colorize_msg(
                    textwrap.dedent(
                        """\
                        - The clipboard module is installed but no clipboard command could be found
                        
                        - This usually means xclip is missing on Linux and needs to be installed:
                            - sudo apt install xclip OR sudo yum install xclip
                            
                        - If you are running in a container or SSH lastResponseToClipboard cannot be used"""
                    ), 'info'
                ), "B"
            )

        if not pyperclip_avail:
            self.printWithNewLines(
                colorize_msg(
                    textwrap.dedent(
                        """\
                        - To send the last response to the clipboard the Python module pyperclip needs to be installed
                            - pip install pyperclip
                            
                        - If you are running in a container or SSH lastResponseToClipboard cannot be used"""
                    ), 'info'
                ), "B"
            )
            return

        try:
            clip = pyperclip.determine_clipboard()
        except ModuleNotFoundError:
            pyperclip_clip_msg()
            return

        try:
            # If __name__ doesn't exist no clipboard tool was available
            _ = clip[0].__name__
        except AttributeError:
            pyperclip_clip_msg()
            return

        # This clipboard gets detected on Linux when xclip isn't installed, but it doesn't work
        if clip[0].__name__ == 'copy_gi':
            pyperclip_clip_msg()
            return

        try:
            pyperclip.copy(self.last_response)
        except pyperclip.PyperclipException as err:
            self.printError(err)

    def do_responseReformatJson(self, args):
        try:
            _ = json.loads(self.last_response)
        except (json.decoder.JSONDecodeError, TypeError):
            print(colorize_msg('The last response isn\'t JSON!', 'warning'))
            return

        self.json_output_format = 'json' if self.json_output_format == 'jsonl' else 'jsonl'
        self.printWithNewLines(self.last_response, 'B')

    @cmd_decorator()
    def do_setOutputFormat(self, **kwargs):
        """
        Set output format for JSON responses

        Syntax:
            setOutputFormat {jsonl|json}
        """

        if not kwargs["parsed_args"].outputFormat:
            self.printResponse(
                colorize_msg(f'Current format is {self.json_output_format}', 'info')
            )
            return

        if kwargs["parsed_args"].outputFormat.lower() not in ('json', 'jsonl'):
            self.printResponse(
                colorize_msg('Format should be json (tall json) or jsonl (json line)', 'warning')
            )
            return

        self.json_output_format = kwargs["parsed_args"].outputFormat.lower()

    @cmd_decorator()
    def do_setTheme(self, **kwargs):
        """
        Switch terminal ANSI colors between default and light

        Syntax:
            setTheme {default|light}
        """
        Colors.set_theme(kwargs["parsed_args"].theme[0])
        print()

    def parse(self, argument_string):
        """Parses command arguments into a list of argument strings"""

        try:
            shlex_list = shlex.split(argument_string)
            return shlex_list
        except ValueError as err:
            self.printError(err, 'Unable to parse arguments')
            raise

    def printWithNewLines(self, ln, pos=''):
        pos = pos.upper()

        try:
            # Test if data is json and format appropriately
            _ = json.loads(ln)
        except (json.decoder.JSONDecodeError, TypeError):
            output = ln
        else:
            if type(ln) not in [dict, list]:
                ln = json.loads(ln)

            if self.json_output_format == 'json':
                json_str = json.dumps(ln, indent=4)
            else:
                json_str = json.dumps(ln)

            if not cli_args.colorDisable:
                output = colorize_json(json_str)
            else:
                output = json_str

        if pos == 'S' or pos == 'START':
            print(f'\n{output}', flush=True)
        elif pos == 'E' or pos == 'END':
            print(f'{output}\n', flush=True)
        elif pos == 'B' or pos == 'BOTH':
            print(f'\n{output}\n', flush=True)
        else:
            print(f'{output}', flush=True)

        # Capture the latest output to send to clipboard or file, removing color codes
        self.last_response = re.sub(r"(\x9B|\x1B\[)[0-?]*[ -/]*[@-~]", '', output)

    def printResponse(self, response, color=''):
        if response:
            resp_str = response.decode() if isinstance(response, bytearray) else response
            self.printWithNewLines(colorize_msg(resp_str, color), 'B')
        else:
            self.printWithNewLines(colorize_msg('No response!', 'info'), 'B')

    def printError(self, err, msg=''):
        self.printWithNewLines(
            colorize_msg(f'ERROR: {msg}{" : " if msg else ""}{err}', "error"),
            'B'
        )

    def do_restart(self, arg):
        self.restart = True
        return True

    def do_restartDebug(self, arg):
        self.restart_debug = True
        return True

    def get_restart(self):
        return self.restart

    def get_restart_debug(self):
        return self.restart_debug

    def do_timer(self, arg):
        if self.timerOn:
            self.timerOn = False
            self.printResponse('Timer is off', 'success')
        else:
            self.timerOn = True
            self.printResponse('Timer is on', 'success')

    # ===== Auto completers =====

    # Auto complete multiple arguments
    def complete_exportCSVEntityReport(self, text, line, begidx, endidx):
        if re.match("exportCSVEntityReport +", line) and not \
           re.match("exportCSVEntityReport +.* +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'exportCSVEntityReport')

        if re.match(".* -f +", line):
            return self.flags_completes(text, line)

    def complete_exportJSONEntityReport(self, text, line, begidx, endidx):
        if re.match("exportJSONEntityReport +", line) and not \
           re.match("exportJSONEntityReport +.* +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'exportJSONEntityReport')

        if re.match(".* -f +", line):
            return self.flags_completes(text, line)

    def complete_hashFile(self, text, line, begidx, endidx):
        if re.match(".* -o +", line):
            path_start = line.find('-o ')
            return self.pathCompletes(text, line, begidx, endidx, line[:path_start + 2])

        if re.match("hashFile +", line) and not re.match("hashFile +.* +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'hashFile')

    # Auto complete engine flags
    def complete_getEntityByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_searchByAttributes(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_processWithInfo(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_addRecordWithInfo(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_replaceRecordWithInfo(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_addRecordWithInfoWithReturnedRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_deleteRecordWithInfo(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findInterestingEntitiesByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_reevaluateRecordWithInfo(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_reevaluateEntity(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_reevaluateEntityWithInfo(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findInterestingEntitiesByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findPathByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findNetworkByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findPathExcludingByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findPathIncludingSourceByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_getEntityByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findPathByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findNetworkByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_whyEntityByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_whyEntityByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_howEntityByEntityID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_getVirtualEntityByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_whyEntities(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_whyRecords(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findPathExcludingByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_findPathIncludingSourceByRecordID(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_getRecord(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    def complete_processRedoRecordWithInfo(self, text, line, begidx, endidx):
        return self.flags_completes(text, line)

    @staticmethod
    def flags_completes(text, line):
        """Auto complete engine flags from G2EngineFlags"""

        if re.match(".* -f +", line):
            engine_flags_list = list(G2EngineFlags.__members__.keys())
            return [flag for flag in engine_flags_list if flag.lower().startswith(text.lower())]
        return None

    # Auto complete paths
    def complete_responseToFile(self, text, line, begidx, endidx):
        if re.match("responseToFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'responseToFile')

    def complete_processFile(self, text, line, begidx, endidx):
        if re.match("processFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'processFile')

    def complete_addConfigFile(self, text, line, begidx, endidx):
        if re.match("addConfigFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'addConfigFile')

    def complete_validateLicenseFile(self, text, line, begidx, endidx):
        if re.match("validateLicenseFile +", line):
            return self.pathCompletes(text, line, begidx, endidx, 'validateLicenseFile')

    def complete_exportConfig(self, text, line, begidx, endidx):
        if re.match(".* -o +", line):
            path_start = line.find('-o ')
            return self.pathCompletes(text, line, begidx, endidx, line[:path_start + 2])

    def complete_exportTokenLibrary(self, text, line, begidx, endidx):
        if re.match(".* -o +", line):
            path_start = line.find('-o ')
            return self.pathCompletes(text, line, begidx, endidx, line[:path_start + 2])

    @staticmethod
    def pathCompletes(text, line, begidx, endidx, callingcmd):
        """Auto complete paths for commands"""

        completes = []
        pathComp = line[len(callingcmd) + 1:endidx]
        fixed = line[len(callingcmd) + 1:begidx]
        for path in glob.glob(f'{pathComp}*'):
            path = path + os.sep if path \
                and os.path.isdir(path) \
                and path[-1] != os.sep \
                else path
            completes.append(path.replace(fixed, '', 1))

        return completes


# ---- Utility functions ----

def get_engine_flags(flags_list):
    """Detect if int or named flags are used and convert to int ready to send to API call"""

    # For Senzing support team
    if flags_list[0] == '-1':
        return int(flags_list[0])

    # An int is used for the engine flags - old method still support
    if len(flags_list) == 1 and flags_list[0].isnumeric():
        return int(flags_list[0])

    # Named engine flag(s) were used, combine when > 1
    try:
        engine_flags_int = int(G2EngineFlags.combine_flags(flags_list))
    except KeyError as err:
        raise KeyError(f'Invalid engine flag: {err}') from err

    return engine_flags_int


def colorize(in_string, color_list='None'):
    return Colors.apply(in_string, color_list)


def colorize_msg(msg_text, msg_type_or_color=''):

    if cli_args.colorDisable:
        return msg_text

    if msg_type_or_color.upper() == 'ERROR':
        msg_color = 'bad'
    elif msg_type_or_color.upper() == 'WARNING':
        msg_color = 'caution,italics'
    elif msg_type_or_color.upper() == 'INFO':
        msg_color = 'highlight2'
    elif msg_type_or_color.upper() == 'SUCCESS':
        msg_color = 'good'
    else:
        msg_color = msg_type_or_color
    return f"{Colors.apply(msg_text, msg_color)}"


def colorize_json(json_str):
    for token in set(re.findall(r'"(.*?)"', json_str)):
        tag = f'"{token}":'
        if tag in json_str:
            json_str = json_str.replace(tag, colorize(tag, 'highlight2'))
        else:
            tag = f'"{token}"'
            if tag in json_str:
                json_str = json_str.replace(tag, colorize(tag, 'caution'))
    return json_str


if __name__ == '__main__':

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        'fileToProcess',
        default=None,
        help=textwrap.dedent('''\
            
            Path and file name of file with commands to process.
        
        '''),
        nargs='?'
    )
    parser.add_argument(
        '-c',
        '--iniFile',
        default='',
        help=textwrap.dedent('''\
            
            Path and file name of optional G2Module.ini to use.
        
        '''),
        nargs=1
    )
    parser.add_argument(
        '-t',
        '--debugTrace',
        action='store_true',
        default=False,
        help=textwrap.dedent('''\
            
            Output debug information.
        
        ''')
    )
    parser.add_argument(
        '-H',
        '--histDisable',
        action='store_true',
        default=False,
        help=textwrap.dedent('''\
            
            Disable history file usage.
        
        ''')
    )
    parser.add_argument(
        '-C',
        '--colorDisable',
        action='store_true',
        default=False,
        help=textwrap.dedent('''\
            
            Disable coloring of output.
        
        ''')
    )
    cli_args = parser.parse_args()

    first_loop = True
    restart = False

    #Check if INI file or env var is specified, otherwise use default INI file
    ini_file_name = None

    if cli_args.iniFile:
        ini_file_name = pathlib.Path(cli_args.iniFile[0])
    elif os.getenv("SENZING_ENGINE_CONFIGURATION_JSON"):
        g2module_params = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON")
    else:
        ini_file_name = pathlib.Path(G2Paths.get_G2Module_ini_path())

    if ini_file_name:
        G2Paths.check_file_exists_and_readable(ini_file_name)
        iniParamCreator = G2IniParams()
        g2module_params = iniParamCreator.getJsonINIParams(ini_file_name)

    # Execute a file of commands
    if cli_args.fileToProcess:
        cmd_obj = G2CmdShell(cli_args.debugTrace, cli_args.histDisable)
        cmd_obj.fileloop(cli_args.fileToProcess)
    # Start command shell
    else:
        # Don't use args.debugTrace here, may need to restart
        debug_trace = cli_args.debugTrace

        while first_loop or restart:
            # Have we been in the command shell already and are trying to quit? Used for restarting
            if 'cmd_obj' in locals() and cmd_obj.ret_quit():
                break

            cmd_obj = G2CmdShell(debug_trace, cli_args.histDisable)
            cmd_obj.cmdloop()

            restart = True if cmd_obj.get_restart() \
                or cmd_obj.get_restart_debug() else False
            debug_trace = True if cmd_obj.get_restart_debug() else False
            first_loop = False
