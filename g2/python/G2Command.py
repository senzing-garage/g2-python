#! /usr/bin/env python3

#--python imports
import cmd
import sys
from G2Engine import G2Engine
from G2Hasher import G2Hasher
from G2Audit import G2Audit
from G2Product import G2Product
from G2Diagnostic import G2Diagnostic
from G2Config import G2Config
from G2ConfigMgr import G2ConfigMgr
from G2IniParams import G2IniParams
import G2Paths
import G2Exception
import json
import shlex
import argparse
import os
import csv
import inspect

class G2CmdShell(cmd.Cmd, object):

    def __init__(self, ini_file = ''):
        cmd.Cmd.__init__(self)
        self.intro = ''
        self.prompt = '(g2) '
        self.g2_module = G2Engine()
        self.g2_hasher_module = G2Hasher()
        self.g2_audit_module = G2Audit()
        self.g2_product_module = G2Product()
        self.g2_diagnostic_module = G2Diagnostic()
        self.g2_config_module = G2Config()
        self.g2_configmgr_module = G2ConfigMgr()
        self.initialized = False
        self.__hidden_methods = ('do_shell', 'do_EOF')
        if ini_file == '':
            self.iniFileName = G2Paths.get_G2Module_ini_path()
        else:
            self.iniFileName = ini_file

        self.parser = argparse.ArgumentParser(prog='G2Command ->', add_help=False)
        subparsers = self.parser.add_subparsers()
      
        jsonOnly_parser = subparsers.add_parser('jsonOnly', usage=argparse.SUPPRESS)
        jsonOnly_parser.add_argument('jsonData')
        
        jsonWithInfo_parser = subparsers.add_parser('jsonWithInfo', usage=argparse.SUPPRESS)
        jsonWithInfo_parser.add_argument('jsonData')
        jsonWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        addConfigFile_parser = subparsers.add_parser('addConfigFile', usage=argparse.SUPPRESS)
        addConfigFile_parser.add_argument('configJsonFile')
        addConfigFile_parser.add_argument('configComments')

        getConfig_parser = subparsers.add_parser('getConfig', usage=argparse.SUPPRESS)
        getConfig_parser.add_argument('configID', type=int)

        setDefaultConfigID_parser = subparsers.add_parser('setDefaultConfigID', usage=argparse.SUPPRESS)
        setDefaultConfigID_parser.add_argument('configID', type=int)

        interfaceName_parser = subparsers.add_parser('interfaceName', usage=argparse.SUPPRESS)  
        interfaceName_parser.add_argument('interfaceName')

        searchByAttributesV2_parser = subparsers.add_parser('searchByAttributesV2', usage=argparse.SUPPRESS)
        searchByAttributesV2_parser.add_argument('jsonData')
        searchByAttributesV2_parser.add_argument('flags', type=int)

        processFile_parser = subparsers.add_parser('processFile', usage=argparse.SUPPRESS)
        processFile_parser.add_argument('inputFile')

        validateLicenseFile_parser = subparsers.add_parser('validateLicenseFile', usage=argparse.SUPPRESS)
        validateLicenseFile_parser.add_argument('licenseFilePath')

        inputFile_parser = subparsers.add_parser('inputFile', usage=argparse.SUPPRESS)
        inputFile_parser.add_argument('inputFile')
        inputFile_parser.add_argument('-o', '--outputFile', required=False)

        processWithResponse_parser = subparsers.add_parser('processWithResponse',  usage=argparse.SUPPRESS)
        processWithResponse_parser.add_argument('jsonData')
        processWithResponse_parser.add_argument('-o', '--outputFile', required=False)

        exportEntityReport_parser = subparsers.add_parser('exportEntityReport', usage=argparse.SUPPRESS)
        exportEntityReport_parser.add_argument('-f', '--flags', required=True, default=0, type=int)
        exportEntityReport_parser.add_argument('-o', '--outputFile', required=False)

        exportEntityCsvV2_parser = subparsers.add_parser('exportEntityCsvV2', usage=argparse.SUPPRESS)
        exportEntityCsvV2_parser.add_argument('-t', '--headersForCSV', required=False)
        exportEntityCsvV2_parser.add_argument('-f', '--flags', required=True, default=0, type=int)
        exportEntityCsvV2_parser.add_argument('-o', '--outputFile', required=False)

        getAuditReport_parser = subparsers.add_parser('getAuditReport', usage=argparse.SUPPRESS)
        getAuditReport_parser.add_argument('-m', '--matchLevel', type=int)
        getAuditReport_parser.add_argument('-f', '--fromDataSource')
        getAuditReport_parser.add_argument('-t', '--toDataSource')
        getAuditReport_parser.add_argument('-o', '--outputFile', required=False)

        recordModify_parser = subparsers.add_parser('recordModify', usage=argparse.SUPPRESS)
        recordModify_parser.add_argument('dataSourceCode')
        recordModify_parser.add_argument('recordID')
        recordModify_parser.add_argument('jsonData')
        recordModify_parser.add_argument('-l', '--loadID', required=False)

        recordModify_parser = subparsers.add_parser('recordModifyWithInfo', usage=argparse.SUPPRESS)
        recordModify_parser.add_argument('dataSourceCode')
        recordModify_parser.add_argument('recordID')
        recordModify_parser.add_argument('jsonData')
        recordModify_parser.add_argument('-l', '--loadID', required=False)
        recordModify_parser.add_argument('-f', '--flags', required=False, type=int)

        recordDelete_parser = subparsers.add_parser('recordDelete', usage=argparse.SUPPRESS)
        recordDelete_parser.add_argument('dataSourceCode')
        recordDelete_parser.add_argument('recordID')
        recordDelete_parser.add_argument('-l', '--loadID', required=False)

        recordDeleteWithInfo_parser = subparsers.add_parser('recordDeleteWithInfo', usage=argparse.SUPPRESS)
        recordDeleteWithInfo_parser.add_argument('dataSourceCode')
        recordDeleteWithInfo_parser.add_argument('recordID')
        recordDeleteWithInfo_parser.add_argument('-l', '--loadID', required=False)
        recordDeleteWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        getEntityByEntityID_parser = subparsers.add_parser('getEntityByEntityID', usage=argparse.SUPPRESS)
        getEntityByEntityID_parser.add_argument('entityID', type=int)

        getEntityByEntityIDV2_parser = subparsers.add_parser('getEntityByEntityIDV2', usage=argparse.SUPPRESS)
        getEntityByEntityIDV2_parser.add_argument('entityID', type=int)
        getEntityByEntityIDV2_parser.add_argument('flags', type=int)

        findPathByEntityID_parser = subparsers.add_parser('findPathByEntityID', usage=argparse.SUPPRESS)
        findPathByEntityID_parser.add_argument('startEntityID', type=int)
        findPathByEntityID_parser.add_argument('endEntityID', type=int)
        findPathByEntityID_parser.add_argument('maxDegree', type=int)

        findPathByEntityIDV2_parser = subparsers.add_parser('findPathByEntityIDV2', usage=argparse.SUPPRESS)
        findPathByEntityIDV2_parser.add_argument('startEntityID', type=int)
        findPathByEntityIDV2_parser.add_argument('endEntityID', type=int)
        findPathByEntityIDV2_parser.add_argument('maxDegree', type=int)
        findPathByEntityIDV2_parser.add_argument('flags', type=int)

        findPathExcludingByEntityID_parser = subparsers.add_parser('findPathExcludingByEntityID', usage=argparse.SUPPRESS)
        findPathExcludingByEntityID_parser.add_argument('startEntityID', type=int)
        findPathExcludingByEntityID_parser.add_argument('endEntityID', type=int)
        findPathExcludingByEntityID_parser.add_argument('maxDegree', type=int)
        findPathExcludingByEntityID_parser.add_argument('excludedEntities')
        findPathExcludingByEntityID_parser.add_argument('flags', type=int)

        findPathIncludingSourceByEntityID_parser = subparsers.add_parser('findPathIncludingSourceByEntityID', usage=argparse.SUPPRESS)
        findPathIncludingSourceByEntityID_parser.add_argument('startEntityID', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('endEntityID', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('maxDegree', type=int)
        findPathIncludingSourceByEntityID_parser.add_argument('excludedEntities')
        findPathIncludingSourceByEntityID_parser.add_argument('requiredDsrcs')
        findPathIncludingSourceByEntityID_parser.add_argument('flags', type=int)

        findNetworkByEntityID_parser = subparsers.add_parser('findNetworkByEntityID', usage=argparse.SUPPRESS)
        findNetworkByEntityID_parser.add_argument('entityList')
        findNetworkByEntityID_parser.add_argument('maxDegree', type=int)
        findNetworkByEntityID_parser.add_argument('buildOutDegree', type=int)
        findNetworkByEntityID_parser.add_argument('maxEntities', type=int)

        findNetworkByEntityIDV2_parser = subparsers.add_parser('findNetworkByEntityIDV2', usage=argparse.SUPPRESS)
        findNetworkByEntityIDV2_parser.add_argument('entityList')
        findNetworkByEntityIDV2_parser.add_argument('maxDegree', type=int)
        findNetworkByEntityIDV2_parser.add_argument('buildOutDegree', type=int)
        findNetworkByEntityIDV2_parser.add_argument('maxEntities', type=int)
        findNetworkByEntityIDV2_parser.add_argument('flags', type=int)

        getEntityDetails_parser = subparsers.add_parser('getEntityDetails', usage=argparse.SUPPRESS)
        getEntityDetails_parser.add_argument('-e', '--entityID', required=True, type=int, default=0)
        getEntityDetails_parser.add_argument('-d', '--includeDerivedFeatures', action='store_true', required=False, default=False)

        getRelationshipDetails_parser = subparsers.add_parser('getRelationshipDetails', usage=argparse.SUPPRESS)
        getRelationshipDetails_parser.add_argument('-r', '--relationshipID', required=True, type=int, default=0)
        getRelationshipDetails_parser.add_argument('-d', '--includeDerivedFeatures', action='store_true', required=False, default=False)

        getMappingStatistics_parser = subparsers.add_parser('getMappingStatistics', usage=argparse.SUPPRESS)
        getMappingStatistics_parser.add_argument('-d', '--includeDerivedFeatures', action='store_true', required=False, default=False)

        getGenericFeatures_parser = subparsers.add_parser('getGenericFeatures', usage=argparse.SUPPRESS)
        getGenericFeatures_parser.add_argument('-t', '--featureType', required=True)
        getGenericFeatures_parser.add_argument('-m', '--maximumEstimatedCount', required=False, type=int, default=1000)

        getEntitySizeBreakdown_parser = subparsers.add_parser('getEntitySizeBreakdown', usage=argparse.SUPPRESS)
        getEntitySizeBreakdown_parser.add_argument('-m', '--minimumEntitySize', required=True, type=int)
        getEntitySizeBreakdown_parser.add_argument('-d', '--includeDerivedFeatures', action='store_true', required=False, default=False)

        getEntityResume_parser = subparsers.add_parser('getEntityResume', usage=argparse.SUPPRESS)
        getEntityResume_parser.add_argument('entityID', type=int)

        getEntityListBySize_parser = subparsers.add_parser('getEntityListBySize', usage=argparse.SUPPRESS)
        getEntityListBySize_parser.add_argument('-s', '--entitySize', type=int)
        getEntityListBySize_parser.add_argument('-o', '--outputFile', required=False)

        getUsedMatchKeys_parser = subparsers.add_parser('getUsedMatchKeys', usage=argparse.SUPPRESS)
        getUsedMatchKeys_parser.add_argument('fromDataSource')
        getUsedMatchKeys_parser.add_argument('toDataSource')
        getUsedMatchKeys_parser.add_argument('matchLevel', type=int)

        getUsedPrinciples_parser = subparsers.add_parser('getUsedPrinciples', usage=argparse.SUPPRESS)
        getUsedPrinciples_parser.add_argument('fromDataSource')
        getUsedPrinciples_parser.add_argument('toDataSource')
        getUsedPrinciples_parser.add_argument('matchLevel', type=int)

        getEntityByRecordID_parser = subparsers.add_parser('getEntityByRecordID', usage=argparse.SUPPRESS)
        getEntityByRecordID_parser.add_argument('dataSourceCode')
        getEntityByRecordID_parser.add_argument('recordID')

        getRecord_parser = subparsers.add_parser('getRecord', usage=argparse.SUPPRESS)
        getRecord_parser.add_argument('dataSourceCode')
        getRecord_parser.add_argument('recordID')

        getRecordV2_parser = subparsers.add_parser('getRecordV2', usage=argparse.SUPPRESS)
        getRecordV2_parser.add_argument('dataSourceCode')
        getRecordV2_parser.add_argument('recordID')
        getRecordV2_parser.add_argument('flags', type=int)

        reevaluateRecord_parser = subparsers.add_parser('reevaluateRecord', usage=argparse.SUPPRESS)
        reevaluateRecord_parser.add_argument('dataSourceCode')
        reevaluateRecord_parser.add_argument('recordID')
        reevaluateRecord_parser.add_argument('flags', type=int)
        
        reevaluateRecordWithInfo_parser = subparsers.add_parser('reevaluateRecordWithInfo', usage=argparse.SUPPRESS)
        reevaluateRecordWithInfo_parser.add_argument('dataSourceCode')
        reevaluateRecordWithInfo_parser.add_argument('recordID')
        reevaluateRecordWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        reevaluateEntity_parser = subparsers.add_parser('reevaluateEntity', usage=argparse.SUPPRESS)
        reevaluateEntity_parser.add_argument('entityID', type=int)
        reevaluateEntity_parser.add_argument('flags', type=int)
        
        reevaluateEntityWithInfo_parser = subparsers.add_parser('reevaluateEntityWithInfo', usage=argparse.SUPPRESS)
        reevaluateEntityWithInfo_parser.add_argument('entityID', type=int)
        reevaluateEntityWithInfo_parser.add_argument('-f', '--flags', required=False, type=int)

        getEntityByRecordIDV2_parser = subparsers.add_parser('getEntityByRecordIDV2', usage=argparse.SUPPRESS)
        getEntityByRecordIDV2_parser.add_argument('dataSourceCode')
        getEntityByRecordIDV2_parser.add_argument('recordID')
        getEntityByRecordIDV2_parser.add_argument('flags', type=int)

        findPathByRecordID_parser = subparsers.add_parser('findPathByRecordID', usage=argparse.SUPPRESS)
        findPathByRecordID_parser.add_argument('startDataSourceCode')
        findPathByRecordID_parser.add_argument('startRecordID')
        findPathByRecordID_parser.add_argument('endDataSourceCode')
        findPathByRecordID_parser.add_argument('endRecordID')
        findPathByRecordID_parser.add_argument('maxDegree', type=int)

        findPathByRecordIDV2_parser = subparsers.add_parser('findPathByRecordIDV2', usage=argparse.SUPPRESS)
        findPathByRecordIDV2_parser.add_argument('startDataSourceCode')
        findPathByRecordIDV2_parser.add_argument('startRecordID')
        findPathByRecordIDV2_parser.add_argument('endDataSourceCode')
        findPathByRecordIDV2_parser.add_argument('endRecordID')
        findPathByRecordIDV2_parser.add_argument('maxDegree', type=int)
        findPathByRecordIDV2_parser.add_argument('flags', type=int)

        findPathExcludingByRecordID_parser = subparsers.add_parser('findPathExcludingByRecordID', usage=argparse.SUPPRESS)
        findPathExcludingByRecordID_parser.add_argument('startDataSourceCode')
        findPathExcludingByRecordID_parser.add_argument('startRecordID')
        findPathExcludingByRecordID_parser.add_argument('endDataSourceCode')
        findPathExcludingByRecordID_parser.add_argument('endRecordID')
        findPathExcludingByRecordID_parser.add_argument('maxDegree', type=int)
        findPathExcludingByRecordID_parser.add_argument('excludedEntities')
        findPathExcludingByRecordID_parser.add_argument('flags', type=int)

        findPathIncludingSourceByRecordID_parser = subparsers.add_parser('findPathIncludingSourceByRecordID', usage=argparse.SUPPRESS)
        findPathIncludingSourceByRecordID_parser.add_argument('startDataSourceCode')
        findPathIncludingSourceByRecordID_parser.add_argument('startRecordID')
        findPathIncludingSourceByRecordID_parser.add_argument('endDataSourceCode')
        findPathIncludingSourceByRecordID_parser.add_argument('endRecordID')
        findPathIncludingSourceByRecordID_parser.add_argument('maxDegree', type=int)
        findPathIncludingSourceByRecordID_parser.add_argument('excludedEntities')
        findPathIncludingSourceByRecordID_parser.add_argument('requiredDsrcs')
        findPathIncludingSourceByRecordID_parser.add_argument('flags', type=int)

        findNetworkByRecordID_parser = subparsers.add_parser('findNetworkByRecordID', usage=argparse.SUPPRESS)
        findNetworkByRecordID_parser.add_argument('recordList')
        findNetworkByRecordID_parser.add_argument('maxDegree', type=int)
        findNetworkByRecordID_parser.add_argument('buildOutDegree', type=int)
        findNetworkByRecordID_parser.add_argument('maxEntities', type=int)

        findNetworkByRecordIDV2_parser = subparsers.add_parser('findNetworkByRecordIDV2', usage=argparse.SUPPRESS)
        findNetworkByRecordIDV2_parser.add_argument('recordList')
        findNetworkByRecordIDV2_parser.add_argument('maxDegree', type=int)
        findNetworkByRecordIDV2_parser.add_argument('buildOutDegree', type=int)
        findNetworkByRecordIDV2_parser.add_argument('maxEntities', type=int)
        findNetworkByRecordIDV2_parser.add_argument('flags', type=int)

        outputOptional_parser = subparsers.add_parser('outputOptional',  usage=argparse.SUPPRESS)
        outputOptional_parser.add_argument('-o', '--outputFile', required=False)

        purgeRepository_parser = subparsers.add_parser('purgeRepository',  usage=argparse.SUPPRESS)
        purgeRepository_parser.add_argument('-n', '--noReset', required=False, nargs='?', const=1, type=int)

    # ----- G2 startup/shutdown -----

    def preloop(self):
        if (self.initialized):
            return

        iniParamCreator = G2IniParams()
        iniParams = iniParamCreator.getJsonINIParams(self.iniFileName)

        print("Initializing engine...")

        self.g2_module.initV2('pyG2E', iniParams, False)
        self.g2_audit_module.initV2('pyG2Audit', iniParams, False)
        self.g2_product_module.initV2('pyG2Product', iniParams, False)
        self.g2_diagnostic_module.initV2('pyG2Diagnostic', iniParams, False)
        self.g2_config_module.initV2('pyG2Config', iniParams, False)
        self.g2_configmgr_module.initV2('pyG2ConfigMgr', iniParams, False)

        exportedConfig = bytearray() 
        exportedConfigID = bytearray() 
        self.g2_module.exportConfig(exportedConfig,exportedConfigID)
        self.g2_hasher_module.initWithConfigV2('pyG2Hasher', iniParams, exportedConfig, False)

        self.initialized = True
        print('\nWelcome to the G2 shell. Type help or ? to list commands.\n')

    def postloop(self):
        if (self.initialized):
            self.g2_module.destroy()
            self.g2_audit_module.destroy()
            self.g2_product_module.destroy()
            self.g2_diagnostic_module.destroy()
            self.g2_config_module.destroy()
            self.g2_configmgr_module.destroy()

            self.g2_hasher_module.destroy()

        self.initialized = False

    # ----- terminal operations -----

    def do_exit(self, arg):
        'Close the G2 window, and exit:  exit\n'
        printWithNewLine('Ending Terminal.')
        return True

    def do_quit(self, arg):
        'Close the G2 window, and exit:  quit\n'
        printWithNewLine('Ending Terminal.')
        return True

    def emptyline(self):
        return

    def do_EOF(self, line):
        return True

    def cmdloop(self, intro=None):
        print(self.intro)
        while True:
            try:
                super(G2CmdShell,self).cmdloop(intro="")
                self.postloop()
                break
            except KeyboardInterrupt:
                ans = userInput('\n\nAre you sure you want to exit?  ')
                if ans in ['y','Y', 'yes', 'YES']:
                    print("^C")
                    break
            except TypeError as ex:
                print("ERROR: " + str(ex))

    # -----------------------------
    def fileloop(self, fileName):
        self.preloop()
        if os.path.exists(fileName): 
            with open(fileName) as data_in:
                for line in data_in:
                    line = line[:-1].strip() #--strips linefeed and spaces
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
                            printWithNewLine('command %s not found' % cmd)
                        else:
                            execCmd = 'self.' + cmd + "('" + parm + "')"
                            exec(execCmd)

        else:
            print('%s not found' % fileName)


    #Hide do_shell from list of APIs. Seperate help section for it

    def get_names(self):
        return [n for n in dir(self.__class__) if n not in self.__hidden_methods]

    # ----- Misc Help -----

    def help_Arguments(self):
        print (
            '\nOptional arguments are surrounded with [ ] \n' \
            'Argument values to specify are surrounded with < >\n\n' \
            '\t[-o <output_file>]\n' \
            '\t\t-o = is an optional argument\n' \
            '\t\t<output_file> = replace with the path and/or filename to output to\n'
            )

    ###
    def help_InterfaceName(self):
        print (
              '\nThe name of a G2 interface (engine, audit, product, diagnostic, hasher).\n\n' \
              )

    def help_KnowledgeCenter(self):
        print('\nSenzing Knowledge Center: https://senzing.zendesk.com/hc/en-us\n')

    def help_Support(self):
        print('\nSenzing Support Request: https://senzing.zendesk.com/hc/en-us/requests/new\n')

    def help_Shell(self):
        print('\nRun OS shell commands: ! <command>\n')

     # ----- basic shell access ----- 

    def do_shell(self,line):
        '\nRun OS shell commands: !<command>\n'
        output = os.popen(line).read()
        print(output)
        
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
            elif args.interfaceName == 'audit':
                self.g2_audit_module.clearLastException()
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
            elif args.interfaceName == 'audit':
                resultString = self.g2_audit_module.getLastException()
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
            elif args.interfaceName == 'audit':
                resultInt = self.g2_audit_module.getLastExceptionCode()
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
            printWithNewLine('')
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
                        
                printWithNewLine('')
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
                    ret_code = self.g2_module.processWithResponse(args.jsonData,processedData)
                    data_out.write(processedData.decode())
                    data_out.write('\n')
                printWithNewLine('')
            else:
                    processedData = bytearray()
                    ret_code = self.g2_module.processWithResponse(args.jsonData,processedData)
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
                            ret_code = self.g2_module.processWithResponse(line.strip(),processedData)
                            data_out.write(processedData.decode())
                            data_out.write('\n')
                printWithNewLine('')
            else:
                with open(args.inputFile.split("?")[0]) as data_in :
                    for line in data_in:
                        processedData = bytearray()
                        ret_code = self.g2_module.processWithResponse(line.strip(),processedData)
                        printResponse(processedData.decode())
        except G2Exception.G2Exception as err:
            print(err)


    def do_exportCSVEntityReport(self, arg):
        '\nExport repository contents as CSV:  exportCSVEntityReport -f <flags> [-o <output_file>]\n' 

        try:
            args = self.parser.parse_args(['exportEntityReport'] + parse(arg))
        except SystemExit:
            print(self.do_exportCSVEntityReport.__doc__)
            return
        try: 
            exportHandle = self.g2_module.exportCSVEntityReport(args.flags)
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
            returnCode = self.g2_configmgr_module.addConfig(configStr,args.configComments,configID)
            if returnCode == 0:
                printWithNewLine('Config added.  [ID = %s]' % configID.decode())
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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

    def do_addRecord(self, arg):
        '\nAdd record:  addRecord <dataSourceCode> <recordID> <jsonData> [-l <loadID>]\n'
        try:
            args = self.parser.parse_args(['recordModify'] + parse(arg))
        except SystemExit:
            print(self.do_addRecord.__doc__)
            return
        try: 
            if args.loadID:
                returnCode = self.g2_module.addRecord(args.dataSourceCode, args.recordID, args.jsonData, args.loadID)
            else:
                returnCode = self.g2_module.addRecord(args.dataSourceCode, args.recordID, args.jsonData)
            if returnCode == 0:
                printWithNewLine('Record added.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
            returnCode = self.g2_module.addRecordWithInfo(args.dataSourceCode, args.recordID, args.jsonData,response,loadId=loadID,flags=flags)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
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
            returnCode = self.g2_module.reevaluateRecord(args.dataSourceCode, args.recordID, args.flags)
            if returnCode == 0:
                printWithNewLine('Record reevaluated.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
            returnCode = self.g2_module.reevaluateRecordWithInfo(args.dataSourceCode,args.recordID,response,flags=flags)            
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            if returnCode == 0:
                printWithNewLine('Record reevaluated.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
            returnCode = self.g2_module.reevaluateEntity(args.entityID, args.flags)
            if returnCode == 0:
                printWithNewLine('Entity reevaluated.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
            returnCode = self.g2_module.reevaluateEntityWithInfo(args.entityID,response,flags=flags)            
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            if returnCode == 0:
                printWithNewLine('Entity reevaluated.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
                returnCode = self.g2_module.replaceRecord(args.dataSourceCode, args.recordID, args.jsonData, args.loadID)
            else:
                returnCode = self.g2_module.replaceRecord(args.dataSourceCode, args.recordID, args.jsonData)
            if returnCode == 0:
                printWithNewLine('Record replaced.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
            returnCode = self.g2_module.replaceRecordWithInfo(args.dataSourceCode,args.recordID,args.jsonData,response,loadId=loadID,flags=flags)
            
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            if returnCode == 0:
                printWithNewLine('Record replaced.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
                returnCode = self.g2_module.deleteRecord(args.dataSourceCode, args.recordID, args.loadID)
            else:
                returnCode = self.g2_module.deleteRecord(args.dataSourceCode, args.recordID)
            if returnCode == 0:
                printWithNewLine('Record deleted.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
            returnCode = self.g2_module.deleteRecordWithInfo(args.dataSourceCode,args.recordID,response,loadId=args.loadID,flags=flags)

            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

            if returnCode == 0:
                printWithNewLine('Record deleted.')
            else:
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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
            ret_code = self.g2_module.searchByAttributes(args.jsonData,response)
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
            ret_code = self.g2_module.searchByAttributesV2(args.jsonData,args.flags,response)
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
            #Define response before calling, needs to be mutable type
            response = bytearray() 

            #Return code is returned
            ret_code = self.g2_module.getEntityByEntityID(args.entityID, response)

            #response object is mutated in g2engine function
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
            ret_code = self.g2_module.getEntityByEntityIDV2(args.entityID,args.flags,response)

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
            ret_code = self.g2_module.findPathByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,response)
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
            ret_code = self.g2_module.findPathByEntityIDV2(args.startEntityID,args.endEntityID,args.maxDegree,args.flags,response)
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
            ret_code = self.g2_module.findNetworkByEntityID(args.entityList,args.maxDegree,args.buildOutDegree,args.maxEntities,response)
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
            ret_code = self.g2_module.findNetworkByEntityIDV2(args.entityList,args.maxDegree,args.buildOutDegree,args.maxEntities,args.flags,response)
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
            ret_code = self.g2_module.findPathExcludingByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,args.excludedEntities,args.flags,response)
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
            ret_code = self.g2_module.findPathIncludingSourceByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,args.excludedEntities,args.requiredDsrcs,args.flags,response)
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
            ret_code = self.g2_module.getEntityByRecordID(args.dataSourceCode, args.recordID,response)
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
            ret_code = self.g2_module.getEntityByRecordIDV2(args.dataSourceCode, args.recordID, args.flags,response)
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
            ret_code = self.g2_module.findPathByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,response)
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
            ret_code = self.g2_module.findPathByRecordIDV2(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.flags,response)
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
            ret_code = self.g2_module.findNetworkByRecordID(args.recordList,args.maxDegree,args.buildOutDegree,args.maxEntities,response)
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
            ret_code = self.g2_module.findNetworkByRecordIDV2(args.recordList,args.maxDegree,args.buildOutDegree,args.maxEntities,args.flags,response)
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
            ret_code = self.g2_module.findPathExcludingByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.excludedEntities,args.flags,response)
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
            ret_code = self.g2_module.findPathIncludingSourceByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.excludedEntities,args.requiredDsrcs,args.flags,response)
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
            ret_code = self.g2_module.getRecord(args.dataSourceCode, args.recordID,response)
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
            ret_code = self.g2_module.getRecordV2(args.dataSourceCode, args.recordID, args.flags,response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')

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
            ret_code = self.g2_diagnostic_module.getEntityDetails(args.entityID,args.includeDerivedFeatures,response)
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
            ret_code = self.g2_diagnostic_module.getRelationshipDetails(args.relationshipID,args.includeDerivedFeatures,response)
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
            ret_code = self.g2_diagnostic_module.getEntityResume(args.entityID,response)
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


    def do_getDataSourceCounts(self,arg):
        '\nGet record counts by data source and entity type:  getDataSourceCounts\n'
        try: 
            response = bytearray() 
            ret_code = self.g2_diagnostic_module.getDataSourceCounts(response)
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
            ret_code = self.g2_diagnostic_module.getMappingStatistics(args.includeDerivedFeatures,response)
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
            ret_code = self.g2_diagnostic_module.getGenericFeatures(args.featureType,args.maximumEstimatedCount,response)
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
            ret_code = self.g2_diagnostic_module.getEntitySizeBreakdown(args.minimumEntitySize,args.includeDerivedFeatures,response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)

    def do_getResolutionStatistics(self,arg):
        '\nGet resolution statistics:  getResolutionStatistics\n'
        try: 
            response = bytearray() 
            ret_code = self.g2_diagnostic_module.getResolutionStatistics(response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)

    def do_stats(self,arg):
        '\nGet engine workload statistics for last process:  stats\n'
        try: 
            response = bytearray() 
            ret_code = self.g2_module.stats(response)
            print('{}'.format(response.decode()))
        except G2Exception.G2Exception as err:
            print(err)

    def do_getSummaryData(self,arg):
        '\nGet summary data:  getSummaryData\n'
        try: 
            sessionHandle = self.g2_audit_module.openSession()
            response = bytearray() 
            self.g2_audit_module.getSummaryData(sessionHandle,response)
            self.g2_audit_module.closeSession(sessionHandle)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)

    def do_getSummaryDataDirect(self,arg):
        '\nGet summary data with optimized speed:  getSummaryDataDirect\n'
        try: 
            response = bytearray() 
            self.g2_audit_module.getSummaryDataDirect(response)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)

    def do_getUsedMatchKeys(self,arg):
        '\nGet usage statistics of match keys:  getUsedMatchKeys <fromDataSource> <toDataSource> <match_level>\n'
        try:
            args = self.parser.parse_args(['getUsedMatchKeys'] + parse(arg))
        except SystemExit:
            print(self.do_getUsedMatchKeys.__doc__)
            return
        try: 
            sessionHandle = self.g2_audit_module.openSession()
            response = bytearray() 
            self.g2_audit_module.getUsedMatchKeys(sessionHandle,args.fromDataSource,args.toDataSource,args.matchLevel,response)
            self.g2_audit_module.closeSession(sessionHandle)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)

    def do_getUsedPrinciples(self,arg):
        '\nGet usage statistics of principles:  getUsedPrinciples <fromDataSource> <toDataSource> <match_level>\n'
        try:
            args = self.parser.parse_args(['getUsedPrinciples'] + parse(arg))
        except SystemExit:
            print(self.do_getUsedPrinciples.__doc__)
            return
        try: 
            sessionHandle = self.g2_audit_module.openSession()
            response = bytearray() 
            self.g2_audit_module.getUsedPrinciples(sessionHandle,args.fromDataSource,args.toDataSource,args.matchLevel,response)
            self.g2_audit_module.closeSession(sessionHandle)
            if response:
                print('{}'.format(response.decode()))
            else:
                print('\nNo response!\n')
        except G2Exception.G2Exception as err:
            print(err)

    def do_getAuditReport(self, arg):
        '\nGet an audit report:  getAuditReport -f <from_data_source> -t <to_data_source> -m <match_level> [-o <output_file>]\n'
        try:
            args = self.parser.parse_args(['getAuditReport'] + parse(arg))
        except SystemExit:
            print(self.do_getAuditReport.__doc__)
            return
        try: 
            sessionHandle = self.g2_audit_module.openSession()
            reportHandle = self.g2_audit_module.getAuditReport(sessionHandle,args.fromDataSource,args.toDataSource,args.matchLevel)
            response = bytearray() 
            rowData = self.g2_audit_module.fetchNext(reportHandle,response)
            resultString = b""
            while rowData:
                resultString += response
                response = bytearray()
                rowData = self.g2_audit_module.fetchNext(reportHandle,response)
            self.g2_audit_module.closeReport(reportHandle)
            self.g2_audit_module.closeSession(sessionHandle)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(resultString.decode())
            else:
                print('{}'.format(resultString.decode()))
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
            ret_code = self.g2_module.exportConfig(response,configID)
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
            ret_code = self.g2_module.getActiveConfigID(response)
            printResponse(response.decode())
        except G2Exception.G2Exception as err:
            print(err)

    def do_getRepositoryLastModifiedTime(self,arg):
        '\nGet the last modified time of the datastore:  getRepositoryLastModifiedTime\n'    
        try: 
            response = bytearray() 
            ret_code = self.g2_module.getRepositoryLastModifiedTime(response)
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
            ret_code = self.g2_hasher_module.exportTokenLibrary(response)
            responseMsg = json.loads(response)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    json.dump(responseMsg,data_out)
            else:
                printResponse(json.dumps(responseMsg))
        except G2Exception.G2Exception as err:
            print(err)

    def do_purgeRepository(self, arg):
        '\nPurge G2 repository:  purgeRepository [-n]\n\n' \
        '                      -n = Skip resetting the resolver\n'
        try:
            args = self.parser.parse_args(['purgeRepository'] + parse(arg))
            confPurge = userInput('\n*** This will purge all currently loaded data from G2! ***\n\nAre you sure? ')
            if confPurge not in ['y','Y', 'yes', 'YES']:
                return
        except SystemExit:
            print(self.do_purgeRepository.__doc__)
            return
        if args.noReset:
            reset_resolver=False
            printWithNewLine('Purging the repository (without resetting resolver)')
        else:
            reset_resolver=True
            printWithNewLine('Purging the repository (and resetting resolver)')            
        try:
            self.g2_module.purgeRepository(reset_resolver) 
        except G2Exception.G2Exception as err:
            print(err)


    def do_hashRecord(self, arg):
        '\naHash an entity record:  hashRecord <json_data>\n'
        try:
            args = self.parser.parse_args(['jsonOnly'] + parse(arg))
        except SystemExit:
            print(self.do_hashRecord.__doc__)
            return
        try: 
            response = bytearray() 
            ret_code = self.g2_hasher_module.process(args.jsonData,response)
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
            printWithNewLine('')
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    with open(args.inputFile.split("?")[0]) as data_in:
                        for line in data_in:
                            response = bytearray() 
                            ret_code = self.g2_hasher_module.process(line.strip(),response)
                            hashedData = response.decode()
                            data_out.write(hashedData)
                            data_out.write('\n')
            else:
                with open(args.inputFile.split("?")[0]) as data_in :
                    for line in data_in:
                        response = bytearray() 
                        ret_code = self.g2_hasher_module.process(line.strip(),response)
                        hashedData = response.decode()
                        printWithNewLine(hashedData)
        except G2Exception.G2Exception as err:
            print(err)


    def do_license(self,arg):
        '\nGet the license information:  license\n'
        try: 
            response = json.dumps(json.loads(self.g2_product_module.license()))
            print('\nG2 license:')
            printWithNewLine(response)
        except G2Exception.G2Exception as err:
            print(err)

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
                printWithNewLine('Error encountered!  Return code = %s' % (returnCode))
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


# ----- Utility functions -----

def parse(argumentString):
    'Parses an argument list into a logical set of argument strings'
    return shlex.split(argumentString)

def printWithNewLine(arg):
    print(arg + '\n')

def printResponse(response):
    print('\n' + response + '\n')

# ----- The main function -----

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("fileToProcess", nargs='?')
    parser.add_argument('-c', '--iniFile', dest='iniFile', default='', help='the name of a G2Module.ini file to use', nargs='?')
    args = parser.parse_args()
    file_to_process = ''
    ini_file_name = ''

    if args.fileToProcess and len(args.fileToProcess) > 0:
        file_to_process = args.fileToProcess
    if args.iniFile and len(args.iniFile) > 0:
        ini_file_name = os.path.abspath(args.iniFile)

    #Python3 uses input, raw_input was removed
    userInput = input
    if sys.version_info[:2] <= (2,7):
        userInput = raw_input

    #--execute a file of commands
    if file_to_process:
        G2CmdShell(ini_file_name).fileloop(file_to_process)

    # go into command shell 
    else:
        G2CmdShell(ini_file_name).cmdloop()

    sys.exit()

