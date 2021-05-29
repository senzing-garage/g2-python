#--python imports
import cmd
import sys
from G2Module import G2Module
from G2AnonModule import G2AnonModule
from G2AuditModule import G2AuditModule
from G2ProductModule import G2ProductModule
import G2Exception
import json
import shlex
import argparse
import os
import csv

class G2CmdShell(cmd.Cmd, object):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.intro = ''
        self.prompt = '(g2) '
        self.g2_module = G2Module('pyG2', 'G2Module.ini', False)
        self.g2_anon_module = G2AnonModule('pyG2Anon', 'G2Module.ini', False)
        self.g2_audit_module = G2AuditModule('pyG2Audit', 'G2Module.ini', False)
        self.g2_product_module = G2ProductModule('pyG2Product', 'G2Module.ini', False)
        self.initialized = False
        self.__hidden_methods = ('do_shell', 'do_EOF')

        self.parser = argparse.ArgumentParser(prog='G2Command ->', add_help=False)
        subparsers = self.parser.add_subparsers()
      
        jsonOnly_parser = subparsers.add_parser('jsonOnly', usage=argparse.SUPPRESS)
        jsonOnly_parser.add_argument('jsonData')

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
        exportEntityReport_parser.add_argument('-m', '--maximumMatchLevel', required=False, default=0, type=int)
        exportEntityReport_parser.add_argument('-s', '--includeSingletons', action='store_true', required=False, default=False)
        exportEntityReport_parser.add_argument('-c', '--includeExtraCols', action='store_true', required=False, default=False)
        exportEntityReport_parser.add_argument('-f', '--flags', required=False, default=0, type=int)
        exportEntityReport_parser.add_argument('-o', '--outputFile', required=False)

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

        recordDelete_parser = subparsers.add_parser('recordDelete', usage=argparse.SUPPRESS)
        recordDelete_parser.add_argument('dataSourceCode')
        recordDelete_parser.add_argument('recordID')
        recordDelete_parser.add_argument('-l', '--loadID', required=False)

        getEntityByEntityID_parser = subparsers.add_parser('getEntityByEntityID', usage=argparse.SUPPRESS)
        getEntityByEntityID_parser.add_argument('entityID', type=int)

        findPathByEntityID_parser = subparsers.add_parser('findPathByEntityID', usage=argparse.SUPPRESS)
        findPathByEntityID_parser.add_argument('startEntityID', type=int)
        findPathByEntityID_parser.add_argument('endEntityID', type=int)
        findPathByEntityID_parser.add_argument('maxDegree', type=int)

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

        findPathByRecordID_parser = subparsers.add_parser('findPathByRecordID', usage=argparse.SUPPRESS)
        findPathByRecordID_parser.add_argument('startDataSourceCode')
        findPathByRecordID_parser.add_argument('startRecordID')
        findPathByRecordID_parser.add_argument('endDataSourceCode')
        findPathByRecordID_parser.add_argument('endRecordID')
        findPathByRecordID_parser.add_argument('maxDegree', type=int)

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

        outputOptional_parser = subparsers.add_parser('outputOptional',  usage=argparse.SUPPRESS)
        outputOptional_parser.add_argument('-o', '--outputFile', required=False)

        purgeRepository_parser = subparsers.add_parser('purgeRepository',  usage=argparse.SUPPRESS)
        purgeRepository_parser.add_argument('-n', '--noReset', required=False, nargs='?', const=1, type=int)

    # ----- G2 startup/shutdown -----

    def preloop(self):
        if (self.initialized):
            return
        print("Initializing engine...")
        self.g2_module.init()
        self.g2_anon_module.init()
        self.g2_audit_module.init()
        self.g2_product_module.init()
        self.initialized = True
        print('\nWelcome to the G2 shell. Type help or ? to list commands.\n')

    def postloop(self):
        if (self.initialized):
            self.g2_module.destroy()
            self.g2_anon_module.destroy()
            self.g2_audit_module.destroy()
            self.g2_product_module.destroy()
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
    def help_MatchLevels_vs_Flags(self):
        print (
              '\nThe core export API uses a flag to determine the level of entity resolution details to include in an export. This flag\n' \
              'is an additive integer that maps to the requested details to export.\n\n' \
              'For example:\n' \
              '\t4  = export only resolved entities\n' \
              '\t16 = export only possibly related entities\n' \
              '\t20 = export both resolved AND possibly related entities (4 + 16)\n\n' \
              'This provides great flexibility but isn\'t always convenient.\n\n' \
              'In addition to the flag (-f) G2Command allows you to provide a simpler match level (-m) providing cumulative levels\n' \
              'of the details to return.\n\n' \
             'For example:\n' \
             '\t1 = Return resolved entities\n' \
             '\t2 = Return resolved AND possibly same entities\n' \
             '\t3 = Return resolved AND possibly same AND possibly related entities\n' \
             '\t4 = Return resolved AND possibly same AND possibly related AND disclosed relationship entities\n\n' \
              'When using -m you can also use the -c and -s arguments:\n\n' \
              '\t-s = Include singleton entities (Those that have not resolved or related)\n' \
              '\t-c = Include additional details in the export\n\n' \
              'The -f and -m are mutually exclusive. For further details see: http://docs.senzing.com/?c#entity-export-flags\n' \
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
        
    # ----- basic G2 commands -----

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
                    processedData = self.g2_module.processWithResponse(args.jsonData)
                    data_out.write(processedData)
                    data_out.write('\n')
                printWithNewLine('')
            else:
                    processedData = self.g2_module.processWithResponse(args.jsonData)
                    printResponse(processedData)
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
                            processedData = self.g2_module.processWithResponse(line.strip())
                            data_out.write(processedData)
                            data_out.write('\n')
                printWithNewLine('')
            else:
                with open(args.inputFile.split("?")[0]) as data_in :
                    for line in data_in:
                        processedData = self.g2_module.processWithResponse(line.strip())
                        printResponse(processedData)
        except G2Exception.G2Exception as err:
            print(err)

    def do_exportCSVEntityReport(self, arg):
        '\nExport repository contents as CSV:  exportCSVEntityReport [ -m <match_level> [-s] [-c] | -f <flags> ] [-o <output_file>]\n' \
        '\nSee also \'help MatchLevels_vs_Flags\' for the difference between -m and -f  \n'

        missingDetails = False

        try:
            args = self.parser.parse_args(['exportEntityReport'] + parse(arg))
        except SystemExit:
            print(self.do_exportCSVEntityReport.__doc__)
            return
        else:
            if args.maximumMatchLevel and args.flags:
                print('\nThe -f and -m arguments are mutually exclusive, use only one!\n')
                return
            if args.maximumMatchLevel == 0 and args.flags == 0:
                args.flags = 4
                missingDetails = True
            if args.flags and (args.includeSingletons or args.includeExtraCols):
                print('\nThe -c and -s arguments are only used with -m, ignoring!\n')

        try: 
            (response, recCnt) = self.g2_module.exportCSVEntityReport(args.maximumMatchLevel, args.flags, args.includeSingletons, args.includeExtraCols)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(response)
            else:
                printResponse(response)
        except G2Exception.G2Exception as err:
            print(err)
        else:
            #Remove 1 for the header on CSV
            print('Number of exported records = %s\n' % (recCnt-1) )
            if missingDetails:
                print('Exporting resolved entities (match level and flags using default values.)\n')


    def do_exportJSONEntityReport(self, arg):
        '\nExport repository contents as JSON:  exportJSONEntityReport [ -m <match_level> [-s] [-c] | -f <flags> ] [-o <output_file>]\n' \
        '\nSee also \'help MatchLevels_vs_Flags\' for the difference between -m and -f  \n'

        missingDetails = False

        try:
            args = self.parser.parse_args(['exportEntityReport'] + parse(arg))
        except SystemExit:
            print(self.do_exportJSONEntityReport.__doc__)
            return
        else:
            if args.maximumMatchLevel and args.flags:
                print('\nThe -f and -m arguments are mutually exclusive, use only one!\n')
                return
            if args.maximumMatchLevel == 0 and args.flags == 0:
                args.flags = 4
                missingDetails = True
            if args.flags and (args.includeSingletons or args.includeExtraCols):
                print('\nThe -c and -s arguments are only used with -m, ignoring!\n')

        try: 
            (response, recCnt) = self.g2_module.exportJSONEntityReport(args.maximumMatchLevel, args.flags, args.includeSingletons, args.includeExtraCols)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(response)
            else:
                printResponse(response)
        except G2Exception.G2Exception as err:
            print(err)
        else:
            print('Number of exported records = %s\n' % recCnt )
            if missingDetails:
                print('Exporting resolved entities (match level and flags using default values.)\n')

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


    def do_searchByAttributes(self, arg):
        '\nSearch by attributes:  searchByAttributes <jsonData>\n'
        try:
            args = self.parser.parse_args(['jsonOnly'] + parse(arg))
        except SystemExit:
            print(self.do_searchByAttributes.__doc__)
            return
        try: 
            response = self.g2_module.searchByAttributes(args.jsonData)
            printResponse(response)
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
            response = self.g2_module.getEntityByEntityID(args.entityID)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findPathByEntityID(args.startEntityID,args.endEntityID,args.maxDegree)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findNetworkByEntityID(args.entityList,args.maxDegree,args.buildOutDegree,args.maxEntities)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findPathExcludingByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,args.excludedEntities,args.flags)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findPathIncludingSourceByEntityID(args.startEntityID,args.endEntityID,args.maxDegree,args.excludedEntities,args.requiredDsrcs,args.flags)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.getEntityByRecordID(args.dataSourceCode, args.recordID)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findPathByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findNetworkByRecordID(args.recordList,args.maxDegree,args.buildOutDegree,args.maxEntities)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findPathExcludingByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.excludedEntities,args.flags)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_module.findPathIncludingSourceByRecordID(args.startDataSourceCode,args.startRecordID,args.endDataSourceCode,args.endRecordID,args.maxDegree,args.excludedEntities,args.requiredDsrcs,args.flags)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
        except G2Exception.G2Exception as err:
            print(err)


    def do_getRecord(self, arg):
        '\nGet current set of attributes for record ID :  getRecord <dataSourceCode> <recordID>\n'
        try:
            args = self.parser.parse_args(['getEntityByRecordID'] + parse(arg))
        except SystemExit:
            print(self.do_getRecord.__doc__)
            return
        try: 
            response = self.g2_module.getRecord(args.dataSourceCode, args.recordID)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')

        except G2Exception.G2Exception as err:
            print(err)


    def do_stats(self,arg):
        '\nGet engine workload statistics for last process:  stats\n'
        try: 
            response = json.dumps(json.loads(self.g2_module.stats()))
            printResponse(response)
        except G2Exception.G2Exception as err:
            print(err)

    def do_getSummaryData(self,arg):
        '\nGet summary data:  getSummaryData\n'
        try: 
            response = json.dumps(self.g2_audit_module.getSummaryData())
            printResponse(response)
        except G2Exception.G2Exception as err:
            print(err)

    def do_getSummaryDataDirect(self,arg):
        '\nGet summary data with optimized speed:  getSummaryDataDirect\n'
        try: 
            response = json.dumps(self.g2_audit_module.getSummaryDataDirect())
            printResponse(response)
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
            response = self.g2_audit_module.getUsedMatchKeys(args.fromDataSource,args.toDataSource,args.matchLevel)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
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
            response = self.g2_audit_module.getUsedPrinciples(args.fromDataSource,args.toDataSource,args.matchLevel)
            if response:
                printResponse(response)
            else:
                printWithNewLine('')
        except G2Exception.G2Exception as err:
            print(err)

    def do_getAuditReport(self, arg):
        '\nExport repository contents as CSV:  getAuditReport -f <from_data_source> -t <to_data_source> -m <match_level> [-o <output_file>]\n'
        try:
            args = self.parser.parse_args(['getAuditReport'] + parse(arg))
        except SystemExit:
            print(self.do_getAuditReport.__doc__)
            return
        try: 
            response = self.g2_audit_module.getAuditReport(args.fromDataSource,args.toDataSource,args.matchLevel)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(response)
            else:
                printResponse(response)
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
            response = json.loads(self.g2_module.exportConfig())
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    json.dump(response,data_out)
            else:
                printResponse(json.dumps(response))
        except G2Exception.G2Exception as err:
            print(err)

    def do_getActiveConfigID(self,arg):
        '\nGet the config identifier:  getActiveConfigID\n'    
        try: 
            response = self.g2_module.getActiveConfigID()
            printResponse(str(response))
        except G2Exception.G2Exception as err:
            print(err)

    def do_getRepositoryLastModifiedTime(self,arg):
        '\nGet the last modified time of the datastore:  getRepositoryLastModifiedTime\n'    
        try: 
            response = self.g2_module.getRepositoryLastModifiedTime()
            printResponse(str(response))
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
            response = self.g2_anon_module.exportTokenLibrary()
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    json.dump(response,data_out)
            else:
                printWithNewLine(json.dumps(response))
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


    def do_anonymize(self, arg):
        '\nAnonymize an entity record:  anonymize <json_data>\n'
        try:
            args = self.parser.parse_args(['jsonOnly'] + parse(arg))
        except SystemExit:
            print(self.do_anonymize.__doc__)
            return
        try: 
            response = self.g2_anon_module.anonymize(args.jsonData)
            printResponse(response)
        except G2Exception.G2Exception as err:
            print(err)

    def do_anonymizeFile(self, arg):
        '\nAnonymize a file of entity records:  anonymizeFile <input_file> [-o <output_file>]\n'
        try:
            args = self.parser.parse_args(['inputFile'] + parse(arg))
        except SystemExit:
            print(self.do_anonymizeFile.__doc__)
            return
        try: 
            printWithNewLine('')
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    with open(args.inputFile.split("?")[0]) as data_in:
                        for line in data_in:
                            anonymizedData = self.g2_anon_module.anonymize(line.strip())
                            data_out.write(anonymizedData)
                            data_out.write('\n')
            else:
                with open(args.inputFile.split("?")[0]) as data_in :
                    for line in data_in:
                        anonymizedData = self.g2_anon_module.anonymize(line.strip())
                        printWithNewLine(anonymizedData)
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

    #Python3 uses input, raw_input was removed
    userInput = input
    if sys.version_info[:2] <= (2,7):
        userInput = raw_input

    #--execute a file of commands
    if len(sys.argv) > 1:
        G2CmdShell().fileloop(sys.argv[1])

    # go into command shell 
    else:
        G2CmdShell().cmdloop()

    sys.exit()
