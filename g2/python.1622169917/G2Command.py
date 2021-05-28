#--python imports
import cmd
import sys
from G2Module import G2Module
from G2AnonModule import G2AnonModule
from G2AuditModule import G2AuditModule
import G2Exception
import json
import shlex
import argparse
import os

class G2CmdShell(cmd.Cmd, object):

    def __init__(self):
        cmd.Cmd.__init__(self)
        self.intro = ''
        self.prompt = '(g2) '
        self.g2_module = G2Module('pyG2', 'G2Module.ini', False)
        self.g2_anon_module = G2AnonModule('pyG2Anon', 'G2Module.ini', False)
        self.g2_audit_module = G2AuditModule('pyG2Audit', 'G2Module.ini', False)
        self.initialized = False
        self.__hidden_methods = ('do_shell', 'do_EOF')

        self.parser = argparse.ArgumentParser(prog='G2Command ->', add_help=False)
        subparsers = self.parser.add_subparsers()
      
        jsonOnly_parser = subparsers.add_parser('jsonOnly', usage=argparse.SUPPRESS)
        jsonOnly_parser.add_argument('jsonData')

        processFile_parser = subparsers.add_parser('processFile', usage=argparse.SUPPRESS)
        processFile_parser.add_argument('inputFile')

        inputFile_parser = subparsers.add_parser('inputFile', usage=argparse.SUPPRESS)
        inputFile_parser.add_argument('inputFile')
        inputFile_parser.add_argument('-o', '--outputFile', required=False)

        processWithResponse_parser = subparsers.add_parser('processWithResponse',  usage=argparse.SUPPRESS)
        processWithResponse_parser.add_argument('jsonData')
        processWithResponse_parser.add_argument('-o', '--outputFile', required=False)

        exportEntityReport_parser = subparsers.add_parser('exportEntityReport', usage=argparse.SUPPRESS)
        exportEntityReport_parser.add_argument('-m', '--maximumMatchLevel', required=False, default=4, type=int)
        exportEntityReport_parser.add_argument('-f', '--flags', required=False, default=1, type=int)
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
        self.initialized = True
        print('\nWelcome to the G2 shell. Type help or ? to list commands.\n')

    def postloop(self):
        if (self.initialized):
            self.g2_module.destroy()
            self.g2_anon_module.destroy()
            self.g2_audit_module.destroy()
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


    #Hide do_shell from list of APIs. Seperate help section for it

    def get_names(self):
        return [n for n in dir(self.__class__) if n not in self.__hidden_methods]

    # ----- Misc Help -----

    def help_Arguments(self):
        print ('\nArguments: Optional arguments are surrounded with [ ] e.g. [-o output_file]\n')

    def help_MatchLevels(self):
        print (
              '\nMatch Level: Specify the level of entity resolves and relations to return\n' \
              '             1 - Same entities\n' \
              '             2 - Possibly same entities\n' \
              '             3 - Possibly related entities\n' \
              '             4 - Disclosed relationships\n\n' 
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
            with open(args.inputFile.split("?")[0]) as data_in:
                for line in data_in:
                    self.g2_module.process(line.strip())
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
        '\nExport repository contents as CSV:  exportCSVEntityReport [-m <maximum_match_level>] [-f <flags>] [-o <output_file>]\n'
        try:
            args = self.parser.parse_args(['exportEntityReport'] + parse(arg))
        except SystemExit:
            print(self.do_exportCSVEntityReport.__doc__)
            return
        try: 
            response = self.g2_module.exportCSVEntityReport(args.maximumMatchLevel, args.flags)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(response)
            else:
                printResponse(response)
        except G2Exception.G2Exception as err:
            print(err)


    def do_exportJSONEntityReport(self, arg):
        'Export repository contents as JSON:  exportJSONEntityReport [-m <maximum_match_level>] [-f <flags>] [-o <output_file>]\n'
        try:
            args = self.parser.parse_args(['exportEntityReport'] + parse(arg))
        except SystemExit:
            print(self.do_exportJSONEntityReport.__doc__)
            return
        try: 
            response = self.g2_module.exportJSONEntityReport(args.maximumMatchLevel,args.flags)
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    data_out.write(response)
            else:
                printResponse(response)
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
            response = json.dumps(self.g2_module.stats())
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
            response = self.g2_module.exportConfig()
            if args.outputFile:
                with open(args.outputFile, 'w') as data_out:
                    json.dump(response,data_out)
            else:
                printResponse(json.dumps(response))
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
            response = json.dumps(self.g2_module.license())
            print('\nG2 module license:')
            print(response)
        except G2Exception.G2Exception as err:
            print(err)
        try: 
            response = json.dumps(self.g2_anon_module.license())
            print('\nG2 anonymizer module license:')
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

    G2CmdShell().cmdloop()

