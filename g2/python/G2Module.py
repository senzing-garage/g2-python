from ctypes import *
import threading
import json
import os

tls_var = threading.local()

from csv import reader as csvreader

from G2Exception import TranslateG2ModuleException, G2ModuleNotInitialized, G2ModuleGenericException

def resize_return_buffer(buf_, size_):
  """  callback function that resizs return buffer when it is too small
  Args:
  size_: size the return buffer needs to be
  """
  try:
    if (sizeof(tls_var.buf) < size_) :
      tls_var.buf = create_string_buffer(size_)
  except AttributeError:
      tls_var.buf = create_string_buffer(size_)
  return addressof(tls_var.buf)


class G2Module(object):
    """G2 module access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_file_name: name and location of .ini file
    """
    def init(self):
        """  Initializes the G2 engine
        This should only be called once per process.  Currently re-initializing the G2 engin
        after a destroy requires unloaded the class loader used to load this class.

        Returns:
            int: 0 on success
        """

        if self._debug:
            print("Initializing G2 module")

        resize_return_buffer(None, 65535)

        p_module_name = self.prepareStringArgument(self._module_name)
        p_ini_file_name = self.prepareStringArgument(self._ini_file_name)

        self._lib_handle.G2_init.argtypes = [c_char_p, c_char_p, c_int]
        retval = self._lib_handle.G2_init(p_module_name,
                                 p_ini_file_name,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(retval))

        if retval == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif retval < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Module")
        return retval


    def __init__(self, module_name_, ini_file_name_, debug_=False):
        # type: (str, str, bool) -> None
        """ G2Module class initialization
        Args:
            moduleName: A short name given to this instance of the engine
            iniFilename: A fully qualified path to the G2 engine INI file (often /opt/senzing/g2/python/G2Module.ini)
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout
        """

        try:
          if os.name == 'nt':
            self._lib_handle = cdll.LoadLibrary("G2.dll")
          else:
            self._lib_handle = cdll.LoadLibrary("libG2.so")
        except OSError as ex:
          print("ERROR: Unable to load G2.  Did you remember to setup your environment by sourcing the setupEnv file?")
          print("ERROR: For more information see https://senzing.zendesk.com/hc/en-us/articles/115002408867-Introduction-G2-Quickstart")
          print("ERROR: If you are running Ubuntu or Debian please also review the ssl and crypto information at https://senzing.zendesk.com/hc/en-us/articles/115010259947-System-Requirements")
          raise G2ModuleGenericException("Failed to load the G2 library")

        self._resize_func_def = CFUNCTYPE(c_char_p, c_char_p, c_size_t)
        self._resize_func = self._resize_func_def(resize_return_buffer)
        self._module_name = module_name_
        self._ini_file_name = ini_file_name_
        self._debug = debug_

    def process(self, input_umf_):
        # type: (str) -> None
        """ Generic process function without return
        This method will send a record for processing in g2.

        Args:
            record: An input record to be processed. Contains the data and control info.

        Return:
            None
        """

        if type(input_umf_) == str:
            input_umf_string = input_umf_.encode('utf-8')
        elif type(input_umf_) == bytearray:
            input_umf_string = str(input_umf_)
        else:
            input_umf_string = input_umf_
        resize_return_buffer(None, 65535)
        self._lib_handle.G2_process.argtypes = [c_char_p]
        self._lib_handle.G2_process.restype = c_int
        ret_code = self._lib_handle.G2_process(input_umf_string)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))

    def processWithResponse(self, input_umf_):
        """ Generic process function that returns results
        This method will send a record for processing in g2. It is a synchronous
        call, i.e. it will wait until g2 actually processes the record, and then
        optionally return any response message.

        Args:
            record: An input record to be processed. Contains the data and control info.
            response: If there is a response to the message it will be returned here.
                     Note there are performance benefits of calling the process method
                     that doesn't need a response message.

        Return:
            str: The response in G2 JSON format.
        """

        # type: (str) -> str
        """  resolves an entity synchronously
        Args:
            input_umf_: G2 style JSON
        """
        if type(input_umf_) == str:
            input_umf_string = input_umf_.encode('utf-8')
        elif type(input_umf_) == bytearray:
            input_umf_string = str(input_umf_)
        else:
            input_umf_string = input_umf_
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_processWithResponseResize.argtypes = [c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_processWithResponseResize(input_umf_string,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Module has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def checkRecord(self, input_umf_, recordQueryList):
        # type: (str,str,str) -> str
        """ Scores the input record against the specified one
        Args:
            input_umf_: A JSON document containing the attribute information
                   for the observation.
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record

        Return:
            str: The response in G2 JSON format.
        """

        if type(input_umf_) == str:
            input_umf_string = input_umf_.encode('utf-8')
        elif type(input_umf_) == bytearray:
            input_umf_string = str(input_umf_)
        else:
            input_umf_string = input_umf_
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_checkRecord.argtypes = [c_char_p, c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_checkRecord(input_umf_string,
                                                   recordQueryList,
                                                   pointer(responseBuf),
                                                   pointer(responseSize),
                                                   self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Module has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def getExportFlagsForMaxMatchLevel(self, max_match_level, includeSingletons, includeExtraCols):
        """ Converts a maximum match level into an appropriate export flag bitmask value.

        Args:
            max_match_level: The maximum match level to use in an export.
            includeSingletons: Also include singletons.
            includeExtraCols: Also include extra export output.

        Return:
            int: A bitmask flag representing the match-levels to include.
        """

        g2ExportFlags = 0
        if max_match_level == 1:
            # Include resolved entities
            g2ExportFlags = 4
        elif max_match_level == 2:
            # Include possibly same relationships in addition to resolved entities
            g2ExportFlags = 12
        elif max_match_level == 3:
            # Include possibly related relationships in addition to resolved entities & possibly same
            g2ExportFlags = 28
        elif max_match_level == 4:
            # Include disclosed relationships in addition to resolved entities & possibly same & possibly related
            g2ExportFlags = 92
        else:
            g2ExportFlags = 0

        #Add 1 to flags if we are including singletons
        if includeSingletons:
            g2ExportFlags += 1
        #Add 2 to flags if we are including extra header columns
        if includeExtraCols:
            g2ExportFlags = g2ExportFlags + 2 

        return g2ExportFlags

    def getExportHandle(self, exportType, max_match_level):
        # type: (str, int) -> c_void_p
        """ Generate a CSV or JSON export
        This is used to export entity data from known entities.  This function
        returns an export-handle that can be read from to get the export data
        in the requested format.  The export-handle should be read using the "G2_fetchNext"
        function, and closed when work is complete. If CSV, the first output row returned
        by the export-handle contains the CSV column headers as a string.  Each
        following row contains the exported entity data.

        Args:
            exportType: CSV or JSON
            max_match_level: The match-level to specify what kind of entity resolves
                         and relations we want to see.
                             1 -- "resolved" relationships
                             2 -- "possibly same" relationships
                             3 -- "possibly related" relationships
                             4 -- "name only" relationships                 *** Internal only
                             5 -- "disclosed" relationships
        Return:
            c_void_p: handle for the export
        """
        g2ExportFlags = self.getExportFlagsForMaxMatchLevel(max_match_level, True, True)

        if exportType == 'CSV':
            self._lib_handle.G2_exportCSVEntityReport.restype = c_void_p
            exportHandle = self._lib_handle.G2_exportCSVEntityReport(g2ExportFlags)
        else:
            self._lib_handle.G2_exportJSONEntityReport.restype = c_void_p
            exportHandle = self._lib_handle.G2_exportJSONEntityReport(g2ExportFlags)
        return exportHandle

    def fetchExportRecord(self, exportHandle):
        # type: (c_void_p) -> str
        """ Fetch a record from an export
        Args:
            exportHandle: handle from generated export

        Returns:
            str: Record fetched, empty if there is no more data
        """

        resultString = ""
        resize_return_buffer(None,65535)
        self._lib_handle.G2_fetchNext.argtypes = [c_void_p, c_char_p, c_size_t]
        rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))
        while rowData:
            resultString += tls_var.buf.value.decode('utf-8')
            if resultString[-1] == '\n':
                resultString = resultString[0:-1]
                break
            else:
                rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))
        return resultString

    def fetchCsvExportRecord(self, exportHandle, csvHeaders = None):
        # type: (c_void_p, str) -> str
        """ Fetch a CSV record from an export
        Args:
            exportHandle: handle from generated export
            csvHeaders: CSV header record

        Returns:
            dict: Record fetched using the csvHeaders as the keys.
                  None if no more data is available.
        """
        resultString = self.fetchExportRecord(exportHandle)
        if resultString:
            csvRecord = next(csvreader([resultString]))
            if csvHeaders:
                csvRecord = dict(list(zip(csvHeaders, csvRecord)))
        else:
            csvRecord = None
        return csvRecord 

    def exportCSVEntityReport(self, max_match_level, g2ExportFlags, includeSingletons, includeExtraCols):
        # type: (int, int) -> str
        """ Generate a CSV Entity Report
        This is used to export entity data from known entities.  This function
        returns an export-handle that can be read from to get the export data
        in CSV format.  The export-handle should be read using the "G2_fetchNext"
        function, and closed when work is complete. Each output row contains the
        exported entity data for a single resolved entity.
   
        Args:
            max_match_level: The match-level to specify what kind of entity resolves
                         and relations we want to see.
                             1 -- "resolved" relationships
                             2 -- "possibly same" relationships
                             3 -- "possibly related" relationships
                             4 -- "name only" relationships                         *** Internal only
                             5 -- "disclosed" relationships
            g2ExportFlags: A bit mask specifying other control flags, such as
                           "G2_EXPORT_INCLUDE_SINGLETONS".  The default and recommended
                           value is "G2_EXPORT_DEFAULT_REPORT_FLAGS".
            includeSingletons: Also include singletons
            includeExtraCols: Also include extra export output


        Return:
            c_void_p: handle for the export
        """

        resultString = b""
        fullG2ExportFlags_ = self.getExportFlagsForMaxMatchLevel(max_match_level, includeSingletons, includeExtraCols)
        fullG2ExportFlags_ = fullG2ExportFlags_ | g2ExportFlags
        self._lib_handle.G2_exportCSVEntityReport.restype = c_void_p
        exportHandle = self._lib_handle.G2_exportCSVEntityReport(fullG2ExportFlags_)
        rowCount = 0
        resize_return_buffer(None,65535)
        self._lib_handle.G2_fetchNext.argtypes = [c_void_p, c_char_p, c_size_t]
        rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))

        while rowData:
            rowCount += 1
            stringData = tls_var.buf
            resultString += stringData.value
            rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))
        self._lib_handle.G2_closeExport(c_void_p(exportHandle))

        return (resultString.decode('utf-8'), rowCount)

    def exportJSONEntityReport(self, max_match_level, g2ExportFlags, includeSingletons, includeExtraCols):
        # type: (int, int) -> str
        """ Generate a JSON Entity Report
        This is used to export entity data from known entities.  This function
        returns an export-handle that can be read from to get the export data
        in JSON format.  The export-handle should be read using the "G2_fetchNext"
        function, and closed when work is complete. Each output row contains the
        exported entity data for a single resolved entity.
   
        Args:
            max_match_level: The match-level to specify what kind of entity resolves
                         and relations we want to see.
                             1 -- "resolved" relationships
                             2 -- "possibly same" relationships
                             3 -- "possibly related" relationships
                             4 -- "name only" relationships
                             5 -- "disclosed" relationships
            g2ExportFlags: A bit mask specifying other control flags, such as
                           "G2_EXPORT_INCLUDE_SINGLETONS".  The default and recommended
                           value is "G2_EXPORT_DEFAULT_REPORT_FLAGS".
            includeSingletons: Also include singletons
            includeExtraCols: Also include extra export output

        Return:
            c_void_p: handle for the export
        """
        resultString = b""
        fullG2ExportFlags_ = self.getExportFlagsForMaxMatchLevel(max_match_level, includeSingletons, includeExtraCols)
        fullG2ExportFlags_ = fullG2ExportFlags_ | g2ExportFlags
        self._lib_handle.G2_exportJSONEntityReport.restype = c_void_p
        exportHandle = self._lib_handle.G2_exportJSONEntityReport(fullG2ExportFlags_)
        rowCount = 0
        resize_return_buffer(None,65535)
        self._lib_handle.G2_fetchNext.argtypes = [c_void_p, c_char_p, c_size_t]
        rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))

        while rowData:
            rowCount += 1
            stringData = tls_var.buf
            resultString += stringData.value
            rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))
        self._lib_handle.G2_closeExport(c_void_p(exportHandle))
        
        return (resultString.decode('utf-8'), rowCount)

    def prepareStringArgument(self, stringToPrepare):
        # type: (str) -> str
        """ Internal processing function """

        if stringToPrepare == None:
            return None
        #if string is unicode, transcode to utf-8 str
        if type(stringToPrepare) == str:
            return stringToPrepare.encode('utf-8')
        #if input is bytearray, assumt utf-8 and convert to str
        elif type(stringToPrepare) == bytearray:
            return str(stringToPrepare)
        #input is already a str
        return stringToPrepare

    def addRecord(self,dataSourceCode,recordId,jsonData,loadId=None):
        # type: (str,str,str,str) -> int
        """ Loads the JSON record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            jsonData: A JSON document containing the attribute information
                   for the observation.
            loadID: The observation load ID for the record, can be null and will default to dataSourceCode

        Return:
            int: 0 on success
        """
   
        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _loadId = self.prepareStringArgument(loadId)
        _recordId = self.prepareStringArgument(recordId)
        _jsonData = self.prepareStringArgument(jsonData)
        resize_return_buffer(None, 65535)
        ret_code = self._lib_handle.G2_addRecord(_dataSourceCode,_recordId,_jsonData,_loadId)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code

    def replaceRecord(self,dataSourceCode,recordId,jsonData,loadId=None):
        # type: (str,str,str,str) -> int
        """ Replace the JSON record, loads if doesn't exist
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            jsonData: A JSON document containing the attribute information
                   for the observation.
            loadID: The load ID for the record, can be null and will default to dataSourceCode

        Return:
            int: 0 on success
        """

        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _loadId = self.prepareStringArgument(loadId)
        _recordId = self.prepareStringArgument(recordId)
        _jsonData = self.prepareStringArgument(jsonData)
        resize_return_buffer(None, 65535)
        ret_code = self._lib_handle.G2_replaceRecord(_dataSourceCode,_recordId,_jsonData,_loadId)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code

    def deleteRecord(self,dataSourceCode,recordId,loadId=None):
        # type: (str,str,str) -> int
        """ Delete the record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            loadID: The load ID for the record, can be null and will default to dataSourceCode

        Return:
            int: 0 on success
        """

        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _loadId = self.prepareStringArgument(loadId)
        _recordId = self.prepareStringArgument(recordId)
        resize_return_buffer(None, 65535)
        ret_code = self._lib_handle.G2_deleteRecord(_dataSourceCode,_recordId,_loadId)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code


    def searchByAttributes(self,jsonData):
        # type: (str) -> str
        """ Find records matching the provided attributes
        Args:
            jsonData: A JSON document containing the attribute information to search.

        Return:
            str: JSON document with results
        """

        _jsonData = self.prepareStringArgument(jsonData)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_searchByAttributes.argtypes = [c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_searchByAttributes(_jsonData,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return tls_var.buf.value.decode('utf-8')

    def findPathByEntityID(self,startEntityID,endEntityID,maxDegree):
        # type: (int) -> str
        """ Find a path between two entities in the system.
        Args:
            startEntityID: The entity ID you want to find the path from
            endEntityID: The entity ID you want to find the path to
            maxDegree: The maximum path length to search for

        Return:
            str: JSON document with results
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findPathByEntityID.restype = c_int
        self._lib_handle.G2_findPathByEntityID.argtypes = [c_longlong, c_longlong, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathByEntityID(startEntityID,endEntityID,maxDegree,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return tls_var.buf.value.decode('utf-8')

    def findNetworkByEntityID(self,entityList,maxDegree,buildOutDegree,maxEntities):
        # type: (int) -> str
        """ Find a network between entities in the system.
        Args:
            entityList: The entities to search for the network of
            maxDegree: The maximum path length to search for between entities
            buildOutDegree: The number of degrees to build out the surrounding network
            maxEntities: The maximum number of entities to include in the result

        Return:
            str: JSON document with results
        """

        _entityList = self.prepareStringArgument(entityList)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findNetworkByEntityID.restype = c_int
        self._lib_handle.G2_findNetworkByEntityID.argtypes = [c_char_p, c_int, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findNetworkByEntityID(_entityList,maxDegree,buildOutDegree,maxEntities,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return tls_var.buf.value.decode('utf-8')

    def findPathByRecordID(self,startDsrcCode,startRecordId,endDsrcCode,endRecordId,maxDegree):
        # type: (str,str) -> str
        """ Find a path between two records in the system.
        Args:
            startDataSourceCode: The data source for the record you want to find the path from
            startRecordID: The ID for the record you want to find the path from
            endDataSourceCode: The data source for the record you want to find the path to
            endRecordID: The ID for the record you want to find the path to
            maxDegree: The maximum path length to search for

        Return:
            str: JSON document with results
        """

        _startDsrcCode = self.prepareStringArgument(startDsrcCode)
        _startRecordId = self.prepareStringArgument(startRecordId)
        _endDsrcCode = self.prepareStringArgument(endDsrcCode)
        _endRecordId = self.prepareStringArgument(endRecordId)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findPathByRecordID.restype = c_int
        self._lib_handle.G2_findPathByRecordID.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathByRecordID(_startDsrcCode,_startRecordId,_endDsrcCode,_endRecordId,maxDegree,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def findNetworkByRecordID(self,recordList,maxDegree,buildOutDegree,maxEntities):
        # type: (str,str) -> str
        """ Find a network between entities in the system.
        Args:
            recordList: The records to search for the network of
            maxDegree: The maximum path length to search for between entities
            buildOutDegree: The number of degrees to build out the surrounding network
            maxEntities: The maximum number of entities to include in the result

        Return:
            str: JSON document with results
        """

        _recordList = self.prepareStringArgument(recordList)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findNetworkByRecordID.restype = c_int
        self._lib_handle.G2_findNetworkByRecordID.argtypes = [c_char_p, c_int, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findNetworkByRecordID(_recordList,maxDegree,buildOutDegree,maxEntities,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def findPathExcludingByEntityID(self,startEntityID,endEntityID,maxDegree,excludedEntities,flags):
        # type: (int) -> str
        """ Find a path between two entities in the system.
        Args:
            startEntityID: The entity ID you want to find the path from
            endEntityID: The entity ID you want to find the path to
            maxDegree: The maximum path length to search for
            excludedEntities: JSON document containing entities to exclude
            flags: control flags

        Return:
            str: JSON document with results
        """

        _excludedEntities = self.prepareStringArgument(excludedEntities)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findPathExcludingByEntityID.restype = c_int
        self._lib_handle.G2_findPathExcludingByEntityID.argtypes = [c_longlong, c_longlong, c_int, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathExcludingByEntityID(startEntityID,endEntityID,maxDegree,_excludedEntities,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return tls_var.buf.value.decode('utf-8')

    def findPathIncludingSourceByEntityID(self,startEntityID,endEntityID,maxDegree,excludedEntities,requiredDsrcs,flags):
        # type: (int) -> str
        """ Find a path between two entities in the system.
        Args:
            startEntityID: The entity ID you want to find the path from
            endEntityID: The entity ID you want to find the path to
            maxDegree: The maximum path length to search for
            excludedEntities: JSON document containing entities to exclude
            requiredDsrcs: JSON document containing data sources to require
            flags: control flags

        Return:
            str: JSON document with results
        """

        _excludedEntities = self.prepareStringArgument(excludedEntities)
        _requiredDsrcs = self.prepareStringArgument(requiredDsrcs)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findPathExcludingByEntityID.restype = c_int
        self._lib_handle.G2_findPathExcludingByEntityID.argtypes = [c_longlong, c_longlong, c_int, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathIncludingSourceByEntityID(startEntityID,endEntityID,maxDegree,_excludedEntities,_requiredDsrcs,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return tls_var.buf.value.decode('utf-8')

    def findPathExcludingByRecordID(self,startDsrcCode,startRecordId,endDsrcCode,endRecordId,maxDegree,excludedEntities,flags):
        # type: (str,str) -> str
        """ Find a path between two records in the system.
        Args:
            startDataSourceCode: The data source for the record you want to find the path from
            startRecordID: The ID for the record you want to find the path from
            endDataSourceCode: The data source for the record you want to find the path to
            endRecordID: The ID for the record you want to find the path to
            maxDegree: The maximum path length to search for
            excludedEntities: JSON document containing entities to exclude
            flags: control flags

        Return:
            str: JSON document with results
        """

        _startDsrcCode = self.prepareStringArgument(startDsrcCode)
        _startRecordId = self.prepareStringArgument(startRecordId)
        _endDsrcCode = self.prepareStringArgument(endDsrcCode)
        _endRecordId = self.prepareStringArgument(endRecordId)
        _excludedEntities = self.prepareStringArgument(excludedEntities)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findPathExcludingByRecordID.restype = c_int
        self._lib_handle.G2_findPathExcludingByRecordID.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathExcludingByRecordID(_startDsrcCode,_startRecordId,_endDsrcCode,_endRecordId,maxDegree,
                                                                 _excludedEntities,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def findPathIncludingSourceByRecordID(self,startDsrcCode,startRecordId,endDsrcCode,endRecordId,maxDegree,excludedEntities,requiredDsrcs,flags):
        # type: (str,str) -> str
        """ Find a path between two records in the system.
        Args:
            startDataSourceCode: The data source for the record you want to find the path from
            startRecordID: The ID for the record you want to find the path from
            endDataSourceCode: The data source for the record you want to find the path to
            endRecordID: The ID for the record you want to find the path to
            maxDegree: The maximum path length to search for
            excludedEntities: JSON document containing entities to exclude
            requiredDsrcs: JSON document containing data sources to require
            flags: control flags

        Return:
            str: JSON document with results
        """

        _startDsrcCode = self.prepareStringArgument(startDsrcCode)
        _startRecordId = self.prepareStringArgument(startRecordId)
        _endDsrcCode = self.prepareStringArgument(endDsrcCode)
        _endRecordId = self.prepareStringArgument(endRecordId)
        _excludedEntities = self.prepareStringArgument(excludedEntities)
        _requiredDsrcs = self.prepareStringArgument(requiredDsrcs)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_findPathIncludingSourceByRecordID.restype = c_int
        self._lib_handle.G2_findPathIncludingSourceByRecordID.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathIncludingSourceByRecordID(_startDsrcCode,_startRecordId,_endDsrcCode,_endRecordId,maxDegree,
                                                                 _excludedEntities,_requiredDsrcs,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def getEntityByEntityID(self,entityID):
        # type: (int) -> str
        """ Find the entity with the given ID
        Args:
            entityID: The entity ID you want returned.  Typically referred to as
                      ENTITY_ID in JSON results.

        Return:
            str: JSON document with results
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_getEntityByEntityID.argtypes = [c_longlong, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getEntityByEntityID(entityID,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return tls_var.buf.value.decode('utf-8')

    def getEntityByRecordID(self,dsrcCode,recordId):
        # type: (str,str) -> str
        """ Get the entity containing the specified record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record

        Return:
            str: JSON document with results
        """

        _dsrcCode = self.prepareStringArgument(dsrcCode)
        _recordId = self.prepareStringArgument(recordId)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_getEntityByRecordID.argtypes = [c_char_p, c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getEntityByRecordID(_dsrcCode,_recordId,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def getRecord(self,dsrcCode,recordId):
        # type: (str,str) -> str
        """ Get the specified record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record

        Return:
            str: JSON document with results
        """

        _dsrcCode = self.prepareStringArgument(dsrcCode)
        _recordId = self.prepareStringArgument(recordId)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_getRecord.argtypes = [c_char_p, c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getRecord(_dsrcCode,_recordId,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def stats(self):
        # type: () -> object
        """ Retrieve the workload statistics for the current process.
        Resets them after retrieved.

        Args:

        Return:
            object: JSON document with statistics
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_stats.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_stats(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Module has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def exportConfig(self):
        # type: () -> object
        """ Retrieve the G2 engine configuration

        Args:

        Return:
            object: JSON document with G2 engine configuration
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2_exportConfig.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_exportConfig(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return responseBuf.value.decode('utf-8')

    def getActiveConfigID(self):
        # type: () -> object
        """ Retrieve the active config ID for the G2 engine

        Args:

        Return:
            object: The numeric active config ID
        """

        configID = c_longlong(0)
        self._lib_handle.G2_getActiveConfigID.argtypes = [POINTER(c_longlong)]
        ret_code = self._lib_handle.G2_getActiveConfigID(configID)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return configID.value

    def getRepositoryLastModifiedTime(self):
        # type: () -> object
        """ Retrieve the last modified time stamp of the entity store repository

        Args:

        Return:
            object: The last modified time stamp, as a numeric integer

        """

        lastModifiedTimeStamp = c_longlong(0)
        self._lib_handle.G2_getRepositoryLastModifiedTime.argtypes = [POINTER(c_longlong)]
        ret_code = self._lib_handle.G2_getRepositoryLastModifiedTime(lastModifiedTimeStamp)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return lastModifiedTimeStamp.value

    def purgeRepository(self, reset_resolver_=True):
        # type: (bool) -> None
        """ Purges the G2 repository

        Args:
            reset_resolver: Re-initializes the engine.  Should be left True.

        Return:
            None
        """

        resize_return_buffer(None, 65535)
        retval = self._lib_handle.G2_purgeRepository()
        if retval == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif retval == -1:
            raise G2ModuleNotInitialized('G2Module has not been succesfully initialized')

        if reset_resolver_ == True:
            self.restart()

    def restart(self):
        """  Internal function """
        self.destroy()
        self.init()

    def destroy(self):
        """ Uninitializes the engine
        This should be done once per process after init(...) is called.
        After it is called the engine will no longer function.

        Args:

        Return:
            None
        """

        self._lib_handle.G2_destroy()

