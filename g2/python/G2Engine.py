from ctypes import *
import threading
import json
import os

class MyBuffer(threading.local):
  def __init__(self):
    self.buf = create_string_buffer(65535)
    self.bufSize = sizeof(self.buf)
    #print("Created new Buffer {} of type {}".format(self.buf,type(self.buf)))

tls_var = MyBuffer()

class MyBuffer2(threading.local):
  def __init__(self, g2_engine, size = 65535):
    self.bufSize = c_size_t(size)
    g2_engine._lib_handle.G2_malloc.restype = c_void_p
    self.buf = g2_engine._lib_handle.G2_malloc(self.bufSize)
    #print("Created new Buffer {} of type {}".format(self.buf,type(self.buf)))

from csv import reader as csvreader

from G2Exception import TranslateG2ModuleException, G2ModuleNotInitialized, G2ModuleGenericException

def resize_return_buffer(buf_, size_):
  """  callback function that resizes return buffer when it is too small
  Args:
  size_: size the return buffer needs to be
  """
  try:
    if not tls_var.buf:
      #print("New RESIZE_RETURN_BUF {}:{}".format(buf_,size_))
      tls_var.buf = create_string_buffer(size_)
      tls_var.bufSize = size_
    elif (tls_var.bufSize < size_):
      #print("RESIZE_RETURN_BUF {}:{}/{}".format(buf_,size_,tls_var.bufSize))
      foo = tls_var.buf
      tls_var.buf = create_string_buffer(size_)
      tls_var.bufSize = size_
      memmove(tls_var.buf, foo, sizeof(foo))
  except AttributeError:
      #print("AttributeError RESIZE_RETURN_BUF {}:{}".format(buf_,size_))
      tls_var.buf = create_string_buffer(size_)
      #print("Created new Buffer {}".format(tls_var.buf))
      tls_var.bufSize = size_
  return addressof(tls_var.buf)
  


class G2Engine(object):
    """G2 engine access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _engine_name: CME engine name
        _ini_file_name: name and location of .ini file
    """

    # flags for exporting entity data
    G2_EXPORT_INCLUDE_ALL_ENTITIES = ( 1 << 0 )
    G2_EXPORT_CSV_INCLUDE_FULL_DETAILS = ( 1 << 1 )
    G2_EXPORT_INCLUDE_RESOLVED = ( 1 << 2 )
    G2_EXPORT_INCLUDE_POSSIBLY_SAME = ( 1 << 3 )
    G2_EXPORT_INCLUDE_POSSIBLY_RELATED = ( 1 << 4 )
    G2_EXPORT_INCLUDE_NAME_ONLY = ( 1 << 5 )
    G2_EXPORT_INCLUDE_DISCLOSED = ( 1 << 6 )
	
    # flags for outputting entity feature data
    G2_ENTITY_INCLUDE_ALL_FEATURES = ( 1 << 7 )
    G2_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES = ( 1 << 8 )
    G2_ENTITY_INCLUDE_SINGLE_FEATURES = ( 1 << 9 )
    G2_ENTITY_INCLUDE_NO_FEATURES = ( 1 << 10 )
	
    # flags for finding entity path data
    G2_FIND_PATH_PREFER_EXCLUDE = ( 1 << 11 )
	
    # flags for outputting entity relation data
    G2_ENTITY_INCLUDE_ALL_RELATIONS = ( 1 << 12 )
    G2_ENTITY_INCLUDE_POSSIBLY_SAME_RELATIONS = ( 1 << 13 )
    G2_ENTITY_INCLUDE_POSSIBLY_RELATED_RELATIONS = ( 1 << 14 )
    G2_ENTITY_INCLUDE_NAME_ONLY_RELATIONS = ( 1 << 15 )
    G2_ENTITY_INCLUDE_DISCLOSED_RELATIONS = ( 1 << 16 )
    G2_ENTITY_INCLUDE_NO_RELATIONS = ( 1 << 17 )
	
    # flag for getting a minimal entity
    G2_ENTITY_MINIMAL_FORMAT = ( 1 << 18 )

    # flag for excluding feature scores from search results
    G2_SEARCH_NO_FEATURE_SCORES = ( 1 << 19 )

    # recommended settings
    G2_EXPORT_DEFAULT_FLAGS = G2_EXPORT_INCLUDE_ALL_ENTITIES
    G2_ENTITY_DEFAULT_FLAGS = G2_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES | G2_ENTITY_INCLUDE_ALL_RELATIONS
    G2_FIND_PATH_DEFAULT_FLAGS = G2_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES | G2_ENTITY_INCLUDE_ALL_RELATIONS

    G2_SEARCH_BY_ATTRIBUTES_DEFAULT_FLAGS = G2_ENTITY_INCLUDE_REPRESENTATIVE_FEATURES
    G2_SEARCH_BY_ATTRIBUTES_MINIMAL_STRONG = G2_ENTITY_MINIMAL_FORMAT | G2_SEARCH_NO_FEATURE_SCORES | G2_ENTITY_INCLUDE_NO_RELATIONS | G2_EXPORT_INCLUDE_RESOLVED | G2_EXPORT_INCLUDE_POSSIBLY_SAME 
    G2_SEARCH_BY_ATTRIBUTES_MINIMAL_ALL = G2_ENTITY_MINIMAL_FORMAT | G2_SEARCH_NO_FEATURE_SCORES | G2_ENTITY_INCLUDE_NO_RELATIONS 

    # backwards compatability flags
    G2_EXPORT_DEFAULT_REPORT_FLAGS = G2_EXPORT_INCLUDE_ALL_ENTITIES

    def init(self, engine_name_, ini_file_name_, debug_=False):
        """  Initializes the G2 engine
        This should only be called once per process.  Currently re-initializing the G2 engin
        after a destroy requires unloaded the class loader used to load this class.
        Args:
            engineName: A short name given to this instance of the engine
            iniFilename: A fully qualified path to the G2 engine INI file (often /opt/senzing/g2/python/G2Module.ini)
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout

        Returns:
            int: 0 on success
        """

        self._engine_name = self.prepareStringArgument(engine_name_)
        self._ini_file_name = self.prepareStringArgument(ini_file_name_)
        self._debug = debug_
        if self._debug:
            print("Initializing G2 engine")

        self._lib_handle.G2_init.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2_init(self._engine_name,
                                 self._ini_file_name,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Engine")

        return ret_code
        

    def initAndGetConfigID(self, engine_name_, ini_file_name_, debug_, configID):
        """  Initializes the G2 engine
        This should only be called once per process.  Currently re-initializing the G2 engin
        after a destroy requires unloaded the class loader used to load this class.
        Args:
            engineName: A short name given to this instance of the engine
            iniFilename: A fully qualified path to the G2 engine INI file (often /opt/senzing/g2/python/G2Module.ini)
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout

        Returns:
            int: 0 on success
        """

        configID[::]=b''
        configIDValue = c_longlong(0)

        self._engine_name = self.prepareStringArgument(engine_name_)
        self._ini_file_name = self.prepareStringArgument(ini_file_name_)
        self._debug = debug_
        if self._debug:
            print("Initializing G2 engine")

        self._lib_handle.G2_initAndGetConfigID.argtypes = [ c_char_p, c_char_p, c_int, POINTER(c_longlong) ]
        ret_code = self._lib_handle.G2_initAndGetConfigID(self._engine_name,
                                 self._ini_file_name,
                                 self._debug,
                                 configIDValue)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Engine")

        configID += (str(configIDValue.value).encode())
        return ret_code

    def initV2(self, engine_name_, ini_params_, debug_=False):

        self._engine_name = self.prepareStringArgument(engine_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._debug = debug_
        if self._debug:
            print("Initializing G2 engine")

        self._lib_handle.G2_init_V2.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2_init_V2(self._engine_name,
                                 self._ini_params,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Engine")

        return ret_code

    def initWithConfigIDV2(self, engine_name_, ini_params_, initConfigID_, debug_):

        configIDValue = self.prepareIntArgument(initConfigID_)

        self._engine_name = self.prepareStringArgument(engine_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._debug = debug_
        if self._debug:
            print("Initializing G2 engine")

        self._lib_handle.G2_initWithConfigID_V2.argtypes = [ c_char_p, c_char_p, c_longlong, c_int ]
        ret_code = self._lib_handle.G2_initWithConfigID_V2(self._engine_name,
                                 self._ini_params,
                                 configIDValue,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Engine")

        return ret_code

    def reinitV2(self, initConfigID_):

        configIDValue = self.prepareIntArgument(initConfigID_)

        self._lib_handle.G2_reinit_V2.argtypes = [ c_longlong ]
        ret_code = self._lib_handle.G2_reinit_V2(configIDValue)

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Engine")

        return ret_code

    def __init__(self):
        # type: () -> None
        """ G2Engine class initialization
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
        self._resize_func_def2 = CFUNCTYPE(c_void_p, c_void_p, c_size_t)
        self._resize_func2 = self._resize_func_def2(self._lib_handle.G2_realloc)

        self.tls_var2 = MyBuffer2(self)
        self.info_buf = MyBuffer2(self)

    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        self._lib_handle.G2_clearLastException.restype = None
        self._lib_handle.G2_clearLastException.argtypes = []
        self._lib_handle.G2_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        self._lib_handle.G2_getLastException.restype = c_int
        self._lib_handle.G2_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        self._lib_handle.G2_getLastExceptionCode.restype = c_int
        self._lib_handle.G2_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2_getLastExceptionCode()
        return exception_code

    def primeEngine(self):
        ret_code = self._lib_handle.G2_primeEngine()
        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Engine")
        return ret_code

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
        self._lib_handle.G2_process.argtypes = [c_char_p]
        self._lib_handle.G2_process.restype = c_int
        ret_code = self._lib_handle.G2_process(input_umf_string)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
    
    def processWithInfo(self, input_umf_,response, flags=0):
        # type: (str, str, int) -> int
        """ Generic process function without return
        This method will send a record for processing in g2.

        Args:
            record: An input record to be processed. Contains the data and control info.
            response: Json document with info about the modified resolved entities
            flags: reserved for future use

        Return:
            None
        """

        if type(input_umf_) == str:
            input_umf_string = input_umf_.encode('utf-8')
        elif type(input_umf_) == bytearray:
            input_umf_string = str(input_umf_)
        else:
            input_umf_string = input_umf_

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_processWithInfo.argtypes = [c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_processWithInfo(input_umf_string,flags,pointer(responseBuf),pointer(responseSize),self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        
        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code

    def processWithResponse(self, input_umf_,response):
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
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_processWithResponseResize.argtypes = [c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_processWithResponseResize(input_umf_string,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')

        response += responseBuf.value
        return ret_code

    def checkRecord(self, input_umf_, recordQueryList, response):
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
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_checkRecord.argtypes = [c_char_p, c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_checkRecord(input_umf_string,
                                                   recordQueryList,
                                                   pointer(responseBuf),
                                                   pointer(responseSize),
                                                   self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')

        response += responseBuf.value
        return ret_code

    def exportJSONEntityReport(self, exportFlags):
        """ Generate a JSON export
        This is used to export entity data from known entities.  This function
        returns an export-handle that can be read from to get the export data
        in the requested format.  The export-handle should be read using the "G2_fetchNext"
        function, and closed when work is complete. 
        """
        self._lib_handle.G2_exportJSONEntityReport.restype = c_void_p
        self._lib_handle.G2_exportJSONEntityReport.argtypes = [c_int]
        exportHandle = self._lib_handle.G2_exportJSONEntityReport(exportFlags)
        if exportHandle == None:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        return exportHandle

    def exportCSVEntityReport(self, exportFlags):
        """ Generate a CSV export
        This is used to export entity data from known entities.  This function
        returns an export-handle that can be read from to get the export data
        in the requested format.  The export-handle should be read using the "G2_fetchNext"
        function, and closed when work is complete.  Tthe first output row returned
        by the export-handle contains the CSV column headers as a string.  Each
        following row contains the exported entity data.
        """
        self._lib_handle.G2_exportCSVEntityReport.restype = c_void_p
        self._lib_handle.G2_exportCSVEntityReport.argtypes = [c_int]
        exportHandle = self._lib_handle.G2_exportCSVEntityReport(exportFlags)
        if exportHandle == None:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        return exportHandle

    def exportCSVEntityReportV2(self, headersForCSV, exportFlags):
        """ Generate a CSV export
        This is used to export entity data from known entities.  This function
        returns an export-handle that can be read from to get the export data
        in the requested format.  The export-handle should be read using the "G2_fetchNext"
        function, and closed when work is complete.  Tthe first output row returned
        by the export-handle contains the CSV column headers as a string.  Each
        following row contains the exported entity data.
        """
        _headersForCSV = self.prepareStringArgument(headersForCSV)
        self._lib_handle.G2_exportCSVEntityReport_V2.restype = c_void_p
        self._lib_handle.G2_exportCSVEntityReport_V2.argtypes = [c_char_p, c_int]
        exportHandle = self._lib_handle.G2_exportCSVEntityReport_V2(_headersForCSV,exportFlags)
        if exportHandle == None:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        return exportHandle

    def fetchNext(self,exportHandle,response):
        """ Fetch a record from an export
        Args:
            exportHandle: handle from generated export

        Returns:
            str: Record fetched, empty if there is no more data
        """
        response[::]=b''
        self._lib_handle.G2_fetchNext.argtypes = [c_void_p, c_char_p, c_size_t]
        rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))
        while rowData:
            response += tls_var.buf.value
            if (response.decode())[-1] == '\n':
                break
            else:
                rowData = self._lib_handle.G2_fetchNext(c_void_p(exportHandle),tls_var.buf,sizeof(tls_var.buf))
        return response
		
    def closeExport(self, exportHandle):
        self._lib_handle.G2_closeExport(c_void_p(exportHandle))

    def prepareStringArgument(self, stringToPrepare):
        # type: (str) -> str
        """ Internal processing function """

        #handle null string
        if stringToPrepare == None:
            return None
        #if string is unicode, transcode to utf-8 str
        if type(stringToPrepare) == str:
            return stringToPrepare.encode('utf-8')
        #if input is bytearray, assumt utf-8 and convert to str
        elif type(stringToPrepare) == bytearray:
            return stringToPrepare.decode().encode('utf-8')
        elif type(stringToPrepare) == bytes:
            return str(stringToPrepare).encode('utf-8')
        #input is already a str
        return stringToPrepare

    def prepareIntArgument(self, valueToPrepare):
        # type: (str) -> int
        """ Internal processing function """
        """ This converts many types of values to an integer """

        #handle null string
        if valueToPrepare == None:
            return None
        #if string is unicode, transcode to utf-8 str
        if type(valueToPrepare) == str:
            return int(valueToPrepare.encode('utf-8'))
        #if input is bytearray, assumt utf-8 and convert to str
        elif type(valueToPrepare) == bytearray:
            return int(valueToPrepare)
        elif type(valueToPrepare) == bytes:
            return int(valueToPrepare)
        #input is already an int
        return valueToPrepare

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
        self._lib_handle.G2_addRecord.argtypes = [c_char_p, c_char_p, c_char_p]
        ret_code = self._lib_handle.G2_addRecord(_dataSourceCode,_recordId,_jsonData,_loadId)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code

    def addRecordWithReturnedRecordID(self,dataSourceCode,recordID,jsonData,loadId=None):
        # type: (str,str,str,str) -> int
        """ Loads the JSON record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: A memory buffer for returning the recordID
            jsonData: A JSON document containing the attribute information
                   for the observation.
            loadID: The observation load ID for the record, can be null and will default to dataSourceCode

        Return:
            int: 0 on success
        """
   
        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _loadId = self.prepareStringArgument(loadId)
        _jsonData = self.prepareStringArgument(jsonData)
        resultString = ""
        self._lib_handle.G2_addRecordWithReturnedRecordID.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_size_t]
        ret_code = self._lib_handle.G2_addRecordWithReturnedRecordID(_dataSourceCode,_jsonData,_loadId, tls_var.buf, sizeof(tls_var.buf))
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        resultString = str(tls_var.buf.value.decode('utf-8'))
        for i in resultString:
            recordID.append(i)        
        return ret_code

    def addRecordWithInfo(self,dataSourceCode,recordId,jsonData,response,loadId=None,flags=0):
        # type: (str,str,str,str,str,int) -> str
        """ Loads the JSON record and returns info about the load
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            jsonData: A JSON document containing the attribute information
                   for the observation.
            response: Json document with info about the modified resolved entities
            loadID: The observation load ID for the record, can be null and will default to dataSourceCode
            flags: reserved for future use

        Return:
            int: 0 on success
        """
   
        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _loadId = self.prepareStringArgument(loadId)
        _recordId = self.prepareStringArgument(recordId)
        _jsonData = self.prepareStringArgument(jsonData)
        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_addRecordWithInfo.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_addRecordWithInfo(_dataSourceCode,_recordId,_jsonData,_loadId,flags,pointer(responseBuf),pointer(responseSize),self._resize_func)

        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
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
        ret_code = self._lib_handle.G2_replaceRecord(_dataSourceCode,_recordId,_jsonData,_loadId)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code

    def replaceRecordWithInfo(self,dataSourceCode,recordId,jsonData,response,loadId=None, flags=0):
        # type: (str,str,str,str) -> int
        """ Replace the JSON record, loads if doesn't exist
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            jsonData: A JSON document containing the attribute information
                   for the observation.
            response: Json document with info about the modified resolved entities
            loadID: The load ID for the record, can be null and will default to dataSourceCode
            flags: reserved for future use

        Return:
            int: 0 on success
        """

        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _loadId = self.prepareStringArgument(loadId)
        _recordId = self.prepareStringArgument(recordId)
        _jsonData = self.prepareStringArgument(jsonData)
        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_replaceRecordWithInfo.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_replaceRecordWithInfo(_dataSourceCode,_recordId,_jsonData,_loadId,flags,pointer(responseBuf),pointer(responseSize),self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
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
        ret_code = self._lib_handle.G2_deleteRecord(_dataSourceCode,_recordId,_loadId)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code

    def deleteRecordWithInfo(self,dataSourceCode,recordId,response,loadId=None, flags=0):
        # type: (str,str,str,str,int) -> int
        """ Delete the record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here
            loadID: The load ID for the record, can be null and will default to dataSourceCode
            flags: reserved for future use

        Return:
            int: 0 on success
        """

        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _loadId = self.prepareStringArgument(loadId)
        _recordId = self.prepareStringArgument(recordId)
        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_deleteRecordWithInfo.argtypes = [c_char_p, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_deleteRecordWithInfo(_dataSourceCode,_recordId,_loadId,flags,pointer(responseBuf),pointer(responseSize),self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code

    def reevaluateRecord(self,dataSourceCode,recordId,flags):
        # type: (str,str,int) -> int
        """ Reevaluate the JSON record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            flags: Bitwise control flags

        Return:
            int: 0 on success
        """
   
        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _recordId = self.prepareStringArgument(recordId)
        ret_code = self._lib_handle.G2_reevaluateRecord(_dataSourceCode,_recordId,flags)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code

    def reevaluateRecordWithInfo(self,dataSourceCode,recordId,response,flags=0):
        # type: (str,str,str,int) -> int
        """ Reevaluate the JSON record and return modified resolved entities
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            response: json document with modified resolved entities
            flags: Bitwise control flags

        Return:
            int: 0 on success
        """
   
        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        _recordId = self.prepareStringArgument(recordId)
        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_reevaluateRecordWithInfo.argtypes = [c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_reevaluateRecordWithInfo(_dataSourceCode,_recordId,flags,pointer(responseBuf),pointer(responseSize),self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code


    def reevaluateEntity(self,entityID,flags):
        # type: (int,int) -> int
        """ Reevaluate the JSON record
        Args:
            entityID: The entity ID to reevaluate.
            flags: Bitwise control flags

        Return:
            int: 0 on success
        """
   
        ret_code = self._lib_handle.G2_reevaluateEntity(entityID,flags)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return ret_code
    
    def reevaluateEntityWithInfo(self,entityID,response,flags=0):
        # type: (int,int,str) -> int
        """ Reevaluate the JSON record and return the modified resolved entities
        Args:
            entityID: The entity ID to reevaluate.
            
            response: json document with modified resolved entities
            flags: Bitwise control flags

        Return:
            int: 0 on success
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_reevaluateEntityWithInfo.argtypes = [c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_reevaluateEntityWithInfo(entityID,flags,pointer(responseBuf),pointer(responseSize),self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code

    def searchByAttributes(self,jsonData,response):
        # type: (str,bytearray) -> int
        """ Find records matching the provided attributes
        Args:
            jsonData: A JSON document containing the attribute information to search.
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.
        Return:
            int: 0 upon success, other for error.
        """
        response[::]=b''
        _jsonData = self.prepareStringArgument(jsonData)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_searchByAttributes.argtypes = [c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_searchByAttributes(_jsonData,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def searchByAttributesV2(self,jsonData,flags,response):
        # type: (str,bytearray) -> int
        """ Find records matching the provided attributes
        Args:
            jsonData: A JSON document containing the attribute information to search.
            flags: control flags.
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.
        Return:
            int: 0 upon success, other for error.
        """
        response[::]=b''
        _jsonData = self.prepareStringArgument(jsonData)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_searchByAttributes_V2.argtypes = [c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_searchByAttributes_V2(_jsonData,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathByEntityID(self,startEntityID,endEntityID,maxDegree,response):
        # type: (int) -> str
        """ Find a path between two entities in the system.
        Args:
            startEntityID: The entity ID you want to find the path from
            endEntityID: The entity ID you want to find the path to
            maxDegree: The maximum path length to search for
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathByEntityID.restype = c_int
        self._lib_handle.G2_findPathByEntityID.argtypes = [c_longlong, c_longlong, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathByEntityID(startEntityID,endEntityID,maxDegree,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathByEntityIDV2(self,startEntityID,endEntityID,maxDegree,flags,response):
        # type: (int) -> str
        """ Find a path between two entities in the system.
        Args:
            startEntityID: The entity ID you want to find the path from
            endEntityID: The entity ID you want to find the path to
            maxDegree: The maximum path length to search for
            flags: control flags.
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathByEntityID_V2.restype = c_int
        self._lib_handle.G2_findPathByEntityID_V2.argtypes = [c_longlong, c_longlong, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathByEntityID_V2(startEntityID,endEntityID,maxDegree,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findNetworkByEntityID(self,entityList,maxDegree,buildOutDegree,maxEntities,response):
        # type: (int) -> str
        """ Find a network between entities in the system.
        Args:
            entityList: The entities to search for the network of
            maxDegree: The maximum path length to search for between entities
            buildOutDegree: The number of degrees to build out the surrounding network
            maxEntities: The maximum number of entities to include in the result
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _entityList = self.prepareStringArgument(entityList)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findNetworkByEntityID.restype = c_int
        self._lib_handle.G2_findNetworkByEntityID.argtypes = [c_char_p, c_int, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findNetworkByEntityID(_entityList,maxDegree,buildOutDegree,maxEntities,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findNetworkByEntityIDV2(self,entityList,maxDegree,buildOutDegree,maxEntities,flags,response):
        # type: (int) -> str
        """ Find a network between entities in the system.
        Args:
            entityList: The entities to search for the network of
            maxDegree: The maximum path length to search for between entities
            buildOutDegree: The number of degrees to build out the surrounding network
            maxEntities: The maximum number of entities to include in the result
            flags: control flags.
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _entityList = self.prepareStringArgument(entityList)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findNetworkByEntityID_V2.restype = c_int
        self._lib_handle.G2_findNetworkByEntityID_V2.argtypes = [c_char_p, c_int, c_int, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findNetworkByEntityID_V2(_entityList,maxDegree,buildOutDegree,maxEntities,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathByRecordID(self,startDsrcCode,startRecordId,endDsrcCode,endRecordId,maxDegree,response):
        # type: (str,str) -> str
        """ Find a path between two records in the system.
        Args:
            startDataSourceCode: The data source for the record you want to find the path from
            startRecordID: The ID for the record you want to find the path from
            endDataSourceCode: The data source for the record you want to find the path to
            endRecordID: The ID for the record you want to find the path to
            maxDegree: The maximum path length to search for
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _startDsrcCode = self.prepareStringArgument(startDsrcCode)
        _startRecordId = self.prepareStringArgument(startRecordId)
        _endDsrcCode = self.prepareStringArgument(endDsrcCode)
        _endRecordId = self.prepareStringArgument(endRecordId)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathByRecordID.restype = c_int
        self._lib_handle.G2_findPathByRecordID.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathByRecordID(_startDsrcCode,_startRecordId,_endDsrcCode,_endRecordId,maxDegree,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathByRecordIDV2(self,startDsrcCode,startRecordId,endDsrcCode,endRecordId,maxDegree,flags,response):
        # type: (str,str) -> str
        """ Find a path between two records in the system.
        Args:
            startDataSourceCode: The data source for the record you want to find the path from
            startRecordID: The ID for the record you want to find the path from
            endDataSourceCode: The data source for the record you want to find the path to
            endRecordID: The ID for the record you want to find the path to
            maxDegree: The maximum path length to search for
            flags: control flags.
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _startDsrcCode = self.prepareStringArgument(startDsrcCode)
        _startRecordId = self.prepareStringArgument(startRecordId)
        _endDsrcCode = self.prepareStringArgument(endDsrcCode)
        _endRecordId = self.prepareStringArgument(endRecordId)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathByRecordID_V2.restype = c_int
        self._lib_handle.G2_findPathByRecordID_V2.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathByRecordID_V2(_startDsrcCode,_startRecordId,_endDsrcCode,_endRecordId,maxDegree,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findNetworkByRecordID(self,recordList,maxDegree,buildOutDegree,maxEntities,response):
        # type: (str,str) -> str
        """ Find a network between entities in the system.
        Args:
            recordList: The records to search for the network of
            maxDegree: The maximum path length to search for between entities
            buildOutDegree: The number of degrees to build out the surrounding network
            maxEntities: The maximum number of entities to include in the result
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _recordList = self.prepareStringArgument(recordList)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findNetworkByRecordID.restype = c_int
        self._lib_handle.G2_findNetworkByRecordID.argtypes = [c_char_p, c_int, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findNetworkByRecordID(_recordList,maxDegree,buildOutDegree,maxEntities,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findNetworkByRecordIDV2(self,recordList,maxDegree,buildOutDegree,maxEntities,flags,response):
        # type: (str,str) -> str
        """ Find a network between entities in the system.
        Args:
            recordList: The records to search for the network of
            maxDegree: The maximum path length to search for between entities
            buildOutDegree: The number of degrees to build out the surrounding network
            maxEntities: The maximum number of entities to include in the result
            flags: control flags.
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _recordList = self.prepareStringArgument(recordList)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findNetworkByRecordID_V2.restype = c_int
        self._lib_handle.G2_findNetworkByRecordID_V2.argtypes = [c_char_p, c_int, c_int, c_int, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findNetworkByRecordID_V2(_recordList,maxDegree,buildOutDegree,maxEntities,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathExcludingByEntityID(self,startEntityID,endEntityID,maxDegree,excludedEntities,flags,response):
        # type: (int) -> str
        """ Find a path between two entities in the system.
        Args:
            startEntityID: The entity ID you want to find the path from
            endEntityID: The entity ID you want to find the path to
            maxDegree: The maximum path length to search for
            excludedEntities: JSON document containing entities to exclude
            flags: control flags
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _excludedEntities = self.prepareStringArgument(excludedEntities)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathExcludingByEntityID.restype = c_int
        self._lib_handle.G2_findPathExcludingByEntityID.argtypes = [c_longlong, c_longlong, c_int, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathExcludingByEntityID(startEntityID,endEntityID,maxDegree,_excludedEntities,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathIncludingSourceByEntityID(self,startEntityID,endEntityID,maxDegree,excludedEntities,requiredDsrcs,flags,response):
        # type: (int) -> str
        """ Find a path between two entities in the system.
        Args:
            startEntityID: The entity ID you want to find the path from
            endEntityID: The entity ID you want to find the path to
            maxDegree: The maximum path length to search for
            excludedEntities: JSON document containing entities to exclude
            requiredDsrcs: JSON document containing data sources to require
            flags: control flags
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _excludedEntities = self.prepareStringArgument(excludedEntities)
        _requiredDsrcs = self.prepareStringArgument(requiredDsrcs)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathIncludingSourceByEntityID.restype = c_int
        self._lib_handle.G2_findPathIncludingSourceByEntityID.argtypes = [c_longlong, c_longlong, c_int, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathIncludingSourceByEntityID(startEntityID,endEntityID,maxDegree,_excludedEntities,_requiredDsrcs,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathExcludingByRecordID(self,startDsrcCode,startRecordId,endDsrcCode,endRecordId,maxDegree,excludedEntities,flags,response):
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
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _startDsrcCode = self.prepareStringArgument(startDsrcCode)
        _startRecordId = self.prepareStringArgument(startRecordId)
        _endDsrcCode = self.prepareStringArgument(endDsrcCode)
        _endRecordId = self.prepareStringArgument(endRecordId)
        _excludedEntities = self.prepareStringArgument(excludedEntities)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathExcludingByRecordID.restype = c_int
        self._lib_handle.G2_findPathExcludingByRecordID.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathExcludingByRecordID(_startDsrcCode,_startRecordId,_endDsrcCode,_endRecordId,maxDegree,
                                                                 _excludedEntities,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def findPathIncludingSourceByRecordID(self,startDsrcCode,startRecordId,endDsrcCode,endRecordId,maxDegree,excludedEntities,requiredDsrcs,flags,response):
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
            response: A bytearray for returning the response document.
        """

        response[::]=b''
        _startDsrcCode = self.prepareStringArgument(startDsrcCode)
        _startRecordId = self.prepareStringArgument(startRecordId)
        _endDsrcCode = self.prepareStringArgument(endDsrcCode)
        _endRecordId = self.prepareStringArgument(endRecordId)
        _excludedEntities = self.prepareStringArgument(excludedEntities)
        _requiredDsrcs = self.prepareStringArgument(requiredDsrcs)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_findPathIncludingSourceByRecordID.restype = c_int
        self._lib_handle.G2_findPathIncludingSourceByRecordID.argtypes = [c_char_p, c_char_p, c_char_p, c_char_p, c_int, c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_findPathIncludingSourceByRecordID(_startDsrcCode,_startRecordId,_endDsrcCode,_endRecordId,maxDegree,
                                                                 _excludedEntities,_requiredDsrcs,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def getEntityByEntityID(self,entityID,response):
        # type: (int,bytearray) -> int
        """ Find the entity with the given ID
        Args:
            entityID: The entity ID you want returned.  Typically referred to as
                      ENTITY_ID in JSON results.
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_getEntityByEntityID.argtypes = [c_longlong, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getEntityByEntityID(entityID,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code

    def getEntityByEntityIDV2(self,entityID,flags,response):
        # type: (int,bytearray) -> int
        """ Find the entity with the given ID
        Args:
            entityID: The entity ID you want returned.  Typically referred to as
                      ENTITY_ID in JSON results.
            flags: control flags.
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_getEntityByEntityID_V2.restype = c_int
        self._lib_handle.G2_getEntityByEntityID_V2.argtypes = [c_longlong, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getEntityByEntityID_V2(entityID,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code

    def getEntityByRecordID(self,dsrcCode,recordId,response):
        # type: (str,str,bytearray) -> int
        """ Get the entity containing the specified record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        _dsrcCode = self.prepareStringArgument(dsrcCode)
        _recordId = self.prepareStringArgument(recordId)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_getEntityByRecordID.argtypes = [c_char_p, c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getEntityByRecordID(_dsrcCode,_recordId,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def getEntityByRecordIDV2(self,dsrcCode,recordId,flags,response):
        # type: (str,str,bytearray) -> int
        """ Get the entity containing the specified record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            flags: control flags.
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        _dsrcCode = self.prepareStringArgument(dsrcCode)
        _recordId = self.prepareStringArgument(recordId)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_getEntityByRecordID_V2.restype = c_int
        self._lib_handle.G2_getEntityByRecordID_V2.argtypes = [c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getEntityByRecordID_V2(_dsrcCode,_recordId,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def getRedoRecord(self,response):
        # type: (bytearray) -> int
        """ Get the next Redo record
        Args:
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_getRedoRecord.restype = c_int
        self._lib_handle.G2_getRedoRecord.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getRedoRecord(pointer(responseBuf),
                                                     pointer(responseSize),
                                                     self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += responseBuf.value
        return ret_code

    def processRedoRecord(self,response):
        # type: (bytearray) -> int
        """ Process the next Redo record
        Args:
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.  response is empty if there was nothing to process.
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_processRedoRecord.restype = c_int
        self._lib_handle.G2_processRedoRecord.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_processRedoRecord(pointer(responseBuf),
                                                         pointer(responseSize),
                                                         self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += responseBuf.value
        return ret_code

    def processRedoRecordWithInfo(self, response, info, flags=0):
        # type: (bytearray) -> int
        """ Process the next Redo record
        Args:
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.
            response: A bytearray for returning the info about changed resolved entities

        Return:
            int: 0 upon success, other for error.  response is empty if there was nothing to process.
        """

        response[::]=b''
        info[::]=b''
        responseBuf = c_char_p(self.tls_var2.buf)
        infoBuf = c_char_p(self.info_buf.buf)

        self._lib_handle.G2_processRedoRecordWithInfo.restype = c_int
        self._lib_handle.G2_processRedoRecordWithInfo.argtypes = [c_int,POINTER(c_char_p), POINTER(c_size_t),POINTER(c_char_p), POINTER(c_size_t),self._resize_func_def2]
        ret_code = self._lib_handle.G2_processRedoRecordWithInfo(flags,
                                                                 pointer(responseBuf),
                                                                 pointer(self.tls_var2.bufSize),
                                                                 pointer(infoBuf),
                                                                 pointer(self.info_buf.bufSize),
                                                                 self._resize_func2)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += responseBuf.value
        info += infoBuf.value
        return ret_code

    def countRedoRecords(self):
        # type: () -> int
        """ Get the redo records left
        Args:

        Return:
            int: >=0 upon success, other for error.
        """

        ret_code = self._lib_handle.G2_countRedoRecords()
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        return ret_code

    def getRecord(self,dsrcCode,recordId,response):
        # type: (str,str,bytearray) -> int
        """ Get the specified record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        _dsrcCode = self.prepareStringArgument(dsrcCode)
        _recordId = self.prepareStringArgument(recordId)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_getRecord.argtypes = [c_char_p, c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getRecord(_dsrcCode,_recordId,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def getRecordV2(self,dsrcCode,recordId,flags,response):
        # type: (str,str,bytearray) -> int
        """ Get the specified record
        Args:
            dataSourceCode: The data source for the observation.
            recordID: The ID for the record
            flags: control flags.
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        _dsrcCode = self.prepareStringArgument(dsrcCode)
        _recordId = self.prepareStringArgument(recordId)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_getRecord_V2.restype = c_int
        self._lib_handle.G2_getRecord_V2.argtypes = [c_char_p, c_char_p, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_getRecord_V2(_dsrcCode,_recordId,flags,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        response += tls_var.buf.value
        return ret_code

    def stats(self,response):
        # type: () -> object
        """ Retrieve the workload statistics for the current process.
        Resets them after retrieved.

        Args:

        Return:
            object: JSON document with statistics
        """

        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_stats.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_stats(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')

        response += tls_var.buf.value
        return ret_code

    
    def exportConfig(self,response,configID):
        # type: (bytearray) -> int
        """ Retrieve the G2 engine configuration

        Args:
            response: A bytearray for returning the response document; if an error occurred, an error response is stored here.

        Return:
            int: 0 upon success, other for error.
        """

        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2_exportConfig.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2_exportConfig(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        response += tls_var.buf.value

        if type(configID) == bytearray:
            configID[::]=b''
            cID = c_longlong(0)
            self._lib_handle.G2_getActiveConfigID.argtypes = [POINTER(c_longlong)]
            ret_code2 = self._lib_handle.G2_getActiveConfigID(cID)
            if ret_code2 == -2:
                self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
                raise TranslateG2ModuleException(tls_var.buf.value)
            elif ret_code2 < 0:
                raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code2))
            configID += (str(cID.value).encode())
        else:
            ret_code2 = 0
        return min(ret_code, ret_code2)


    def getActiveConfigID(self, configID):
        # type: (bytearray) -> object
        """ Retrieve the active config ID for the G2 engine

        Args:
            configID: A bytearray for returning the identifier value for the config

        Return:
            int: 0 upon success, other for error.
        """

        configID[::]=b''
        cID = c_longlong(0)
        self._lib_handle.G2_getActiveConfigID.argtypes = [POINTER(c_longlong)]
        ret_code = self._lib_handle.G2_getActiveConfigID(cID)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        configID += (str(cID.value).encode())
        return ret_code


    def getRepositoryLastModifiedTime(self, lastModifiedTime):
        # type: (bytearray) -> object
        """ Retrieve the last modified time stamp of the entity store repository

        Args:
            lastModifiedTime: A bytearray for returning the last modified time of the data repository

        Return:
            int: 0 upon success, other for error.
        """

        lastModifiedTime[::]=b''
        lastModifiedTimeStamp = c_longlong(0)
        self._lib_handle.G2_getRepositoryLastModifiedTime.argtypes = [POINTER(c_longlong)]
        ret_code = self._lib_handle.G2_getRepositoryLastModifiedTime(lastModifiedTimeStamp)
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        lastModifiedTime += (str(lastModifiedTimeStamp.value).encode())
        return ret_code

    def purgeRepository(self, reset_resolver_=True):
        # type: (bool) -> None
        """ Purges the G2 repository

        Args:
            reset_resolver: Re-initializes the engine.  Should be left True.

        Return:
            None
        """

        ret_code = self._lib_handle.G2_purgeRepository()
        if ret_code == -2:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Engine has not been succesfully initialized')


    def destroy(self):
        """ Uninitializes the engine
        This should be done once per process after init(...) is called.
        After it is called the engine will no longer function.

        Args:

        Return:
            None
        """

        return self._lib_handle.G2_destroy()

