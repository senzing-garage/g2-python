from ctypes import *
import threading
import json
import os

class MyBuffer(threading.local):
  def __init__(self):
    self.buf = create_string_buffer(65535)
    self.bufSize = sizeof(self.buf)
    #print("Created new Buffer {}".format(self.buf))

tls_var = MyBuffer()

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
  


class G2Audit(object):
    """G2 audit access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_file_name: name and location of .ini file
    """
    def init(self, module_name_, ini_file_name_, debug_=False):
        """  Initializes the G2 audit module engine
        This should only be called once per process.
        Args:
            moduleName: A short name given to this instance of the audit module
            iniFilename: A fully qualified path to the G2 engine INI file (often /opt/senzing/g2/python/G2Module.ini)
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout
        Returns:
            int: 0 on success
        """

        self._module_name = self.prepareStringArgument(module_name_)
        self._ini_file_name = self.prepareStringArgument(ini_file_name_)
        self._debug = debug_

        if self._debug:
            print("Initializing G2 audit module")

        self._lib_handle.G2Audit_init.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2Audit_init(self._module_name,
                                 self._ini_file_name,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Audit")
        return ret_code


    def initV2(self, module_name_, ini_params_, debug_=False):

        self._module_name = self.prepareStringArgument(module_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._debug = debug_

        if self._debug:
            print("Initializing G2 audit module")

        self._lib_handle.G2Audit_init_V2.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2Audit_init_V2(self._module_name,
                                 self._ini_params,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Audit")
        return ret_code


    def initWithConfigIDV2(self, engine_name_, ini_params_, initConfigID_, debug_):

        configIDValue = self.prepareIntArgument(initConfigID_)

        self._engine_name = self.prepareStringArgument(engine_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._debug = debug_
        if self._debug:
            print("Initializing G2 audit module")

        self._lib_handle.G2Audit_initWithConfigID_V2.argtypes = [ c_char_p, c_char_p, c_longlong, c_int ]
        ret_code = self._lib_handle.G2Audit_initWithConfigID_V2(self._engine_name,
                                 self._ini_params,
                                 configIDValue,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Audit")

        return ret_code

    def reinitV2(self, initConfigID):

        configIDValue = int(self.prepareStringArgument(initConfigID))

        self._lib_handle.G2Audit_reinit_V2.argtypes = [ c_longlong ]
        ret_code = self._lib_handle.G2Audit_reinit_V2(configIDValue)

        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2 audit module has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Audit")

        return ret_code

    def __init__(self):
        # type: (str, str, bool) -> None
        """ G2AuditModule class initialization
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

    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        self._lib_handle.G2Audit_clearLastException.restype = None
        self._lib_handle.G2Audit_clearLastException.argtypes = []
        self._lib_handle.G2Audit_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        self._lib_handle.G2Audit_getLastException.restype = c_int
        self._lib_handle.G2Audit_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2Audit_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        self._lib_handle.G2Audit_getLastExceptionCode.restype = c_int
        self._lib_handle.G2Audit_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2Audit_getLastExceptionCode()
        return exception_code

    def openSession(self):
        self._lib_handle.G2Audit_openSession.restype = c_void_p
        sessionHandle = self._lib_handle.G2Audit_openSession()
        if sessionHandle == None:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        return sessionHandle

    def cancelSession(self, sessionHandle):
        self._lib_handle.G2Audit_cancelSession(sessionHandle)

    def closeSession(self, sessionHandle):
        self._lib_handle.G2Audit_closeSession(sessionHandle)


    def getSummaryData(self, sessionHandle, response):
        # type: () -> object
        """ Get the summary data for the G2 data repository.
        """

        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Audit_openSession.restype = c_void_p
        sessionHandle = self._lib_handle.G2Audit_openSession()
        if sessionHandle == None:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        self._lib_handle.G2Audit_getSummaryData.restype = c_int
        self._lib_handle.G2Audit_getSummaryData.argtypes = [c_void_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Audit_getSummaryData(sessionHandle,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        self._lib_handle.G2Audit_closeSession.argtypes = [c_void_p]
        self._lib_handle.G2Audit_closeSession(sessionHandle)
        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')

        response += responseBuf.value
        return ret_code

    def getSummaryDataDirect(self, response):
        # type: () -> object
        """ Get the summary data for the G2 data repository, with optimizations.
        """

        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Audit_getSummaryDataDirect.restype = c_int
        self._lib_handle.G2Audit_getSummaryDataDirect.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Audit_getSummaryDataDirect(
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')

        response += responseBuf.value
        return ret_code

    def getUsedMatchKeys(self,sessionHandle,fromDataSource,toDataSource,matchLevel,response):
        # type: (str,str,int) -> str
        """ Get the usage frequency of match keys
        Args:
            fromDataSource: The data source to search for matches
            toDataSource: The data source to compare against
            matchLevel: The matchLevel of matches to return

        Return:
            str: JSON document with results
        """

        _fromDataSource = self.prepareStringArgument(fromDataSource)
        _toDataSource = self.prepareStringArgument(toDataSource)
        _matchLevel = matchLevel
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Audit_getUsedMatchKeys.restype = c_int
        self._lib_handle.G2Audit_getUsedMatchKeys.argtypes = [c_void_p, c_char_p, c_char_p, c_longlong, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Audit_getUsedMatchKeys(sessionHandle,_fromDataSource,_toDataSource,_matchLevel,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')
        response += responseBuf.value
        return ret_code


    def getUsedPrinciples(self,sessionHandle,fromDataSource,toDataSource,matchLevel,response):
        # type: (str,str,int) -> str
        """ Get the usage frequency of principles
        Args:
            fromDataSource: The data source to search for matches
            toDataSource: The data source to compare against
            matchLevel: The matchLevel of matches to return

        Return:
            str: JSON document with results
        """

        _fromDataSource = self.prepareStringArgument(fromDataSource)
        _toDataSource = self.prepareStringArgument(toDataSource)
        _matchLevel = matchLevel
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Audit_getUsedPrinciples.restype = c_int
        self._lib_handle.G2Audit_getUsedPrinciples.argtypes = [c_void_p, c_char_p, c_char_p, c_longlong, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Audit_getUsedPrinciples(sessionHandle,_fromDataSource,_toDataSource,_matchLevel,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')
        response += responseBuf.value
        return ret_code


    def getAuditReport(self,sessionHandle,fromDataSource,toDataSource,matchLevel):
        # type: (str,str,int) -> str
        """ Generate an Audit Report
        This is used to get audit entity data from known entities.
   
        Args:
            fromDataSource: The data source to search for matches
            toDataSource: The data source to compare against
            match_level: The match-level to specify what kind of entity resolves
                         and relations we want to see.
                             1 -- same entities
                             2 -- possibly same entities
                             3 -- possibly related entities
                             4 -- disclosed relationships

        Return:
            str: string of several JSON documents with results
        """
        resultString = b""
        _fromDataSource = self.prepareStringArgument(fromDataSource)
        _toDataSource = self.prepareStringArgument(toDataSource)
        _matchLevel = matchLevel
        self._lib_handle.G2Audit_getAuditReport.restype = c_void_p
        reportHandle = self._lib_handle.G2Audit_getAuditReport(sessionHandle,_fromDataSource,_toDataSource,_matchLevel)
        if reportHandle == None:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        return reportHandle

    def fetchNext(self,reportHandle,response):
        if reportHandle == None:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        self._lib_handle.G2Audit_fetchNext.argtypes = [c_void_p, c_char_p, c_size_t]
        rowData = self._lib_handle.G2Audit_fetchNext(c_void_p(reportHandle),tls_var.buf,sizeof(tls_var.buf))

        while rowData:
            response += tls_var.buf.value
            if (response.decode())[-1] == '\n':
                break
            else:
                rowData = self._lib_handle.G2Audit_fetchNext(c_void_p(reportHandle),tls_var.buf,sizeof(tls_var.buf))
        return response

    def closeReport(self, reportHandle):
        self._lib_handle.G2Audit_closeReport(c_void_p(reportHandle))

    def destroy(self):
        """ Uninitializes the engine
        This should be done once per process after init(...) is called.
        After it is called the engine will no longer function.

        Args:

        Return:
            None
        """

        return self._lib_handle.G2Audit_destroy()

