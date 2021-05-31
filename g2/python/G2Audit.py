from ctypes import *
import threading
import json
import os

tls_var = threading.local()

from csv import reader as csvreader

from G2Exception import TranslateG2ModuleException, G2ModuleNotInitialized, G2ModuleGenericException

def resize_return_buffer(buf_, size_):
  """  callback function that resizes return buffer when it is too small
  Args:
  size_: size the return buffer needs to be
  """
  try:
    if (sizeof(tls_var.buf) < size_) :
      tls_var.buf = create_string_buffer(size_)
  except AttributeError:
      tls_var.buf = create_string_buffer(size_)
  return addressof(tls_var.buf)


class G2Audit(object):
    """G2 audit module access library

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

        self._module_name = module_name_
        self._ini_file_name = ini_file_name_
        self._debug = debug_

        if self._debug:
            print("Initializing G2 audit module")

        resize_return_buffer(None, 65535)

        self._lib_handle.G2Audit_init.argtypes = [c_char_p, c_char_p, c_int]
        retval = self._lib_handle.G2Audit_init(self._module_name.encode('utf-8'),
                                 self._ini_file_name.encode('utf-8'),
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(retval))

        if retval == -2:
            self._lib_handle.G2Audit_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif retval == -1:
            raise G2ModuleNotInitialized('G2Audit has not been succesfully initialized')
        elif retval < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Module")
        return retval


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

    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Audit_clearLastException.restype = None
        self._lib_handle.G2Audit_clearLastException.argtypes = []
        self._lib_handle.G2Audit_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Audit_getLastException.restype = c_int
        self._lib_handle.G2Audit_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2Audit_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Audit_getLastExceptionCode.restype = c_int
        self._lib_handle.G2Audit_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2Audit_getLastExceptionCode()
        return exception_code

    def openSession(self):
        sessionHandle = self._lib_handle.G2Audit_openSession()
        return sessionHandle

    def cancelSession(self, sessionHandle):
        self._lib_handle.G2Audit_cancelSession(sessionHandle)

    def closeSession(self, sessionHandle):
        self._lib_handle.G2Audit_closeSession(sessionHandle)


    def getSummaryData(self, sessionHandle, response):
        # type: () -> object
        """ Get the summary data for the G2 data repository.
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Audit_openSession.restype = c_void_p
        sessionHandle = self._lib_handle.G2Audit_openSession()
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
            raise G2ModuleNotInitialized('G2Module has not been succesfully initialized')

        stringRet = str(responseBuf.value.decode('utf-8'))
        for i in stringRet:
            response.append(i)
        return ret_code

    def getSummaryDataDirect(self, response):
        # type: () -> object
        """ Get the summary data for the G2 data repository, with optimizations.
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
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
            raise G2ModuleNotInitialized('G2Module has not been succesfully initialized')

        stringRet = str(responseBuf.value.decode('utf-8'))
        for i in stringRet:
            response.append(i)
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
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
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
        stringRet = str(responseBuf.value.decode('utf-8'))
        for i in stringRet:
            response.append(i)
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
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
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
        stringRet = str(responseBuf.value.decode('utf-8'))
        for i in stringRet:
            response.append(i)
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
        return reportHandle

    def fetchNext(self,reportHandle):
        resultString = b""
        if reportHandle == None:
            self._lib_handle.G2_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        rowCount = 0
        resize_return_buffer(None,65535)
        self._lib_handle.G2Audit_fetchNext.argtypes = [c_void_p, c_char_p, c_size_t]
        rowData = self._lib_handle.G2Audit_fetchNext(c_void_p(reportHandle),tls_var.buf,sizeof(tls_var.buf))

        while rowData:
            rowCount = rowCount + 1
            stringData = tls_var.buf
            resultString += stringData.value
            rowData = self._lib_handle.G2Audit_fetchNext(c_void_p(reportHandle),tls_var.buf,sizeof(tls_var.buf))
        return resultString.decode('utf-8')

    def closeReport(self, reportHandle):
        self._lib_handle.G2Audit_closeReport(c_void_p(reportHandle))

    def restart(self):
        """  Internal function """
        moduleName = self._engine_name
        iniFilename = self._ini_file_name
        self.destroy()
        self.init(moduleName, iniFilename, False)

    def destroy(self):
        """ Uninitializes the engine
        This should be done once per process after init(...) is called.
        After it is called the engine will no longer function.

        Args:

        Return:
            None
        """

        return self._lib_handle.G2Audit_destroy()

