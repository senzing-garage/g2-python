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


class G2Diagnostic(object):
    """G2 diagnostic module access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_file_name: name and location of .ini file
    """
    def init(self, module_name_, ini_file_name_, debug_=False):
        """  Initializes the G2 diagnostic module engine
        This should only be called once per process.
        Args:
            moduleName: A short name given to this instance of the diagnostic module
            iniFilename: A fully qualified path to the G2 engine INI file (often /opt/senzing/g2/python/G2Module.ini)
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout
        Returns:
            int: 0 on success
        """

        self._module_name = module_name_
        self._ini_file_name = ini_file_name_
        self._debug = debug_

        if self._debug:
            print("Initializing G2 diagnostic module")

        resize_return_buffer(None, 65535)

        self._lib_handle.G2Diagnostic_init.argtypes = [c_char_p, c_char_p, c_int]
        retval = self._lib_handle.G2Diagnostic_init(self._module_name.encode('utf-8'),
                                 self._ini_file_name.encode('utf-8'),
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(retval))

        if retval == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif retval == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')
        elif retval < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Diagnostic")
        return retval


    def __init__(self):
        # type: (str, str, bool) -> None
        """ G2Diagnostic class initialization
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

    def prepareBooleanArgument(self, booleanToPrepare):
        booleanValue = 0
        if booleanToPrepare:
            booleanValue = 1
        return booleanToPrepare

    def getEntityDetails(self,entityID,includeDerivedFeatures):
        # type: (int) -> str
        """ Get the details for the resolved entity
        Args:
            entityID: The entity ID to get results for
            includeDerivedFeatures: boolean value indicating whether to include derived features

        Return:
            str: JSON document with results
        """

        _includeDerivedFeatures = self.prepareBooleanArgument(includeDerivedFeatures);
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getEntityDetails.argtypes = [c_longlong, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getEntityDetails(entityID, _includeDerivedFeatures,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')
        return tls_var.buf.value.decode('utf-8')

    def getRelationshipDetails(self,relationshipID,includeDerivedFeatures):
        # type: (int) -> str
        """ Get the details for the resolved entity relationship
        Args:
            relationshipID: The relationshp ID to get results for
            includeDerivedFeatures: boolean value indicating whether to include derived features

        Return:
            str: JSON document with results
        """

        _includeDerivedFeatures = self.prepareBooleanArgument(includeDerivedFeatures);
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getRelationshipDetails.argtypes = [c_longlong, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getRelationshipDetails(relationshipID, _includeDerivedFeatures,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')
        return tls_var.buf.value.decode('utf-8')

    def getEntityResume(self,entityID):
        # type: (int) -> str
        """ Get the related records for the resolved entity
        Args:
            entityID: The entity ID to get results for

        Return:
            str: JSON document with results
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getEntityResume.argtypes = [c_longlong, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getEntityResume(entityID,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')
        return tls_var.buf.value.decode('utf-8')

    def getEntityListBySize(self,entitySize):
        # type: (str,str,int) -> str
        """ Generate a list of resolved entities of a particular size
   
        Args:
            entitySize: The size of the resolved entity (observed entity count)

        Return:
            str: string of several JSON documents with results
        """
        resultString = b""
        _entitySize = entitySize
        self._lib_handle.G2Diagnostic_getEntityListBySize.restype = c_void_p
        sizedEntityHandle = self._lib_handle.G2Diagnostic_getEntityListBySize(_entitySize)
        if sizedEntityHandle == None:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        rowCount = 0
        resize_return_buffer(None,65535)
        self._lib_handle.G2Diagnostic_fetchNextEntityBySize.argtypes = [c_void_p, c_char_p, c_size_t]
        rowData = self._lib_handle.G2Diagnostic_fetchNextEntityBySize(c_void_p(sizedEntityHandle),tls_var.buf,sizeof(tls_var.buf))

        while rowData:
            rowCount = rowCount + 1
            stringData = tls_var.buf
            resultString += stringData.value
            rowData = self._lib_handle.G2Diagnostic_fetchNextEntityBySize(c_void_p(sizedEntityHandle),tls_var.buf,sizeof(tls_var.buf))
        self._lib_handle.G2Diagnostic_closeEntityListBySize(c_void_p(sizedEntityHandle))
        return resultString.decode('utf-8')

    def checkDBPerf(self,secondsToRun):
        # type: () -> object,int
        """ Retrieve JSON of DB performance test

        Return:
            object: JSON document with results
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_checkDBPerf.argtypes = [c_int,POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_checkDBPerf(secondsToRun,pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def getDataSourceCounts(self):
        # type: () -> object
        """ Retrieve record counts by data source and entity type.

        Return:
            object: JSON document with results
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getDataSourceCounts.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getDataSourceCounts(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def getMappingStatistics(self,includeDerivedFeatures):
        # type: () -> object
        """ Retrieve data source mapping statistics.
        Args:
            includeDerivedFeatures: boolean value indicating whether to include derived features

        Return:
            object: JSON document with results
        """

        _includeDerivedFeatures = self.prepareBooleanArgument(includeDerivedFeatures);
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getMappingStatistics.argtypes = [c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getMappingStatistics(
                                             _includeDerivedFeatures,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def getGenericFeatures(self,featureType,maximumEstimatedCount):
        # type: () -> object
        """ Retrieve generic features.
        Args:
            featureType: the feature type to find generics for
            maximumEstimatedCount: the maximum estimated count for the generics to find

        Return:
            object: JSON document with results
        """

        _featureType = self.prepareStringArgument(featureType)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getGenericFeatures.argtypes = [ c_char_p, c_size_t, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getGenericFeatures(
                                             _featureType,
                                             maximumEstimatedCount,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def getEntitySizeBreakdown(self,minimumEntitySize,includeDerivedFeatures):
        # type: () -> object
        """ Retrieve data source mapping statistics.
        Args:
            minimumEntitySize: the minimum entity size to report on
            includeDerivedFeatures: boolean value indicating whether to include derived features

        Return:
            object: JSON document with results
        """

        _includeDerivedFeatures = self.prepareBooleanArgument(includeDerivedFeatures);
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getEntitySizeBreakdown.argtypes = [c_size_t, c_int, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getEntitySizeBreakdown(
                                             minimumEntitySize,
                                             _includeDerivedFeatures,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')

    def getResolutionStatistics(self):
        # type: () -> object
        """ Retrieve resolution statistics.

        Return:
            object: JSON document with results
        """

        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Diagnostic_getResolutionStatistics.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Diagnostic_getResolutionStatistics(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Diagnostic_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Diagnostic has not been succesfully initialized')

        return responseBuf.value.decode('utf-8')




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

        return self._lib_handle.G2Diagnostic_destroy()

    def getPhysicalCores(self):
        # type: () -> object
        """ Retrieve number of physical CPU cores

        Return:
            int: number of cores
        """

        self._lib_handle.G2Diagnostic_getPhysicalCores.argtypes = []
        return self._lib_handle.G2Diagnostic_getPhysicalCores()

    def getLogicalCores(self):
        # type: () -> object
        """ Retrieve number of logical CPU cores

        Return:
            int: number of cores
        """

        self._lib_handle.G2Diagnostic_getLogicalCores.argtypes = []
        return self._lib_handle.G2Diagnostic_getLogicalCores()

    def getTotalSystemMemory(self):
        # type: () -> object
        """ Retrieve total system memory

        Return:
            int: number of bytes
        """

        self._lib_handle.G2Diagnostic_getTotalSystemMemory.argtypes = []
        self._lib_handle.G2Diagnostic_getTotalSystemMemory.restype = c_longlong
        return self._lib_handle.G2Diagnostic_getTotalSystemMemory()

    def getAvailableMemory(self):
        # type: () -> object
        """ Retrieve available memory

        Return:
            int: number of bytes
        """

        self._lib_handle.G2Diagnostic_getAvailableMemory.argtypes = []
        self._lib_handle.G2Diagnostic_getAvailableMemory.restype = c_longlong
        return self._lib_handle.G2Diagnostic_getAvailableMemory()

    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Diagnostic_clearLastException.restype = None
        self._lib_handle.G2Diagnostic_clearLastException.argtypes = []
        self._lib_handle.G2Diagnostic_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Diagnostic_getLastException.restype = c_int
        self._lib_handle.G2Diagnostic_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2Diagnostic_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Diagnostic_getLastExceptionCode.restype = c_int
        self._lib_handle.G2Diagnostic_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2Diagnostic_getLastExceptionCode()
        return exception_code
  


