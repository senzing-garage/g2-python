from ctypes import *
import threading
import json
import os

tls_var = threading.local()

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


class G2ConfigModule(object):
    """G2 config module access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_file_name: name and location of .ini file
    """
    def init(self):
        """  Initializes the G2 config module engine
        This should only be called once per process.
        Returns:
            int: 0 on success
        """

        if self._debug:
            print("Initializing G2 config module")

        resize_return_buffer(None, 65535)

        self._lib_handle.G2Config_init.argtypes = [c_char_p, c_char_p, c_int]
        retval = self._lib_handle.G2Config_init(self._module_name.encode('utf-8'),
                                 self._ini_file_name.encode('utf-8'),
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(retval))

        if retval == -2:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2Config_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif retval < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Config Module")
        return retval


    def __init__(self, module_name_, ini_file_name_, debug_=False):
        # type: (str, str, bool) -> None
        """ G2ConfigModule class initialization
        Args:
            moduleName: A short name given to this instance of the config module
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


    def create(self):
        """ Creates a new config handle from the stored template
        """
        self._lib_handle.G2Config_create.restype = c_void_p
        configHandle = self._lib_handle.G2Config_create()
        return configHandle

    def load(self,jsonConfig):
        """ Creates a new config handle from a json config string
        """
        _jsonConfig = jsonConfig
        self._lib_handle.G2Config_load.restype = c_void_p
        self._lib_handle.G2Config_load.argtypes = [c_char_p]
        configHandle = self._lib_handle.G2Config_load(_jsonConfig.encode("utf-8"))
        return configHandle

    def close(self,configHandle):
        """ Closes a config handle
        """
        self._lib_handle.G2Config_close.argtypes = [c_void_p]
        self._lib_handle.G2Config_close(configHandle)

    def save(self,configHandle):
        """ Saves a config handle
        """
        resize_return_buffer(None, 200000)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Config_save.argtypes = [c_void_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Config_save(configHandle,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)

        if ret_code == -2:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2Config_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def listDataSources(self,configHandle):
        """ lists a set of data sources
        """
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Config_listDataSources.argtypes = [c_void_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Config_listDataSources(configHandle,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)

        if ret_code == -2:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            self._lib_handle.G2Config_clearLastException()
            raise TranslateG2ModuleException(tls_var.buf.value)
        return responseBuf.value.decode('utf-8')

    def addDataSource(self,configHandle,dataSourceCode):
        """ Adds a data source
        """
        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        self._lib_handle.G2Config_addDataSource.argtypes = [c_void_p, c_char_p]
        self._lib_handle.G2Config_addDataSource(configHandle,_dataSourceCode)

    def addDataSourceWithID(self,configHandle,dataSourceCode,id):
        """ Adds a data source
        """
        _dataSourceCode = self.prepareStringArgument(dataSourceCode)
        self._lib_handle.G2Config_addDataSourceWithID.argtypes = [c_void_p, c_char_p, c_int]
        self._lib_handle.G2Config_addDataSourceWithID(configHandle,_dataSourceCode,id)

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

        self._lib_handle.G2Config_destroy()

