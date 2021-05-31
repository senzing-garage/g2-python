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


class G2ConfigMgr(object):
    """G2 config-manager module access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_params: a JSON string containing INI parameters
    """
    def initV2(self, module_name_, ini_params_, debug_=False):
        """  Initializes the G2 config manager
        This should only be called once per process.
        Args:
            moduleName: A short name given to this instance of the config module
            iniParams: A json document that contains G2 system parameters.
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout
        Returns:
            int: 0 on success
        """
        self._module_name = module_name_
        self._ini_params = ini_params_
        self._debug = debug_

        if self._debug:
            print("Initializing G2 Config Manager")

        resize_return_buffer(None, 65535)

        self._lib_handle.G2ConfigMgr_init_V2.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2ConfigMgr_init_V2(self._module_name.encode('utf-8'),
                                 self._ini_params.encode('utf-8'),
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -2:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Config Manager")
        return ret_code

    def __init__(self):
        # type: () -> None
        """ G2ConfigModule class initialization
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


    def addConfig(self, configStr, configComments, configID):
        """ registers a new configuration document in the datastore
        """
        _configStr = self.prepareStringArgument(configStr)
        _configComments = self.prepareStringArgument(configComments)
        configID[::]=b''
        cID = c_longlong(0)
        self._lib_handle.G2ConfigMgr_addConfig.argtypes = [c_char_p, c_char_p, POINTER(c_longlong)]
        self._lib_handle.G2ConfigMgr_addConfig.restype = c_int
        ret_code = self._lib_handle.G2ConfigMgr_addConfig(_configStr,_configComments,cID)

        if ret_code == -2:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been succesfully initialized')
        configID += (str(cID.value).encode())
        return ret_code

    def getConfig(self,configID,response):
        """ retrieves the registered configuration document from the datastore
        """
        response[::]=b''
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2ConfigMgr_getConfig.restype = c_int
        self._lib_handle.G2ConfigMgr_getConfig.argtypes = [c_longlong, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2ConfigMgr_getConfig(configID,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been succesfully initialized')

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code

    def getConfigList(self,response):
        """ retrieves a list of known configurations from the datastore
        """
        response[::]=b''
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2ConfigMgr_getConfigList.restype = c_int
        self._lib_handle.G2ConfigMgr_getConfigList.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2ConfigMgr_getConfigList(
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been succesfully initialized')

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

        #Return the RC 
        return ret_code

    def setDefaultConfigID(self,configID):
        """ sets the default config identifier in the datastore
        """
        self._lib_handle.G2ConfigMgr_setDefaultConfigID.restype = c_int
        self._lib_handle.G2ConfigMgr_setDefaultConfigID.argtypes = [c_longlong]
        ret_code = self._lib_handle.G2ConfigMgr_setDefaultConfigID(configID)
        if ret_code == -2:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been succesfully initialized')

        return ret_code

    def getDefaultConfigID(self, configID):
        """ gets the default config identifier from the datastore
        """
        configID[::]=b''
        cID = c_longlong(0)
        self._lib_handle.G2ConfigMgr_getDefaultConfigID.argtypes = [POINTER(c_longlong)]
        self._lib_handle.G2ConfigMgr_getDefaultConfigID.restype = c_int
        ret_code = self._lib_handle.G2ConfigMgr_getDefaultConfigID(cID)

        if ret_code == -2:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been succesfully initialized')
        configID += (str(cID.value).encode())
        return ret_code


    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2ConfigMgr_clearLastException.restype = None
        self._lib_handle.G2ConfigMgr_clearLastException.argtypes = []
        self._lib_handle.G2ConfigMgr_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2ConfigMgr_getLastException.restype = c_int
        self._lib_handle.G2ConfigMgr_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2ConfigMgr_getLastExceptionCode.restype = c_int
        self._lib_handle.G2ConfigMgr_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2ConfigMgr_getLastExceptionCode()
        return exception_code


    def destroy(self):
        """ Uninitializes the engine
        This should be done once per process after init(...) is called.
        After it is called the engine will no longer function.

        Args:

        Return:
            None
        """

        return self._lib_handle.G2ConfigMgr_destroy()

