from ctypes import *
import threading
import json
import os
import functools
import warnings


class MyBuffer(threading.local):
  def __init__(self):
    self.buf = create_string_buffer(65535)
    self.bufSize = sizeof(self.buf)
    #print("Created new Buffer {}".format(self.buf))

tls_var = MyBuffer()

from .G2Exception import TranslateG2ModuleException, G2ModuleNotInitialized, G2ModuleGenericException

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


SENZING_PRODUCT_ID = "5027"  # See https://github.com/Senzing/knowledge-base/blob/master/lists/senzing-product-ids.md

def deprecated(instance):
    def the_decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            warnings.simplefilter('always', DeprecationWarning)  # turn off filter
            warnings.warn(
                "senzing-{0}{1:04d}W Call to deprecated function {2}.".format(SENZING_PRODUCT_ID, instance, func.__name__),
                category=DeprecationWarning,
                stacklevel=2)
            warnings.simplefilter('default', DeprecationWarning)  # reset filter
            return func(*args, **kwargs)
        return wrapper
    return the_decorator


class G2ConfigMgr(object):
    """G2 config-manager module access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_params: a JSON string containing INI parameters
    """

    @deprecated(1401)
    def initV2(self, module_name_, ini_params_, debug_=False):
        self.init(module_name_,ini_params_,debug_)

    def init(self, module_name_, ini_params_, debug_=False):
        """  Initializes the G2 config manager
        This should only be called once per process.
        Args:
            moduleName: A short name given to this instance of the config module
            iniParams: A json document that contains G2 system parameters.
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout
        """
        self._module_name = self.prepareStringArgument(module_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._debug = debug_

        if self._debug:
            print("Initializing G2 Config Manager")

        self._lib_handle.G2ConfigMgr_init.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2ConfigMgr_init(self._module_name,
                                 self._ini_params,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

    def __init__(self):
        # type: () -> None
        """ Class initialization
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

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        configID += (str(cID.value).encode())

    def getConfig(self,configID,response):
        """ retrieves the registered configuration document from the datastore
        """
        configID_ = self.prepareIntArgument(configID)
        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2ConfigMgr_getConfig.restype = c_int
        self._lib_handle.G2ConfigMgr_getConfig.argtypes = [c_longlong, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2ConfigMgr_getConfig(configID_,
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

    def getConfigList(self,response):
        """ retrieves a list of known configurations from the datastore
        """
        response[::]=b''
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2ConfigMgr_getConfigList.restype = c_int
        self._lib_handle.G2ConfigMgr_getConfigList.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2ConfigMgr_getConfigList(
                                                                 pointer(responseBuf),
                                                                 pointer(responseSize),
                                                                 self._resize_func)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        #Add the bytes to the response bytearray from calling function
        response += tls_var.buf.value

    def setDefaultConfigID(self,configID):
        """ sets the default config identifier in the datastore
        """
        configID_ = self.prepareIntArgument(configID)
        self._lib_handle.G2ConfigMgr_setDefaultConfigID.restype = c_int
        self._lib_handle.G2ConfigMgr_setDefaultConfigID.argtypes = [c_longlong]
        ret_code = self._lib_handle.G2ConfigMgr_setDefaultConfigID(configID_)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

    def replaceDefaultConfigID(self,oldConfigID,newConfigID):
        """ sets the default config identifier in the datastore
        """
        oldConfigID_ = self.prepareIntArgument(oldConfigID)
        newConfigID_ = self.prepareIntArgument(newConfigID)
        self._lib_handle.G2ConfigMgr_replaceDefaultConfigID.restype = c_int
        self._lib_handle.G2ConfigMgr_replaceDefaultConfigID.argtypes = [c_longlong,c_longlong]
        ret_code = self._lib_handle.G2ConfigMgr_replaceDefaultConfigID(oldConfigID_,newConfigID_)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

    def getDefaultConfigID(self, configID):
        """ gets the default config identifier from the datastore
        """
        configID[::]=b''
        cID = c_longlong(0)
        self._lib_handle.G2ConfigMgr_getDefaultConfigID.argtypes = [POINTER(c_longlong)]
        self._lib_handle.G2ConfigMgr_getDefaultConfigID.restype = c_int
        ret_code = self._lib_handle.G2ConfigMgr_getDefaultConfigID(cID)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2ConfigMgr has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        if cID.value:
            configID += (str(cID.value).encode())

    def clearLastException(self):
        """ Clears the last exception
        """

        self._lib_handle.G2ConfigMgr_clearLastException.restype = None
        self._lib_handle.G2ConfigMgr_clearLastException.argtypes = []
        self._lib_handle.G2ConfigMgr_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        self._lib_handle.G2ConfigMgr_getLastException.restype = c_int
        self._lib_handle.G2ConfigMgr_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2ConfigMgr_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

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

        self._lib_handle.G2ConfigMgr_destroy()

