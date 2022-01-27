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

from senzing.G2Exception import TranslateG2ModuleException, G2ModuleNotInitialized, G2ModuleGenericException

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


class G2Config(object):
    """G2 config module access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_file_name: name and location of .ini file
    """

    @deprecated(1301)
    def initV2(self, module_name_, ini_params_, debug_=False):
        self.init(module_name_,ini_params_,debug_)

    def init(self, module_name_, ini_params_, debug_=False):

        self._module_name = self.prepareStringArgument(module_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._debug = debug_

        if self._debug:
            print("Initializing G2 Config")

        self._lib_handle.G2Config_init.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2Config_init(self._module_name,
                                 self._ini_params,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Config has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

    def __init__(self):
        # type: () -> None
        """ G2Config class initialization
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


    def clearLastException(self):
        """ Clears the last exception
        """

        self._lib_handle.G2Config_clearLastException.restype = None
        self._lib_handle.G2Config_clearLastException.argtypes = []
        self._lib_handle.G2Config_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        self._lib_handle.G2Config_getLastException.restype = c_int
        self._lib_handle.G2Config_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2Config_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        self._lib_handle.G2Config_getLastExceptionCode.restype = c_int
        self._lib_handle.G2Config_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2Config_getLastExceptionCode()
        return exception_code

    def create(self):
        """ Creates a new config handle from the stored template
        """
        self._lib_handle.G2Config_create.restype = c_int
        self._lib_handle.G2Config_create.argtypes = [POINTER(c_void_p)]
        configHandle = c_void_p(0)
        ret_code = self._lib_handle.G2Config_create(byref(configHandle))

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Config has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        return configHandle.value


    def load(self,jsonConfig):
        """ Creates a new config handle from a json config string
        """
        _jsonConfig = self.prepareStringArgument(jsonConfig)
        self._lib_handle.G2Config_load.restype = c_int
        self._lib_handle.G2Config_load.argtypes = [c_char_p,POINTER(c_void_p)]
        configHandle = c_void_p(0)
        ret_code = self._lib_handle.G2Config_load(_jsonConfig,byref(configHandle))

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Config has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        return configHandle.value

    def close(self,configHandle):
        """ Closes a config handle
        """
        self._lib_handle.G2Config_close.restype = c_int
        self._lib_handle.G2Config_close.argtypes = [c_void_p]
        self._lib_handle.G2Config_close(c_void_p(configHandle))

    def save(self,configHandle,response):
        """ Saves a config handle
        """
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Config_save.argtypes = [c_void_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Config_save(configHandle,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Config has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        response += responseBuf.value

    def listDataSources(self,configHandle,response):
        """ lists a set of data sources
        """
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Config_listDataSources.argtypes = [c_void_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Config_listDataSources(configHandle,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Config has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        response += responseBuf.value

    def addDataSource(self,configHandle,inputJson,response):
        """ Adds a data source
        """
        _inputJson = self.prepareStringArgument(inputJson)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Config_addDataSource.argtypes = [c_void_p, c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Config_addDataSource(configHandle,_inputJson,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Config has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        response += responseBuf.value

    def deleteDataSource(self,configHandle,inputJson):
        """ Deletes a data source
        """
        _inputJson = self.prepareStringArgument(inputJson)
        self._lib_handle.G2Config_deleteDataSource.argtypes = [c_void_p, c_char_p]
        ret_code = self._lib_handle.G2Config_deleteDataSource(configHandle,_inputJson)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Config has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Config_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

    def destroy(self):
        """ Uninitializes the engine
        This should be done once per process after init(...) is called.
        After it is called the engine will no longer function.

        Args:

        Return:
            None
        """

        self._lib_handle.G2Config_destroy()

