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


class G2Hasher(object):
    """G2 hasher access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _hasher_name: CME hasher name
        _ini_file_name: name and location of .ini file
    """

    @deprecated(1501)
    def initV2(self, hasher_name_, ini_params_, debug_=False):
        self.init(hasher_name_,ini_params_,debug_)

    def init(self, hasher_name_, ini_params_, debug_=False):

        if self._hasherSupported == False:
            return

        self._hasher_name = self.prepareStringArgument(hasher_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._debug = debug_
        if self._debug:
            print("Initializing G2 Hasher")

        self._lib_handle.G2Hasher_init.argtypes = [c_char_p, c_char_p, c_int]
        ret_code = self._lib_handle.G2Hasher_init(self._hasher_name,
                                 self._ini_params,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Hasher has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Hasher_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

    @deprecated(1502)
    def initWithConfigV2(self, hasher_name_, ini_params_, config_, debug_):
        self.initWithConfig(hasher_name_,ini_params_,config_,debug_)

    def initWithConfig(self, hasher_name_, ini_params_, config_, debug_):

        if self._hasherSupported == False:
            return

        self._hasher_name = self.prepareStringArgument(hasher_name_)
        self._ini_params = self.prepareStringArgument(ini_params_)
        self._config = self.prepareStringArgument(config_)
        self._debug = debug_

        if self._debug:
            print("Initializing G2 Hasher")

        self._lib_handle.G2Hasher_initWithConfig.argtypes = [ c_char_p, c_char_p, c_char_p, c_int ]
        ret_code = self._lib_handle.G2Hasher_initWithConfig(self._hasher_name,
                                 self._ini_params,
                                 self._config,
                                 self._debug)

        if self._debug:
            print("Initialization Status: " + str(ret_code))

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Hasher has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Hasher_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

    def __init__(self):
        try:
            if os.name == 'nt':
              self._lib_handle = cdll.LoadLibrary("G2Hasher.dll")
            else:
              self._lib_handle = cdll.LoadLibrary("libG2Hasher.so")
            self._hasherSupported = True
        except OSError:
            self._hasherSupported = False

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

    def reportHasherNotIncluded(self):
        raise G2ModuleGenericException("Hashing functions not available")

    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        self._lib_handle.G2Hasher_clearLastException.restype = None
        self._lib_handle.G2Hasher_clearLastException.argtypes = []
        self._lib_handle.G2Hasher_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        self._lib_handle.G2Hasher_getLastException.restype = c_int
        self._lib_handle.G2Hasher_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2Hasher_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        self._lib_handle.G2Hasher_getLastExceptionCode.restype = c_int
        self._lib_handle.G2Hasher_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2Hasher_getLastExceptionCode()
        return exception_code

    def exportTokenLibrary(self,response):
        '''  gets the token library from G2Hasher '''
        if self._hasherSupported == False:
            self.reportHasherNotIncluded()
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Hasher_exportTokenLibrary.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Hasher_exportTokenLibrary(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Hasher has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Hasher_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        response += responseBuf.value

    def process(self,record,response):
        '''  process a G2Hasher record '''
        if self._hasherSupported == False:
            self.reportHasherNotIncluded()
        _record = self.prepareStringArgument(record)
        responseBuf = c_char_p(addressof(tls_var.buf))
        responseSize = c_size_t(tls_var.bufSize)
        self._lib_handle.G2Hasher_process.argtypes = [c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Hasher_process(_record,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)

        if ret_code == -1:
            raise G2ModuleNotInitialized('G2Hasher has not been successfully initialized')
        elif ret_code < 0:
            self._lib_handle.G2Hasher_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)

        response += responseBuf.value

    def destroy(self):
        """ shuts down G2Module
        """
        if self._hasherSupported == True:
            self._lib_handle.G2Hasher_destroy()

