from ctypes import *
import threading
import json
import os

tls_var = threading.local()

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


class G2Hasher(object):
    """G2 hasher access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _hasher_name: CME hasher name
        _ini_file_name: name and location of .ini file
    """
    def init(self, hasher_name_, ini_file_name_, debug_=False):
        """  initializes and starts the G2Module
        Args:
            hasherName: A short name given to this instance of the hasher
            iniFilename: A fully qualified path to the G2 engine INI file (often /opt/senzing/g2/python/G2Module.ini)
            verboseLogging: Enable diagnostic logging which will print a massive amount of information to stdout
        """
        if self._hasherSupported == False:
            return 0

        self._hasher_name = hasher_name_
        self._ini_file_name = ini_file_name_
        self._debug = debug_

        if self._debug:
            print("Initializing G2 Hasher")

        resize_return_buffer(None, 65535)

        p_hasher_name = self.prepareStringArgument(self._hasher_name)
        p_ini_file_name = self.prepareStringArgument(self._ini_file_name)

        self._lib_handle.G2Hasher_init.argtypes = [c_char_p, c_char_p, c_int]
        retval = self._lib_handle.G2Hasher_init(p_hasher_name,
                                 p_ini_file_name,
                                 self._debug)


        if self._debug:
            print("Initialization Status: " + str(retval))

        if retval == -2:
            self._lib_handle.G2Hasher_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif retval == -1:
            raise G2ModuleNotInitialized('G2Hasher has not been succesfully initialized')
        elif retval < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Hasher")
        return retval


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
        #handle null string
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

    def reportHasherNotIncluded(self):
        raise G2ModuleGenericException("Hashing functions not available")

    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Hasher_clearLastException.restype = None
        self._lib_handle.G2Hasher_clearLastException.argtypes = []
        self._lib_handle.G2Hasher_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Hasher_getLastException.restype = c_int
        self._lib_handle.G2Hasher_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2Hasher_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Hasher_getLastExceptionCode.restype = c_int
        self._lib_handle.G2Hasher_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2Hasher_getLastExceptionCode()
        return exception_code

    def exportTokenLibrary(self,response):
        '''  gets the token library from G2Hasher '''
        if self._hasherSupported == False:
            self.reportHasherNotIncluded()
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Hasher_exportTokenLibrary.argtypes = [POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Hasher_exportTokenLibrary(pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Hasher_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Hasher has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        response += responseBuf.value
        return ret_code

    def process(self,record,response):
        '''  process a G2Hasher record '''
        if self._hasherSupported == False:
            self.reportHasherNotIncluded()
        _record = self.prepareStringArgument(record)
        resize_return_buffer(None, 65535)
        responseBuf = c_char_p(None)
        responseSize = c_size_t(0)
        self._lib_handle.G2Hasher_process.argtypes = [c_char_p, POINTER(c_char_p), POINTER(c_size_t), self._resize_func_def]
        ret_code = self._lib_handle.G2Hasher_process(_record,
                                             pointer(responseBuf),
                                             pointer(responseSize),
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Hasher_getLastException(tls_var.buf, sizeof(tls_var.buf))
            raise TranslateG2ModuleException(tls_var.buf.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2Hasher has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        response += responseBuf.value
        return ret_code

    def restart(self):
        """  restarts G2 resolver """
        moduleName = self._engine_name
        iniFilename = self._ini_file_name
        debugFlag = self._debug
        self.destroy()
        self.init(moduleName, iniFilename, debugFlag)

    def destroy(self):
        """ shuts down G2Module
        """
        if self._hasherSupported == True:
            return self._lib_handle.G2Hasher_destroy()
        return 0

