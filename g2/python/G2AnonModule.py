from ctypes import *
import threading
import json
import os

tls_var = threading.local()

from csv import reader as csvreader

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


class G2AnonModule(object):
    """G2 module access library

    Attributes:
        _lib_handle: A boolean indicating if we like SPAM or not.
        _return_buffer_size: An integer count of the eggs we have laid..
        _return_buffer: An integer count of the eggs we have laid..
        _resize_func_def: resize function definiton
        _resize_func: resize function pointer
        _module_name: CME module name
        _ini_file_name: name and location of .ini file
    """
    def init(self):
        """  initializes and starts the G2Module
        """
        if self._anonimizerSupported == False:
            return 0

        if self._debug:
            print("Initializing G2 module")

        resize_return_buffer(None, 65535)

        self._lib_handle.G2Anonymizer_init.argtypes = [c_char_p, c_char_p, c_int]
        retval = self._lib_handle.G2Anonymizer_init(self._module_name.encode('utf-8'),
                                 self._ini_file_name.encode('utf-8'),
                                 self._debug)


        if self._debug:
            print("Initialization Status: " + str(retval))

        if retval == -2:
            self._lib_handle.G2Anonymizer_getLastException(self._return_buffer, self._return_buffer_size)
            raise TranslateG2ModuleException(self._return_buffer.value)
        elif retval == -1:
            raise G2ModuleNotInitialized('G2AnonModule has not been succesfully initialized')
        elif retval < 0:
            raise G2ModuleGenericException("Failed to initialize G2 Module")
        return retval


    def __init__(self, module_name_, ini_file_name_, debug_=False):
        try:
            if os.name == 'nt':
              self._lib_handle = cdll.LoadLibrary("G2Anonymizer.dll")
            else:
              self._lib_handle = cdll.LoadLibrary("libG2Anonymizer.so")
            self._return_buffer_size = 65535
            self._return_buffer = create_string_buffer(self._return_buffer_size)
            self._resize_func_def = CFUNCTYPE(c_char_p, c_int)
            self._resize_func = self._resize_func_def(resize_return_buffer)
            self._anonimizerSupported = True
        except OSError:
            self._anonimizerSupported = False

        self._module_name = module_name_
        self._ini_file_name = ini_file_name_
        self._debug = debug_

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

    def reportAnonymizationNotIncluded(self):
        raise G2ModuleGenericException("Anonymization functions not available")

    def clearLastException(self):
        """ Clears the last exception

        Return:
            None
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Anonymizer_clearLastException.restype = None
        self._lib_handle.G2Anonymizer_clearLastException.argtypes = []
        self._lib_handle.G2Anonymizer_clearLastException()

    def getLastException(self):
        """ Gets the last exception
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Anonymizer_getLastException.restype = c_int
        self._lib_handle.G2Anonymizer_getLastException.argtypes = [c_char_p, c_size_t]
        self._lib_handle.G2Anonymizer_getLastException(tls_var.buf,sizeof(tls_var.buf))
        resultString = tls_var.buf.value.decode('utf-8')
        return resultString

    def getLastExceptionCode(self):
        """ Gets the last exception code
        """

        resize_return_buffer(None, 65535)
        self._lib_handle.G2Anonymizer_getLastExceptionCode.restype = c_int
        self._lib_handle.G2Anonymizer_getLastExceptionCode.argtypes = []
        exception_code = self._lib_handle.G2Anonymizer_getLastExceptionCode()
        return exception_code

    def exportTokenLibrary(self):
        '''  gets the token library from G2Anonymizer '''
        if self._anonimizerSupported == False:
            self.reportAnonymizationNotIncluded()
        ret_code = self._lib_handle.G2Anonymizer_exportTokenLibrary(self._return_buffer,
                                             self._return_buffer_size,
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Anonymizer_getLastException(self._return_buffer, self._return_buffer_size)
            raise TranslateG2ModuleException(self._return_buffer.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2AnonModule has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return json.loads(self._return_buffer.value.decode('utf-8'))

    def anonymize(self,record):
        '''  anonymize a G2Anonymizer record '''
        if self._anonimizerSupported == False:
            self.reportAnonymizationNotIncluded()
        _record = self.prepareStringArgument(record)
        ret_code = self._lib_handle.G2Anonymizer_anonymize(_record,
                                             self._return_buffer,
                                             self._return_buffer_size,
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Anonymizer_getLastException(self._return_buffer, self._return_buffer_size)
            raise TranslateG2ModuleException(self._return_buffer.value)
        elif ret_code == -1:
            raise G2ModuleNotInitialized('G2AnonModule has not been succesfully initialized')
        elif ret_code < 0:
            raise G2ModuleGenericException("ERROR_CODE: " + str(ret_code))
        return self._return_buffer.value.decode('utf-8')

    def restart(self):
        """  restarts G2 resolver """
        self.destroy()
        self.init()

    def destroy(self):
        """ shuts down G2Module
        """
        if self._anonimizerSupported == True:
            self._lib_handle.G2Anonymizer_destroy()

