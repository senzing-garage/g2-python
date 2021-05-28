from ctypes import *
import json
import os

from csv import reader as csvreader

from G2Exception import TranslateG2ModuleException, G2ModuleNotInitialized, G2ModuleGenericException


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

        self._lib_handle.G2Anonymizer_init.argtypes = [c_char_p, c_char_p, c_int]
        retval = self._lib_handle.G2Anonymizer_init(self._module_name.encode('utf-8'),
                                 self._ini_file_name.encode('utf-8'),
                                 self._debug)


        if self._debug:
            print("Initialization Status: " + str(retval))

        if retval == -2:
            self._lib_handle.G2Anonymizer_getLastException(self._return_buffer, self._return_buffer_size)
            self._lib_handle.G2Anonymizer_clearLastException()
            raise TranslateG2ModuleException(self._return_buffer.value)
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
            self._resize_func = self._resize_func_def(self.resize_return_buffer)
            self._anonimizerSupported = True
        except OSError:
            self._anonimizerSupported = False

        self._module_name = module_name_
        self._ini_file_name = ini_file_name_
        self._debug = debug_

    def resize_return_buffer(self, size_):
        """  callback function that resizs return buffer when it is too small
        Args:
            size_: size the return buffer needs to be
        """
        self._return_buffer_size = size_
        self._return_buffer = create_string_buffer('\000' * self._return_buffer_size)
        address_of_new_buffer = addressof(self._return_buffer)
        return address_of_new_buffer

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

    def exportTokenLibrary(self):
        '''  gets the token library from G2Anonymizer '''
        if self._anonimizerSupported == False:
            self.reportAnonymizationNotIncluded()
        ret_code = self._lib_handle.G2Anonymizer_exportTokenLibrary(self._return_buffer,
                                             self._return_buffer_size,
                                             self._resize_func)
        if ret_code == -2:
            self._lib_handle.G2Anonymizer_getLastException(self._return_buffer, self._return_buffer_size)
            self._lib_handle.G2Anonymizer_clearLastException()
            raise TranslateG2ModuleException(self._return_buffer.value)
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
            self._lib_handle.G2Anonymizer_clearLastException()
            raise TranslateG2ModuleException(self._return_buffer.value)
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

