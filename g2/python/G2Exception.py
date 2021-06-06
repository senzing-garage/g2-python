

class G2Exception(Exception):
    '''Base exception for G2 related python code'''
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)

class G2UnsupportedFileTypeException(G2Exception):
    def __init__(self, *args, **kwargs):
        G2Exception.__init__(self, *args, **kwargs)

class G2InvalidFileTypeContentsException(G2Exception):
    def __init__(self, *args, **kwargs):
        G2Exception.__init__(self, *args, **kwargs)

class G2DBException(G2Exception):
    '''Base exception for G2 DB related python code'''
    def __init__(self, *args, **kwargs):
        G2Exception.__init__(self, *args, **kwargs)

class G2DBUnknownException(G2DBException):
    def __init__(self, *args, **kwargs):
        G2DBException.__init__(self, *args, **kwargs)

class G2UnsupportedDatabaseType(G2DBException):
    def __init__(self, *args, **kwargs):
        G2DBException.__init__(self, *args, **kwargs)

class G2TableNoExist(G2DBException):
    def __init__(self, *args, **kwargs):
        G2DBException.__init__(self, *args, **kwargs)

class G2DBMNotStarted(G2DBException):
    def __init__(self, *args, **kwargs):
        G2DBException.__init__(self, *args, **kwargs)

class G2DBNotFound(G2DBException):
    def __init__(self, *args, **kwargs):
        G2DBException.__init__(self, *args, **kwargs)

class G2DBUniqueConstraintViolation(G2DBException):
    def __init__(self, *args, **kwargs):
        G2DBException.__init__(self, *args, **kwargs)

class G2ModuleException(G2Exception):
    '''Base exception for G2 Module related python code'''
    def __init__(self, *args, **kwargs):
        G2Exception.__init__(self, *args, **kwargs)

class G2ModuleNotInitialized(G2ModuleException):
    '''G2 Module called but has not been initialized '''
    def __init__(self, *args, **kwargs):
        G2ModuleException.__init__(self, *args, **kwargs)

class G2ModuleGenericException(G2ModuleException):
    '''Generic exception for non-subclassed G2 Module exception '''
    def __init__(self, *args, **kwargs):
        G2ModuleException.__init__(self, *args, **kwargs)

class G2ModuleMySQLNoSchema(G2ModuleException):
    def __init__(self, *args, **kwargs):
        G2ModuleException.__init__(self, *args, **kwargs)

class G2ModuleEmptyMessage(G2ModuleException):
    def __init__(self, *args, **kwargs):
        G2ModuleException.__init__(self, *args, **kwargs)

class G2ModuleInvalidXML(G2ModuleException):
    def __init__(self, *args, **kwargs):
        G2ModuleException.__init__(self, *args, **kwargs)

class G2ModuleResolveMissingResEnt(G2ModuleException):
    def __init__(self, *args, **kwargs):
        G2ModuleException.__init__(self, *args, **kwargs)

class G2ModuleLicenseException(G2ModuleException):
    def __init__(self, *args, **kwargs):
        G2ModuleException.__init__(self, *args, **kwargs)

def TranslateG2ModuleException(ex):
    exInfo = ex.decode('utf-8').split('|', 1)
    if exInfo[0] == '7213E':
        return G2ModuleMySQLNoSchema(ex)
    elif exInfo[0] == '0002E':
        return G2ModuleInvalidXML(ex)
    elif exInfo[0] == '0007E':
        return G2ModuleEmptyMessage(ex)
    elif exInfo[0] == '2134E':
        return G2ModuleResolveMissingResEnt(ex)
    elif exInfo[0] == '9000E':
        return G2ModuleLicenseException(ex)
    else:
        return G2ModuleGenericException(ex)
