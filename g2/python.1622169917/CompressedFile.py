import gzip
import io

def peekLine(file_):
    pos = file_.tell()
    line = file_.readline()
    file_.seek(pos)
    return line

def getSuffix(str_):
    suffixIdx = str_.rfind('.')
    if suffixIdx == -1:
        return None
    return str_[suffixIdx:]

def isCompressedFile(filename_):
    if getSuffix(filename_).lower() in ('.gz', '.gzip', '.zip'):
        return True
    return False

def openPossiblyCompressedFile(filename_, options_):
    suffix = getSuffix(filename_).lower()
    if suffix and suffix == '.zip':
        raise G2UnsupportedFileTypeException('zip files are not currently supported.  try gzip')
    if suffix and suffix in ('.gz','.gzip'):
        try:
            f = gzip.open(filename_, options_)
            #read the first line to make sure we can read this gzip file
            peekLine(f)
            return io.TextIOWrapper(io.BufferedReader(f), encoding='utf-8-sig', errors='ignore')
        except IOError as e:
#handle regular ZIP (non gzip) files later
#            if 'Not a gzipped file' in e.message:
#                return zipfile.ZipFile(filename_, mode=options_)
            raise

    #not a compressed archive
    return io.open(filename_, options_, encoding="utf-8-sig")
