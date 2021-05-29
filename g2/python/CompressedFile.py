import gzip
import io
import json

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

def openPossiblyCompressedFile(filename_, options_, encoding_ = None):
    if not encoding_:
        encoding = 'utf-8-sig'
    suffix = getSuffix(filename_).lower()
    if suffix and suffix == '.zip':
        raise G2UnsupportedFileTypeException('zip files are not currently supported.  try gzip')
    if suffix and suffix in ('.gz','.gzip'):
        try:
            f = gzip.open(filename_, options_)
            #read the first line to make sure we can read this gzip file
            peekLine(f)
            return io.TextIOWrapper(io.BufferedReader(f), encoding=encoding_, errors='ignore')
        except IOError as e:
#handle regular ZIP (non gzip) files later
#            if 'Not a gzipped file' in e.message:
#                return zipfile.ZipFile(filename_, mode=options_)
            raise

    #not a compressed archive
    return io.open(filename_, options_, encoding=encoding_)


def fileRowParser(line, fileData, rowNum = 0):
    if line[-1] == '\n':
        line = line[0:-1].strip()
    else:
        line = line.strip()

    if len(line) == 0:
        print('  WARNING: row %s is blank' % rowNum) 
        return '' #--skip rows with no data

    #--its a json string
    if fileData['FILE_FORMAT'] == 'JSON':
        try: rowData = json.loads(line)
        except:
            print('  WARNING: Invalid json in row %s (%s) ' % (rowNum, line[0:50]))
            return ''
        return rowData

    #--its a umf string
    if fileData['FILE_FORMAT'] == 'UMF':
        if not (line.upper().startswith('<UMF_DOC') and line.upper().endswith('/UMF_DOC>')):
            print('  WARNING: invalid umf in row %s (%s)' % (rowNum, line[0:50]))
            return ''
        return line

    #--its a csv variant
    else:
        try: rowData = [removeQuoteChar(x.strip()) for x in line.split(fileData['DELIMITER'])]
        except: 
            print('  WARNING: row %s could not be parsed' % rowNum)
            try: print(line)
            except: pass
            return '' #--skip rows with no data
        if len(''.join(map(str, rowData)).strip()) == 0:
            print('  WARNING: row %s is blank' % rowNum) 
            return '' #--skip rows with no data
        if 'HEADER_ROW' in fileData:
            rowData = dict(zip(fileData['HEADER_ROW'], rowData))
        return rowData

def removeQuoteChar(s):
    if len(s)>2 and s[0] + s[-1] in ("''", '""'):
        return s[1:-1] 
    return s 

