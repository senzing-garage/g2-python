import sys
import os
import argparse
import errno
import shutil

def find_replace_in_file(filename, old_string, new_string):
    # Safely read the input filename using 'with'
    with open(filename) as f:
        s = f.read()

    # Safely write the changed content, if found in the file
    with open(filename, 'w') as f:
        s = s.replace(old_string, new_string)
        f.write(s)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a per-user instance of Senzing in a new folder with the specified name.')
    parser.add_argument('folder', metavar='F', nargs='?', default='senzing', help='the name of the folder to create (default is "senzing")')
    args = parser.parse_args()

    fullPathName = os.path.normpath( os.path.join(os.getcwd(), args.folder) )
    # if senzing is not the last folder, add it
    if os.path.basename(fullPathName) != 'senzing':
        fullPathName = os.path.join(fullPathName, 'senzing')
    
    # Change this if senzing is installed to a different location
    senzingPath = '/opt/senzing'
    
    print("Create Senzing instance at " + fullPathName )
    try:
        shutil.copytree(senzingPath, fullPathName)
    except OSError as e:
        if e.errno == errno.EEXIST:
            print('"' + args.folder + '" already exists. Please specify a folder that does not already exist')
            sys.exit(1)
        else:
            raise
    
    filesToUpdate = ['g2/setupEnv', 'g2/python/G2Module.ini', 'g2/python/G2Project.ini']
    
    for f in filesToUpdate:
        find_replace_in_file(os.path.join(fullPathName, f), '/opt/senzing', fullPathName)

