# -*- coding: utf-8 -*-
"""
Created on Wed Dec 13 22:54:40 2017

@author: Isaac

*** NOTE: requires ConfigObj
***   use: pip install configobj 
***   to acquire and install it
"""

import glob
from configobj import ConfigObj

iniCached = False
config = None
iniFilename = 'config.ini'

# =============================================================================
def isCached ():
    return iniCached

# =============================================================================
def readINI (filename = 'config.ini'):
    if (isCached()): return True

    localpath = './' + filename
    bFileExist = len(glob.glob (localpath))
    if (bFileExist == 0):
        print ('**', localpath, 'does not exist.')
        return False

    global config
    config = ConfigObj(localpath)
    
    global iniCached
    iniCached = True
    
    global iniFilename
    iniFilename = filename

    print ('** Not cached, loaded INI file:', iniFilename)
    return True
    
# =============================================================================
def get_sectionNames():
    if (False == isCached()):
        if (False == readINI (iniFilename)): return False, None

    return True, config.sections

# =============================================================================
def get_sectionKeys(sName):
    if (False == isCached()):
        readINI (iniFilename)
    
    sKeys = None
    if (sName not in config.sections): return False, None
    
    sSection = config[sName]
    sKeys = [k for k in sSection]

    return True, sKeys

# =============================================================================
def get_sectionKeyValues (sName, kName):
    bSuccess, sKeys = get_sectionKeys (sName)

    if (False == bSuccess): return False, None
    
    if (kName not in sKeys): return False, None
    
    return True, config[sName][kName]

# =============================================================================

bSuccess = readINI()
 
if (__name__ == '__main__'):
    print ('readINI() returns:', bSuccess)
    bSuccess, SectionNameList = get_sectionNames ()
    print ('get_sectionNames() returns', bSuccess)
    if (bSuccess): 
        print ('* sectionNames are:', SectionNameList)
    
    bSuccess, kValues = get_sectionKeyValues ('input', 'delimitor')
    print ("get_sectionKeyValues ('input', 'delimitor'): returns:", bSuccess)
    
    bSuccess, kValues = get_sectionKeyValues ('Inputs', 'delimiter')
    print ("get_sectionKeyValues ('inputs', 'delimiter'): returns:", bSuccess)
    print ('=> kValues:', kValues)
    
    bSuccess, kValues = get_sectionKeyValues ('Inputs', 'inputFilePattern')
    print ("get_sectionKeyValues ('inputs', 'inputFilePattern'): returns:", bSuccess)
    print ('=> kValues:', kValues)
    
    bSuccess, kValues = get_sectionKeyValues ('Users', 'exclude')
    print ("get_sectionKeyValues ('Users', 'exclude') returns: {0}".format(bSuccess))
    print ('=> kValues:', kValues)
    b = []
    if ('str' == type(kValues).__name__):
        b = [kValues]
    else:
        b = kValues

    sType = type(kValues).__name__
    print ('Type of kValues is "{0}" and b = "{1}"'.format(sType, b))

