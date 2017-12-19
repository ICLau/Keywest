# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 09:56:27 2017

@author: Isaac
"""
import logging
import readIni as ini
from datetime import datetime as dt

# basicConfig() can only be called ONCE!!!
logFilename = dt.today().strftime('%Y-%m-%d') + '.log'
bInit = False
logLevel = logging.INFO     # default at INFO level
logWarning = None
logInfo = None
logDebug = None
logCritical = None

logFunc = {logging.DEBUG    : logging.debug,
           logging.INFO     : logging.info,
           logging.WARN     : logging.warning,
           logging.CRITICAL : logging.critical}

_iDEBUG    = logging.DEBUG
_iINFO     = logging.INFO
_iWARNING  = logging.WARNING
_iERROT    = logging.ERROR
_iCRITICAL = logging.CRITICAL

logLevelDict = {'DEBUG'    : logging.DEBUG,         # 10
                'INFO'     : logging.INFO,          # 20
                'WARNING'  : logging.WARNING,       # 30
                'ERROR'    : logging.ERROR,         # 40
                'CRITICAL' : logging.CRITICAL}      # 50

consoleTimeFormat = '%Y-%m-%d %H:%M:%S'
# =============================================================================
def isInit ():
    return bInit

# =============================================================================
def logMsg (module, level, message):
    oMsg = "[{0}]: {1}".format(module, message)

    if (level >= logLevel):
        logFunc[level](oMsg)
        if (bConsole):
            print ("{0} {1}".format(dt.today().strftime(consoleTimeFormat), 
                                    oMsg))

# =============================================================================
def initLog():
    if (isInit() == False):
        # should get the config.ini setting 
        logging.basicConfig(filename=logFilename, 
                            filemode='a+', 
                            level=logLevel, 
                            format='%(asctime)s %(message)s')    # default date/time display (ISO8601)
        print ('*** logFilename="{0}" initialized.'.format(logFilename))
        
        global bInit
        bInit = True

    global logWarning
    logWarning = logging.warning
    
    global logInfo
    logInfo = logging.warning
    
    global logDebug
    logDebug = logging.debug
    
    global logCritical
    logCritical = logging.critical

# =============================================================================

bOK, sLogLevel = ini.get_sectionKeyValues ("logging", "logLevel")
if (bOK == True and sLogLevel != None):
    logLevel = logLevelDict[sLogLevel]
print ('*** logLevel = {0}'.format(logLevel))

bConsole = False
bOK, sConsole = ini.get_sectionKeyValues ('logging', 'outputToConsole')
if (bOK == True):
    sConsole = sConsole.strip()
    if (sConsole != '' and sConsole.startswith(tuple('1tT')) ):
        bConsole = True
    
initLog()
logMsg (__name__, logging.INFO, "LogFile '{0}' ready...".format(logFilename))

if (__name__ == '__main__'):

    logMsg ('[module1]', logging.WARN, "warning message")
    logMsg ('[module2]', logging.DEBUG, "debug message")
    logMsg ('[module3]', logging.INFO, "info message")
    logMsg ('[module4]', logging.CRITICAL, "critical message")

