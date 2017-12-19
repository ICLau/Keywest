# Originally from test.py
import pandas as pd
#import numpy as np
#import matplotlib.pyplot as plt
import sqlite3
import readIni as myIni
import glob
import appLogging as appLog

dbCur = None
conn = None

def disconnectDB ():
    global dbCur
    global conn
    
    if (dbCur != None and conn != None):
        appLog.logMsg (__name__, appLog._iINFO, "~~ disconnectDB()")
        dbCur.close()
        conn.close()
        
    dbCur = None
    conn = None

# =============================================================================
def connectDB (dbName):
    global conn
    global dbCur

    if (dbCur != None and conn != None):
        appLog.logMsg  (__name__, appLog._iWARNING, "~~ calling disconnectDB()")
        disconnectDB()
        
    conn = sqlite3.connect (dbName)
    dbCur = conn.cursor()
    
    dbCur.execute ("CREATE TABLE IF NOT EXISTS AccessLog (datestamp TEXT, user TEXT, action TEXT, PRIMARY KEY (user, datestamp))")
    appLog.logMsg (__name__, appLog._iINFO, "~~ DB connected")
    return dbCur

# =============================================================================

# =============================================================================

# =============================================================================
def writeRec (dbCursor, dttm, user, action):
    # Need to check if the record already exists...
    sqlStatement = str.format("SELECT datestamp, user FROM AccessLog WHERE datestamp = '{0}' AND user = '{1}'", dttm, user)
    dbCursor.execute (sqlStatement)

    rowWritten = 0
    if (len(dbCursor.fetchall()) <= 0):
        dbCursor.execute ("INSERT INTO AccessLog (datestamp, user, action) VALUES (?, ?, ?) ",
                   (dttm, user, action))
        conn.commit()
        rowWritten = 1
#    else:
#        print ('*** data already exists in db -- skipped (not added)')

    return rowWritten
# =============================================================================
def processInputFile (thisFile):
    df = pd.read_csv(thisFile, delimiter = delim)
    appLog.logMsg (__name__, appLog._iINFO, "processing '{0}'".format(thisFile))
    
    #fetch the column names
    cols = df.columns
    
    # create a new dataframe with less columns
    # we are only interested in columns 5,6,7
    df2 = df [[cols[5], cols[6], cols[7]]]
    appLog.logMsg (__name__, appLog._iINFO, "- # of records read: {0}".format(len(df2)))


    df3 = df2[df2[cols[7]].isin([fltrBadgedIn, fltrBadgeOut])]
    appLog.logMsg (__name__, appLog._iINFO, "- reduced to {0} rows".format(len(df3)))
    
    # rename columns
    newCols = ['DateTime', 'User', 'Action']
    df3.columns = newCols
    
    
    # Extract and shorten the name
    # DON'T do this more than ONCE...
    
    strPrefix = 'User: '
    strSuffix = ', card:'
    
    df3.loc[:,(df3.columns[1])] = df3.loc[:,(df3.columns[1])].map(lambda x: x[x.find(strPrefix)+len(strPrefix) : x.find(strSuffix)])
    #df3[df3.columns[1]] = df3[df3.columns[1]].map ( lambda x: x[x.find(strPrefix)+len(strPrefix) : x.find(strSuffix)] )
#    print (df3[df3.columns[1]].head())    
    
    
    # =============================================================================
    #     reads the dbFile name from config.ini
    # =============================================================================
    doorDBDefault = 'DoorAccess.db'
    bOK, doorDB = myIni.get_sectionKeyValues ('CleansedData', 'dbFile')
    if (bOK == False or doorDB.strip() ==''):
        doorDB = doorDBDefault

    dbCursor = connectDB (doorDB)
    
    appLog.logMsg (__name__, appLog._iINFO, '*** Writing recs to db...')
    rowsWritten = 0
    maxRow = len (df3)
    for i in range(maxRow):
          rowsWritten += writeRec (dbCursor,
                                   df3.iloc[i][0], 
                                   df3.iloc[i][1],
                                   df3.iloc[i][2])
    
    appLog.logMsg (__name__, appLog._iINFO, "Finished writing to db!")
    appLog.logMsg (__name__, appLog._iINFO, "===> rows written to db/rows in dataframe: {0}/{1}".format(rowsWritten, maxRow))
    
    disconnectDB ()


# =============================================================================
# =============================================================================

sectionName = 'Inputs'
keyNames = {'inputFilePattern'  : 'inputFilePattern', 
            'delimiter'         : 'delimiter',
            'badgeIn'           : 'badgeIn',
            'badgeOut'          :'badgeOut'}
# =============================================================================
# gets the defaults from config.ini
# =============================================================================
bOK, inputFilePattern = myIni.get_sectionKeyValues(sectionName, keyNames['inputFilePattern'])
if (bOK == False):
    print ("[INI] section '{0}', name '{1}' not found. defaulting to 'BasicEventReport.csv'".format(sectionName, keyNames['inputFilePattern']))
   
if (inputFilePattern is None or inputFilePattern.strip() == ''):
    inputFilePattern = 'BasicEventReport.csv'

bOK, delim = myIni.get_sectionKeyValues (sectionName, keyNames['delimiter'])
if (bOK == False):
    print ("[INI] section '{0}', name '{1}' not found, defaulting to ','".format(sectionName, keyNames['delimiter']))

if (delim is None or delim.strip() == ''):
    delim = ','

fltrBadgedInDefault = 'Access Granted'
fltrBadgeOutDefault = 'Exit Granted'

bOK, fltrBadgedIn = myIni.get_sectionKeyValues (sectionName, keyNames['badgeIn'])
if (bOK == False or fltrBadgedIn.strip() == ''):
        fltrBadgedIn = fltrBadgedInDefault
        
bOK, fltrBadgeOut = myIni.get_sectionKeyValues (sectionName, keyNames['badgeOut'])
if (bOK == False or fltrBadgeOut.strip() == ''):
    fltrBadgeOut = fltrBadgeOutDefault



# =============================================================================
# gets the filter strings from config.ini    
# =============================================================================

inputFiles = glob.glob (inputFilePattern)
for inFile in inputFiles:
    processInputFile (inFile)
    print ("* Done with '{0}'".format(inFile))
    
print ('*** Done processing all input CSV files.')


