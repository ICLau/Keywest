import sys
import pandas as pd
import glob

import readIni as myIni
import appLogging as appLog
import appDB

# =============================================================================
def processInputFile (thisFile):
    appLog.logMsg (modName, appLog._iINFO, "processing '{0}".format(thisFile))

    # only reads in cols 5,6,7 from input file
    df = pd.read_csv(thisFile, delimiter=delim, usecols=[5,6,7])
    
    # reset col names
    newCols = ['DateTime', 'User', 'Action']
    df.columns = newCols
    
    recsRead = len(df)
    df = df[df[newCols[2]].isin([fltrBadgedIn, fltrBadgeOut])]
    recsInDF = len(df)
    appLog.logMsg (modName, appLog._iINFO, "...# of records read/reduced to: {0}/{1}'".format(recsRead, recsInDF))

    # Extract and shorten the name
    # DON'T do this more than ONCE...
    strPrefix = 'User: '
    strSuffix = ', card:'
    df['User'] = df['User'].map(lambda x: str(x[x.find(strPrefix)+len(strPrefix) : x.find(strSuffix)]))
    
    dbConn = appDB.connectBadgeDB ()
    
    appLog.logMsg (modName, appLog._iINFO, '*** Writing recs to db...')
    rowsWritten = 0

    for row in df.itertuples():
        # the 1st element is the row's index value
        result = appDB.insertBadgeRecord ( dbConn, 
                                          (row[1], 
                                           row[2],
                                           row[3])
                                         )
        if (result > 0) :
             rowsWritten += 1

    appLog.logMsg (modName, appLog._iINFO, "Finished writing to db!")
    appLog.logMsg (modName, appLog._iINFO, "===> rows written to db/rows in dataframe: {0}/{1}".format(rowsWritten, recsInDF))
    
    appDB.disconnectDB (dbConn)


# =============================================================================
# =============================================================================
# Main()
# =============================================================================
modName = __name__
if __name__ == '__main__':
    modName = "{0}({1})".format(sys.argv[0], __name__)
    
sectionName = 'Inputs'
keyNames = {'inputFilePattern'  : 'inputFilePattern', 
            'delimiter'         : 'delimiter',
            'badgeIn'           : 'badgeIn',
            'badgeOut'          : 'badgeOut'}
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

# =============================================================================
# gets the filter strings from config.ini    
# =============================================================================
fltrBadgedInDefault = 'Access Granted'
fltrBadgeOutDefault = 'Exit Granted'

bOK, fltrBadgedIn = myIni.get_sectionKeyValues (sectionName, keyNames['badgeIn'])
if (bOK == False or fltrBadgedIn.strip() == ''):
        fltrBadgedIn = fltrBadgedInDefault
        
bOK, fltrBadgeOut = myIni.get_sectionKeyValues (sectionName, keyNames['badgeOut'])
if (bOK == False or fltrBadgeOut.strip() == ''):
    fltrBadgeOut = fltrBadgeOutDefault

# =============================================================================
# process all the input CSV's 
# =============================================================================
inputFiles = glob.glob (inputFilePattern)
for inFile in inputFiles:
    processInputFile (inFile)
    print ("* Done with '{0}'".format(inFile))
    
print ('*** Done processing all input CSV files.')


