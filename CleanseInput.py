import sys
import pandas as pd
import glob

import readIni as myIni
import appLogging as appLog
import appDB

# =============================================================================
def processInputFile (thisFile):
    df = pd.read_csv(thisFile, delimiter = delim)
    appLog.logMsg (modName, appLog._iINFO, "processing '{0}'".format(thisFile))
    
    #fetch the column names
    cols = df.columns
    
    # create a new dataframe with less columns
    # we are only interested in columns 5,6,7
    df2 = df [[cols[5], cols[6], cols[7]]]
    appLog.logMsg (modName, appLog._iINFO, "- # of records read: {0}".format(len(df2)))


    df3 = df2[df2[cols[7]].isin([fltrBadgedIn, fltrBadgeOut])]
    appLog.logMsg (modName, appLog._iINFO, "- reduced to {0} rows".format(len(df3)))
    
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
    
    dbConn = appDB.connectBadgeDB ()
    
    appLog.logMsg (modName, appLog._iINFO, '*** Writing recs to db...')
    rowsWritten = 0
    maxRow = len (df3)
    for i in range(maxRow):
        result = appDB.insertBadgeRecord ( dbConn,
                                          (df3.iloc[i][0], 
                                           df3.iloc[i][1],
                                           df3.iloc[i][2])
                                         )
        if (result > 0) :
            rowsWritten += 1

    
    appLog.logMsg (modName, appLog._iINFO, "Finished writing to db!")
    appLog.logMsg (modName, appLog._iINFO, "===> rows written to db/rows in dataframe: {0}/{1}".format(rowsWritten, maxRow))
    
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


