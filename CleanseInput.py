# Originally from test.py
import pandas as pd
#import numpy as np
#import matplotlib.pyplot as plt
import sqlite3

# =============================================================================
def writeRec (dttm, user, action):
    # Need to check if the record already exists...
    sqlStatement = str.format("SELECT datestamp, user FROM AccessLog WHERE datestamp = '{0}' AND user = '{1}'", dttm, user)
    dbCur.execute (sqlStatement)

    rowWritten = 0
    if (len(dbCur.fetchall()) <= 0):
        dbCur.execute ("INSERT INTO AccessLog (datestamp, user, action) VALUES (?, ?, ?) ",
                   (dttm, user, action))
        conn.commit()
        rowWritten = 1
#    else:
#        print ('*** data already exists in db -- skipped (not added)')

    return rowWritten
# =============================================================================

logFile = 'BasicEventReport.csv'
delim = ','
df = pd.read_csv(logFile, delimiter = delim)

#fetch the column names
cols = df.columns

#create a new table with less columns
#we are only interested in columns 5,6,7
df2 = df [[cols[5], cols[6], cols[7]]]

fltrBadgedIn = 'Access Granted'
fltrBadgeOut = 'Exit Granted'
df3 = df2[df2[cols[7]].isin([fltrBadgedIn, fltrBadgeOut])]

# rename columns
newCols = ['DateTime', 'User', 'Action']
df3.columns = newCols


"""
Extract and shorten the name
DON'T do this more than ONCE...
"""
strPrefix = 'User: '
strSuffix = ', card:'

df3.loc[:,(df3.columns[1])] = df3.loc[:,(df3.columns[1])].map(lambda x: x[x.find(strPrefix)+len(strPrefix) : x.find(strSuffix)])
#df3[df3.columns[1]] = df3[df3.columns[1]].map ( lambda x: x[x.find(strPrefix)+len(strPrefix) : x.find(strSuffix)] )
print (df3[df3.columns[1]].head())    

"""
# use SQLite3 to 
## create a db table
## writes to it (permanent "cleased" records)
## read back into a new DataFrame
"""
doorDB = 'DoorAccess.db'

"""
## Opens the named db & obtains the db cursor
"""    
conn = sqlite3.connect (doorDB)
dbCur = conn.cursor()


"""
# opens the table - create if not exists
"""
dbCur.execute ("CREATE TABLE IF NOT EXISTS AccessLog (datestamp TEXT, user TEXT, action TEXT, PRIMARY KEY (user, datestamp))")

"""
# write them out to our own db!
"""
print ('Writing recs to db...')
rowsWritten = 0
maxRow = len (df3)
for i in range(maxRow):
      rowsWritten += writeRec (df3.iloc[i][0], 
                               df3.iloc[i][1],
                               df3.iloc[i][2])

print ('Finished writing to db!')
print ('===> rows written/rows in CSV:', rowsWritten, '/', maxRow)


"""
# close cursor and connection
"""
dbCur.close()
conn.close()

