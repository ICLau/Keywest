# -*- coding: utf-8 -*-
"""
Created on Sun Nov 12 16:02:32 2017

@author: Isaac
"""

import pandas as pd
from datetime import datetime as dt
from datetime import date as dt2
from datetime import time as tm
import statistics

import barplot
import exports
import appDB
import appLogging as appLog
import readIni as appIni

# select datestamp from AccessLog order by datestamp asc limit 5

def convTimeObjToSeconds (_tObj):
      return (_tObj.hour * 3600 +
              _tObj.minute * 60 +
              _tObj.second)

# =============================================================================
def convSecondsToTimeObj (_seconds):
      ss = int (_seconds % 60)
      mm = int (_seconds / 60)
      hh = int (mm / 60)
      mm %= 60
      return (tm(hh, mm, ss))

# =============================================================================
def convToDateTimeObj (strDateT):
    return dt.strptime (strDateT, '%Y-%m-%d %H:%M:%S')

# =============================================================================
### True if both datetime objects' year, month and day matches
### False otherwise
def isSameDay (d1, d2):
    ## need to assert that both d1 and d2 are datetime objects
    return (d1.year  == d2.year and
            d1.month == d2.month and
            d1.day   == d2.day)

# =============================================================================
def breakupDateTime (d):
      return (dt2(d.year, d.month, d.day),
              tm(d.hour, d.minute, d.second))

# =============================================================================
### Input args:
###   dfBadging - (input) dataframe, either badge-in or badge-out from db
###   userlist - (input) list, unique users (the index of userCounts)
###   strOut - (input) for debugging purposes (default = 'Checkin')
###   sortAscending - (input) True for checkin, False (must be passed in) for checkout
### 
### Return:
###   userBadging - list, e.g. user_daily_checkin, as a return list for plotting
###   
###
### - split the date and time part as we parse each row for each user
### - [('username'),({date},[time])]
### 
### ** Note ** 
### This is an overkill to use userBadging this way - it is a complex list 
### - each list element contains 5 tuples
###   - name of user - str (size 1)
###   - list of datetime.date (representing the day of checkin/checkout)
###   - list of corresponding earliest/latest time of the above date checkin/checkout (size is the same as above)
###   - list of corresponding time (checkin/checkout) in seconds (size is the same as above) - so we can calculate the median (below)
###   - median time of checkin/checkout (size 1)
### 
def assembleStats (dfBadging, userList, strOut='Checkin', sortAscending=True):
#      userList = userCounts.index
      userBadging = []
      for eachUser in userList:
            _datePart = []
            _timePart = []
            _timePartInSec = []
      
            df_user = dfBadging[dfBadging['User'].isin([eachUser])]
            df_user = df_user.sort_values (by='DateTime', ascending=sortAscending)
            workingDate = dt2(1970,1,1)
            
#            print ([eachUser], '-' *4, strOut, 'Date/Time', (len(df_user)))
            ##
            ## For checkin, need to take the "earliest" time for the day
            ## For checkout, need to take the "lastest" time for the day
            ##
            for i in range(len(df_user)):
                  ck_in_date, ck_in_time = breakupDateTime (convToDateTimeObj(df_user.iat[i,0]))
                  if (not isSameDay (workingDate, ck_in_date)):
                        _datePart.append(ck_in_date)
                        _timePart.append(ck_in_time)
                        _timePartInSec.append (convTimeObjToSeconds(ck_in_time))
#                        print (ck_in_date, '~'*3, ck_in_time)
                        workingDate = ck_in_date
                        
      
            ## now add these tuples into the list
            _median_checkin = convSecondsToTimeObj (statistics.median (_timePartInSec))
            userBadging.append ([eachUser, _datePart, _timePart, _timePartInSec, _median_checkin])
            
      return userBadging      

# =============================================================================
# input arg - strSearch
#     a search string - if the beginning and/or end is a '*' - return True and a copy of the string without leading & trailing '*'
#     otherwise returns False
def isSubstringSearch (strSearch):
    bSubString = False
    iStart = 0
    iEnd = len(strSearch)
    if (strSearch.startswith('*')):
        iStart = 1
        bSubString = True
    
    if (strSearch.endswith('*')):
        iEnd -= 1
        bSubString = True

    return bSubString, strSearch[iStart:iEnd]

# =============================================================================
#    dfThis = dataframe - must contain a column named "User" (colNames[1] variable)
#    exactMatch, containMatch - lists that contain users to be removed from 'dfThis'
def removeUsers (dfThis, exactMatch, containMatch):

    # for exact matches, use .isin() - case sensitive
    if (len(exactMatch) > 0):
        dfThis = dfThis[ ~ dfThis[colNames[1]].isin(exactMatch) ]
    
    # for substring or wild card elimination, it will be case INsensitive
    if (len(excludeUsersContain) > 0):
        for eachExclUser in containMatch:
            dfThis = dfThis[ ~ dfThis[colNames[1]].str.contains (eachExclUser, case=False) ]

    return dfThis

# =============================================================================
# main
# =============================================================================

dbConn = appDB.connectBadgeDB()
assert (dbConn != None)

#doorDB = 'DoorAccess.db'
#conn = sqlite3.connect (doorDB)
#dbCur = conn.cursor()
#
#dbCur.execute ("CREATE TABLE IF NOT EXISTS AccessLog (datestamp TEXT, user TEXT, action TEXT, PRIMARY KEY (user, datestamp))")

# =============================================================================
# Reads everything in from DB into a DataFrame
# =============================================================================
#sqlStmt = str.format ('SELECT datestamp, user, action FROM AccessLog')
#dbCur.execute (sqlStmt)
#
#df = pd.DataFrame (dbCur.fetchall())
df = pd.DataFrame(appDB.readAllBadgeRecords(dbConn))
appLog.logMsg(__name__,
              appLog._iINFO,
              "# of records read from db = {0}".format(len(df)))

# =============================================================================
# Closes the DB
# =============================================================================
appDB.disconnectDB(dbConn)

# =============================================================================
# read config.ini
# =============================================================================
sectionNames = {'Inputs' : 'Inputs',
                'Users'  : 'Users',
                'Exports' : 'Exports'}
keyNames = {'badgeIn'  : 'badgeIn',
            'badgeOut' : 'badgeOut',
            'exclude'  : 'exclude',
            'exportFile' : 'exportFile',
            'exportFileName' : 'exportFileName'}
defaultFilters = {'In':'Access Granted',
                  'Out'  :'Exit Granted',
                  'ExportFileName' : 'Badge In-Out Median.csv'}

# sets the Badge-In filter string
bOK, fltrBadgedIn = appIni.get_sectionKeyValues (sectionNames['Inputs'], keyNames['badgeIn'])
if (bOK == False):
    print ("[INI] section '{0}', name '{1}' not found. defaulting to '{2}'".format(sectionNames['Inputs'], 
              keyNames['badgeIn']),
              defaultFilters['In'])
   
if (fltrBadgedIn is None or fltrBadgedIn.strip() == ''):
    fltrBadgedIn = defaultFilters['In']

# sets the Badge-Out filter string
bOK, fltrBadgedOut = appIni.get_sectionKeyValues (sectionNames['Inputs'], keyNames['badgeOut'])
if (bOK == False):
    print ("[INI] section '{0}', name '{1}' not found. defaulting to '{3}'".format(sectionNames['Inputs'], 
               keyNames['badgeOut']))
   
if (fltrBadgedOut is None or fltrBadgedOut.strip() == ''):
    fltrBadgedOut = defaultFilters['Out']

# gets the list of exclude users
excludeUsers = []
bOK, temp = appIni.get_sectionKeyValues (sectionNames['Users'], keyNames['exclude'])
if (bOK == True and len(temp) != 0):
    # temp is an instance of str if exclude=abc
    # temp is an instance of list if exclude=abc,def
    if ('str' == type(temp).__name__):
        excludeUsers = [temp]
    else:
        excludeUsers = temp

# split the exclusion list into 2 groups
excludeExactUsers = []
excludeUsersContain = []
for eachExcludeUser in excludeUsers:
    bContainSearch, tempUser = isSubstringSearch(eachExcludeUser)
    if (bContainSearch):
        excludeUsersContain.append(tempUser)
    else:
        excludeExactUsers.append(tempUser)



# set df col names
colNames = ['DateTime', 'User', 'Action']
df.columns = colNames

df_In = df[df[colNames[2]].isin([fltrBadgedIn])]        # filter badging in action
df_In = df_In[[colNames[0], colNames[1]]]               # drop the action col - no need to carry it around

# knocks out the exclude users
df_In = removeUsers (df_In, excludeExactUsers, excludeUsersContain)


df_Out = df[df[colNames[2]].isin([fltrBadgedOut])]    # filter badging out action
df_Out = df_Out[[colNames[0], colNames[1]]]           # drop the action column - all related to badging out
df_Out = removeUsers (df_Out, excludeExactUsers, excludeUsersContain)

userCounts = df_In[colNames[1]].value_counts()
#print ("="*10, 'In', [len(userCounts)], '='*10)
#print (userCounts)

print ("-" * 80)
user_daily_checkin = []
user_daily_checkout = []

user_daily_checkin = assembleStats (df_In, userCounts.index, 'CheckIn')

userCounts = df_Out[colNames[1]].value_counts()
#print ("="*10, 'Out',[len(userCounts)], '='*10)
#print (userCounts)

user_daily_checkout = assembleStats (df_Out, userCounts.index, strOut='CheckOut', sortAscending=False)      

imax = len(user_daily_checkin)
timein_users = [user_daily_checkin[i][0] for i in range(imax)]
timein_median = [user_daily_checkin[i][4] for i in range(imax)]

imax= len(user_daily_checkout)
timeout_users = [user_daily_checkout[i][0] for i in range(imax)]
timeout_median = [user_daily_checkout[i][4] for i in range(imax)]

### merge the list before plotting
sMedianTimeIn  = 'MedianTimeIn'
sMedianTimeOut = 'MedianTimeOut'
dfUserTimeIn  = pd.DataFrame(timein_median,  index=timein_users,  columns=[sMedianTimeIn])
dfUserTimeOut = pd.DataFrame(timeout_median, index=timeout_users, columns=[sMedianTimeOut])
dfMerged = pd.merge (dfUserTimeIn, dfUserTimeOut, 
                     how='outer',
                     left_index=True,
                     right_index=True)

# remove NaN from merged dataframe
dfMerged.fillna(tm(0,0,0), inplace=True)

barplot.BarPlotTime(dfMerged)

# exports the merged dataframe to csv
bExport = False
bOK, exportFile = appIni.get_sectionKeyValues (sectionNames['Exports'], keyNames['exportFile'])
if (bOK == False):
    print ("[INI] section '{0}', name '{1}' not found. defaulting to '{2}'".format(sectionNames['Exports'], 
              keyNames['exportFile']),
              'No export')
else:
    exportFile = exportFile.strip()
    if (exportFile != '' and exportFile.startswith(tuple('1tT')) ):
        bExport = True
    
if (bExport):
    bOK, expFileName = appIni.get_sectionKeyValues (sectionNames['Exports'], keyNames['exportFileName'])
    if (bOK == False):
        print ("[INI] section '{0}', name '{1}' not found. defaulting to '{2}'".format(sectionNames['Exports'], 
                  keyNames['exportFile']),
                  defaultFilters['ExportFileName'])
       
    if (expFileName is None or expFileName.strip() == ''):
        expFileName = defaultFilters['ExportFileName']
    
    appLog.logMsg (__name__,
                   appLog._iINFO,
                   "Exporting 'median' dataframe to '{0}'".format(expFileName))
    exports.export2CSV (dfMerged, expFileName)
