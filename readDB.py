# -*- coding: utf-8 -*-
"""
Created on Sun Nov 12 16:02:32 2017

@author: Isaac
"""

import pandas as pd
import numpy as np
from datetime import datetime as dt
from datetime import date as dt2
from datetime import time as tm
import statistics

import barplot
import exports
import appDB
import appLogging as appLog
import readIni as appIni


# =============================================================================
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
def assembleRawStats (dfBadging, userList, sortAscending=True):
    appLog.logMsg(__name__, appLog._iINFO, "assembleRawStats <Begins>")

    userBadging = []
    for eachUser in userList:
        _datePart = []
        _timePart = []
        _timePartInSec = []
      
        ##
        ## For checkin, need to take the "earliest" time for the day
        ## For checkout, need to take the "lastest" time for the day
        ##
        df_user = dfBadging[dfBadging[colNames[1]].isin([eachUser])]
        df_user = df_user.sort_values (by=colNames[0], ascending=sortAscending)
        workingDate = dt2(1970,1,1)

        for eachBadgeEvent in df_user[colNames[0]]:
            evDate, evTime = breakupDateTime (convToDateTimeObj (eachBadgeEvent))
            if (not isSameDay(workingDate, evDate)):
                _datePart.append(evDate)
                _timePart.append(evTime)
                _timePartInSec.append( convTimeObjToSeconds(evTime) )
                
                workingDate = evDate

        ## now add these tuples into the list
        _median_checkin = convSecondsToTimeObj (statistics.median (_timePartInSec))
        userBadging.append ([eachUser, _datePart, _timePart, _timePartInSec, _median_checkin])

    appLog.logMsg(__name__, appLog._iINFO, "assembleRawStats <Ends>")
    return userBadging      

# =============================================================================
# input arg - strSearch
#     usually this is read and parsed from config.ini 
#      -- we want to remove specific users from our dataframe that are not employees
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
#    dfThis = dataframe - must contain a named column "User" (colNames[1] variable)
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
def loadDataFrameFromDB():
    dbConn = appDB.connectBadgeDB()
    assert (dbConn != None)
    
    # Reads everything in from DB into a DataFrame
    df = pd.DataFrame(appDB.readAllBadgeRecords(dbConn))
    appLog.logMsg(__name__,
                  appLog._iINFO,
                  "# of records read from db = {0}".format(len(df)))
    
    # Closes the DB
    appDB.disconnectDB(dbConn)

    # set df col names
    df.columns = colNames
    return df

# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# this utitlity function can also be implemented within assembleRawStats()
# However, it is cleaner to have assembleRawStats() to do just one thing --
# leaving this separate utility to flush the Daily In/Out times and user to the 
# DB
#  
#    userDailyStats - the complex list we are tracking
#    aTag - access Tag --> "In" or "Out"
# =============================================================================
def saveUserDailyBadgeTime(userDailyStats, aTag):
    assert (aTag != None)
    assert (type(aTag).__name__ == 'str')
    assert (aTag.strip() != '')
    assert (aTag == 'In' or aTag == 'Out')
    
    theNames = list(map(lambda n: n[0].strip(), userDailyStats))
    theTimes  = list(map(lambda dude: [dude[2]], userDailyStats))
    
    conn = appDB.connectBadgeDB()
    for eachDude in theNames:
        # is this dude already in 'users' table
        userRow = appDB.getUserByName(conn, eachDude)
        if userRow == None:
            rowid = appDB.insertUser (conn, eachDude)
        else:
            rowid = userRow[0]
    
        # make sure rowid is valid
        idx = theNames.index(eachDude)
        if (rowid != None and rowid > 0):
            userTimes = theTimes[idx][0]
            for eachTime in userTimes:
                appDB.insertUserTime (conn, (rowid, "{0:%H}:{0:%M}:{0:%S}".format(eachTime), aTag))
        
    appDB.disconnectDB(conn)
    
# =============================================================================
# this function assumes the input dataframe is fresh from db
#    with 3 columns: 'DateTime', 'User', 'Action'
# - will split it into 2 groups - In and Out
# - will refresh and populate the "cached" lists --  _cachedUserStatsIn, _cachedUserStatsOut
#    
#    if input is None AND one of the cached list is empty, then we will have to 
#    refresh the cache from the db!!!
# =============================================================================
def collectStats (dfThis=None):
    global _cachedUserStatsIn, _cachedUserStatsOut
    appLog.logMsg (__name__, appLog._iINFO, "collectStats - <Begins>")

    if (dfThis is None and
                (len(_cachedUserStatsIn) == 0 and len(_cachedUserStatsOut) == 0)):
        dfThis = loadDataFrameFromDB()
    
    # knocks out the exclude users
    dfThis = removeUsers (dfThis, excludeExactUsers, excludeUsersContain)

    # applies filter to "Action" column
    df_In = dfThis[ dfThis[colNames[2]].isin([fltrBadgedIn]) ]
    
    # drop the "Action" column, no need to carry it around
    del df_In[colNames[2]]
    
    df_Out = dfThis[ dfThis[colNames[2]].isin([fltrBadgedOut]) ]
    del df_Out[colNames[2]]
    
    userCountsIn = df_In[colNames[1]].value_counts()
    user_daily_checkin = assembleRawStats (df_In, userCountsIn.index)
    saveUserDailyBadgeTime (user_daily_checkin, "In")
    
    userCountsOut = df_Out[colNames[1]].value_counts()
    user_daily_checkout = assembleRawStats (df_Out, userCountsOut.index, sortAscending=False)      
    saveUserDailyBadgeTime (user_daily_checkout, "Out")
    
    # save to cache
    _cachedUserStatsIn  = user_daily_checkin.copy()
    _cachedUserStatsOut = user_daily_checkout.copy()

    appLog.logMsg (__name__,
                   appLog._iINFO,
                   "collectStats - <Ends>")


# =============================================================================
#    input arg: none
#    returns: dataframe - user median timeIn and timeOut
# =============================================================================
def analyzeUsersMedian ():
    appLog.logMsg (__name__, appLog._iINFO, "analyzeUsersMedian - <Begins>")
    
    imax = len(_cachedUserStatsIn)
    timein_users = [_cachedUserStatsIn[i][0] for i in range(imax)]
    timein_median = [_cachedUserStatsIn[i][4] for i in range(imax)]
    
    imax= len(_cachedUserStatsOut)
    timeout_users = [_cachedUserStatsOut[i][0] for i in range(imax)]
    timeout_median = [_cachedUserStatsOut[i][4] for i in range(imax)]

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
    
    appLog.logMsg (__name__,
                   appLog._iINFO,
                   "analyzeUsersMedian - <Ends>")
    
    return dfMerged


# =============================================================================
# main
# =============================================================================

# Initialize some variables
# =============================================================================
# read config.ini
# =============================================================================
appLog.logMsg(__name__, appLog._iINFO, "Init...<Begins>")

sectionNames =   {   'Inputs'          : 'Inputs',
                     'Users'           : 'Users',
                     'Exports'         : 'Exports'
                 }

keyNames =       {   'badgeIn'         : 'badgeIn',
                     'badgeOut'        : 'badgeOut',
                     'exclude'         : 'exclude',
                     'exportFile'      : 'exportFile',
                     'exportFileName'  : 'exportFileName'        
                 }

defaultFilters = {   'In'              :'Access Granted',
                     'Out'             :'Exit Granted',
                     'ExportFileName'  : 'Badge In-Out Median.csv'
                 }

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

# do we want to export dataframe to csv, if so - what's the output csv filename
bExport = False
bOK, exportFile = appIni.get_sectionKeyValues (sectionNames['Exports'], keyNames['exportFile'])
if (bOK == False):
    print ("[INI] section '{0}', name '{1}' not found. defaulting to '{2}'".format(sectionNames['Exports'], 
              keyNames['exportFile']),
              'No export')
else:
    exportFile = exportFile.strip()
    if (exportFile != '' and exportFile.startswith(tuple('1tTyY')) ):
        bExport = True

if (bExport):
    bOK, expFileName = appIni.get_sectionKeyValues (sectionNames['Exports'], keyNames['exportFileName'])
    if (bOK == False):
        print ("[INI] section '{0}', name '{1}' not found. defaulting to '{2}'".format(sectionNames['Exports'], 
                  keyNames['exportFile']),
                  defaultFilters['ExportFileName'])
       
    if (expFileName is None or expFileName.strip() == ''):
        expFileName = defaultFilters['ExportFileName']
    


# hard wired the column names
colNames = ['DateTime', 'User', 'Action']

# cached user stats
_cachedUserStatsIn = []
_cachedUserStatsOut = []

appLog.logMsg(__name__, appLog._iINFO, "Init...<Ends>")

# Self test ---
if (__name__ == '__main__'):
    dfDB = loadDataFrameFromDB()

    # remove users that we don't care about
    # not really need to since collectStats() will do removeUsers()
    # this is just to demonstrate that a clien can use this helper
    dfEmployees = removeUsers (dfDB, excludeExactUsers, excludeUsersContain)

    # assemble and cache user stats
    # if called without df argument, we may load the data from db (if one or both of the cached list is empty)
    # That means the client code can just call this without doing the 2 calls before
    collectStats (dfEmployees)

    # analyzes each employee's median time in & out 
    dfUsersMedian = analyzeUsersMedian ()
    
    barplot.BarPlotTime(dfUsersMedian)

    if (bExport):
        appLog.logMsg (__name__,
                   appLog._iINFO,
                   "Exporting 'median' dataframe to '{0}'".format(expFileName))
        
        exports.export2CSV (dfUsersMedian, expFileName)

#--- Test: exporting the _cache dataframe --- <Begin>
    if (False):
#
# It would be far easier to just deal with In & Out individually
#        
        # Extracts the time part from the lists
        
        namesIn  = list(map(lambda n: n[0], _cachedUserStatsIn))
        namesOut = list(map(lambda n: n[0], _cachedUserStatsOut))
        
        xIn  = list(map(lambda dude: [dude[2]], _cachedUserStatsIn))
        xOut = list(map(lambda dude: [dude[2]], _cachedUserStatsOut))
        
        dfxIn  = pd.DataFrame(xIn, index=namesIn, columns=['In'])
        dfxOut = pd.DataFrame(xOut, index=namesOut, columns=['Out'])
        dfxMerged = pd.merge(dfxIn, dfxOut, 
                             how='outer',
                             left_index=True,
                             right_index=True)
        
        # remove NaN from merged dataframe
    #    dfxMerged.fillna(tm(0,0,0), inplace=True)
        
        dudes = dfxMerged.index
        for eachDude in dudes:
            print ("name: {0}".format(eachDude))
            inLen = -1
            outLen = -1
            if (type(dfxMerged['In'][eachDude]).__name__ == 'list'):
                inLen = len( dfxMerged['In'][eachDude] )
            else:
                dfxMerged['In'][eachDude] = []
                
            if (type(dfxMerged['Out'][eachDude]).__name__ == 'list'):
                outLen = len( dfxMerged['Out'][eachDude] )
            else:
                dfxMerged['Out'][eachDude] = []
    
            
            print ("len of In = {0}, Out = {1}".format(inLen, outLen))
        
        
        
        
    #    for eachDude in _cachedUserStatsIn:
    #        print ( "Dude's name: '{0}', # of Badged In: {1}".format(eachDude[0],
    #                                                                len(eachDude[2])) )
    #        
    #        
    #        exports.export2CSV(pd.DataFrame(eachDude[2]), "Badged--{0}--In.csv".format(eachDude[0]))
    #    
    #    for eachDude in _cachedUserStatsOut:
    #        print ( "Dude's name: '{0}', # of Badged Out: {1}".format(eachDude[0],
    #                                                                len(eachDude[2])) )
    #        
    #        
    #        exports.export2CSV(pd.DataFrame(eachDude[2]), "Badged--{0}--Out.csv".format(eachDude[0]))
       
    
#--- Test: exporting the _cache dataframe --- <End>
