# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 08:53:53 2017

@author: Isaac
"""
import sys
import sqlite3
from sqlite3 import Error
from datetime import datetime as dt
from datetime import time as tm

import readIni as myIni
import appLogging as appLog

dbConnList = []
_maxConn = 5

defaultDBName = 'DoorAccess.db'
defaultTableName = 'AccessLog'
bOK, doorDB = myIni.get_sectionKeyValues ('CleansedData', 'dbFile')
if (bOK == False or doorDB.strip() ==''):
    doorDB = defaultDBName

# table = AccessLog
strCreateTableSQL = "CREATE TABLE IF NOT EXISTS {0} (datestamp TEXT, user TEXT, action TEXT, PRIMARY KEY (user, datestamp))".format(defaultTableName)
strCheckDateTimeUserSQL = "SELECT datestamp, user FROM AccessLog WHERE datestamp = '{0}' AND user = '{1}'"
strInsertBadgeRecSQL = "INSERT INTO AccessLog (datestamp, user, action) VALUES (?, ?, ?) "
strReadAllRecs = 'SELECT datestamp, user, action FROM {0}'.format(defaultTableName)
strSelectMINdateSQL = "select MIN(datestamp) from {0}".format(defaultTableName)
strSelectMAXdateSQL = "select MAX(datestamp) from {0}".format(defaultTableName)

# table = user
defaultUserTable = 'user'
strCreateUserTableSQL = """CREATE TABLE IF NOT EXISTS `users` (
                        	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
                        	`name`	TEXT NOT NULL UNIQUE
                        );"""
strFindUserSQL = "SELECT id, name FROM users WHERE name LIKE '{0}' "
strFindUserByIdSQL = "SELECT id, name FROM users WHERE id = {0} "
strInsertUserSQL = "INSERT INTO users (name) VALUES (?) "
strGetAllUsersSQL = 'SELECT id, name FROM users '


# table = userTimes
defaultUserTimesTable = 'userTimes'
strCreateUserTimesTableSQL = """ CREATE TABLE IF NOT EXISTS `userTimes` (
                            	`id`	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                            	`fk_userid`	INTEGER NOT NULL,
                            	`time_io`	TEXT NOT NULL,
                            	`t_type`	TEXT NOT NULL,
                                FOREIGN KEY (fk_userid) REFERENCES users (id)
                            ); """


# =============================================================================
def disconnectDB (conn):
    global dbConnList
    
    if (conn not in dbConnList):
        appLog.logMsg (modName, 
                       appLog._iCRITICAL, 
                       "!!! calling disconnectDB() with invalid dbConnection")
        #should throw an exception here
    
    idx = dbConnList.index(conn)
    dbConn = dbConnList[idx]
    
    dbCur = (dbConnList[idx]).cursor()
    dbConnList.remove(dbConnList[idx])

    dbCur.close()
    dbConn.close()
    appLog.logMsg (modName, 
                   appLog._iINFO, 
                   "~~ disconnectDB() - connectList length = {0}".format(len(dbConnList)))
    
# =============================================================================
""" creates a db table from the createTableSQL
param dbConn - connection obj
param createTableSQL - a CREATE TABLE statement
"""
def createTable (dbConn, createTableSQL):
    try: 
        dbCur = dbConn.cursor()
        dbCur.execute (createTableSQL)
    except Error as e:
        appLog.logMsg(modName, 
                      appLog._iCRITICAL,
                      "CRITICAL exception - createTable()")
        print (e)


# =============================================================================
def connectDB (dbName):
    global dbConnList
    
    if (len(dbConnList) >= _maxConn):
        appLog.logMsg (modName, 
                       appLog._iCRITICAL, 
                       "!!! max db connections reached [{0}]".format(_maxConn))
        return None

# need more checking on db connection and cursor
    
    dbConn = sqlite3.connect (dbName)
    dbConnList.append (dbConn)

    createTable (dbConn, strCreateTableSQL)
    createTable (dbConn, strCreateUserTableSQL)
    createTable (dbConn, strCreateUserTimesTableSQL)
    
    appLog.logMsg(modName,
                  appLog._iINFO,
                  "~~~ db connected ({0})".format(dbName))
    return dbConn

# =============================================================================
# returns a date object
# =============================================================================
def selectMINDate (dbConn, selectMINDateSQL=strSelectMINdateSQL):
    return (selectMINDateTime(dbConn, selectMINDateSQL)).date()

# =============================================================================
# returns a datetime object
# =============================================================================
def selectMINDateTime (dbConn, selectMINDateSQL=strSelectMINdateSQL):
    try:
        dbCur = dbConn.cursor()
        dbCur.execute (selectMINDateSQL)
        sDateTime  = (dbCur.fetchone())[0]
        minDateTime = dt.strptime(sDateTime, '%Y-%m-%d %H:%M:%S')
        return minDateTime
    except Error as e:
        appLog.logMsg(modName,
                      appLog._iCRITICAL,
                      "CRITICAl exception - selectMINDateTime()")
        print (e)
        
# =============================================================================
# returns a date object
# =============================================================================
def selectMAXDate (dbConn, selectMAXDateSQL=strSelectMAXdateSQL):
    return (selectMINDateTime(dbConn, selectMAXDateSQL)).date()

# =============================================================================
# returns a datetime object
# =============================================================================
def selectMAXDateTime (dbConn, selectMAXDateSQL=strSelectMAXdateSQL):
    try:
        dbCur = dbConn.cursor()
        dbCur.execute (selectMAXDateSQL)
        sDateTime  = (dbCur.fetchone())[0]
        maxDateTime = dt.strptime(sDateTime, '%Y-%m-%d %H:%M:%S')
        return maxDateTime
    except Error as e:
        appLog.logMsg(modName,
                      appLog._iCRITICAL,
                      "CRITICAl exception - selectMAXDateTime()")
        print (e)
        
# =============================================================================
# returns True or False
# Checks to see if the query comes back with a record.
# Does the record already exist in db?
# =============================================================================
def recordExist (dbConn, selectSQL):
    try:
        dbCur = dbConn.cursor()
        dbCur.execute (selectSQL)
        rows = dbCur.fetchone()
        return False if rows == None else True
    except Error as e:
        appLog.logMsg(modName,
                      appLog._iCRITICAL,
                      "CRITICAl exception - recordExist()")
        print (e)

# =============================================================================
# insertRecord
# dbConn - db connection
# strSQL - SQL statement
# tVar - (tuple) variables to insert (must match the # of ? in strSQL)
# =============================================================================
def insertRecord (dbConn, strSQL, tVar):
    try:
        dbCur = dbConn.cursor()
        dbCur.execute (strSQL, tVar)
        dbConn.commit()
        return dbCur.lastrowid
    except Error as e:
        appLog.logMsg(modName,
                      appLog._iCRITICAL,
                      "CRITICAl exception - insertRecord()")
        print(e)

# =============================================================================
# specifically check the datetime + user exists in db
# dbConn - db connection
# tVars - (tuple) (datetime obj, user str)
#
#    this will use strCheckDateTimeUserSQL str as the query
# =============================================================================
def badgeEventExist(dbConn, tVars):
    assert (len(tVars) > 1)
    strSQL = strCheckDateTimeUserSQL.format(tVars[0], tVars[1])

    return recordExist(dbConn, strSQL)    

# =============================================================================
#    for clients accessing DoorAccess.db - not needing to know the dbName
#    returning a dbConnection for client to keep so they can close the connection
# =============================================================================
def connectBadgeDB ():
    return (connectDB(doorDB))

# =============================================================================
# insertBadgeRecord ()
# dbConn - dbConnection obj
# tVars - (tuple) - must be (datetime obj, userName str, Access str)
# ** will make user of strInsertBadgeRecSQL statement which has THREE ?
#
# returns 
#   0 if something went wrong - client code should check the return value
#   otherwise returns lastrowid
# =============================================================================
def insertBadgeRecord (dbConn, tVars):
    assert (len(tVars) > 2)
    
    if not badgeEventExist (dbConn, tVars):
        return insertRecord (dbConn, strInsertBadgeRecSQL, tVars)

    appLog.logMsg(modName,
                  appLog._iDEBUG,
                  "insertBadgeRecord() -- Badge record {0}/{1} already exists in db - NOT inserted.".format(tVars[0], tVars[1]))
    return 0

# =============================================================================
#    readAllBadgeRecords ()
#    - dbConn - dbConnection obj
#    returns
#    all records
#    --- uses strReadAllRecs SQL statement
# =============================================================================
def readAllBadgeRecords (dbConn):
    dbCur = dbConn.cursor()
    dbCur.execute(strReadAllRecs)
    return dbCur.fetchall()

# =============================================================================
# user table
# =============================================================================
    
def userExist(dbConn, username):
    assert (username.strip() != '')
    strSQL = strFindUserSQL.format(username)

    return recordExist(dbConn, strSQL)    


# =============================================================================
#    input arg: username - str
#    returns: unique rowId of user
#    otherwise: None
def insertUser (dbConn, username):
    assert (username.strip() != '')
    
    try:
        username = username.strip()
        if not userExist (dbConn, username):
            return insertRecord (dbConn, strInsertUserSQL, (username,))
        
        appLog.logMsg(modName,
                      appLog._iDEBUG,
                      "insertUser() - user '{0}' already exists in db - NOT inserted.".format(username))
        return None
    except Error as e:
        appLog.logMsg(modName,
                      appLog._iCRITICAL,
                      "CRITICAl exception - insertUser()")
        print(e)
        

# =============================================================================
#    input arg: username - str
#    returns: 
#        row - tuple (int:rowId, str:username)
#        None if not found or error
def getUserByName (dbConn, username):
    username = username.strip()
    assert (username != '')
    strSQL = strFindUserSQL.format(username)

    try:
        dbCur = dbConn.cursor()
        dbCur.execute (strSQL)
        rows = dbCur.fetchone()
        return rows
    except Error as e:
        appLog.logMsg(modName,
                      appLog._iCRITICAL,
                      "CRITICAl exception - getUserByName()")
        print (e)
        return None

# =============================================================================
#        input arg: int:userId
#        returns:
#            row - tuple (int:rowId, str:username)
#            None if not found or error
def getUserById (dbConn, userId):
    assert (userId > 0)
    strSQL = strFindUserByIdSQL.format(userId)

    try:
        dbCur = dbConn.cursor()
        dbCur.execute (strSQL)
        rows = dbCur.fetchone()
        return rows
    except Error as e:
        appLog.logMsg(modName, appLog._iCRITICAL, "CRITICAl exception - getUserById()")
        print (e)
        return None

# =============================================================================
#        Need testing
def fetchAllUsers (dbConn):
    dbCur = dbConn.cursor()
    dbCur.execute(strGetAllUsersSQL)
    return dbCur.fetchall()



# =============================================================================
# userTimes table
# =============================================================================
# =============================================================================
#    Need testing
#    fetches all userTime records for ONE particular user
#    input args:
#        user = can be int:userID or str:username
#        aType = str 
#            default is None - which fetches both "in" and "Out" Access Types
#            "In" - only fetches "In" Access Type for this user
#            "Out" - only fetches "Out" Access Type for this user
#            ?? unrecognized aType str will set it to Default (None)
#    return:
#        list - tuple (id, fk_userId, time_io, t_type)
#        None if error
def fetchAllUserTimes (conn, user, aType=None):
    bUseId = True if type(user).__name__ == 'int' else False
    if (bUseId):
        userRow = getUserById(conn, user)
    else:
        userRow = getUserByName(conn, user)
    
    if (userRow == None or userRow == 0):
        return None

    # aType == None (default) => retieves both "In" and "Out" access types
    # aType should either be "In" or "Out" -- value stored in db
    if (aType != None):
        if (type(aType).__name__ != 'str' and (aType != 'In' and aType != 'Out')):
            aType = None
    strSelectUserTimesSQL = "SELECT id, fk_userid, time_io, t_type FROM userTimes WHERE fk_userid = {0} "
    strSQL = strSelectUserTimesSQL.format(userRow[0])
    
    if aType != None:
        strAddPredicate = " AND t_type = '{0}'  "
        if aType == 'In' or aType == 'Out':
            strSQL = strSQL + strAddPredicate.format(aType)

    # look up the times from userTimes table and return a list
    try:
        dbCur = conn.cursor()
        dbCur.execute(strSQL)
        rows = dbCur.fetchall()
        return rows
    except Error as e:
        appLog.logMsg(modName, appLog._iCRITICAL, "CRITICAL exception - fetchAllUserTimes()")
        print (e)
    
# =============================================================================
# =============================================================================
# check to see if a userTime record exist in db
#  input arg: tVars => tuple (usernameORuserId, strTime, "In")
#  return: tuple (int:rowId, int:fk_userId, str:time_io, str:t_type)
#          None if not found or error
def userTimeExist (conn, tVars):
    user = tVars[0]
    bUseId = True if type(user).__name__ == 'int' else False
    if (bUseId):
        userRow = getUserById(conn, user)
    else:
        userRow = getUserByName(conn, user)
    
    if (userRow == None or userRow == 0):
        return None

    userId = userRow[0]
    strSQL = "SELECT id, fk_userid, time_io, t_type FROM userTimes WHERE fk_userid = {0} AND time_io = '{1}' AND t_type = '{2}' ".format(userId, tVars[1], tVars[2])
    try:
        dbCur = conn.cursor()
        dbCur.execute(strSQL)
        row = dbCur.fetchone()
        return row
    except Error as e:
        appLog.logMsg(modName, appLog._iCRITICAL, "CRITICAL exception - userTimeExist()")
        print (e)
        return None

# =============================================================================
#    input arg
#        tVars - tuple (int or str:userId, str:strTime, str:aTag)
#    return
#        rowId of inserted record
#        None if error or NOT inserted (already exists in db)
#        
#    NOTE: if useId in input arg is an Id, BUT the user itself is NOT in db,
#          then we are NOT able to insert the user for the client code since we
#          NEED a username in string to do that!
def insertUserTime(conn, tVars):
    # tVars must be (userId, strTime, "In")
    assert (tVars != None)
    assert (len(tVars) == 3)
    
    # check to see if this entry is already in db
    rows = userTimeExist (conn, tVars)
    if (rows == None): # userTime doesn't exist, insert it

        # if user not exists, insert the user        
        if (type(tVars[0]).__name__ != 'int'):
            userRow = getUserByName(conn, tVars[0])
        else:
            userRow = getUserById (conn, tVars[0])
        
        if userRow == None:
            # didn't find the user
            # if tVars[0] is an ID, we cannot insert a user with ID, need a name!!!
            if (type(tVars[0]).__name__ == 'int'):
                appLog.logMsg(modName, appLog._iERROR, "insertUserTime() - userID passed in but User not found in db. tVars[{0}]".format(tVars))
                return None
            fkId = insertUser (conn, tVars[0])
        else:
            fkId = userRow[0]
        
        # insert userTime record
        # now we have the user_fkId
        strInsertUserTimeSQL = "INSERT INTO userTimes (fk_userid, time_io, t_type) VALUES (?, ?, ?) "
        ttVars = (fkId,tVars[1],tVars[2])
        return insertRecord (conn, strInsertUserTimeSQL, ttVars)
        
    else:  # userTime already exists in db, do NOT insert again
        appLog.logMsg(modName, appLog._iDEBUG, "insertUserTime() - record already in DB: tVars:({0})".format(tVars))
        
    return None
    
        
        
# =============================================================================
# =============================================================================
# =============================================================================
modName = __name__
if (__name__ == '__main__'):
    modName = "{0}({1})".format(sys.argv[0], __name__)

    conn = connectBadgeDB()
    
    #--- Test AccessLog table
    bTestAccessLogTable = False
    if (bTestAccessLogTable):
        dbTableName = 'AccessLog'
        selectMINdateSQL = "select MIN(datestamp) from {0}".format(dbTableName)
        minDate = selectMINDate (conn, selectMINdateSQL)
        print ("min Date from db is: '{0}'".format(minDate))
    
        minDate = selectMINDateTime (conn, selectMINdateSQL)
        print ("min DateTime from db is: '{0}'".format(minDate))
    
        print ("dbConnList len = {0}".format(len(dbConnList)))
        print ("Maximum db connections = {0}".format(_maxConn))
    
        selectMAXdateSQL = "select MAX(datestamp) from {0}".format(dbTableName)
        maxDate = selectMAXDate (conn, selectMAXdateSQL)
        print ("max Date from db is: '{0}'".format(maxDate))
    
        maxDate = selectMAXDateTime (conn, selectMAXdateSQL)
        print ("max DateTime from db is: '{0}'".format(maxDate))
    
        userName = 'Temp-User5'
        dTime = dt(2017,12,25,13,14)
        bExist = badgeEventExist (conn, (dTime,userName))
        
        if (not bExist):
    #        insertSQL = 'INSERT INTO AccessLog (datestamp, user, action) VALUES (?, ?, ?)  '
            tVar = (dt(2017,12,25,13,14),userName,'Access Granted')
    #        lastrowid = insertRecord(conn, strInsertBadgeRecSQL, tVar)
            lastrowid = insertBadgeRecord (conn, tVar)
            print ("lastrowid = {0}".format(lastrowid))
        else:
            print("badge event : '{0}' '{1}' already exists in db".format(dTime, userName))
    
    #--------------------------------------------------------------------------
    # Test user table
    bTestUserTable = False
    if (bTestUserTable):
        userRow = getUserByName(conn, '*!*')
        print("getUserByName(conn, '*!*') - returns: {0}".format(userRow))
        
        userRow = getUserById (conn, 10000)
        print("getUserById (conn, 10000) - returns: {0}".format(userRow))
        
        testUser = '  Lee C   '
        rowid = insertUser (conn, testUser)
        print ("*"*20)
        print ("rowId inserted = {}".format(rowid))
        
        if (rowid == None):
            # user already exists, get it by name
            print ("   getUserByName()")
            row = getUserByName (conn, testUser)
        else:
            # inserted successfully, get it back by id
            print ("   getUserById()")
            row = getUserById (conn, rowid)
    
        print ('-' * 20)
        print (row)
        print ('-' * 20)
    
        rows = fetchAllUsers(conn)
        print ('fetchAllUsers() - {0} rows returned'.format(len(rows)))
    #    print (rows)

    #--------------------------------------------------------------------------
    # Test: userTime table
    bTestUserTimeTable = True
    if (bTestUserTimeTable):
        print ('+*'*15)
        tTime = tm(11,11,11)
        sTime = "{0:%H}:{0:%M}:{0:%S}".format(tTime)
        userId = 5
        aTag = 'In'
        tVars = (userId, sTime, aTag)
        rowId = insertUserTime(conn, tVars)
        print ("insertUserTime() - {0}, rowId returned = {1}".format(tVars, rowId))
    
        #-- invalid userId
        userId = 50
        tVars = (userId, sTime, aTag)
        rowId = insertUserTime(conn, tVars)
        print ("insertUserTime() - {0}, rowId returned = {1}".format(tVars, rowId))
    
        #-- new user
        username = 'Test Insert userTime'
        tVars = (username, sTime, aTag)
        rowId = insertUserTime(conn, tVars)
        print ("insertUserTime() - {0}, rowId returned = {1}".format(tVars, rowId))
    
        utRows = fetchAllUserTimes (conn, username, aType=None)
        print ("# of records for '{0}' is: {1}".format(username, len(utRows)))
    
        userId = 5
        utRows = fetchAllUserTimes (conn, userId)
        assert (utRows != None)
        print ("# of records for '{0}' is: {1}".format(getUserById(conn,userId), len(utRows)))

    #--------------------------------------------------------------------------
    # Housekeeping - close db connection
    disconnectDB (conn)
    print ("~Called disconnectDB(), now dbConnList len is: {0}".format(len(dbConnList)))
