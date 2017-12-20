# -*- coding: utf-8 -*-
"""
Created on Tue Dec 19 08:53:53 2017

@author: Isaac
"""

import sqlite3
from sqlite3 import Error
from datetime import datetime as dt

import readIni as myIni
import appLogging as appLog

dbConnList = []
_maxConn = 5

defaultDBName = 'DoorAccess.db'
defaultTableName = 'AccessLog'
bOK, doorDB = myIni.get_sectionKeyValues ('CleansedData', 'dbFile')
if (bOK == False or doorDB.strip() ==''):
    doorDB = defaultDBName

strCreateTableSQL = "CREATE TABLE IF NOT EXISTS {0} (datestamp TEXT, user TEXT, action TEXT, PRIMARY KEY (user, datestamp))".format(defaultTableName)
strCheckDateTimeUserSQL = "SELECT datestamp, user FROM AccessLog WHERE datestamp = '{0}' AND user = '{1}'"
strInsertBadgeRecSQL = "INSERT INTO AccessLog (datestamp, user, action) VALUES (?, ?, ?) "
strReadAllRecs = 'SELECT datestamp, user, action FROM {0}'.format(defaultTableName)

# =============================================================================
def disconnectDB (conn):
    global dbConnList
    
    if (conn not in dbConnList):
        appLog.logMsg (__name__, 
                       appLog._iCRITICAL, 
                       "!!! calling disconnectDB() with invalid dbConnection")
        #should throw an exception here
    
    idx = dbConnList.index(conn)
    dbConn = dbConnList[idx]
    
    dbCur = (dbConnList[idx]).cursor()
    dbConnList.remove(dbConnList[idx])

    dbCur.close()
    dbConn.close()
    appLog.logMsg (__name__, 
                   appLog._iINFO, 
                   "~~ disconnectDB() - connectList length = {0}".format(len(dbConnList)))
    
# =============================================================================
""" creates a db table from the createTableSQL
oaram dbConn - connection obj
param createTableSQL - a CREATE TABLE statement
"""
def createTable (dbConn, createTableSQL):
    try: 
        dbCur = dbConn.cursor()
        dbCur.execute (createTableSQL)
    except Error as e:
        appLog.logMsg(__name__, 
                      appLog._iCRITICAL,
                      "CRITICAL exception - createTable()")
        print (e)


# =============================================================================
def connectDB (dbName):
    global dbConnList
    
    if (len(dbConnList) >= _maxConn):
        appLog.logMsg (__name__, 
                       appLog._iCRITICAL, 
                       "!!! max db connections reached [{0}]".format(_maxConn))
        return None

# need more checking on db connection and cursor
    
    dbConn = sqlite3.connect (dbName)
    dbConnList.append (dbConn)

    createTable (dbConn, strCreateTableSQL)
    appLog.logMsg(__name__,
                  appLog._iINFO,
                  "~~~ db connected ({0})".format(dbName))
    return dbConn

# =============================================================================
# returns a date object
# =============================================================================
def selectMINDate (dbConn, selectMINDateSQL):
    return (selectMINDateTime(dbConn, selectMINDateSQL)).date()

# =============================================================================
# returns a datetime object
# =============================================================================
def selectMINDateTime (dbConn, selectMINDateSQL):
    try:
        dbCur = dbConn.cursor()
        dbCur.execute (selectMINDateSQL)
        sDateTime  = (dbCur.fetchone())[0]
        minDateTime = dt.strptime(sDateTime, '%Y-%m-%d %H:%M:%S')
        return minDateTime
    except Error as e:
        appLog.logMsg(__name__,
                      appLog._iCRITICAL,
                      "CRITICAl exception - selectMINDateTime()")
        print (e)
        
# =============================================================================
# returns a date object
# =============================================================================
def selectMAXDate (dbConn, selectMAXDateSQL):
    return (selectMINDateTime(dbConn, selectMAXDateSQL)).date()

# =============================================================================
# returns a datetime object
# =============================================================================
def selectMAXDateTime (dbConn, selectMAXDateSQL):
    try:
        dbCur = dbConn.cursor()
        dbCur.execute (selectMAXDateSQL)
        sDateTime  = (dbCur.fetchone())[0]
        maxDateTime = dt.strptime(sDateTime, '%Y-%m-%d %H:%M:%S')
        return maxDateTime
    except Error as e:
        appLog.logMsg(__name__,
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
        appLog.logMsg(__name__,
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
#    if (recordExist(dbConn, ))
    dbCur = dbConn.cursor()
    dbCur.execute (strSQL, tVar)
    dbConn.commit()
    return dbCur.lastrowid

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

    appLog.logMsg(__name__,
                  appLog._iDEBUG,
                  "insertBadgeRecord() -- Badge record {0}/{1} already exists in db - NOT inserted".format(tVars[0], tVars[1]))
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

if (__name__ == '__main__'):
    conn = connectDB('DoorAccess.db')
    
    createTableSQL = """CREATE TABLE IF NOT EXISTS AccessLog (
                           datestamp TEXT, 
                           user TEXT, 
                           action TEXT, 
                           PRIMARY KEY (user, datestamp)
                        );"""

    createTable (conn, createTableSQL)

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
    bExist = badgeEventExist (conn, (dt(2017,12,25,13,14),userName, userName))
    
    if (False):
#        insertSQL = 'INSERT INTO AccessLog (datestamp, user, action) VALUES (?, ?, ?)  '
        tVar = (dt(2017,12,25,13,14),userName,'Access Granted')
#        lastrowid = insertRecord(conn, strInsertBadgeRecSQL, tVar)
        lastrowid = insertBadgeRecord (conn, tVar)
    
    disconnectDB (conn)
    print ("~Called disconnectDB(), now dbConnList len is: {0}".format(len(dbConnList)))

