# -*- coding: utf-8 -*-
"""
Created on Sat Dec 23 17:04:45 2017

@author: Isaac
"""
# =============================================================================
# Given a user, plot his/her Time-In/Out distributions
# gets data from userTimes table
# also plots the normal probability distribution best-fits
#
# ASSUMPTIONS:
#     - divide each hour into 4 '15-min' slots
#     - 24 hours per day ==> 24x4=96 slots
# =============================================================================
from datetime import datetime as dt
from datetime import time as tm
import pandas as pd
import numpy as np
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt

import appDB

# =============================================================================
# input arg:
#    timeObj: time object
#    assume we have 4 slots in an hour
# returns:
#     slot # (integer)
def timeSlot (timeObj):
    return ( int (timeObj.minute/15) )

#-- test code - should be deleted
#def grab_data (filename='test3.csv'):
#    df = pd.read_csv(filename, delimiter = ',', parse_dates=True)
#    
#    return df

# =============================================================================
# input arg:
#     strDateTime (str representing a datetime - see the format we use to parse it)
# returns:
#     time object
def timePart (strDateTime):
    tempDT = dt.strptime (strDateTime, '%Y-%m-%d %H:%M')
    return tm(tempDT.hour, tempDT.minute, tempDT.second)

# =============================================================================
# input arg:
#     strTime (str representing a time - '%H:%M:%S' format)
# returns:
#     time object
def makeTimeObj (strTime):
    tempDT = dt.strptime (strTime, '%H:%M:%S')
    return tm(tempDT.hour, tempDT.minute, tempDT.second)

# =============================================================================
# input arg:
#     tSlot - int: slot number 
#         if we have 24 hours and 4 slots/hr, then this input param must be 0..96:
#     sInOneHr - int: # of slots in one hour
#         currently, we hardwired it to 4
# returns:
#     str - a formatted string used by x-ticker label
def formatLabel (tSlot, sInOneHr):  # usually it is 4 slots in an hour (15min)
    tH = int(tSlot / sInOneHr)
    tM = int(tSlot % sInOneHr) * (int(60/sInOneHr))
    
    isAM = True
    if (tH == 12 and tM == 0):
        return "noon"
    elif tH > 12:
            tH -= 12
            isAM = False
    
    return "{0}:{1:02}{2}".format(tH, tM, "am" if isAM else "pm")
            
# =============================================================================
def plotUserTimeDistributions (user):
    conn = appDB.connectBadgeDB()

    if (type(user).__name__ == 'int'):
        userRow = appDB.getUserById(conn, user)
    else:
        userRow = appDB.getUserByName(conn, user)
    if userRow == None:
        return
    
    user = userRow[1]
    
    timeIns  = appDB.fetchAllUserTimes (conn, user, 'In')
    timeOuts = appDB.fetchAllUserTimes (conn, user, 'Out')
    
    minDate = appDB.selectMINDate(conn)
    maxDate = appDB.selectMAXDate(conn)

    appDB.disconnectDB (conn)

    tIns  = list(map(lambda t: t[2], timeIns))
    tOuts = list(map(lambda t: t[2], timeOuts))

    tObjIns  = list(map(makeTimeObj, tIns))
    tObjOuts = list(map(makeTimeObj, tOuts))

    buckets = 24
    slotsPerBucket = 5
    minPerSlot = 60./slotsPerBucket

    inSlots  = list (map(lambda t: t.hour*slotsPerBucket+int(t.minute/minPerSlot), tObjIns))
    outSlots = list (map(lambda t: t.hour*slotsPerBucket+int(t.minute/minPerSlot), tObjOuts))
    
    inLabel  = "In "  + formatLabel(np.median(inSlots) , slotsPerBucket)
    outLabel = "Out " + formatLabel(np.median(outSlots), slotsPerBucket)
    
    fig, ax = plt.subplots()
    nIn , binsIn , patchesIn  = ax.hist(inSlots , buckets*slotsPerBucket, label=inLabel)
    nOut, binsOut, patchesOut = ax.hist(outSlots, buckets*slotsPerBucket, label=outLabel)

    #best fits
    beginsAt = 5 * slotsPerBucket     # 5am * slotsPerBucket
    endsAt   = (12+8)*slotsPerBucket  # 8pm * slotsPerBucket
    incrementsAt = 30/96
    myBinsIn = np.arange(beginsAt, endsAt, incrementsAt)
    
    # vCounts is used to "scale" up the npd curves
    vCounts   = ((pd.Series(inSlots) ).value_counts()).iat[0]
    vCountOut = ((pd.Series(outSlots)).value_counts()).iat[0]
    if (vCountOut > vCounts):
        vCounts = vCountOut
    
    yIn = mlab.normpdf(myBinsIn, np.median(inSlots), np.std(inSlots))
    ax.plot(myBinsIn, yIn*vCounts*10, '--')
    
    yOut = mlab.normpdf(myBinsIn, np.median(outSlots), np.std(outSlots))
    ax.plot(myBinsIn, yOut*vCounts*10,'--')

#
    plt.xlim (beginsAt, endsAt)
    ind = np.arange(beginsAt,endsAt+slotsPerBucket,slotsPerBucket )
    xiterSlots = [slotsPerBucket]*len(ind)
    xlabels = list( map(formatLabel, ind, xiterSlots) )
    ax.set_xticks(ind)
    ax.set_xticklabels(xlabels)
    for xTickLabel in ax.xaxis.get_ticklabels():
        xTickLabel.set_rotation (85)
#
    ax.legend()

    ax.set_title(user + 
                 '\n' +
                 minDate.strftime('%b-%d, %Y') +
                 ' to '+
                 maxDate.strftime('%b-%d, %Y') )
    
    fig.tight_layout()
    plt.show()
#


# =============================================================================


if (__name__ == '__main__'):
    plotUserTimeDistributions (5)
    plotUserTimeDistributions (19)
    plotUserTimeDistributions('Isaac Lau')
    plotUserTimeDistributions('Guest Two')
