# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 21:46:51 2017

@author: Isaac
"""
import sys
import numpy as np
import matplotlib.pyplot as plt
import datetime
from datetime import time as tm
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from matplotlib.dates import HourLocator
from io import BytesIO
import xml.etree.ElementTree as ET

import appLogging as appLog
import readIni as appIni
import appDB

# =============================================================================
# def autolabel(rects):
#     """
#     Attach a text label above each bar displaying its height
#     """
#     for rect in rects:
#         height = rect.get_height()
#         print('h=',height)
#         ax.text(rect.get_x() + rect.get_width()/2., 
#                 1.03*height,
#                 '%d' % int(height),
#                 ha='center', 
#                 va='bottom',
#                 rotation='vertical')
# 
# =============================================================================

# =============================================================================
# BarPlotTime (dfUserMedians)
# Arg:
# - dfUserMedians - input
#   - index = username (unique)
#   - col0 = median checkin time (DateTime Time obj - no date part)
#   - col1 = median checkout time (DateTime Time obj - no date part)
# =============================================================================

# --- Test codes ---    
tRects = None
tt_labels = []
names = None
mTimeFrom = None
mTimeTo = None

ET.register_namespace("", "http://www.w3.org/2000/svg")

# --- Test codes ---    

def BarPlotTime (dfUserMedians):
    n = len(dfUserMedians)
    ind = np.arange(n)  # the x locations for the groups
    width = 0.8         # the width of the bars
      
    myDay = datetime.date (2017,1,1)    # static date to assemble a datetime object for matplotlib barplot
    yTemp  = dfUserMedians[dfUserMedians.columns[0]].tolist()
    yTemp2 = dfUserMedians[dfUserMedians.columns[1]].tolist()
    y  = [datetime.datetime.combine(myDay, t) for t in yTemp]
    y2 = [datetime.datetime.combine(myDay, t) for t in yTemp2]

    # MUST use matplotlib "numeric" dates
    ymDt  = [mdates.date2num(d) for d in y]
    y2mDt = [mdates.date2num(d) for d in y2]
      
    missingTime = tm(0,0,0)
    for i in range(len(y)):
        if yTemp[i] == missingTime or yTemp2[i] == missingTime:
            ymDt[i] = y2mDt[i] = 0
        else:
            y2mDt[i] -= ymDt[i]
                  
      
    fig, ax = plt.subplots()
#      rects1 = ax.bar (ind, ymDt, width, color='r', label="Time In")
    rects2 = ax.bar (ind + width/2, y2mDt, width, bottom=ymDt, color='b', label="Time In/Out")
    
      
    # add some text for labels, title and axes ticks
    ax.set_ylabel('Time')
    ax.set_title('Median Time In & Out (' + 
                    minDate.strftime('%b-%d, %Y') +
                    ' to '+
                    maxDate.strftime('%b-%d, %Y') +
                    ')')
    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(dfUserMedians.index)
    for xTickLabel in ax.xaxis.get_ticklabels():
        xTickLabel.set_rotation (85)
      
    ax.legend()
    #This must be called before the locator/formatter
    ax.yaxis_date()
    ax.yaxis.set_major_locator(HourLocator())
    ax.yaxis.set_major_formatter(DateFormatter('%I:%M%p'))
      
    #autolabel(rects1)
    #autolabel(rects2)
      
    startHour = iStartHour  # y min at 7am
    endHour   = iEndHour    # used to be 23:59, but now is configurable via ini, defaults to 7pm
    minYTime = datetime.datetime.combine (myDay, tm(startHour,0,0))
    maxYTime = datetime.datetime.combine (myDay, tm(endHour,0,0))
    plt.ylim (minYTime, maxYTime)
      
    myYTicks = []
    yTickHrApart = iHoursApart
    numberOfTicks = int((endHour-startHour)/(yTickHrApart))+1
    for eachTick in range(numberOfTicks):
        myYTicks.append(datetime.datetime.combine(myDay,tm(eachTick*yTickHrApart+startHour,0,0)))
      
    plt.yticks (myYTicks)
      
    if (iShowGrid == 1): 
        plt.grid(axis='x', linestyle='solid')
    elif (iShowGrid == 2):
        plt.grid(axis='y', linestyle='solid')
    elif (iShowGrid == 3):
        plt.grid(axis='both', linestyle='solid')
    else:
        plt.grid(b=None)


    shadeBegins = datetime.datetime.combine(myDay, tm(iShadeStartHour,0,0))
    shadeEnds   = datetime.datetime.combine(myDay, tm(iShadeEndHour  ,0,0))
    
    # alpha indicates transparency
    plt.axhspan(shadeBegins, shadeEnds, facecolor='0.8', alpha=0.3)

    
# --- Test codes <Begins> ---  
    if (False):
        global tRects, tt_labels, names, mTimeFrom, mTimeTo
        tRects = rects2
        
        names = (dfUserMedians.index).tolist()
        mTimeFrom = yTemp.copy()
        mTimeTo = yTemp2.copy()
        
        # find a better way to assemble the tooltip text
        for i in range(len(names)):
            tt_labels.append( "{0}\n{1:%I}:{1:%M}{1:%p}\n{2:%I}:{2:%M}{2:%p}".format(names[i],mTimeFrom[i],mTimeTo[i]) )
        
        for i, (item, label) in enumerate(zip(tRects, tt_labels)):
            patch = ax.add_patch(item)
            annotate = ax.annotate(tt_labels[i], xy=item.get_xy(), xytext=(0, 0),
                                   textcoords='offset points', color='w', ha='left',
                                   fontsize=9, bbox=dict(boxstyle='round, pad=.5',
                                                         fc=(.1, .1, .1, .92),
                                                         ec=(1., 1., 1.), lw=1,
                                                         zorder=1))
        
            ax.add_patch(patch)
            patch.set_gid('mypatch_{:03d}'.format(i))
            annotate.set_gid('mytooltip_{:03d}'.format(i))
    
        # Save the figure in a fake file object
        f = BytesIO()
        plt.savefig(f, format="svg")
    
        # --- Add interactivity ---
        
        # Create XML tree from the SVG file.
        tree, xmlid = ET.XMLID(f.getvalue())
        tree.set('onload', 'init(evt)')
    
        for i in tRects:
            # Get the index of the shape
            index = tRects.index(i)
            # Hide the tooltips
            if i._height > 0.:
                tooltip = xmlid['mytooltip_{:03d}'.format(index)]
                tooltip.set('visibility', 'hidden')
                # Assign onmouseover and onmouseout callbacks to patches.
                mypatch = xmlid['mypatch_{:03d}'.format(index)]
                mypatch.set('onmouseover', "ShowTooltip(this)")
                mypatch.set('onmouseout', "HideTooltip(this)")
    
        # This is the script defining the ShowTooltip and HideTooltip functions.
        script = """
            <script type="text/ecmascript">
            <![CDATA[
        
            function init(evt) {
                if ( window.svgDocument == null ) {
                    svgDocument = evt.target.ownerDocument;
                    }
                }
        
            function ShowTooltip(obj) {
                var cur = obj.id.split("_")[1];
                var tip = svgDocument.getElementById('mytooltip_' + cur);
                tip.setAttribute('visibility',"visible")
                }
        
            function HideTooltip(obj) {
                var cur = obj.id.split("_")[1];
                var tip = svgDocument.getElementById('mytooltip_' + cur);
                tip.setAttribute('visibility',"hidden")
                }
        
            ]]>
            </script>
            """
         # Insert the script at the top of the file and save it.
        tree.insert(0, ET.XML(script))
        ET.ElementTree(tree).write('Employee Median Time-in-out.svg')
    
# --- Test codes <Ends> ---    
    fig.tight_layout()
    plt.show()
      
# =============================================================================
# main
# =============================================================================
modName = __name__
if (__name__ == '__main__'):
    modName = "{0}({1})".format(sys.argv[0], __name__)

# initialize some variables
dbConn = appDB.connectBadgeDB()
minDate = appDB.selectMINDate(dbConn)
maxDate = appDB.selectMAXDate(dbConn)
appDB.disconnectDB(dbConn)

appLog.logMsg (modName,
               appLog._iINFO,
               "minDate = {0}".format(minDate.strftime('%b-%d, %Y')))
appLog.logMsg (modName,
               appLog._iINFO,
               "maxDate = {0}".format(maxDate.strftime('%b-%d, %Y')))

# read from config.ini
iniSection = {'BarGraph' : 'BarGraph'}
iniKey = {'startHour'     : 'startHour',
          'endHour'       : 'endHour',
          'hoursApart'    : 'hoursApart',
          'showGrid'      : 'showGrid',     # showGrid = 0 (no grid), 1 (vertical lines), 2 (horizontal lines), 3 (both)
          'shadeStartHour':'shadeStartHour',
          'shadeEndHour'  :'shadeEndHour'}
defaultValue =  {'startHour'      : 7,
                 'endHour'        : 12+7,
                 'hoursApart'     : 1,
                 'showGrid'       : 3,
                 'shadeStartHour' : 9,
                 'shadeEndHour'   : 12+5}

iStartHour = defaultValue['startHour']
iEndHour   = defaultValue['endHour']
iHoursApart = defaultValue['hoursApart']
iShowGrid = defaultValue['showGrid']
iShadeStartHour = defaultValue['shadeStartHour']
iShadeEndHour = defaultValue['shadeEndHour']

bOK, tempStr = appIni.get_sectionKeyValues(iniSection['BarGraph'],iniKey['startHour'])
if (bOK == True and tempStr is not None and tempStr.strip() != ''):
    tempHr = abs(int(float(tempStr)/1.0))
    if (tempHr <= 20): # kind of silly to start plotting at 8pm
        iStartHour = tempHr
#print("iStartHour = {0}".format(iStartHour))
    
bOK, tempStr = appIni.get_sectionKeyValues(iniSection['BarGraph'], iniKey['endHour'])
if (bOK == True and tempStr is not None and tempStr.strip() != ''):
    tempHr = abs(int(float(tempStr)/1.0))
    if (tempHr > 7 and tempHr <= 23):
        iEndHour = tempHr
#print("iEndHour = {0}".format(iEndHour))

bOK, tempStr = appIni.get_sectionKeyValues(iniSection['BarGraph'], iniKey['hoursApart'])
if (bOK == True and tempStr is not None and tempStr.strip() != ''):
    tempHr = abs(int(float(tempStr)/1.0))
    if (tempHr > 0 and tempHr <= 5): # silly to have 5 hours apart
        iHoursApart = tempHr
#print("iHoursApart = {0}".format(iHoursApart))

# hoping that (iEndHour-iStartHour) is divisible by iHoursApart
bResetToDefaults = False
if (iStartHour >= iEndHour):
    bResetToDefaults = True
elif ((iEndHour - iStartHour) < iHoursApart): # (15:00-12:00) with 4 hrs apart
    bResetToDefaults = True

if (bResetToDefaults):
    iStartHour = defaultValue['startHour']
    iEndHour   = defaultValue['endHour']
    iHoursApart = defaultValue['hoursApart']

#print("bResetToDefaults = {0}".format(bResetToDefaults))

bOK, tempStr = appIni.get_sectionKeyValues(iniSection['BarGraph'], iniKey['showGrid'])
if (bOK == True and tempStr is not None and tempStr.strip() != ''):
    tempHr = abs(int(float(tempStr)/1.0))
    if (tempHr <= 3):
        iShowGrid = tempHr
#print("iShowGrid = {0}".format(iShowGrid))

bOK, tempStr = appIni.get_sectionKeyValues(iniSection['BarGraph'], iniKey['shadeStartHour'])
if (bOK == True and tempStr is not None and tempStr.strip() != ''):
    tempHr = abs(int(float(tempStr)/1.0))
    if (tempHr > iStartHour and tempHr < iEndHour):
        iShadeStartHour = tempHr
#print("iShadeStartHour = {0}".format(iShadeStartHour))

bOK, tempStr = appIni.get_sectionKeyValues(iniSection['BarGraph'], iniKey['shadeEndHour'])
if (bOK == True and tempStr is not None and tempStr.strip() != ''):
    tempHr = abs(int(float(tempStr)/1.0))
    if (tempHr > iStartHour and tempHr < iEndHour and tempHr > iShadeStartHour):
        iShadeEndHour = tempHr
#print("iShadeEndHour = {0}".format(iShadeEndHour))



# =============================================================================
# =============================================================================
# =============================================================================
# self test and diagnostics
if (__name__ == '__main__'):
    
    import pandas as pd

    users = ['Albert',
             'Betty',
             'Charles',
             'David',
             'Edmond',
             'Fiona',
             'George',
             'Heidi',
             'Ilia',
             'Joanna']
             
    meanTime = [  [tm(7,50), tm(17,50)],
                  [tm(8,33), tm(17,50)],
                  [tm(9,12), tm(0,0)  ],
                  [tm(10,15),tm(18,12)],
                  [tm(7,55), tm(14,15)],
                  [tm(8,10), tm(16,55)],
                  [tm(9,22), tm(18,10)],
                  [tm(10,34),tm(17,22)],
                  [tm(0,0),  tm(17,34)],
                  [tm(8,45), tm(16,45)]
                ]
 
                

    print ('size of users = {0} meanTime = {1}'.format(len(users), len(meanTime)))
    print("iStartHour = {0}".format(iStartHour))
    print("iEndHour = {0}".format(iEndHour))
    print("iHoursApart = {0}".format(iHoursApart))
    print("bResetToDefaults = {0}".format(bResetToDefaults))
    print("iShowGrid = {0}".format(iShowGrid))
    print("iShadeStartHour = {0}".format(iShadeStartHour))
    print("iShadeEndHour = {0}".format(iShadeEndHour))

    dfTest = pd.DataFrame(meanTime, index=users,  columns=['MedianTimeIn','MedianTimeOut'])
    BarPlotTime(dfTest)
    
