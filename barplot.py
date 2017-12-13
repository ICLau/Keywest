# -*- coding: utf-8 -*-
"""
Created on Thu Nov 23 21:46:51 2017

@author: Isaac
"""
import numpy as np
import matplotlib.pyplot as plt
import datetime
from datetime import time as tm
import matplotlib.dates as mdates
from matplotlib.dates import DateFormatter
from matplotlib.dates import HourLocator

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
# =============================================================================
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
      
      for i in range(len(y)):
          if yTemp[i] == tm(0,0,0) or yTemp2[i] == tm(0,0,0):
              ymDt[i] = y2mDt[i] = 0
          else:
              y2mDt[i] -= ymDt[i]
                  
      
      fig, ax = plt.subplots()
#      rects1 = ax.bar (ind, ymDt, width, color='r', label="Time In")
      rects2 = ax.bar (ind + width/2, y2mDt, width, bottom=ymDt, color='b', label="Time In/Out")
      
      # add some text for labels, title and axes ticks
      ax.set_ylabel('Time')
      ax.set_title('Median Time In & Out')
      ax.set_xticks(ind + width / 2)
      ax.set_xticklabels(dfUserMedians.index)
      for xTickLabel in ax.xaxis.get_ticklabels():
          xTickLabel.set_rotation (85)
      
      ax.legend()
      #This must be called before the locator/formatter
      ax.yaxis_date()
      ax.yaxis.set_major_locator(HourLocator())
      ax.yaxis.set_major_formatter(DateFormatter('%H:%M'))
      
      #autolabel(rects1)
      #autolabel(rects2)
      
      startHour = 7  # y min at 7am
      minYTime = datetime.datetime.combine (myDay, tm(startHour,0,0))
      maxYTime = datetime.datetime.combine (myDay, tm(23,59))
      plt.ylim (minYTime, maxYTime)
      
      myYTicks = []
      yTickHrApart = 2
      numberOfTicks = int((24-startHour)/yTickHrApart)
      adjustHr = 0 if ((24-startHour) == numberOfTicks * yTickHrApart) else 1
      
      for eachTick in range(numberOfTicks + adjustHr):
          myYTicks.append(datetime.datetime.combine(myDay,tm(eachTick*yTickHrApart+startHour,0,0)))
      
      if (not adjustHr):    # add the 24hr mark to it
          myYTicks.append(datetime.datetime.combine(myDay,tm(23,59)))
          
      plt.yticks (myYTicks)
      
#      plt.grid(axis='y')
      plt.grid()
      plt.subplots_adjust (top=0.9,
                           bottom=0.35,
                           left=0.1,
                           right=0.97,
                           hspace=0.2,
                           wspace=0.2)      
      plt.show()
      
# =============================================================================