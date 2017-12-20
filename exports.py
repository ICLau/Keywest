# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 15:05:39 2017

@author: Isaac
"""
import readIni as appIni

expFileName = ''
# =============================================================================
# export to csv
# Input args:
# - dfExport - dataframe with the following columns
#   - username
#   - datetime.time (median checkin time)
#   - datetime.time (median checkout time)
# - fname - CSV filename (with default value)
#
# Output: None
# =============================================================================
def export2CSV (dfExport, fname=expFileName):
    dfExport.to_csv (fname)

# =============================================================================

# =============================================================================
#  Grab the settings from config.ini - set defaults if missing
# =============================================================================
sectionNames = {'Exports':'Exports'}
keyNames = {'exportFile':'exportFile'}
defaultFilters = {'ExportFileName':'Badge In-Out Median.csv'}

bOK, expFileName = appIni.get_sectionKeyValues (sectionNames['Exports'], keyNames['exportFile'])
if (bOK == False):
    print ("[INI] section '{0}', name '{1}' not found. defaulting to '{2}'".format(sectionNames['Exports'], 
              keyNames['exportFile']),
              defaultFilters['ExportFileName'])
   
if (expFileName is None or expFileName.strip() == ''):
    expFileName = defaultFilters['ExportFileName']

