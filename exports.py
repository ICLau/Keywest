# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 15:05:39 2017

@author: Isaac
"""
import pandas as pd


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
def export2CSV (dfExport, fname='Badge In-Out Average.csv'):
    dfExport.to_csv (fname)


