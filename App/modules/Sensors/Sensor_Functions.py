# Functions to work with any sensor in our database

# Data Manipulation

import pandas as pd

# Database

from psycopg2 import sql
import modules.Database.Basic_PSQL as psql
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~        
def Map_to_Health_Descriptors(values, thresholds):
    '''
    This function maps a pandas series of values to health descriptors
    
    Assumes that 0 is the lowest possible value of sensor (SUBJECT TO CHANGE in future)
    
    parameters
    
    values - pandas series of floats - raw sensor value with same health benchmarks
    thresholds - list - list of 7 values dividing the following health descriptors (left inclusive)
    
    ERROR (too low), good, moderate, unhealthy for sensitive groups, unhealthy, very unhealthy, hazardous, ERROR (too high)
    
    returns a pandas series with above descriptors
    '''      
    
    health_descriptors = ['ERROR (too low)', 'good', 'moderate', 'unhealthy for sensitive groups',
                    'unhealthy', 'very unhealthy', 'hazardous', 'ERROR (too high)']
                    
    bins = [-float('inf')] + thresholds + [float('inf')]
    
    descriptor_series = pd.cut(values, bins,
                 right = False, include_lowest = True,
                 labels = health_descriptors)

    return descriptor_series
        
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
def Flag_channel_states(sensor_ids):
    '''
    
    To be used on sensors that haven't been seen in a while...

    Parameters: sensor_ids (a list of integers) from "Sensors" table
    
    Sets all channel_states to zero and channel_flags to 3
    '''
    
    cmd = sql.SQL('''UPDATE "Sensors"
SET channel_state = 0, channel_flags = 3
WHERE sensor_id = ANY ( {} );
    ''').format(sql.Literal(sensor_ids))
    
    psql.send_update(cmd)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Update_Sensors(correct_df):
    '''
    Updates any fields in our database to match the input dataframe

    Parameters:

    correct_df - pd.dataframe with the following specs
    
    Fields needed in this dataframe are:

    'sensor_id' (integer)
    and
    any of the following fields:
    
	name - str
	date_created - str - datetime.strftime('%Y-%m-%d %H:%M:%S')
	last_seen - str - datetime.strftime('%Y-%m-%d %H:%M:%S')
	last_elevated - str - datetime.strftime('%Y-%m-%d %H:%M:%S')
    channel_state - pd.series.astype("Int64")
    channel_flags - pd.series.astype("Int64")
	altitude - pd.series.astype("Int64")
    '''

    psql.update_table(correct_df, 'Sensors', 'sensor_id')

# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
