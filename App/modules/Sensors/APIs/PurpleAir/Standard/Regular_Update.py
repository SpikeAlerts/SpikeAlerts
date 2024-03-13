# Functions to Get a daily update for Standard PurpleAir Monitors

# Libraries

# Time

import datetime as dt
import pytz # Timezones

# Data Manipulation

import numpy as np
import pandas as pd
import geopandas as gpd

# Database

import modules.Database.Basic_PSQL as psql
from modules.Database.Queries import General as query
from modules.Database.Queries import Sensor as sensor_queries

# Sensors
import modules.Sensors.Sensor_Functions as sensors
import modules.Sensors.APIs.PurpleAir.API_functions as purp

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Workflow(monitor_dict, monitor_api_df, timezone):
    '''
    This function performs a regular update for Standard PurpleAir PM2.5 Sensors in our database
    
    returns sensors_df (pd.DataFrame)
    
    Parameters: monitor_api_df is a dataframe with the following columns:
    
    sensor_id - int - our unique identifier
    sensor_type - text - our sensor_type identifier
    api_id - text - identifier for api
    pollutant - text - abbreviated name for pollutant sensor reads
    metric - text - a unit to append to readings
    thresholds - list - list of 5 floats corresponding to health benchmarks
    radius_meters - int - integer representing a distance a sensor accurately represents (on an average day),
    api_fieldname - string - string to query api for this value
        
    timezone - a timezone for pytz
    
    returned sensors_df fields are:

    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    update_frequency - int - frequency this sensor is updated (in minutes)
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks
    radius_meters - int - max distance sensor is relevant
    is_flagged - binary - is the sensor flagged?
    '''
    
    # Initialize storage
    
    sensors_df = pd.DataFrame(columns = ['sensor_id', 'current_reading', 'update_frequency',
                              'pollutant', 'metric', 'health_descriptor',
                              'radius_meters', 'is_flagged']
                             )
    
    # Iterate through sensors on the monitor
    
    for sensor_type in monitor_api_df.sensor_type.unique():
    
        # Get the sensor_type dictionary for thresholds, pollutant, metric, radius_meters, api_fieldname
        sensor_dict = monitor_dict[sensor_type]
        
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Select the relevant rows from monitor_api_df
        temp_api_df = monitor_api_df[monitor_api_df.sensor_type == sensor_type]

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Load information from PurpleAir by sensor_index
        
        sensor_indices = temp_api_df.api_id.astype(int) # Sensor indices from api_id

        fields = ['sensor_index', 'channel_flags', 'last_seen', sensor_dict['api_fieldname']] # The PurpleAir fields we want
        
        purpleAir_df, runtime = purp.Get_with_sensor_index(sensor_indices, fields, timezone)
        
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Merge purpleAir & temp_api_df
        
        purpleAir_df['api_id'] = purpleAir_df.api_id.astype(str)
        
        merged_df = pd.merge(temp_api_df, purpleAir_df,
                                   on = 'api_id', how = 'outer')
         
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
        # QAQC
        
        merged_df['current_reading'] = merged_df[sensor_dict['api_fieldname']].astype(float) # Convert api_fieldname values into floats and rename to current_reading
        # Perfom QAQC - adds a column called 'flagged'
        merged_df = QAQC(merged_df, timezone)
        
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~                           
        # Format Columns/values for sensors_df
        
        # Values straight from sensor_type dictionary
        
        sensor_dict_vals = ['update_frequency', 'pollutant', 'metric',
                            'radius_meters']
        for key in sensor_dict_vals:
            merged_df[key] = sensor_dict[key]
            
        # Health Descriptor
        
        merged_df['health_descriptor'] = sensors.Map_to_Health_Descriptors(merged_df.current_reading,
                                                                   sensor_dict['thresholds'])
                                                              
        # Flagged status
        
        merged_df['is_flagged'] = merged_df.flagged.copy()
                 
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
        # Concatenate to sensors_df
        temp_sensors_df = merged_df[sensors_df.columns]
        
        sensors_df = pd.concat([sensors_df.astype(temp_sensors_df.dtypes), # if not sensors_df.empty else None,
                                temp_sensors_df.astype(sensors_df.dtypes)], # if not temp_sensors_df.empty else None], 
                                ignore_index = True)
        
        return sensors_df

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~        
def QAQC(merged_df, timezone):
    '''
    This function performs QAQC on merged_df
    
    returns purpleAir_df with an additional column:
    
    flagged - boolean - True = flagged
    '''      
    
    # Get important values as variables for ease of use
    vals = merged_df.current_reading
    last_seens = merged_df.last_seen
    channel_flags = merged_df.channel_flags
    
    # Flags
    is_na = vals.isna()
    is_neg = vals < 0
    is_too_high = vals > 1000
    is_api_flagged = channel_flags != 0
    is_not_seen = last_seens < dt.datetime.now(pytz.timezone(timezone)) - dt.timedelta(minutes=60) # Not seen in past hour
    # Add the above
    is_flagged = is_na + is_neg + is_too_high + is_api_flagged + is_not_seen
    
    # Make new column
    merged_df['flagged'] = is_flagged
    
    return merged_df
