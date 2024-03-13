### Import Packages

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 


from modules.Database.Queries import Sensor as sensor_query
from modules.Database.Queries import Alert as alert_query

# Data Manipulation

import numpy as np
import pandas as pd

# Importing Libraries
from importlib import import_module

## Workflow

def workflow(runtime, timezone):
    '''
    Runs the full workflow to get data from the apis  
    
    returns sensors_df (pd.DataFrame), sensor_types_to_update (set of strings related to sensor_type)

    sensors_df fields are:

    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    update_frequency - int - frequency this sensor is updated (in minutes)
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks
    radius_meters - int - max distance sensor is relevant
    is_flagged - binary - is the sensor flagged?
    '''
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    
    # 0 - Initialize storage of above

    sensors_df = pd.DataFrame(columns = ['sensor_id', 'current_reading', 'update_frequency',
                              'pollutant', 'metric', 'health_descriptor',
                              'radius_meters', 'is_flagged']
                             )

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    # 1 - Prepare for API calls
    
    # 1.a - Get the sensor_types ready for update (from "Sensor Type Information")
    #       All sensors with runtime <= last_update + update_frequency
    
    sensor_types_ready_to_update = sensor_query.Get_Sensor_Types_Ready_to_Update(runtime)
    
    # 1.b Get a dataframe with info necessary for api calls
    # Not flagged (channel_flags = 0, channel_state = 1)
    
    api_df = sensor_query.Get_Sensor_Info(fields = ['sensor_id', 'sensor_type', 'api_id'], sensor_types = sensor_types_ready_to_update,
                                          channel_flags=[0], channel_states = [1])

    # 1.c See which sensor_types_ready_to_update are active
    
    sensor_types_to_update = set(api_df.sensor_type.unique()) # The types of sensors that are active (a set)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    # 2 - Query all the apis (if they have a type in 1.d)

    sensor_api_dict = sensor_query.Get_Sensor_APIs_Information() # information on the different types of apis/monitors/sensors

    for api_name in sensor_api_dict:

        for monitor_name in sensor_api_dict[api_name]:

            # Monitor Dictionary with sensor_types, update_frequencies, thresholds, metric, radius_meters, and api_fieldnames
            monitor_dict = sensor_api_dict[api_name][monitor_name]
            
            # Get the sensor_types to update for this monitor
            monitor_sensor_types_to_update = sensor_types_to_update.intersection(monitor_dict.keys())

            # Do we need to call this monitor type?
            
            if len(monitor_sensor_types_to_update) > 0:
            
                # If yes, Get a dataframe with information for this specific api call
            
                # 2.a - Select from api_df (1.a)
                
                monitor_api_df = api_df[api_df.sensor_type.isin(monitor_sensor_types_to_update)] # Select from greater api df
            
                # Get the module name
                module = f'modules.Sensors.APIs.{api_name}.{monitor_name}.Regular_Update'
    
                # 2.b Call APIs for a regular update
                temp_sensors_df = import_module(module).Workflow(monitor_dict, monitor_api_df,
                                             timezone) # Run Regular Update code for monitor
                                             
                # Concatenate to sensors_df
                sensors_df = pd.concat([sensors_df.astype(temp_sensors_df.dtypes), # if not sensors_df.empty else None,
                                    temp_sensors_df.astype(sensors_df.dtypes)],
                                    ignore_index = True)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Check if there was any to update
    
    if len(sensors_df) > 0:
    
        sensors_df = sensors_df.copy()
    
    else:
        print('\n~~~\nWarning: No sensors in database to update. \n\nPlease wait a little longer for a regular update\nor conduct a daily update to pull new sensors from APIs\n~~~\n')

    return sensors_df, sensor_types_to_update
