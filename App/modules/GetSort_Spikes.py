### Import Packages

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 


from modules.Queries import Sensor as sensor_query
from modules.Queries import Alert as alert_query

# Data Manipulation

import numpy as np
import pandas as pd

# Importing Libraries
from importlib import import_module

## Workflow

def workflow(base_config):
    '''
    Runs the full workflow to get data from the apis and begins interpretation of the information.  
    
    returns sensors_df (pd.DataFrame) and runtime (datetime timestamp)

    sensors_df fields are:

    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    update_frequency - int - frequency this sensor is updated
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks
    radius_meters - int - max distance sensor is relevant
    is_flagged - binary - is the sensor flagged?
    sensor_status - text - one of these categories: ordinary, new_spike, ongoing_spike, ended_spike, 
    '''
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    
    runtime = dt.datetime.now(pytz.timezone(base_config['TIMEZONE']))
    
    # 0 - Initialize storage of above w/out sensor_status and one extra column 'last_elevated' used to determine sensor_status later

    sensors_df = pd.DataFrame(columns = ['sensor_id', 'current_reading', 'update_frequency',
                              'pollutant', 'metric', 'health_descriptor',
                              'radius_meters', 'is_flagged', 'last_elevated']
                             )

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    # 1 - Prepare for API calls
    
    # 1.a - Get the sensor_types ready for update (from "Sensor Type Information")
    #       All sensors with runtime < last_update + update_frequency
    
    sensor_types_ready_to_update = sensor_query.Get_Sensor_Types_Ready_to_Update(runtime)
    
    # 1.b Get a dataframe with info necessary for api calls
    # Not flagged (channel_flags = 0, channel_state = 1)
    
    api_df = sensor_query.Get_Sensor_Info(fields = ['sensor_id', 'sensor_type', 'api_id', 'last_elevated'], sensor_types = sensor_types_ready_to_update,
                                          channel_flags=[0], channel_states = [1])

    # 1.c See which sensor_types_ready_to_update are active
    
    sensor_types_to_update = set(api_df.sensor_type.unique()) # The types of sensors that are active (a set)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    # 2 - Query all the apis (if they have a type in 1.d)
    # and fill in sensors_df (sensor_status not determined yet)

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
                module = f'modules.Sensor_APIs.{api_name}.{monitor_name}.Regular_Update'
    
                # 2.b Call APIs for a regular update
                temp_sensors_df = import_module(module).Workflow(monitor_dict, monitor_api_df,
                                             base_config['TIMEZONE']) # Run Regular Update code for monitor
                                             
                # Concatenate to sensors_df
                sensors_df = pd.concat([sensors_df, temp_sensors_df],  ignore_index = True)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # 3 - Sort out the sensors_df column 'sensor_status'
    
    if len(sensors_df) > 0:
    
        sensors_df = Sort_sensors_df(sensors_df, runtime)
    
    else:
        print('\n~~~\nWarning: No sensors in database to update. \n\nPlease wait a little longer for a regular update\nor conduct a daily update to pull new sensors from APIs\n~~~\n')

    return sensors_df, runtime
  
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    
### Function to sort the sensor indices
    
def Sort_sensors_df(sensors_df, runtime):
    '''
    This sorts the sensor indices into sets based on if they are ordinary, new_spike, ongoing_spike, ended_spike
    
    Inputs: sensors_df from above, runtime (datetime
            
    returns final_sensors_df with its column 'sensor_status' properly sorted and 'last_elevated' column removed
    '''
    
    # Initialize Storage
    
    final_sensors_df = sensors_df.copy()
    final_sensors_df['sensor_status'] = 'unknown'
    

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    # Initialize dictionary for mapping sensor_status column
    
    sensor_id_dict = {'ordinary': set(), # Overlaps with all the others but is overwritten in above database
               'new_spike': set(),
               'ongoing_spike': set(),
               'ended_spike': set()
                }
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
    # Categorize sensor_ids by above map
    
    # Using set operations between:
    
    # 1) All sensor_ids that are being updated
    
    all_sensor_ids = set(sensors_df.sensor_id.to_list())
    
    # 2) Spiked from api call
    spike_descriptors = ['unhealthy for sensitive groups', 'unhealthy', 'very unhealthy', 'hazardous']
    spiked_sensors = set(sensors_df[sensors_df.health_descriptor.isin(spike_descriptors)
                                    ].sensor_id) # From most recent api call
    
    
    # 3) Actively alerted sensors (from this api_update)
    alerted_sensors = set(alert_query.Get_alerted_sensor_ids()).intersection(all_sensor_ids)
    
    # 4) Recently elevated (all sensors elevated in past "alert_lag" minutes)
#    recent_elevated_sensors = set(sensors_df[sensors_df.last_elevated + dt.timedelta(minutes=alert_lag
#                                    ) < np.datetime64(runtime)
#                                ].sensor_id)

    # 5) Flagged Sensors
    flagged = set(sensors_df[sensors_df.is_flagged == True].sensor_id.to_list())
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    # The Final sets:

    # A) Ordinary set_1 - set_5
    sensor_id_dict['ordinary'] = all_sensor_ids - flagged

    # B) new = set_2 - set_3
    sensor_id_dict['new_spike'] = spiked_sensors - alerted_sensors

    # C) ongoing = set_2 AND set_3
    sensor_id_dict['ongoing_spike'] = alerted_sensors.intersection(spiked_sensors)
    
    # D) Ended = set_3 - set_C
    sensor_id_dict['ended_spike'] = alerted_sensors - sensor_id_dict['ongoing_spike']
    

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  

    # Map the categories to dataframe
    
    status_order = ['ordinary', 'new_spike', 'ongoing_spike', 'ended_spike']
    
    for status in status_order:
        sensor_ids_w_status = sensor_id_dict[status] # Get sensor_ids 
        if len(sensor_ids_w_status)>0:
            is_status = final_sensors_df.sensor_id.isin(sensor_ids_w_status) # Boolean series
            final_sensors_df.loc[is_status, 'sensor_status'] = status # Overwrite

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

    # Drop last_elevated column

    final_sensors_df = final_sensors_df.drop(columns = ['last_elevated']).copy()
    
    return final_sensors_df
