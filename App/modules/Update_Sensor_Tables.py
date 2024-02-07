### Import Packages

# File manipulation

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

from modules import Basic_PSQL as psql
from psycopg2 import sql

# Data Manipulation

import pandas as pd

# Sensor Functions

import modules.Sensor_Functions as sensors

## Workflow

def workflow(sensors_df, sensor_types_updated, runtime):
    '''
    Runs the full workflow to update our database table "Sensors" with the following:

    channel_flag - if flagged
    last_elevated - if new/ongoing spikes
    last_seen - if not flagged
    current_reading - for all
    
    and "Sensor Type Information" last_updated
    
    Parameters:
    
    sensors_df - a dataframe with the following columns:

    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    update_frequency - int - frequency this sensor is updated
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks
    radius_meters - int - max distance sensor is relevant
    is_flagged - binary - is the sensor flagged?
    sensor_status - text - one of these categories: ordinary, new_spike, ongoing_spike, ended_spike
    
    sensor_types_updated - iterable of sensor_types that were updated
    
    runtime - approximate time that the values for above dataframe were acquired
    '''

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # Update channel_flag - if flagged

    flagged_ids = sensors_df[sensors_df.is_flagged == True].sensor_id.to_list()
    flag_sensors(flagged_ids)
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # last_elevated - if new/ongoing spikes

    elevated_ids = sensors_df[(sensors_df.sensor_status == 'new_spike') |
                              (sensors_df.sensor_status == 'ongoing_spike')
                             ].sensor_id.to_list()
    Update_last_elevated(elevated_ids, runtime)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # last_seen - if not flagged

    not_flagged_ids = sensors_df[sensors_df.is_flagged == False].sensor_id.to_list()
    Update_last_seen(not_flagged_ids, runtime)

    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # current_reading - for all not flagged 

    current_reading_update_df = sensors_df[sensors_df.is_flagged == False][['sensor_id', 'current_reading']]
    sensors.Update_Sensors(current_reading_update_df)
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    # last_updated ("Sensor Type Information")
    
    Update_last_update(sensor_types_updated, runtime)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

### Function to flag sensors in our database

def flag_sensors(sensor_ids):
    '''
    This function sets the channel_flags = 4 in our database on the given sensor_ids (list)
    '''

    if len(sensor_ids) > 0:
        cmd = sql.SQL('''UPDATE "Sensors"
        SET channel_flags = 4
        WHERE sensor_id = ANY ( {} );
        ''').format(sql.Literal(sensor_ids))
    
        psql.send_update(cmd)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    
### Function to update all last_elevateds

def Update_last_elevated(sensor_ids, runtime):
    '''
    This function updates all the sensors' last_elevated that are currently spiked
    '''

    if len(sensor_ids) > 0:
    
        update_time = runtime.strftime('%Y-%m-%d %H:%M:%S')
        
        cmd = sql.SQL('''UPDATE "Sensors"
        SET last_elevated = {}
        WHERE sensor_id = ANY ( {} );
        '''
        ).format(sql.Literal(update_time),
                sql.Literal(sensor_ids))
                
        psql.send_update(cmd)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
    
### Function to update all last_seens

def Update_last_seen(sensor_ids, runtime):
    '''
    This function updates all the sensors' last_seens that are currently spiked
    '''

    if len(sensor_ids) > 0:
    
        update_time = runtime.strftime('%Y-%m-%d %H:%M:%S')
        
        cmd = sql.SQL('''UPDATE "Sensors"
        SET last_seen = {}
        WHERE sensor_id = ANY ( {} );
        '''
        ).format(sql.Literal(update_time),
                sql.Literal(sensor_ids))
                
        psql.send_update(cmd)
        
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
        
### Function to update last_update in "Sensor Type Information"

def Update_last_update(sensor_types, runtime):
    '''
    This function updates last_update in "Sensor Type Information" for the specified sensor_types
    
    parameters:
    
    sensor_types - list of strings
    runtime - datetime object
    '''

    if len(sensor_types) > 0:
    
        sensor_types = list(sensor_types)
        update_time = runtime.strftime('%Y-%m-%d %H:%M:%S')
        
        cmd = sql.SQL('''UPDATE "Sensor Type Information"
        SET last_update = {}
        WHERE sensor_type = ANY ( {} );
        ''').format(sql.Literal(update_time),
                sql.Literal(sensor_types))
    
        psql.send_update(cmd)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
