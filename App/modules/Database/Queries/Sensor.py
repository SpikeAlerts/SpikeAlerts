# Functions to work with any sensor in our database

# Data Manipulation

import pandas as pd

# Time

import pytz # Timezones

# Database

from psycopg2 import sql
import modules.Database.Basic_PSQL as psql


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_Sensor_APIs_Information():
    '''
    Gets all sensor type information from our database
    
    returns sensor_api_dict
    formatted as

    {api_name : 
        {monitor_name : {
            sensor_type : {
                update_frequency : minutes between regular updates
                pollutant : abbreviated name for pollutant sensor reads
                metric : a unit to append to readings
                thresholds : [list of 5 floats corresponding to health benchmarks],
                radius_meters : integer representing a distance a sensor accurately represents (on an average day),
                api_fieldname : string to query api for this value
                }, ...
            }, ...
        }, ...
    }
    '''

    cmd = sql.SQL('''
        WITH temp as
	        (SELECT api_name, monitor_name, sensor_type, json_build_object('update_frequency', s.update_frequency,
                                                                           'pollutant', s.pollutant,
	                                                                       'metric', s.metric,
	                                                                       'thresholds', s.thresholds,
	                                                                       'radius_meters', s.radius_meters,
	                                                                       'api_fieldname', api_fieldname) as sensor_info_dict
	        FROM base."Sensor Type Information" as s
	        GROUP BY (api_name, monitor_name, sensor_type, update_frequency, pollutant, metric, thresholds, radius_meters, api_fieldname)
        ), monitor_gps as
	        (
	        SELECT api_name, monitor_name, json_object_agg(sensor_type, sensor_info_dict) as info_dict
	        FROM temp
	        GROUP BY(api_name, monitor_name)
	        )
        SELECT api_name, json_object_agg(monitor_name, info_dict) as monitor_dict
        FROM monitor_gps
        GROUP BY (api_name);
        ''')

    response = psql.get_response(cmd)

    # Unpack response into dictionary

    sensor_api_dict = dict(response)
    
    return sensor_api_dict   
    
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_Sensor_Types_Ready_to_Update(runtime):
    '''
    Gets the sensor types from "Sensor Type Information" that are ready for a regular update
    needs runtime (datetime object)
    
    returns a list of sensor_types
    '''
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
    
    update_time = runtime.strftime('%Y-%m-%d %H:%M:%S')
    
    # Make command
    
    cmd = sql.SQL('''
    SELECT sensor_type 
    FROM "Sensor Type Information"
    WHERE last_update + INTERVAL '1 Minutes' * update_frequency <= {};''').format(sql.Literal(update_time))

    response = psql.get_response(cmd)

    # Unpack response into list

    sensor_types = [i[0] for i in response] # Unpack results into list
    
    return sensor_types

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_Sensor_Info(fields=['sensor_id'], sensor_types='All', channel_flags=[0,1,2,3,4], channel_states = [0,1,3]):
    '''
    Gets data from sensors in our database (except geometry)

    parameters
    
    fields - list of strings - corresponds to fields of the database. Options:
    
    sensor_id' serial, -- Our Unique Identifier
	sensor_type text, -- Relates to above table
	api_id text, -- The unique identifier for the api
	name varchar(100), -- A name for the sensor (for humans)
	date_created timestamp DEFAULT CURRENT_TIMESTAMP,
	last_seen timestamp DEFAULT CURRENT_TIMESTAMP,
	channel_state int, -- Indicates whether the sensor is active or not
	channel_flags int, -- Indicates whether sensor is depricated
	altitude int,
	"last_value" float -- The last value of the sensor
	
    sensor_type - list of strings - corresponds to sensor_type in database, default is all
    
    channel_flags/state - list of integers - used to limit queries to 
    
    returns sensors_df with formatted columns
    '''
    
    # Initialize
    
    sensors_df = pd.DataFrame()
    
    field_options = ['sensor_id', 'sensor_type', 'api_id', 'name',
                     'date_created', 'last_seen',
                     'channel_state', 'channel_flags', 'altitude', 'last_value'
                     ]
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~                 
    # Check that given fields are appropriate
                     
    if len(set(field_options).intersection(set(fields))) == 0:
        print('ERROR in internal sensor query. No appropriate fields selected')
    elif len(set(fields).difference(set(field_options))) > 0:
        print('ERROR in internal sensor query. Incorrect fields selected')
    else:
    
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  
    
        # Initialize command
        
        cmd = sql.SQL('''SELECT ''')
        
        for i, field in enumerate(fields):
        
            if i > 0: # Comma separate
                cmd += sql.SQL(',')
            
            cmd += sql.SQL('{}').format(sql.Identifier(field))
            
        

        if sensor_types == 'All':
            cmd += sql.SQL('''FROM "Sensors"
                              WHERE channel_state = ANY ( {} ) AND channel_flags = ANY ( {} );''').format(sql.Literal(channel_states),
                        sql.Literal(channel_flags))
        else:
            cmd += sql.SQL('''FROM "Sensors" WHERE sensor_type = ANY ( {} )
            AND channel_state = ANY ( {} ) AND channel_flags = ANY ( {} ); 
            ''').format(sql.Literal(sensor_types),
                        sql.Literal(channel_states),
                        sql.Literal(channel_flags))

        response = psql.get_response(cmd)

        # Unpack response into pandas series

        df = pd.DataFrame(response, columns = fields)
        
   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  

        # Datatype corrections (for non-strings)
        
        if len(df) > 0:
        
            # Integers
            
            int_cols = {'sensor_id', 'channel_flags', 'channel_state', 'altitude'}
        
            for col in set(fields).intersection(int_cols):
            
                df[col] = df[col].astype(int)
        
            # Datetimes
            
            datetime_cols = {'last_seen', 'date_created'}
            
            for col in set(fields).intersection(datetime_cols):
            
                df[col] = pd.to_datetime(df[col])
            
            # Floats
            
            float_cols = {'current_reading'}
            
            for col in set(fields).intersection(float_cols):

                df[col] = df[col].astype(float)
            
    # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    
         
        # Copy and return    
        
        sensors_df = df.copy()
    
    return sensors_df
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

def Get_next_regular_update(timezone):
    '''
    This function will query the "Sensor Type Information" table for the next time to run a regular update
    
    parameters:
    
    timezone = pytz timezone
    
    returns a timezone aware datetime
    '''
    
    cmd = sql.SQL('''SELECT MIN(last_update + INTERVAL '1 Minutes' * update_frequency)
FROM "Sensor Type Information";
    ;''')
    
    response = psql.get_response(cmd)
    
    # Unpack response into datetime
    
    if response[0][0] != None:

        next_regular_update = pytz.timezone(timezone).localize(response[0][0])
    else:
        print('ERROR: Cannot calculate the next regular update. Please see modules/Database/Queries/Sensor.py')
        
    return next_regular_update
    
