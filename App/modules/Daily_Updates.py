### Import Packages

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

# from modules import Basic_PSQL as psql
from modules.Sensors import Sensor_Functions as sensors
from modules.Database.Queries import Sensor as sensor_queries
from modules.Database.Queries import General as query
from modules.Database import Basic_PSQL as psql
from psycopg2 import sql
# import psycopg2

# Importing Libraries
from importlib import import_module

# Analysis

# import pandas as pd
# import geopandas as gpd
# import numpy as np

# Load our functions

# from modules import PurpleAir_Functions as purp
# from modules import REDCap_Functions as redcap
# from modules import Twilio_Functions as our_twilio

# Messaging

# from modules import Create_messages
# from modules import Send_Alerts

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

## Workflow

def workflow(base_config, next_update_time):
    '''
    This is the full workflow for Daily Updates
    
    returns the next_update_time (datetime timestamp)
    '''
    
    # Check Last Update
    
    last_update_date = query.Get_last_Daily_Log()
      
    if last_update_date < next_update_time.date(): # If haven't updated full system today
    
        print('Running Daily Update')

        # Update Sensors from their respective APIs

        sensor_api_dict = sensor_queries.Get_Sensor_APIs_Information() # information on the different types of apis/monitors/sensors

        for api_name in sensor_api_dict:

            for monitor_name in sensor_api_dict[api_name]:

                # Monitor Dictionary with sensor_types, thresholds, and radius_meters
                monitor_dict = sensor_api_dict[api_name][monitor_name]
                
                # Get the module name
                module = f'modules.Sensors.APIs.{api_name}.{monitor_name}.Daily_Update'

                # Call for a daily update
                import_module(module).Workflow(monitor_dict,
                                                   base_config['TIMEZONE']) # Run Daily Update code for sensor type

        # Update the Points of Interest
        
        Update_POIs_active(base_config['EPSG_CODE'])
        
#         # Update "Sign Up Information" from REDCap - See Daily_Updates.py

#         max_record_id = query.Get_newest_user(pg_connection_dict)
#         REDCap_df = redcap.Get_new_users(max_record_id, redCap_token_signUp)
#         Add_new_users(REDCap_df, pg_connection_dict)
        
        # Initialize Daily Log

        initialize_daily_log(0)
        
        
        
        # Morning Alert Reminders
        
#         ongoing_record_ids = sensor_query.Get_ongoing_alert_record_ids(pg_connection_dict)
        
#         if len(ongoing_record_ids) > 0:
        
#             messages = [Create_messages.morning_alert_message()] * len(ongoing_record_ids)
            
#             Send_Alerts.send_all_messages(messages, poi_ids)
    
        print('Completed Daily Update')
        
    # Get next update time (in 1 day)
    next_update_time += dt.timedelta(days=1)
    
    return next_update_time
    
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


def initialize_daily_log(len_new_users):
    '''
    This function initializes a new daily log - IT IS NOT DONE
    
    Fields
     ("date" date DEFAULT CURRENT_DATE,
     new_POIs int DEFAULT 0,
     new_sensors int DEFAULT 0,
     retired_sensors int DEFAULT 0,
	 alerts_sent int DEFAULT 0
    '''

    cmd = sql.SQL('''INSERT INTO "Daily Log"
    (new_POIs, new_sensors) 
    VALUES ({}, {});
    ''').format(sql.Literal(len_new_users),
                sql.Literal(len_new_users))

    psql.send_update(cmd)
   
 # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 
def Update_POIs_active(epsg_code):
    '''
    This function updates the "active" field in the table,
    base."Places of Interest"
    based upon if a sensor is nearby or not
    
    parameters:
    
    epsg_code - something to cast as int - The local UTM Coordinate Reference System EPSG code
    '''

    cmd = sql.SQL('''
    WITH active_sensors as
    (
	    SELECT s.sensor_id, i.radius_meters, s.geometry
	    FROM base."Sensors" s
	    INNER JOIN base.sensor_ids_w_info i ON i.sensor_id = s.sensor_id
	    WHERE s.channel_state = 1
    ), pois_w_no_nearby_sensors as
    (
	    SELECT p.poi_id, 
			    BOOL_OR ( ST_DWithin(ST_Transform(p.geometry, {}), -- CHANGE THIS!!
					       ST_Transform(s.geometry, {}), -- CHANGE THIS!!
					       s.radius_meters)) as nearby_sensor
	    FROM active_sensors s, base."Places of Interest" p
	    WHERE p.active = TRUE
	    GROUP BY p.poi_id
    )
    UPDATE base."Places of Interest" pois
    SET active = p.nearby_sensor
    FROM pois_w_no_nearby_sensors p
    WHERE pois.poi_id = p.poi_id;
    ''').format(sql.Literal(int(epsg_code)),
                sql.Literal(int(epsg_code))
                )

    psql.send_update(cmd)
