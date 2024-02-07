### Import Packages

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

# from modules import Basic_PSQL as psql
from modules import Sensor_Functions as sensors
from modules.Queries import Sensor as sensor_queries
from modules.Queries import General as query
from modules import Basic_PSQL as psql
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

        # Update Sensors from their respective APIs

        sensor_api_dict = sensor_queries.Get_Sensor_APIs_Information() # information on the different types of apis/monitors/sensors

        for api_name in sensor_api_dict:

            for monitor_name in sensor_api_dict[api_name]:

                # Monitor Dictionary with sensor_types, thresholds, and radius_meters
                monitor_dict = sensor_api_dict[api_name][monitor_name]
                
                # Get the module name
                module = f'modules.Sensor_APIs.{api_name}.{monitor_name}.Daily_Update'

                # Call for a daily update
                import_module(module).Workflow(monitor_dict,
                                                   base_config['TIMEZONE']) # Run Daily Update code for sensor type

        # Update the Points of Interest/Sign Ups
        
#         # Update "Sign Up Information" from REDCap - See Daily_Updates.py
#         max_record_id = query.Get_newest_user(pg_connection_dict)
#         REDCap_df = redcap.Get_new_users(max_record_id, redCap_token_signUp)
#         Add_new_users(REDCap_df, pg_connection_dict)
        
        # Initialize Daily Log

        initialize_daily_log(0)
        
#         # Send reports stored from yesterday
        
#         afterhour_reports = query.Get_afterhour_reports(pg_connection_dict)
        
#         if len(afterhour_reports) > 0:
#             record_ids = [afterhour_report[0] for afterhour_report in afterhour_reports]
#             messages = [afterhour_report[1] for afterhour_report in afterhour_reports]
#             Send_Alerts.send_all_messages(record_ids, messages, redCap_token_signUp, pg_connection_dict)
#             Clear_afterhour_reports(pg_connection_dict)
    
#         print(len(REDCap_df), 'new users')
        
#         # Morning Alert Reminders
        
#         ongoing_record_ids = query.Get_ongoing_alert_record_ids(pg_connection_dict)
        
#         if len(ongoing_record_ids) > 0:
        
#             messages = [Create_messages.morning_alert_message()] * len(ongoing_record_ids)
            
#             Send_Alerts.send_all_messages(ongoing_record_ids, messages,
#                                           redCap_token_signUp, pg_connection_dict)
    
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

# def Clear_afterhour_reports(pg_connection_dict):
#     '''
#     This function clears the "Afterhour Reports" table
#     '''
    
#     cmd = sql.SQL('''TRUNCATE TABLE "Afterhour Reports";''')
    
#     psql.send_update(cmd, pg_connection_dict)

# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# # Sign Up Information
    
# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      

# # def Add_new_users(df, pg_connection_dict):
# #     '''
# #     This function inserts the new users from REDCap into our database
# #     The dataframe must have "phone", "record_id" and "wkt" as columns with the Well Known Text in WGS84 (EPSG:4326) "Lat/lon"
# #     '''
    
# #     if len(df) > 0:
    
# #         # Get Twilio Creds
        
# #         load_dotenv()

# #         account_sid = os.environ['TWILIO_ACCOUNT_SID']
# #         auth_token = os.environ['TWILIO_AUTH_TOKEN']
# #         twilio_number = os.environ['TWILIO_NUMBER']
        
# #         # See if users entered a proper location
# #         is_no_location = df[['lat','lon']].isna().sum(axis=1) != 0
        
# #         no_loc_df = df[is_no_location]
# #         good_df = df[~is_no_location]
        
# #         if len(no_loc_df) >0: # Incorrect location entry
        
# #             signUp_url = os.environ['SIGNUP_URL']
            
# #             # Now message those new users with errored locations
            
# #             numbers = no_loc_df.phone.to_list()
# #             messages = [Create_messages.no_location(signUp_url)]*len(numbers)
            
# #             our_twilio.send_texts(numbers, messages)
        
# #         if len(good_df) > 0: # Correct location entry - insert into location
            
# #             good_df['geometry'] = good_df.wkt
            
# #             #print(df.geometry[0])
# #             #print(type(df))
            
# #             df_for_db = good_df[['record_id', 'geometry']]
            
# #             psql.insert_into(df_for_db, "Sign Up Information", pg_connection_dict, is_spatial = True)
            
# #             # Now message those new users
            
# #             numbers = good_df.phone.to_list()
# #             messages = [Create_messages.welcome_message()]*len(numbers)
            
# #             our_twilio.send_texts(numbers, messages)
    

# # Subscriptions

# # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~       
    
    
# #    # REDCap <- need permissions to delete records
# #    
# #    # Select by record_id <- probably not the best way, but I couldn't get 'record' to work properly in data
# #    
# #    record_id_strs = [str(record_id) for record_id in record_ids]
# #    filterLogic_str = '[record_id]=' + ' OR [record_id]='.join(record_id_strs)
# #    
# #    data = {
# #    'token': redCap_token_signUp,
# #    'content': 'record',
# #    'fields' : field_names,
# #    'action': 'delete',
# #    'filterLogic': filterLogic_str  
# #    }
# #    r = requests.post('https://redcap.ahc.umn.edu/api/',data=data)    

#     # TWILIO - See twilio_functions.py
# #    phone_numbers_to_unsubscribe = Something from REDCap
# #    delete_twilio_info(phone_numbers_to_unsubscribe, TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
