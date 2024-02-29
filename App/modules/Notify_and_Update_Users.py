### Import Packages

# File manipulation

# Time

import datetime as dt # Working with dates/times
import pytz # Timezones

# Database 

from modules.Database import Basic_PSQL as psql
from psycopg2 import sql
from modules.Database.Queries import User as user_queries

# Data Manipulation

import pandas as pd

# Sensor Functions

import modules.Sensors.Sensor_Functions as sensors

## Workflow

def workflow(reports_dict, base_config):
    '''
    Runs the full workflow to check our "Users" table for folks to contact,
     compose and send messages, and update the "Users" table alerted & last_contact fields.
     
     Steps:
     
     0) Check if there is a city-wide air quality alert for all groups
     
        returns True or False - NOT DONE
     
     1) Send new alerts:
        
        a) Check for unalerted users with alerted POIs where now is within their messaging hours/days and they haven't been messaged too recent
        
        returns a dataframe with columns: user_id, poi_id, sensitive, contact_method, api_id
        
        b) Use above dataframe to compose messages
        
        returns a list of tuples (contact_method, api_id, message)
        
        c) Send Messages 
        
        d) Update internal database of these users with 
        last_contact = CURRENT TIMESTAMP AT TIME ZONE {base_config['TIMEZONE']}
        alerted = TRUE
     
     2) Send end alert messages:
        
        a) Check for alerted users with POIs that recently had a report written (see reports_dict parameter)
        
        returns a dataframe with columns: user_id, report_id, contact_method, api_id, in_contact_hours (True or False)
        
        b) Use above dataframe to compose messages
        
        returns a list of tuples (contact_method, api_id, message)
        
        c) Send Messages (if in_contact_hours = True)
        
        d) Update internal database of all these users with
        last_contact = CURRENT TIMESTAMP AT TIME ZONE {base_config['TIMEZONE']}
        alerted = FALSE
    
    Parameters:
    
    reports_dict - dictionary - has this format:
    
                  {
              'TRUE' : list of tuples of (poi_id, report_id)
              'FALSE' : list of tuples of (poi_id, report_id)
              }
                              
                 where TRUE = for sensitive populations
                        FALSE = for all populations 
                        
    base_config - dictionary - information from the .env file  
    '''
    
    # Get users who should be alerted
    # Returns a dataframe w/ user_id, poi_id, sensitive, contact_method, api_id
    
    new_alert_user_df = user_queries.Get_Users_to_alert(base_config['TIMEZONE'], base_config['MIN_MESSAGE_FREQUENCY'])
        
    print(new_alert_user_df)
    
    # Get users who should be unalerted

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`  

#    # Now get the information from that report
#   cmd = sql.SQL('''SELECT start_time, duration_minutes
#             FROM "Reports Archive"
#             WHERE report_id = {};
#''').format(sql.Literal(report_id))           
