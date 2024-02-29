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

def workflow(poi_id_dict, base_config):
    '''
    Runs the full workflow to check our "Users" table, compose and send messages, and update the "Users" table alerted & last_contact fields.
    
    Parameters:

    '''
    # Initialize returned dataframe
        
    user_df = pd.DataFrame(columns = ['user_id', 'poi_id', 'sensitive',
                              'contact_method', 'api_id', 'last_contact',
                              'type_of_message']
                             )
    
    # Get users who should be alerted
    # Returns a dataframe w/ user_id, poi_id, sensitive, contact_method, api_id
    
    new_alert_user_df = user_queries.Get_Users_to_alert(base_config['TIMEZONE'], base_config['MIN_MESSAGE_FREQUENCY'])
    new_alert_user_df['type_of_message'] = 'start'
        
    print(new_alert_user_df)
    
    # Get users who should be unalerted

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`                
