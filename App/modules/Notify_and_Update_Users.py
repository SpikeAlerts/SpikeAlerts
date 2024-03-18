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

# Messaging Functions

from modules.Users import Send_Messages
from modules.Users import Compose_Messages

## Workflow

def workflow(reports_dict, base_config):
    '''
    Runs the full workflow to check our "Users" table for folks to contact,
     compose and send messages, and update the "Users" table alerted & last_contact fields.
     
    To Test: Alter sensor health thresholds with
    
    UPDATE "Sensor Type Information"
    SET thresholds = ARRAY[0, 12.1, 35.5, 55.5, 150.5, 250.5, 1000],
    last_update = last_update - Interval '10 minutes';
     
     Steps:
     
     0) Check if there is a city-wide air quality alert for all groups
     
        returns True or False - NOT DONE
        
        
     ### WE NEED TO CHANGE THIS ORDER! First, Ended Alerts then Ongoing alerts (in morning) then new alerts
     
     1) Send new alerts:
        
        a) Check for unalerted users with alerted POIs where now is within their messaging hours/days and they haven't been messaged too recent
        
        returns a dataframe with columns: user_id, poi_id, sensitive, contact_method, api_id
        
        b) Use above dataframe to compose messages
        
        returns a dataframe, messaging_df, with fields: contact_method, api_id, message
        
        c) Send Messages - see modules/Users/Send_Messages.py
        
        d) Update internal database of these users with 
        last_contact = CURRENT TIMESTAMP AT TIME ZONE {base_config['TIMEZONE']}
        alerted = TRUE
     
     2) Send end alert messages :
        
        a) Check for alerted users with POIs that recently had a report written (see reports_dict parameter)
        
        returns a dataframe with columns: user_id, report_id, contact_method, api_id, in_contact_hours (True or False)
        
        b) Use above dataframe to compose messages
        
        returns a dataframe, messaging_df, with fields: contact_method, api_id, message
        
        c) Send Messages (if in_contact_hours = True) - see modules/Users/Sen_Messages.py
        
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
    
    # ~~~~~~~~~~~~~~~~~~~~~~~
    
    # 0) Check if there is a city-wide air quality alert for all groups
     
    #    returns True or False - NOT DONE
    
    # ~~~~~~~~~~~~~~~~~~~~~~~
    
    ### WE NEED TO CHANGE THIS ORDER! First, Ended Alerts then Ongoing alerts (in morning) then new alerts
    
    # 1) Send new alerts:
    
    # ~~~~~~~~~~~~~~~~~~~~~~~
    
    # a) Get users who should be alerted (See modules/Database/Queries/User.py)
    # Returns a dataframe w/ user_id, poi_id, sensitive, contact_method, api_id
    
    new_alert_user_df = user_queries.Get_Users_to_message_alert(base_config['TIMEZONE'])
    
    if len(new_alert_user_df) > 0:
    
        # ~~~~~~~~~~~~~~~~~~~~~~~
            
        # b) Use above dataframe to compose messages
        # returns a dataframe, messaging_df, with fields: contact_method, api_id, message
        
        messaging_df = Parse_new_alert_user_df(new_alert_user_df, 
                                               base_config['EPSG_CODE'],
                                               base_config['WEBMAP_LINK'])
                                               
        
        # ~~~~~~~~~~~~~~~~~~~~~~~
        
        # c) Send Messages
        
        Send_Messages.workflow(messaging_df, base_config['CONTACT_INFO_API'], base_config['TIMEZONE'])
        
        # ~~~~~~~~~~~~~~~~~~~~~~~
        
        # d) Update internal database of these users with 
        # last_contact = CURRENT TIMESTAMP AT TIME ZONE {base_config['TIMEZONE']}
        # alerted = TRUE
        
        Update_users_after_message(new_alert_user_df.user_id.to_list(), 'TRUE', base_config['TIMEZONE'])
    
    # ~~~~~~~~~~~~~~~~~~~~~~~
     
    # 2) Send end alert messages:
    
    # ~~~~~~~~~~~~~~~~~~~~~~~
        
    # a) Check for users to unalert (See modules/Database/Queries/User.py) 
    
    # with POIs that recently had a report written (see reports_dict parameter)
    
    #reports_dict = {'TRUE': [(123, '00002-030624')], 'FALSE': []}
    
    end_alert_user_df = user_queries.Get_Users_to_message_unalert(reports_dict, base_config['TIMEZONE'])
    
    if len(end_alert_user_df) > 0:
    
        # ~~~~~~~~~~~~~~~~~~~~~~~
            
        # b) Use above dataframe to compose messages
        # returns a dataframe, messaging_df, with fields: contact_method, api_id, message
        
        messaging_df = Parse_end_alert_user_df(end_alert_user_df, 
                                               base_config['EPSG_CODE'],
                                               base_config['OBSERVATION_BASEURL'])
                                               
        
        # ~~~~~~~~~~~~~~~~~~~~~~~
        
        # c) Send Messages
        
        Send_Messages.workflow(messaging_df, base_config['CONTACT_INFO_API'], base_config['TIMEZONE'])
        
        # ~~~~~~~~~~~~~~~~~~~~~~~
        
        # d) Update internal database of these users with 
        # last_contact = CURRENT TIMESTAMP AT TIME ZONE {base_config['TIMEZONE']}
        # alerted = FALSE
        
        Update_users_after_message(end_alert_user_df.user_id.to_list(), 'FALSE', base_config['TIMEZONE'])
    
    # ~~~~~~~~~~~~~~~~~~~~~~~
    
    # 3) Unalert Users (with reports written outside of contact hours)
    
    # Please see app/modules/Database/Queries/User.py for query
    
    Unalert_Users()

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

def Parse_new_alert_user_df(new_alert_user_df, epsg_code, webmap_link):
    '''
    Uses the new_alert_user_df Dataframe to compose unique messages 
    
    returns a dataframe, messaging_df, with fields: poi_name, contact_method, api_id, message
    
    '''
    # Initialize returned value
        
    messaging_df = pd.DataFrame(columns = ['contact_method', 'api_id', 'message'])
    
    for i, new_alert_user in new_alert_user_df.iterrows():
    
        poi_name = new_alert_user.poi_name
    
        message = Compose_Messages.new_alert_message(poi_name, webmap_link)
        
        messaging_df.loc[i] = [new_alert_user.contact_method, new_alert_user.api_id, message]
    
    return messaging_df  

# ~~~~~~~~~~~~~

def Parse_end_alert_user_df(end_alert_user_df, epsg_code, report_url):
    '''
    Uses the end_alert_user_df Dataframe to compose unique messages 
    
    returns a dataframe, messaging_df, with fields: poi_name, contact_method, api_id, message
    
    '''
    # Initialize returned value
        
    messaging_df = pd.DataFrame(columns = ['contact_method', 'api_id', 'message'])
    
    for i, end_alert_user in end_alert_user_df.iterrows():
    
        duration = end_alert_user.duration_minutes
        severity = end_alert_user.severity
    
        message = Compose_Messages.end_alert_message(end_alert_user.duration_minutes, 
                                                    end_alert_user.severity,
                                                    report_url)
        
        messaging_df.loc[i] = [end_alert_user.contact_method, end_alert_user.api_id, message]
    
    return messaging_df 

# ~~~~~~~~~~~~~

def Update_users_after_message(user_ids, alerted, timezone):
    '''
    This function updates the users in our database.
    
    Changes alerted = True or False and last_contact = CURRENT_TIMESTAMP
    
    parameters:
    
    user_ids - list - user_ids in our database to update
    alerted - string - TRUE or FALSE (not case senstive)
    timezone - string - pytz timezone
    '''

    cmd = sql.SQL('''
    UPDATE "Users"
    SET alerted = {},
    last_contact = (CURRENT_TIMESTAMP AT TIME ZONE {})::timestamp
    WHERE user_id = ANY ( {} );
    ''').format(sql.Literal(alerted),
                sql.Literal(timezone),
                sql.Literal(user_ids)
                )
                
    psql.send_update(cmd)     
    
# ~~~~~~~~~~~~~

def Unalert_Users():
    '''
    To catch the users that had reports written outside of contact hours.
    
    This function queries the database for alerted users that have active alerts
    but their Place of Interest has empty cached and active alerts
    
    Changes alerted = False where this is true
    
    potential expansion?
    Archive a message to send to the user when within contact hours
    
    parameters:
    time_buffer - int - minutes to queue a message until contact hours begin 

    '''
                                      
    # Iterate through the sensitivities
                                      
    for is_sensitive in ['TRUE', 'FALSE']:
    
        active_field = 'active_alerts'
        cache_field = 'cached_alerts'
        
        if is_sensitive == 'TRUE':
            cache_field += '_sensitive'
            active_field += '_sensitive'
        
        
        # Select unalerted pois
        # Then query users that are:
        # active, alerted, match this sensitivity, and do not have a poi in above
        
        # Update these users' alerted to false
        
        cmd = sql.SQL('''
                        WITH unalerted_pois as
                        (
	                        SELECT poi_id
	                        FROM "Places of Interest"
	                        WHERE 
	                        {} = {}
	                        AND {} = {}
                        ), users_to_update as
                        (
	                        SELECT u.user_id
	                        FROM "Users" u
	                        LEFT JOIN unalerted_pois p ON (u.poi_id = p.poi_id)
	                        WHERE active = TRUE
	                        AND p.poi_id IS NOT NULL
	                        AND alerted = TRUE
	                        AND sensitive = {}
                        )
                        UPDATE "Users" u
                        SET alerted = FALSE
                        FROM users_to_update uu
                        WHERE u.user_id = uu.user_id
                        ;
                 ''').format(sql.Identifier(active_field), sql.Literal('{}'),
                            sql.Identifier(cache_field), sql.Literal('{}'),
                            sql.Literal(is_sensitive)
                        )
                
        psql.send_update(cmd)    
