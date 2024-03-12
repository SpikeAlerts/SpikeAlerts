# Functions to work with any sensor in our database

# Data Manipulation

import pandas as pd

# Time

import pytz # Timezones

# Database

from psycopg2 import sql
import modules.Database.Basic_PSQL as psql

def Get_newest_api_id():
    '''
    Gets the "newest" user's api_id

    Assuming that this is enough to acquire new users from the external storage

    returns a string ('-1' if no users)
    '''

    cmd = sql.SQL('''
    WITH newest_user as
	(
        SELECT MAX(user_id) as user_id
        FROM "Users"
	)
    SELECT api_id 
    FROM "Users" u, newest_user
    WHERE u.user_id = newest_user.user_id;
    ''')

    response = psql.get_response(cmd)
    
    if response[0][0] == None:
        max_api_id ='-1'
    else:
        max_api_id = response[0][0]
    
    return max_api_id

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# NEW Alerts

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_Users_to_message_alert(timezone):
    '''
    This function queries the database for unalerted users that:
    have alerted poi_ids
    today is in days to contact
    and the time is within their desired contact hours
    
    parameters:
    
    timezone - string - timezone from the base .env file
    
    returns a dataframe with fields
    
    user_id - int - our unique identifier for users
    poi_name - string - names for Places of Interest
    sensitive - boolean - user gets sensitive alerts
    contact_method - str - corresponds to a script in modules/Users/Contact_Methods
    api_id - str - id for the user information in remote database
    '''
    
    fields = ['user_id', 'poi_name', 'sensitive', 'contact_method', 'api_id']

    cmd = sql.SQL('''
    -- Users to Alert (user_id, poi_name, sensitive, contact_method, api_id)
    WITH alerted_pois as
    (
	    SELECT poi_id, name, TRUE as "sensitive" -- POIs alerted for sensitive groups
	    FROM "Places of Interest"
	    WHERE active_alerts_sensitive != {}
	    UNION ALL
	    SELECT poi_id, name, FALSE as "sensitive" -- POIs alerted for everyone
	    FROM "Places of Interest"
	    WHERE active_alerts != {}
    )
    SELECT u.user_id, p.name, u.sensitive, u.contact_method, u.api_id
    FROM "Users" u
    LEFT JOIN alerted_pois p ON (u.poi_id = p.poi_id
								    AND u.sensitive = p.sensitive
								    ) -- Sensitive Users
    WHERE
    p.poi_id IS NOT NULL
    AND alerted = FALSE -- Not Alerted
    AND EXTRACT(dow FROM CURRENT_DATE AT TIME ZONE {}) = ANY ( days_to_contact ) -- Days to contact user
    AND start_time < CURRENT_TIME AT TIME ZONE {} -- Current time less than Start time
    AND end_time > CURRENT_TIME AT TIME ZONE {} -- Current time greater than Start time
    AND last_contact + INTERVAL '1 Minutes' * message_freq <= CURRENT_TIMESTAMP AT TIME ZONE {}; -- has the user been contacted too recently?
    ''').format(sql.Literal('{}'),
                sql.Literal('{}'),
                sql.Literal(timezone),
                sql.Literal(timezone),
                sql.Literal(timezone),
                sql.Literal(timezone)
                )
                
    response = psql.get_response(cmd) 
    
    # Unpack response into pandas dataframe

    df = pd.DataFrame(response, columns = fields)
    
   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  

    # Datatype corrections (for non-strings)
    
    if len(df) > 0:
    
        # Integers
        
        df['user_id'] = df['user_id'].astype(int)
        
        # Booleans
        
        bool_cols = {'sensitive'}
        
        for col in set(fields).intersection(bool_cols):

            df[col] = df[col].astype(bool)
        
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    
     
    # Copy and return    
    
    user_df = df.copy()
    
    return user_df
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ENDED Alerts

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_Users_to_message_unalert(reports_dict, timezone):
    '''
    This function queries the database for alerted users that:
    have recently written reports in reports_dict
    today is in days to contact
    and the time is within their desired contact hours
    
    parameters:
    
    reports_dict - dict - has this format:
    
                  {
              'TRUE' : list of tuples of (poi_id, report_id)
              'FALSE' : list of tuples of (poi_id, report_id)
              }
                              
                 where TRUE = for sensitive populations
                        FALSE = for all populations 
    timezone - string - timezone from the base .env file
    
    returns a dataframe with fields
    
    user_id - int - our unique identifier for users
    contact_method - str - corresponds to a script in modules/Users/Contact_Methods
    api_id - str - id for the user information in remote database
    report_id - str - our unique identifier for reports
    duration_minutes - int - number of minutes the alerts lasted
    severity - str - health descriptor of worst severity of alerts in this report
    '''
    
    # Initialize Dataframe for the to return
    
    user_df = pd.DataFrame(columns = ['user_id', 'contact_method', 'api_id', # User Informations
                                      'report_id', 'duration_minutes', 'severity' # Report Informations
                                      ]
                           )
                                      
    # Iterate through the sensitivities
                                      
    for is_sensitive in reports_dict:
    
        # Iterate through the tuples of (poi_id, report_id)
        
        for poi_id, report_id in reports_dict[is_sensitive]:
        
            # First, get the users' info that are attached to this poi
            # That should be contacted (within desired contact hours/days)
            
            fields = ['user_id', 'contact_method', 'api_id']
            
            cmd = sql.SQL('''
                -- Users to UnAlert (user_id, contact_method, api_id)
                SELECT u.user_id, u.contact_method, u.api_id
                FROM "Users" u
                WHERE
                alerted = TRUE -- Alerted
                AND sensitive = {} -- Correct sensitivity?
                AND poi_id = {} -- Is the user attached to this poi_id?
                AND EXTRACT(dow FROM CURRENT_DATE AT TIME ZONE {}) = ANY ( days_to_contact ) -- Days to contact user
                AND start_time < CURRENT_TIME AT TIME ZONE {} -- Current time less than Start time
                AND end_time > CURRENT_TIME AT TIME ZONE {} -- Current time greater than Start time
                ''').format(sql.Literal(is_sensitive),
                            sql.Literal(poi_id),
                            sql.Literal(timezone),
                            sql.Literal(timezone),
                            sql.Literal(timezone)
                            )
                            
            response = psql.get_response(cmd) 
    
            # Unpack response into pandas dataframe

            temp_df = pd.DataFrame(response, columns = fields)
                    
            # ~~~~~~~~~~~~~~~~~~
            
            # Next, get the report's information and add to dataframe
            
            cmd = sql.SQL('''
                          WITH report as
                        (
	                        SELECT report_id, duration_minutes, alert_ids
	                        FROM "Reports Archive"
	                        WHERE report_id = {}
                        ), alerts as
                        (
	                        SELECT r.report_id, a.sensor_id, a.max_reading
	                        FROM "Archived Alerts" a
	                        INNER JOIN report r ON (a.alert_id = ANY (r.alert_ids))
                        ), health as
                        (
	                        SELECT MAX(map_to_health(a.max_reading, s.thresholds)) as health_descriptor
	                        FROM alerts a
	                        INNER JOIN sensor_ids_w_info s ON (a.sensor_id = s.sensor_id)
                        )
                        SELECT r.duration_minutes, 
	                           right(h.health_descriptor, -2) as severity
                        FROM report r, health h;
                          ''').format(sql.Literal(report_id))
                          
            response = psql.get_response(cmd)
    
            # Unpack response & add to dataframe
            
            temp_df['report_id'] = report_id
            temp_df['duration_minutes'] = response[0][0]
            temp_df['severity'] = response[0][1]
            
            # ~~~~~~~~~~~~~~~~~~
            
            # Lastly, concatenate to user_df
            
            user_df = pd.concat([user_df, temp_df], ignore_index = True)
               
    
   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  

    # Datatype corrections (for non-strings)
    
    if len(user_df) > 0:
    
        # Integers
        
        int_cols = {'user_id', 'duration_minutes'}
        
        for col in set(user_df.columns).intersection(int_cols):

            user_df[col] = user_df[col].astype(int)
    
    return user_df
    
def Get_Users_to_unalert():
    '''
    To catch the users that had reports written outside of contact hours.
    
    This function queries the database for alerted users that:
    have active alerts
    but their place of interest has empty cached and active alerts
    
    returns a list of user_ids (to update internal database and maybe notify users?)
    '''
    
    # Initialize list for the to return
    
    user_ids = []
                                      
    # Iterate through the sensitivities
                                      
    for is_sensitive in reports_dict:
    
        # Iterate through the tuples of (poi_id, report_id)
        
        for poi_id, report_id in reports_dict[is_sensitive]:
        
            # First, get the users' info that are attached to this poi
            # That should be contacted (within desired contact hours/days)
            
            fields = ['user_id', 'contact_method', 'api_id']
            
            cmd = sql.SQL('''
                -- Users to UnAlert (user_id, contact_method, api_id)
                SELECT u.user_id
                FROM "Users" u
                WHERE
                alerted = TRUE -- Alerted
                AND sensitive = {} -- Correct sensitivity?
                AND poi_id = {} -- Is the user attached to this poi_id?
                ''').format(sql.Literal(is_sensitive),
                            sql.Literal(poi_id),
                            sql.Literal(timezone),
                            sql.Literal(timezone),
                            sql.Literal(timezone)
                            )
                            
            response = psql.get_response(cmd) 
    
            # Unpack response into pandas dataframe

            temp_df = pd.DataFrame(response, columns = fields)
                    
            # ~~~~~~~~~~~~~~~~~~
            
            # Next, get the report's information and add to dataframe
            
            cmd = sql.SQL('''
                          WITH report as
                        (
	                        SELECT report_id, duration_minutes, alert_ids
	                        FROM "Reports Archive"
	                        WHERE report_id = {}
                        ), alerts as
                        (
	                        SELECT r.report_id, a.sensor_id, a.max_reading
	                        FROM "Archived Alerts" a
	                        INNER JOIN report r ON (a.alert_id = ANY (r.alert_ids))
                        ), health as
                        (
	                        SELECT MAX(map_to_health(a.max_reading, s.thresholds)) as health_descriptor
	                        FROM alerts a
	                        INNER JOIN sensor_ids_w_info s ON (a.sensor_id = s.sensor_id)
                        )
                        SELECT r.duration_minutes, 
	                           right(h.health_descriptor, -1) as severity
                        FROM report r, health h;
                          ''').format(sql.Literal(report_id))
                          
            response = psql.get_response(cmd)
    
            # Unpack response & add to dataframe
            
            temp_df['report_id'] = report_id
            temp_df['duration_minutes'] = response[0][0]
            temp_df['severity'] = response[0][1]
            
            # ~~~~~~~~~~~~~~~~~~
            
            # Lastly, concatenate to user_df
            
            user_df = pd.concat([user_df, temp_df], ignore_index = True)
               
    
   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  

    # Datatype corrections (for non-strings)
    
    if len(user_df) > 0:
    
        # Integers
        
        int_cols = {'user_id', 'duration_minutes'}
        
        for col in set(user_df.columns).intersection(int_cols):

            user_df[col] = user_df[col].astype(int)
    
    return user_df
