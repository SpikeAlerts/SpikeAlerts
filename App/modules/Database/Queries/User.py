# Functions to work with any sensor in our database

# Data Manipulation

import pandas as pd

# Time

import pytz # Timezones

# Database

from psycopg2 import sql
import modules.Database.Basic_PSQL as psql


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_Users_to_alert(timezone, min_message_frequency):
    '''
    This function queries the database for unalerted users that:
    have alerted poi_ids
    today is the day to contact
    and the time is within their desired contact hours
    
    parameters:
    
    timezone - string - timezone from the base .env file
    min_message_frequency - str - minimum number of minutes between ending and sending a new alert to a user
    
    returns a dataframe with fields
    
    user_id - int - our unique identifier for users
    poi_id - int - our unique identifier for Places of Interest
    sensitive - boolean - user gets sensitive alerts
    contact_method - str - corresponds to a script in modules/Users/Contact_Methods
    api_id - str - id for the user information in remote database
    '''
    
    fields = ['user_id', 'poi_id', 'sensitive', 'contact_method', 'api_id']

    cmd = sql.SQL('''
    -- Users to Alert (user_id, poi_id, sensitive, contact_method, api_id)
    WITH alerted_pois as
    (
	    SELECT poi_id, TRUE as "sensitive" -- POIs alerted for sensitive groups
	    FROM "Places of Interest"
	    WHERE active_alerts_sensitive != {}
	    UNION ALL
	    SELECT poi_id, FALSE as "sensitive" -- POIs alerted for everyone
	    FROM "Places of Interest"
	    WHERE active_alerts != {}
    )
    SELECT u.user_id, p.poi_id, u.sensitive, u.contact_method, u.api_id
    FROM "Users" u
    LEFT JOIN alerted_pois p ON (u.poi_id = p.poi_id
								    AND u.sensitive = p.sensitive
								    ) -- Sensitive Users
    WHERE
    alerted = FALSE -- Not Alerted
    AND EXTRACT(dow FROM CURRENT_DATE AT TIME ZONE {}) = ANY ( days_to_contact ) -- Days to contact user
    AND start_time < CURRENT_TIME AT TIME ZONE {} -- Current time less than Start time
    AND end_time > CURRENT_TIME AT TIME ZONE {} -- Current time greater than Start time
    AND last_contact + INTERVAL '1 Minutes' * {} <= CURRENT_TIMESTAMP AT TIME ZONE {}; -- has the user been contacted too recently?
    ''').format(sql.Literal('{}'),
                sql.Literal('{}'),
                sql.Literal(timezone),
                sql.Literal(timezone),
                sql.Literal(timezone),
                sql.Literal(int(min_message_frequency)),
                sql.Literal(timezone)
                )
                
    response = psql.get_response(cmd) 
    
    # Unpack response into pandas series

    df = pd.DataFrame(response, columns = fields)
    
   # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  

    # Datatype corrections (for non-strings)
    
    if len(df) > 0:
    
        # Integers
        
        df['user_id'] = df['user_id'].astype(int)
        df['poi_id'] = df['poi_id'].astype(int)
        
        # Booleans
        
        bool_cols = {'sensitive'}
        
        for col in set(fields).intersection(bool_cols):

            df[col] = df[col].astype(bool)
        
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~    
     
    # Copy and return    
    
    user_df = df.copy()
    
    return user_df
