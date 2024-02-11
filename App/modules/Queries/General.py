# Queries for our database

## Load modules

from psycopg2 import sql
from modules import Basic_PSQL as psql
import pandas as pd
import datetime as dt

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Daily_Updates

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~      

def Get_last_Daily_Log():
    '''
    This function gets the highest last_seen (only updated daily)
    
    returns timezone aware datetime
    '''

    cmd = sql.SQL('''SELECT MAX(date)
    FROM "Daily Log";
    ''')
    
    response = psql.get_response(cmd)

    # Unpack response into timezone aware datetime
    
    if response[0][0] != None:

        last_update_date = response[0][0]
    else:
        last_update_date = dt.date(2000, 1, 1)
    
    return last_update_date
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_extent(): 
    '''
    Gets the bounding box of our project's extent + 100 meters
    
    Specifically for PurpleAir api
    
    returns nwlng, selat, selng, nwlat AS strings
    '''   
    
    # Query for bounding box of boundary buffered 100 meters

    cmd = sql.SQL('''SELECT minlng, minlat, maxlng, maxlat from "extent"
    ''')

    response = psql.get_response(cmd)
    
    # Convert into PurpleAir API notation
    nwlng, selat, selng, nwlat = response[0]
    
    return nwlng, selat, selng, nwlat
    

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 

def Get_newest_user(pg_connection_dict):
    '''
    This function gets the newest user's record_id
    
    returns an integer
    '''

    cmd = sql.SQL('''SELECT MAX(record_id)
    FROM "Sign Up Information";
    ''')

    response = psql.get_response(cmd, pg_connection_dict)
    
    if response[0][0] == None:
        max_record_id = 0
    else:
        max_record_id = response[0][0]
    
    return max_record_id

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_afterhour_reports(pg_connection_dict):
    '''
    This function gets all the afterhour reports
    
    returns a list of tuples
    '''

    cmd = sql.SQL('''SELECT *
    FROM "Afterhour Reports";
    ''')

    response = psql.get_response(cmd, pg_connection_dict)
    
    return response
    
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_ongoing_alert_record_ids(pg_connection_dict):
    '''
    This function gets users' record_ids that have an active alert
    
    returns a list
    '''

    cmd = sql.SQL('''SELECT record_id FROM "Sign Up Information"
    WHERE ARRAY_LENGTH(active_alerts, 1) > 0;
    ''')

    response = psql.get_response(cmd, pg_connection_dict)
    
    record_ids = [i[0] for i in response] # Unpack results into list
    
    return record_ids

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# GetSort_Spikes

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### Function to get the sensor_ids from our database

def Get_sensor_ids(pg_connection_dict):
    '''
    This function gets the sensor_ids of all sensors in our database that are not flagged from previous days.
    Returns a list of integers
    '''

    cmd = sql.SQL('''SELECT sensor_index 
    FROM "PurpleAir Stations"
    WHERE channel_flags = ANY (ARRAY[0,4]) AND channel_state = 3; -- channel_flags are updated daily, channel_state <- managed by someone - 3 = on, 0 = off
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)

    # Unpack response into pandas series

    sensor_ids = [int(i[0]) for i in response]

    return sensor_ids

# ~~~~~~~~~~~~~~~~~~
    
def Get_previous_active_sensors(pg_connection_dict):
    '''
    Get active alerts from database sensor_indices.
    Returns active_alerts pd.DataFrame with a column of sensor_indices (lists are the elements)
    '''
    
    cmd = sql.SQL('''SELECT sensor_indices 
    FROM "Active Alerts Acute PurpleAir";
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)   
    # Convert response into dataframe
    
    cols_for_active_alerts = ['sensor_indices']
    active_alerts_df = pd.DataFrame(response, columns = cols_for_active_alerts)

    
    return active_alerts_df

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_not_elevated_sensors(pg_connection_dict, alert_lag=20):
    '''
    Get sensor_indices from database where the sensor has not been elevated in 30 minutes
    Returns sensor_indices
    '''
    
    cmd = sql.SQL(f'''SELECT sensor_index 
    FROM "PurpleAir Stations"
    WHERE last_elevated + INTERVAL '{alert_lag} Minutes' < CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago';
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)   
    # Convert response into dataframe
    
    sensor_indices = [i[0] for i in response] # Unpack results into list

    return sensor_indices
    
### ~~~~~~~~~~~~~~~~~

##  New_Alerts

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_active_users_nearby_sensor(pg_connection_dict, sensor_index, distance=1000):
    '''
    This function will return a list of record_ids from "Sign Up Information" that are within the distance from the sensor and subscribed
    
    sensor_index = integer
    distance = integer (in meters)
    
    returns record_ids (a list)
    '''

    cmd = sql.SQL('''
    WITH sensor as -- query for the desired sensor
    (
    SELECT sensor_index, geometry
    FROM "PurpleAir Stations"
    WHERE sensor_index = {}
    )
    SELECT record_id
    FROM "Sign Up Information" u, sensor s
    WHERE u.subscribed = TRUE AND ST_DWithin(ST_Transform(u.geometry,26915), -- query for users within the distance from the sensor
										    ST_Transform(s.geometry, 26915),{}); 
    ''').format(sql.Literal(sensor_index),
                sql.Literal(distance))

    response = psql.get_response(cmd, pg_connection_dict)

    record_ids = [i[0] for i in response] # Unpack results into list

    return record_ids

# ~~~~~~~~~~~~~~


def Get_users_to_message_new_alert(pg_connection_dict, record_ids):
    '''
    This function will return a list of record_ids from "Sign Up Information" that have empty active and cached alerts and are in the list or record_ids given
    
    record_ids = a list of ids to check
    
    returns record_ids_to_text (a list)
    '''

    cmd = sql.SQL('''
    SELECT record_id
    FROM "Sign Up Information"
    WHERE active_alerts = {} AND cached_alerts = {} AND record_id = ANY ( {} );
    ''').format(sql.Literal('{}'), sql.Literal('{}'), sql.Literal(record_ids))

    response = psql.get_response(cmd, pg_connection_dict)

    record_ids_to_text = [i[0] for i in response]

    return record_ids_to_text
    
# ~~~~~~~~~~~~~~~~~~~~~~~~

# Ended Alerts

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_users_to_message_end_alert(pg_connection_dict):
    '''
    This function will return a list of record_ids from "Sign Up Information" that are subscribed, have empty active_alerts, non-empty cached_alerts
    
    returns record_ids_to_text (a list)
    '''

    cmd = sql.SQL('''
    SELECT record_id
    FROM "Sign Up Information"
    WHERE subscribed = TRUE
        AND active_alerts = {}
    	AND ARRAY_LENGTH(cached_alerts, 1) > 0;
    ''').format(sql.Literal('{}'))

    response = psql.get_response(cmd, pg_connection_dict)

    record_ids_to_text = [i[0] for i in response]

    return record_ids_to_text
    
# ~~~~~~~~~~~~~~ 

def Get_reports_for_day(pg_connection_dict):
    '''
    This function gets the count of reports for the day (we're considering overnights to be reports from previous day)
    '''

    cmd = sql.SQL('''SELECT reports_for_day
FROM "Daily Log"
WHERE date = DATE(CURRENT_TIMESTAMP AT TIME ZONE 'America/Chicago' - INTERVAL '8 hours');
    ''')
    
    response = psql.get_response(cmd, pg_connection_dict)

    # Unpack response into timezone aware datetime
    
    if response[0][0] != None:

        reports_for_day = int(response[0][0])
    else:
        reports_for_day = 0
    
    return reports_for_day
