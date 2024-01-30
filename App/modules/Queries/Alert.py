# Queries for our database

## Load modules

from psycopg2 import sql
from modules import Basic_PSQL as psql

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~
    
def Get_previous_active_sensors(all_sensor_ids):
    '''
    Get sensor_ids for sensors with an active alert from database
    where sensor_id in all_sensor_ids
    Returns a list
    '''
    
    cmd = sql.SQL('''SELECT sensor_id 
    FROM "Active Alerts"
    WHERE sensor_id = ANY ( {} );
    ''').format(sql.Literal(all_sensor_ids))
    
    response = psql.get_response(cmd)   
    # Convert response into dataframe
    
    sensor_ids = [i[0] for i in response] # Unpack results into list

    
    return sensor_ids

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_not_elevated_sensors(all_sensor_ids, alert_lag=20):
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
