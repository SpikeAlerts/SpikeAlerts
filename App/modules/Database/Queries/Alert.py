# Queries for our database

## Load modules

from psycopg2 import sql
from modules.Database import Basic_PSQL as psql

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~
    
def Get_alerted_sensor_ids(sensitive, sensor_id_filter = []):
    '''
    Get sensor_ids for sensors with an active alert from database
    where sensitive = sensitive
    AND sensor_id is any of the sensor_ids in sensor_id_filter (an
    Returns a list
    '''
    
    cmd = sql.SQL('''SELECT sensor_id 
    FROM "Active Alerts"
    WHERE sensitive = {}
    AND sensor_id = ANY ( {} );
    ''').format(sql.Literal(sensitive),
                sql.Literal(sensor_id_filter))
    
    response = psql.get_response(cmd)   
    # Convert response into list
    
    sensor_ids = [i[0] for i in response] # Unpack results into list
    
    return sensor_ids
 
# ~~~~~~~~~~~~~~~~~~ 
   
def Get_max_active_alert_id():
    '''
    Get the highest alert_id in active alerts
    It will return 0 if there are none
    Returns an integer
    '''
    
    cmd = sql.SQL('''SELECT MAX(alert_id) 
    FROM "Active Alerts";
    ''')
    
    
    response = psql.get_response(cmd)
    
    if response[0][0] == None:
        max_alert_id = 0
    else:
        max_alert_id = response[0][0]

    return max_alert_id 
    
