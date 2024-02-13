# Queries for our database

## Load modules

from psycopg2 import sql
from modules.Database import Basic_PSQL as psql

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~
    
def Get_alerted_sensor_ids():
    '''
    Get sensor_ids for sensors with an active alert from database
    where sensor_id in all_sensor_ids
    Returns a list
    '''
    
    cmd = sql.SQL('''SELECT sensor_id 
    FROM "Active Alerts";
    ''')
    
    response = psql.get_response(cmd)   
    # Convert response into dataframe
    
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
    
