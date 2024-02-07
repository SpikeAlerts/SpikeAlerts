# Queries for our database

## Load modules

from psycopg2 import sql
from modules import Basic_PSQL as psql

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~
    
def Get_alerted_sensor_ids():
    '''
    Get sensor_ids for sensors with an active alert from database
    where sensor_id in all_sensor_ids
    Returns a list
    '''
    
    cmd = sql.SQL('''SELECT sensor_id 
    FROM "Active Alerts"
    ''')
    
    response = psql.get_response(cmd)   
    # Convert response into dataframe
    
    sensor_ids = [i[0] for i in response] # Unpack results into list

    
    return sensor_ids
