# Queries for our database

## Load modules

from psycopg2 import sql
from modules.Database import Basic_PSQL as psql
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

    cmd = sql.SQL('''SELECT minlng, minlat, maxlng, maxlat from "extent";
    ''')

    response = psql.get_response(cmd)
    
    # Convert into PurpleAir API notation
    nwlng, selat, selng, nwlat = response[0]
    
    return nwlng, selat, selng, nwlat

# ~~~~~~~~~~~~~~ 

def Get_reports_for_day(runtime):
    '''
    This function gets the count of reports for the day (we're considering overnights to be reports from previous day)
    '''

    formatted_runtime = runtime.strftime('%Y-%m-%d')

    cmd = sql.SQL(f'''SELECT reports_for_day
FROM "Daily Log"
WHERE date = DATE('{formatted_runtime}');
    ''')#.format(sql.SQL(formatted_runtime))
    
    response = psql.get_response(cmd)

    # Unpack response
    
    if response[0][0] != None:

        reports_for_day = int(response[0][0])
    else:
        reports_for_day = 0
    
    return reports_for_day
