# Functions to work with any sensor in our database

# Data Manipulation

import pandas as pd

# Database

from psycopg2 import sql
import modules.Database.Basic_PSQL as psql
import modules.Database.Queries.POI as poi_query
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 
def Add_POI_pts(lats, lons):
    '''
    This function adds points to the table
    "Places of Interest"
    
    parameters: 
    
    lats - list of floats - latitudes
    lons - list of floats - longitudes
    
    returns a list of new_poi_ids (ints) with the same indexing as above
    '''
    
    # Get Max poi_id
    
    max_poi_id = poi_query.Get_max_poi_id()
    
    # Calculate the next n poi_ids
    
    n = len(lats)
    
    new_poi_ids = list(range(max_poi_id+1, max_poi_id+1+n))
    
    # Add the new points

    cmd = sql.SQL('')
    
    for lat, lon in zip(lats, lons):
    
        cmd += sql.SQL('''
        INSERT INTO "Places of Interest" 
	        (
	        geometry -- Can be any geometry
	        )
        VALUES 
	        (
	        ST_SetSRID(ST_MakePoint({}, {}), 4326)
	        );
        ''').format(sql.Literal(int(lon)),
                    sql.Literal(int(lat))
                    )

    psql.send_update(cmd)
    
    return new_poi_ids  
    

 # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 
def Update_POIs_active(epsg_code):
    '''
    This function updates the "active" field in the table,
    base."Places of Interest"
    based upon if a sensor is nearby or not
    
    parameters:
    
    epsg_code - something to cast as int - The local UTM Coordinate Reference System EPSG code
    '''

    cmd = sql.SQL('''
    WITH active_sensors as
    (
	    SELECT s.sensor_id, i.radius_meters, s.geometry
	    FROM "Sensors" s
	    INNER JOIN sensor_ids_w_info i ON i.sensor_id = s.sensor_id
	    WHERE s.channel_state = 1
    ), pois_w_no_nearby_sensors as
    (
	    SELECT p.poi_id, 
			    BOOL_OR ( ST_DWithin(ST_Transform(p.geometry, {}), -- CHANGE THIS!!
					       ST_Transform(s.geometry, {}), -- CHANGE THIS!!
					       s.radius_meters)) as nearby_sensor
	    FROM active_sensors s, "Places of Interest" p
	    WHERE p.active = TRUE
	    GROUP BY p.poi_id
    )
    UPDATE "Places of Interest" pois
    SET active = p.nearby_sensor
    FROM pois_w_no_nearby_sensors p
    WHERE pois.poi_id = p.poi_id;
    ''').format(sql.Literal(int(epsg_code)),
                sql.Literal(int(epsg_code))
                )

    psql.send_update(cmd)
