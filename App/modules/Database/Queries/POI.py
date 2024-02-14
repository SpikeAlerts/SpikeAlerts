# Queries for our database

## Load modules

from psycopg2 import sql
from modules.Database import Basic_PSQL as psql

### ~~~~~~~~~~~~~~~~~

##  New_Alerts

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_pois_to_alert():
    '''
    This function will return a list of poi_ids from "Points of Interest" that are within the distance from an active alert, active, and have empty active and cached alerts
    '''
    
    cmd = sql.SQL('''
    SELECT a.poi_id
    FROM pois_w_alert_ids a
    INNER JOIN "Points of Interest" p ON (a.poi_id = p.poi_id)
    WHERE p.active_alerts = {}
    AND p.cached_alerts = {};
    ''').format(sql.Literal('{}'),
                sql.Literal('{}'))
                
    response = psql.get_response(cmd)
    
    poi_ids_to_alert = [i[0] for i in response] # Unpack results into list
    
    return poi_ids_to_alert
    
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_pois_to_end_alert():
    '''
    This function will return a list of poi_ids from "Points of Interest" that have empty active_alerts and non-empty cached_alerts
    '''
    
    cmd = sql.SQL('''
    SELECT poi_id
    FROM "Points of Interest" p
    WHERE active = True
    AND active_alerts = {}
    AND ARRAY_LENGTH(cached_alerts, 1) > 0;
    ''').format(sql.Literal('{}'))
                
    response = psql.get_response(cmd)
    
    poi_ids_to_end_alert = [i[0] for i in response] # Unpack results into list
    
    return poi_ids_to_end_alert
    
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


    
# ~~~~~~~~~~~~~~~~~~~ MISC SQL
'''
WITH alerted_pois as
(
SELECT p.poi_id,
	   s.sensor_id, s.name, ARRAY_AGG(s.alert_id), s.start_time, s.last_seen,
	   s.pollutant, s.metric, s.thresholds,
       s.current_reading, s.avg_reading, s.max_reading,
	   p.geometry
FROM "Points of Interest" p, alerts_w_info s
WHERE ST_DWithin(
				ST_Transform(p.geometry, 26915),
				ST_Transform(s.geometry, 26915),
				s.radius_meters
				)
GROUP BY p.poi_id,
	   s.sensor_id, s.name, s.start_time, s.last_seen,
	   s.pollutant, s.metric, s.thresholds, s.radius_meters,
       s.current_reading, s.avg_reading, s.max_reading,
	   p.geometry
)
SELECT * 
from alerted_pois p;
'''
