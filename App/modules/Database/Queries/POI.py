# Queries for our database

## Load modules

from psycopg2 import sql
from modules.Database import Basic_PSQL as psql

### ~~~~~~~~~~~~~~~~~

##  New_Alerts

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_newly_alerted_pois(sensor_ids, is_sensitive, epsg_code):
    '''
    This function will return a list of poi_ids from "Places of Interest" 
    that are within the distance from an active alert, active, and have empty active and cached alerts
    '''
    
    active_field = 'active_alerts'
    cache_field = 'cached_alerts'
    
    if is_sensitive == 'TRUE':
        cache_field += '_sensitive'
        active_field += '_sensitive'
    
    cmd = sql.SQL('''
    WITH unalerted_pois as
    (
	    SELECT poi_id, geometry
	    FROM "Places of Interest"
	    WHERE {} = {}
	    AND {} = {}
	    AND active = TRUE
    ), alerted_sensors as
    (
	    SELECT radius_meters, geometry
	    FROM alerts_w_info
	    WHERE sensitive = {}
	    AND sensor_id = ANY ( {} )
    )
    SELECT poi_id 
    FROM unalerted_pois p
    INNER JOIN alerted_sensors s 
    ON (ST_DWithin(ST_Transform(p.geometry, {}),
				           ST_Transform(s.geometry, {}),
				           s.radius_meters))
    GROUP BY poi_id
    ;
    ''').format(sql.Identifier(active_field), sql.Literal('{}'),
                sql.Identifier(cache_field), sql.Literal('{}'),
                sql.Literal(is_sensitive),
                sql.Literal(sensor_ids),
                sql.Literal(int(epsg_code)),
                sql.Literal(int(epsg_code))
                )
                
    response = psql.get_response(cmd)
    
    poi_ids_to_alert = [i[0] for i in response] # Unpack results into list
    
    return poi_ids_to_alert
    
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Get_pois_to_end_alert(runtime, report_lag, is_sensitive):
    '''
    This function will return a list of poi_ids from "Places of Interest" 
    that have empty active_alerts and non-empty cached_alerts
            where end_time of any alert in cache + report_lag < runtime 
    '''
    
    active_field = 'active_alerts'
    cache_field = 'cached_alerts'
    
    if is_sensitive == 'TRUE':
        cache_field += '_sensitive'
        active_field += '_sensitive'
    
    formatted_runtime = runtime.strftime('%Y-%m-%d %H:%M:%S')
    
    cmd = sql.SQL('''
    
    -- Select the pois with empty active_alerts & non-empty cached_alerts
    WITH not_alerted_pois as
    (
	    SELECT poi_id, {} as cached_alerts
	    FROM "Places of Interest" p
	    WHERE active = True
	    AND {} = {}
	    AND ARRAY_LENGTH({}, 1) > 0
    ), pois_w_endtimes as
    
    -- Get max endtime of all alerts in cache for each poi
    (
    SELECT p.poi_id, MAX(a.start_time + INTERVAL '1 Minutes' * a.duration_minutes) as endtime
    FROM "Archived Alerts" a
    RIGHT JOIN not_alerted_pois p ON (a.alert_id = ANY (p.cached_alerts))
    GROUP BY p.poi_id
    ) 
    
    -- Select the poi_ids where endtime + report_lag < runtime (nowish)
    SELECT poi_id
    FROM pois_w_endtimes
    WHERE endtime +  INTERVAL '1 Minutes' * {} <= {};
    ''').format(sql.Identifier(cache_field),
                sql.Identifier(active_field),
                sql.Literal('{}'),
                sql.Identifier(cache_field),
                sql.Literal(report_lag),
                sql.Literal(formatted_runtime))
                
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
FROM "Places of Interest" p, alerts_w_info s
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
