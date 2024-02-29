### Import Packages

# Time

import datetime as dt # Working with dates/times

# Database 

from modules.Database import Basic_PSQL as psql
from modules.Database.Queries import POI as poi_query
from modules.Database.Queries import General as query
from psycopg2 import sql

## Workflow

def workflow(sensor_id_dict, ended_alert_ids, runtime, base_config):
    '''
    Runs the full workflow to update our database tables "Places of Interest" and "Reports Archive". 
    This involves the following:

    Iterate through new/ended alerts in sensor_id_dict
    
    a) Update active_alerts for new alerts
    b) Update active_alerts and cached_alerts for ended alerts
    
    Then 
    
    c) Check for POIs with empty active_alerts and non-empty cached_alerts 
        where end_time of any alert_id in cached_alerts + report_lag < runtime
        
    d) if len(c) > 0: Write Reports & Clear cached_alerts for all in C
    
    To Test: Alter sensor health thresholds with
    
    UPDATE "Sensor Type Information"
    SET thresholds = ARRAY[0, 12.1, 35.5, 55.5, 150.5, 250.5, 1000],
    last_update = last_update - Interval '10 minutes';
    
    Parameters:
    
    sensor_id_dict - dictionary used to determine which steps to conduct. Has the following structure:
    
    {'TRUE' : {'new' : set of sensor_ids,
               'ongoing' : set of sensor_ids,
               'ended' : set of sensor_ids}
      'FALSE' : {'new' : set of sensor_ids,
                 'ongoing' : set of sensor_ids,
                 'ended' : set of sensor_ids}
      }
                      
         where TRUE = for sensitive populations
                FALSE = for all populations
                
    ended_alert_ids - list of integers - this is a list of the alert_ids that just ended
    
    runtime - datetime - approximate time that the values for above dataframe were acquired
    
    base_config - dictionary - environment variables
    
    returns a dictionary (reports_dict) with the following format:
     
              {
              'TRUE' : list of tuples of (poi_id, report_id)
              'FALSE' : list of tuples of (poi_id, report_id)
              }
                              
                 where TRUE = for sensitive populations
                        FALSE = for all populations   
    '''
    
    # Unpack important variables from base_config
    
    # report_lag - int - minutes to delay writing a report
    
    # epsg_code - string - epsg code for local UTM coordinate reference system (for distance calculations)
    
    report_lag, epsg_code = base_config['REPORT_LAG'], base_config['EPSG_CODE']
    
    # Initialize dictionary to return
    
    reports_dict = {
                    'TRUE': [],
                    'FALSE': []
                    }
                    
    # Initialize iterators

    alert_types = ['new', 'ended'] # 'new' or 'ended' alerts (don't update pois if ongoing alert)
    sensitivities = ['FALSE', 'TRUE'] # Matches "sensitive" in database alert tables
                                           # True = alert for sensitive pops, False = alert for all pops
    
    # Iterate
    
    for alert_type in alert_types:
    
        for is_sensitive in sensitivities:
    
            sensor_ids = sensor_id_dict[is_sensitive][alert_type]
            
            # ~~~~~~~~~~~~~~~~
            # a) New Alerts
            
            if (alert_type == 'new') and (len(sensor_ids) > 0):
            
                # Update the active_alerts
                
                Add_alerts_to_pois(list(sensor_ids), is_sensitive, epsg_code)
                
            # ~~~~~~~~~~~~~~~~
            # b) Ended Alerts

            if (alert_type == 'ended') and (len(sensor_ids) > 0):

                # Update the cached_alerts
                
                Cache_alerts(is_sensitive, ended_alert_ids)
    
    # ~~~~~~~~~~~~~~~~
    # Finally, we will write reports
    
    for is_sensitive in sensitivities:
    
        # c) Check for POIs with empty active_alerts and non-empty cached_alerts 
        # that haven't been alerted for report_lag minutes
        
        poi_ids_to_end_alert = poi_query.Get_pois_to_end_alert(runtime, report_lag, is_sensitive)
        
        # ~~~~~~~~~~~~~~~~
        # d) Write Reports & clear cache for poi_ids_to_end_alert
        
        if len(poi_ids_to_end_alert) > 0:
        
            # Get reports_for_day
        
            reports_for_day = query.Get_reports_for_day(runtime)
            
            new_reports = [] # This is a list of tuples containing (poi_id, report_id)
        
            for poi_id in poi_ids_to_end_alert:
            
                report_id = Initialize_report(poi_id, reports_for_day, runtime, is_sensitive)
                
                new_reports += [(poi_id, report_id)]
                
                reports_for_day += 1 
                
            reports_dict[is_sensitive] = new_reports # Save for notifications
                
            # Update reports for day
            
            Update_reports_for_day(runtime, reports_for_day)
            
            # Clear the cached alerts
            
            Clear_cached_alerts(poi_ids_to_end_alert, is_sensitive)
            
    return reports_dict

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# NEW ALERTS

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Add_alerts_to_pois(sensor_ids, is_sensitive, epsg_code):
    '''
    This function will update the active_alerts for the POIs
    
    parameters:
    
    sensor_ids - list of sensor_ids
    is_sensitive - 'TRUE' or 'FALSE' corresponding to sensitive field in database alert tables 
    epsg_code - string - epsg code for local UTM coordinate reference system (for distance calculations)
    '''
    
    update_field = 'active_alerts'
    
    if is_sensitive == 'TRUE':
        update_field += '_sensitive'
    
    cmd = sql.SQL('''
    WITH alerts_to_update as
	(
	    SELECT alert_id, radius_meters, geometry
	    FROM alerts_w_info
	    WHERE sensor_id = ANY ( {} )
	    AND sensitive = {}
	), pois_w_alert_ids AS
    (
	    SELECT p.poi_id, ARRAY_AGG(s.alert_id) as new_alerts
	    FROM alerts_to_update s, "Places of Interest" p
	    WHERE p.active = TRUE
	    AND ST_DWithin(ST_Transform(p.geometry, {}), -- CHANGE THIS!!
				       ST_Transform(s.geometry, {}), -- CHANGE THIS!!
				       s.radius_meters)
	    GROUP BY p.poi_id
    )
    UPDATE "Places of Interest" p
    SET {} = ARRAY_CAT(p.{}, a.new_alerts)
    FROM pois_w_alert_ids a
    WHERE p.poi_id = a.poi_id
    ;
    ''').format(sql.Literal(sensor_ids),
                sql.Literal(is_sensitive),
                sql.Literal(int(epsg_code)),
                sql.Literal(int(epsg_code)),
                sql.Identifier(update_field),
                sql.Identifier(update_field))
    
    psql.send_update(cmd)
    
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Ended Alerts

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Cache_alerts(is_sensitive, ended_alert_ids):
    '''
    This function will update the cached_alerts for the POIs
    
    parameters:
    
    ended_alert_ids - list of integers - the alert_ids that just ended
    '''
    
    active_field = 'active_alerts'
    cache_field = 'cached_alerts'
    
    if is_sensitive == 'TRUE':
        cache_field += '_sensitive'
        active_field += '_sensitive'
    
    for alert_id in ended_alert_ids:
    
        cmd = sql.SQL('''
        UPDATE "Places of Interest" p
        SET {} = ARRAY_REMOVE(p.{}, {}),
            {} = ARRAY_APPEND(p.{}, {})
        WHERE {} = ANY ( p.{} )
        ;
        ''').format(sql.Identifier(active_field),
                    sql.Identifier(active_field),
                    sql.Literal(alert_id),
                    sql.Identifier(cache_field),
                    sql.Identifier(cache_field),
                    sql.Literal(alert_id),
                    sql.Literal(alert_id),
                    sql.Identifier(active_field)
                    )
    
        psql.send_update(cmd)
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# Write Reports

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Initialize_report(poi_id, reports_for_day, runtime, is_sensitive):
    '''
    This function will initialize a unique report for a poi in the database.

    It will also return the report_id of the report
    '''
    
    active_field = 'active_alerts'
    cache_field = 'cached_alerts'
    
    if is_sensitive == 'TRUE':
        cache_field += '_sensitive'
        active_field += '_sensitive'
    
    # Create Report_id
    
    report_date = runtime
    formatted_runtime = runtime.strftime('%Y-%m-%d %H:%M:%S')
    
    report_id = str(reports_for_day).zfill(5) + '-' + report_date.strftime('%m%d%y') # XXXXX-MMDDYY
    # Use the poi_id to query for the poi's cached_alerts
    # Then aggregate from those alerts the start_time, time_difference
    # Lastly, it will insert all the information into "Reports Archive"
    
    cmd = sql.SQL('''WITH poi_alert_cache as
(
	SELECT name, {} as cached_alerts
	FROM "Places of Interest"
	WHERE poi_id = {} --inserted record_id
), alerts as
(
	SELECT MIN(p.start_time) as start_time,
			{} - MIN(p.start_time) as time_diff
	FROM "Archived Alerts" p, poi_alert_cache c
	WHERE p.alert_id = ANY (c.cached_alerts)
)
INSERT INTO "Reports Archive" (report_id, poi_name, start_time, duration_minutes, sensitive, alert_ids)
SELECT {}, -- Inserted report_id
        p.name,
        a.start_time, -- start_time
		(((DATE_PART('day', a.time_diff) * 24) + 
    		DATE_PART('hour', a.time_diff)) * 60 + 
		 	DATE_PART('minute', a.time_diff)) as duration_minutes,
			{}, -- is this report for sensitive populations?
		p.cached_alerts
FROM poi_alert_cache p, alerts a; 
''').format(sql.Identifier(cache_field),
            sql.Literal(poi_id),
            sql.Literal(formatted_runtime),
            sql.Literal(report_id),
            sql.Literal(is_sensitive))

    psql.send_update(cmd)

    return report_id    
    
# ~~~~~~~~~~~~~~~~

def Update_reports_for_day(runtime, reports_for_day):
    '''
    This function updates reports for day to the new value
    '''
    
    formatted_runtime = runtime.strftime('%Y-%m-%d')
    
    cmd = sql.SQL(f'''UPDATE "Daily Log"
                    SET reports_for_day = {reports_for_day}
                    WHERE date = DATE('{formatted_runtime}');
                   ''')#.format(sql.Literal(reports_for_day),
                        #       sql.Literal(formatted_runtime)
                         #      )
                   
    psql.send_update(cmd)
    
# ~~~~~~~~~~~~~~~~

# 3) Transfer these alerts from "Sign Up Information" active_alerts to "Sign Up Information" cached_alerts

def Clear_cached_alerts(poi_ids_to_end_alert, is_sensitive):
    '''
    This function clears the cached_alerts in the poi_ids given
    '''
    active_field = 'active_alerts'
    cache_field = 'cached_alerts'
    
    if is_sensitive == 'TRUE':
        cache_field += '_sensitive'
        active_field += '_sensitive'
    
    cmd = sql.SQL('''
    UPDATE "Places of Interest"
    SET {} = {}
    WHERE poi_id = ANY ({});
    ''').format(sql.Identifier(cache_field),
                sql.Literal('{}'),
                sql.Literal(poi_ids_to_end_alert)
               )
    
    psql.send_update(cmd) 
    
    
    

