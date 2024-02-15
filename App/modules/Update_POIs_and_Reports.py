### Import Packages

# Time

import datetime as dt # Working with dates/times

# Database 

from modules.Database import Basic_PSQL as psql
from modules.Database.Queries import POI as poi_query
from modules.Database.Queries import General as query
from psycopg2 import sql

## Workflow

def workflow(sensors_df, runtime):
    '''
    Runs the full workflow to update our database tables "Places of Interest" and "Reports Archive". 
    This involves the following:

    a) Check for newly alerted POIs
    b) Update active_alerts and cached_alerts
    c) Check for POIs with empty active_alerts and non-empty cached_alerts
    d) Write Reports & Clear cached_alerts for all in C
    
    Parameters:
    
    sensors_df - a dataframe with the following columns:

    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    update_frequency - int - frequency this sensor is updated
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks
    radius_meters - int - max distance sensor is relevant
    sensor_status - text - one of these categories: new_spike, ongoing_spike, ended_spike, unknown, or ordinary
    
    runtime - approximate time that the values for above dataframe were acquired
    
    returns 2 lists: poi_ids_to_alert, poi_ids_to_end_alert
    '''

    # ~~~~~~~~~~~~~~~~
    # a) New Alerts
    
    poi_ids_to_alert = poi_query.Get_pois_to_alert()
    
    # ~~~~~~~~~~~~~~~~
    # b) Update active and cached alerts (probably should check to see that there are actually alerts to update)
    
    Update_active_and_cached_alerts()
    
    # ~~~~~~~~~~~~~~~~
    # c) Check for POIs with empty active_alerts and non-empty cached_alerts
    
    poi_ids_to_end_alert = poi_query.Get_pois_to_end_alert()
    
    # ~~~~~~~~~~~~~~~~
    # d) Write Reports & clear cache for poi_ids_to_end_alert
    
    if len(poi_ids_to_end_alert) > 0:
    
        # Get reports_for_day
    
        reports_for_day = query.Get_reports_for_day(runtime)
        
        print(reports_for_day)
    
        for poi_id in poi_ids_to_end_alert:
        
            start_time, duration_minutes, severity, report_id = Initialize_report(poi_id, reports_for_day, runtime)
            
            print(start_time, duration_minutes, severity, report_id)
            
            reports_for_day += 1 
            
        # Update reports for day
        
        Update_reports_for_day(runtime, reports_for_day)
        
        # Clear the cached alerts
        
        Clear_cached_alerts(poi_ids_to_end_alert)
            
    print(poi_ids_to_alert, poi_ids_to_end_alert)
    
    return poi_ids_to_alert, poi_ids_to_end_alert

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Update_active_and_cached_alerts():
    '''
    This function will update the active and cached_alerts for the POIs
    active = nearby_alerts for each poi
    cached = previous_active - nearby_alerts
    '''
    
    cmd = sql.SQL('''
    WITH merged as
    (
        SELECT p.poi_id,
		       COALESCE(a.nearby_alerts, {}) as nearby_alerts, 
		       p.active_alerts,
		       p.cached_alerts
        FROM "Places of Interest" p
        LEFT JOIN pois_w_alert_ids a ON (a.poi_id = p.poi_id)
    )
    UPDATE "Places of Interest" p
    SET active_alerts = a.nearby_alerts,
	    cached_alerts = ARRAY_CAT(a.cached_alerts, ARRAY_DIFF(a.active_alerts, a.nearby_alerts))
    FROM merged a
    WHERE p.poi_id = a.poi_id
    AND (ARRAY_LENGTH(a.nearby_alerts, 1) > 0
    OR ARRAY_LENGTH(p.active_alerts, 1) > 0);
    ''').format(sql.Literal('{}'))
    
    psql.send_update(cmd)
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
# ~~~~~~~~~~~~~~ 
def Initialize_report(poi_id, reports_for_day, runtime):
    '''
    This function will initialize a unique report for a poi in the database.

    It will also return the start_time/duration_minutes/severity/report_id of the report
    '''
    
    # Create Report_id
    
    report_date = runtime
    formatted_runtime = runtime.strftime('%Y-%m-%d %H:%M:%S')
    
    report_id = str(reports_for_day).zfill(5) + '-' + report_date.strftime('%m%d%y') # XXXXX-MMDDYY
    # Use the poi_id to query for the poi's cached_alerts
    # Then aggregate from those alerts the start_time, time_difference
    # Lastly, it will insert all the information into "Reports Archive"
    
    cmd = sql.SQL('''WITH alert_cache as
(
	SELECT cached_alerts
	FROM "Places of Interest"
	WHERE poi_id = {} --inserted record_id
), alerts as
(
	SELECT MIN(p.start_time) as start_time,
			{} - MIN(p.start_time) as time_diff
	FROM "Archived Alerts" p, alert_cache c
	WHERE p.alert_id = ANY (c.cached_alerts)
)
INSERT INTO "Reports Archive"
SELECT {}, -- Inserted report_id
        a.start_time, -- start_time
		(((DATE_PART('day', a.time_diff) * 24) + 
    		DATE_PART('hour', a.time_diff)) * 60 + 
		 	DATE_PART('minute', a.time_diff)) as duration_minutes,
			'unhealthy', -- NOT DONE - the severity of the alert
		c.cached_alerts
FROM alert_cache c, alerts a; 
''').format(sql.Literal(poi_id),
            sql.Literal(formatted_runtime),
            sql.Literal(report_id))

    psql.send_update(cmd)

    # Now get the information from that report

    cmd = sql.SQL('''SELECT start_time, duration_minutes, severity
             FROM "Reports Archive"
             WHERE report_id = {};
''').format(sql.Literal(report_id))

    response = psql.get_response(cmd)

    # Unpack response
    start_time, duration_minutes, severity = response[0]

    return start_time, duration_minutes, severity, report_id    
    
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

def Clear_cached_alerts(poi_ids_to_end_alert):
    '''
    This function clears the cached_alerts in the poi_ids given
    '''
    
    
    cmd = sql.SQL('''
    UPDATE "Places of Interest"
    SET cached_alerts = {}
    WHERE poi_id = ANY ({});
    ''').format(sql.Literal('{}'),
                sql.Literal(poi_ids_to_end_alert)
               )
    
    psql.send_update(cmd) 
    
    
    

