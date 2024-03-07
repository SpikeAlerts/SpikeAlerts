### Import Packages

# Time

import datetime as dt # Working with dates/times

# Database 

from modules.Database.Queries import Alert as alert_query
from modules.Database import Basic_PSQL as psql
from psycopg2 import sql

## Workflow

def workflow(sensors_df, runtime):
    '''
    Runs the full workflow to update our database tables "Active Alerts" and "Archived Alerts". 
    This involves the following:
    
    1) Sort the sensor_ids into sets which are used to inform updates
        Categories - {'TRUE' : {'new' : set of sensor_ids,
                                     'ongoing' : set of sensor_ids,
                                     'ended' : set of sensor_ids}
                      'FALSE' : {'new' : set of sensor_ids,
                                   'ongoing' : set of sensor_ids,
                                   'ended' : set of sensor_ids}
                      }
                      
         where TRUE = for sensitive populations
                FALSE = for all populations
                
      This dictionary is returned by this function

    2) For each category (sensitive & for_all):

        a) New Alerts - Add to "Active Alerts"
        b) Ongoing Alerts - 
            i) Update alerts' avg_reading and max_reading in "Active Alerts" 
        c) Ended Alerts - Add alerts to "Archived Alerts" and remove from "Active Alerts"
    
    Parameters:
    
    sensors_df - a dataframe with the following columns:

    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks: good, moderate, unhealthy for sensitive groups, unhealthy, very unhealthy, hazardous
    radius_meters - int - max distance sensor is relevant
    
    runtime - approximate time that the values for above dataframe were acquired
    
    returns sensor_id_dict, ended_alert_ids
    '''
    
    # Initialize returns
    
    ended_alert_ids = []

    # ~~~~~~~~~~~~~~~~
    # 1) Sort sensor_ids
    
    sensor_id_dict = Sort_sensor_ids(sensors_df)
    
    # 2) Iterate through the categories and perform the necessary updates
    
    for is_sensitive in sensor_id_dict: # Is this for sensitive populations or not?
    
        sub_dict = sensor_id_dict[is_sensitive]
        
        if len(sub_dict['new']) > 0:
        
            print('inserting new alerts. Sensitive = ', is_sensitive)
            
            new_spikes_df = sensors_df[sensors_df.sensor_id.isin(sub_dict['new'])]
            
            Add_active_alerts(new_spikes_df, runtime, is_sensitive)
            
        if len(sub_dict['ongoing']) > 0:
        
            print('updating ongoing alerts. Sensitive = ', is_sensitive)
            
            ongoing_spikes_df = sensors_df[sensors_df.sensor_id.isin(sub_dict['ongoing'])]
            
            Update_active_alerts(ongoing_spikes_df, runtime, is_sensitive)
    
        if len(sub_dict['ended']) > 0:
        
            print('ending alerts. Sensitive = ', is_sensitive)
            
            ended_spikes_df = sensors_df[sensors_df.sensor_id.isin(sub_dict['ended'])]
            
            # Add to alert archive
    
            Add_archived_alerts(ended_spikes_df, is_sensitive)
        
            # Remove from active alerts
        
            ended_alert_ids += Remove_active_alerts(ended_spikes_df, is_sensitive)
            
    return sensor_id_dict, ended_alert_ids

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Sort_sensor_ids(sensors_df):
    '''
    This sorts the sensor indices into sets based on
    if they are spiked only for sensitive populations ("TRUE") or for all ("FALSE")
    ("TRUE" should contain the "FALSE" sets of sensor_ids)
    with further subcategories of new, ongoing, or ended alerts
    
    The returned dictionary has the following structure:
    
    {'TRUE' : {'new' : set of sensor_ids,
                    'ongoing' : set of sensor_ids,
                    'ended' : set of sensor_ids}
      'FALSE' : {'new' : set of sensor_ids,
               'ongoing' : set of sensor_ids,
               'ended' : set of sensor_ids}
                  }
    
    Inputs: sensors_df with columns
    
    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    update_frequency - int - frequency this sensor is updated (in minutes)
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks
    radius_meters - int - max distance sensor is relevant
    is_flagged - binary - is the sensor flagged?
            
    returns sensor_id_dict
    '''
    
    # Initialize the dictionary
                
    sensor_id_dict = {'TRUE':{'new': set(),
                              'ongoing': set(),
                              'ended': set()
                            },
                     'FALSE':{'new': set(),
                              'ongoing': set(),
                              'ended': set()
                            }
                    }
                    
    # Get a list of all sensor_ids that have been called
    
    all_sensor_ids = sensors_df.sensor_id.to_list()
    
    # Get dataframe of spiked sensors (sensitive)
    
    sensitive_spike_descriptors = ['unhealthy for sensitive groups', 'unhealthy', 'very unhealthy', 'hazardous']
    
    spiked_df_sensitive = sensors_df[sensors_df.health_descriptor.isin(sensitive_spike_descriptors)]
    
    # Get dataframe of spiked sensors (all)
    
    spiked_df = spiked_df_sensitive[spiked_df_sensitive.health_descriptor != 'unhealthy for sensitive groups']
    
    # Categorize using set operations
    
    for df, is_sensitive in [(spiked_df_sensitive, 'TRUE'), 
                             (spiked_df, 'FALSE')]:
    
        # Set_1 = spiked sensor ids (from the api call)
    
        spiked_sensor_ids = set(df.sensor_id.to_list())
        
        # Set_2 = alerted sensor ids (from our database)
        #                with this sensitivity & within this subset of sensor_ids
        
        alerted_sensor_ids = set(alert_query.Get_alerted_sensor_ids(sensitive = is_sensitive,
                                                                    sensor_id_filter = all_sensor_ids))
        
        # Set_A = "new" = Set_1 - Set_2
        
        sensor_id_dict[is_sensitive]['new'] = spiked_sensor_ids - alerted_sensor_ids
        
        # Set_B = "ongoing" = Set_1 AND Set_2
        
        sensor_id_dict[is_sensitive]['ongoing'] = spiked_sensor_ids.intersection(alerted_sensor_ids)
        
        # Set_C = "ended" = Set_2 - Set_B
        
        sensor_id_dict[is_sensitive]['ended'] = alerted_sensor_ids - sensor_id_dict[is_sensitive]['ongoing']    
    
    return sensor_id_dict
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ NEW
    
def Add_active_alerts(new_spikes_df, runtime, is_sensitive):
    '''This function adds new alerts to the "Active Alerts" table. 

    Parameters:
    Filtered sensors_df (sensor_status == 'new_spike') with columns: sensor_id and current_reading

    runtime - approximate time that the values for above dataframe were acquired
    '''
    # Initialize the DataFrame to insert into "Active Alerts"
    
    formatted_df = new_spikes_df[['sensor_id']].copy()

    # Get important values as Pandas Series for ease of use

    current_readings = new_spikes_df.current_reading # Get the current readings
    start_times = new_spikes_df.update_frequency.apply(lambda x: runtime - dt.timedelta(minutes=x))  # Technically the alert's start_time is earlier. Depends on the time interval of the averages' reported by the monitor 

    # Add necessary columns to formatted_df
    
    formatted_df['start_time'] = start_times.apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
    formatted_df['last_update'] = runtime.strftime('%Y-%m-%d %H:%M:%S')
    formatted_df['avg_reading'] = current_readings
    formatted_df['max_reading'] = current_readings
    formatted_df['sensitive'] = is_sensitive

    # Insert into database

    psql.insert_into(formatted_df, 'Active Alerts')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ONGOING

def Update_active_alerts(ongoing_spikes_df, runtime, is_sensitive):
    '''This function updates the alerts in the "Active Alerts" table. 

    The fields it updates are max_reading and avg_reading (could be improved)

    Parameters:
    Filtered sensors_df with columns: sensor_id and current_reading

    runtime - approximate time that the values for above dataframe were acquired
    is_sensitive - "TRUE" or "FALSE" is this for sensitive alerts?
    '''

    # Get the necessary variables needed for calculations
    formatted_runtime = runtime.strftime('%Y-%m-%d %H:%M:%S') # Time formatted for database

    # Write the template sql command for updating
    # First, temp_table gets the necessary time differences (previous duration of the alert (t) & timestep between now and last update (delta))
    # Second, update_table converts the above into minutes
    # Last, we compute/update last_update, max_reading, avg_reading
    
    # The average part is tricky. We are using this equation:
    # Let the previous duration (in minutes) of the alert be, t, the average be, u, and the number of minutes since that update be delta
    # Then
    # avg = u_(t+delta) = ((t x u_t) + (delta x current_reading))/(t + delta) 

    cmd_template = sql.SQL('''
    WITH temp_table as
	(SELECT sensor_id,
	        last_update - start_time as previous_time_diff,
	    {} - last_update as timestep_time_diff
	FROM "Active Alerts"
	WHERE sensor_id = {}
	AND sensitive = {}
    ),
time_table as
	(SELECT sensor_id,
	 (((DATE_PART('day', previous_time_diff) * 24) + 
    	DATE_PART('hour', previous_time_diff)) * 60 + 
	    DATE_PART('minute', previous_time_diff)) as t,
	 (((DATE_PART('day', timestep_time_diff) * 24) + 
    	DATE_PART('hour', timestep_time_diff)) * 60 + 
	    DATE_PART('minute', timestep_time_diff)) as delta
	 FROM temp_table
	)	
UPDATE "Active Alerts" a
SET max_reading = GREATEST({}, a.max_reading),
	avg_reading = (u.t*a.avg_reading +
					 (u.delta*{}))/(u.t + u.delta),
	last_update = {}
From time_table u
WHERE a.sensor_id = u.sensor_id
AND a.sensitive = {};
    ''')

    for i, row in ongoing_spikes_df.iterrows():
        sensor_id = int(row.sensor_id)
        current_reading = int(row.current_reading)
        
        cmd = cmd_template.format(sql.Literal(formatted_runtime),
                                  sql.Literal(sensor_id),
                                  sql.Literal(is_sensitive),
                                  sql.Literal(current_reading),
                                  sql.Literal(current_reading),
                                  sql.Literal(formatted_runtime),
                                  sql.Literal(is_sensitive)
                                 )

        psql.send_update(cmd)
        
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ENDED

def Add_archived_alerts(ended_spikes_df, is_sensitive):
    '''
    This function migrates active alerts to archived alerts
    '''

    # Get relevant sensor indices as list
    sensor_indices = ended_spikes_df.sensor_id.to_list()

    # This command selects the ended alerts from active alerts
    # Then it gets the difference from the current time and when it started
    # Lastly, it inserts this selection while converting that time difference into minutes for duration_minutes column
    cmd = sql.SQL('''
    WITH ended_alerts as
    (
SELECT alert_id, sensor_id, sensitive, start_time, last_update - start_time as time_diff, avg_reading, max_reading
FROM "Active Alerts"
WHERE sensor_id = ANY ({})
AND sensitive = {}
    )
    INSERT INTO "Archived Alerts" (alert_id, sensor_id, sensitive, start_time, duration_minutes, avg_reading, max_reading)
    SELECT alert_id, sensor_id, sensitive, start_time, (((DATE_PART('day', time_diff) * 24) + 
    DATE_PART('hour', time_diff)) * 60 + DATE_PART('minute', time_diff)) as duration_minutes, avg_reading, max_reading
    FROM ended_alerts;
    ''').format(sql.Literal(sensor_indices),
                sql.Literal(is_sensitive))
    
    psql.send_update(cmd)
    

#~~~~~~~~~~~~~~~~

def Remove_active_alerts(ended_spikes_df, is_sensitive):
    '''
    This function removes the ended_spikes from the Active Alerts Table 
    
    It returns the ended_alert_ids  
    '''

    # Get relevant sensor indices as list
    sensor_indices = ended_spikes_df.sensor_id.to_list()
    
    cmd = sql.SQL('''
    SELECT alert_id
    FROM "Active Alerts"
    WHERE sensor_id = ANY ({})
    AND sensitive = {};
    ''').format(sql.Literal(sensor_indices),
                sql.Literal(is_sensitive))
                
    response = psql.get_response(cmd)
    
    # Unpack response into list

    ended_alert_ids = [i[0] for i in response] # Unpack results into list
    
    # Now clear out the active alerts
    
    cmd = sql.SQL('''
    DELETE FROM "Active Alerts"
    WHERE sensor_id = ANY ({})
    AND sensitive = {};
    ''').format(sql.Literal(sensor_indices),
                sql.Literal(is_sensitive))
    
    psql.send_update(cmd)
    
    return ended_alert_ids
