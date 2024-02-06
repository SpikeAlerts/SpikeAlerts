### Import Packages

# Time

import datetime as dt # Working with dates/times

# Database 

from modules import Basic_PSQL as psql
from psycopg2 import sql

## Workflow

def workflow(sensors_df, runtime):
    '''
    Runs the full workflow to update our database tables "Active Alerts" and "Archived Alerts". 
    This involves the following:

    a) New Alerts - Add new_spike sensors to "Active Alerts"
    b) Ongoing Alerts - Update ongoing_spike sensors' avg_reading and max_reading in "Active Alerts"
    c) Ended Alerts - Add ended_spike sensors to "Archived Alerts" and remove from "Active Alerts"
    
    Parameters:
    
    sensors_df - a dataframe with the following columns:

    sensor_id - int - our unique identifier
    current_reading - float - the raw sensor value
    update_frequency - int - frequency this sensor is updated
    pollutant - str - abbreviated name of pollutant sensor reads
    metric - str - unit to append to readings
    health_descriptor - str - current_reading related to current health benchmarks
    radius_meters - int - max distance sensor is relevant
    sensor_status - text - one of these categories: not_spike, new_spike, ongoing_spike, ended_spike, flagged
    
    runtime - approximate time that the values for above dataframe were acquired
    '''

    # ~~~~~~~~~~~~~~~~
    # a) New Alerts

    # Filter sensors_df

    new_spikes_df = sensors_df[sensors_df.sensor_status == 'new_spike']

    if len(new_spikes_df) > 0:

        print('inserting new alerts')

        # Add to Active Alerts
        
        Add_active_alerts(new_spikes_df, runtime)

    # ~~~~~~~~~~~~~~~~
    # b) Ongoing Alerts

    # Filter sensors_df

    ongoing_spikes_df = sensors_df[sensors_df.sensor_status == 'ongoing_spike']
    
    if len(ongoing_spikes_df) > 0:
        
        print('updating ongoing alerts')

        # Update active alerts

        Update_active_alerts(ongoing_spikes_df, runtime)

    # ~~~~~~~~~~~~~~~~
    # c) Ended Alerts

    # Filter sensors_df & get the sensor_ids

    ended_spikes_df = sensors_df[sensors_df.sensor_status == 'ended_spike']

    if len(ended_spikes_df) > 0:

        print('ending alerts')
        
        # Add to alert archive
    
        Add_archived_alerts(ended_spikes_df)
    
        # Remove from active alerts
    
        Remove_active_alerts(ended_spikes_df)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ NEW
    
def Add_active_alerts(new_spikes_df, runtime):
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

    # Insert into database

    psql.insert_into(formatted_df, 'Active Alerts')

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ONGOING

def Update_active_alerts(ongoing_spikes_df, runtime):
    '''This function updates the alerts in the "Active Alerts" table. 

    The fields it updates are max_reading and avg_reading (could be improved)

    Parameters:
    Filtered sensors_df (sensor_status == 'ongoing_spike') with columns: sensor_id and current_reading

    runtime - approximate time that the values for above dataframe were acquired
    timestep - timesteps between updates 
    '''

    # Get the necessary variables needed for calculations
    formatted_runtime = runtime.strftime('%Y-%m-%d %H:%M:%S') # Time formatted for database

    # Write the template sql command for updating
    # First, temp_table gets the necessary time differences (previous duration of the alert (t) & timestep between now and last update (delta))
    # Second, update_table converts the above into minutes
    # Last, we compute/update last_update, max_reading, avg_reading
    
    # The average part is tricky. We are using this equation:
    # Let the previous duration (in minutes) of the alert be, t, and the number of minutes since that update be delta
    # Then
    # avg = u_(t+delta) = ((t x u_t) + (delta x current_reading))/(t + delta) 

    cmd_template = sql.SQL('''
    WITH temp_table as
	(SELECT sensor_id,
	 	last_update - start_time as previous_time_diff,
	    {} - last_update as timestep_time_diff
	FROM "Active Alerts"
	WHERE sensor_id = {}
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
WHERE a.sensor_id = u.sensor_id;
    ''')

    for i, row in ongoing_spikes_df.iterrows():
        sensor_id = int(row.sensor_id)
        current_reading = int(row.current_reading)
        
        cmd = cmd_template.format(sql.Literal(formatted_runtime),
                                  sql.Literal(sensor_id),
                                  sql.Literal(current_reading),
                                  sql.Literal(current_reading),
                                  sql.Literal(formatted_runtime)
                                 )

        psql.send_update(cmd)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ENDED

def Add_archived_alerts(ended_spikes_df):
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
SELECT alert_id, sensor_id, start_time, last_update - start_time as time_diff, avg_reading, max_reading
FROM "Active Alerts"
WHERE sensor_id = ANY ({})
    )
    INSERT INTO "Archived Alerts" (alert_id, sensor_id, start_time, duration_minutes, avg_reading, max_reading)
    SELECT alert_id, sensor_id, start_time, (((DATE_PART('day', time_diff) * 24) + 
    DATE_PART('hour', time_diff)) * 60 + DATE_PART('minute', time_diff)) as duration_minutes, avg_reading, max_reading
    FROM ended_alerts;
    ''').format(sql.Literal(sensor_indices))
    
    psql.send_update(cmd)
    

#~~~~~~~~~~~~~~~~

def Remove_active_alerts(ended_spikes_df):
    '''
    This function removes the ended_spikes from the Active Alerts Table   
    '''

    # Get relevant sensor indices as list
    sensor_indices = ended_spikes_df.sensor_id.to_list()
    
    cmd = sql.SQL('''
    DELETE FROM "Active Alerts"
    WHERE sensor_id = ANY ({});
    ''').format(sql.Literal(sensor_indices))
    
    psql.send_update(cmd)
