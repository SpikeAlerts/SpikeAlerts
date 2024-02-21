''' 
This function is the workflow of one regular update

# It Sequentially calls many other workflows within the same directory

# 0 = Daily Updates - Less regular System-wide updates for Sensors and POIs
# 1 = Call_APIs - Gets real-time sensors' information
                        Returns a pd.dataframe of this called sensors_df
# 2 = Update_Sensor_Tables - Update last_seen, channel_flags, and values in "Sensors" and last_update in "Sensor Type Information"

 sensors_df fields are used to inform the next steps:

# 3 = Update_Alerts_Tables - Create new alerts, update ongoing alerts, and end old alerts
 
# 4 = Update_POIs_and_Reports - Updates the POI & reports tables

At the end there are 2 lists of poi_ids (Places of Interest IDs)

poi_ids_to_alert (POIs with new AQ event)
and 
poi_ids_to_end_alert (POIs with a new report written "end of event")     

And one list of (start_time, duration_minutes, severity, report_id)
                     with the same index as poi_ids_to_end_alert  
                     
# 5 = Send Alerts - NOT DONE

# 6 = Periodically send reports to manager/orgs & Archive Database - NOT DONE

# 7 = Calculate next update time
'''

# Import the modules listed above
# from modules import Daily_Updates, GetSort_Spikes, New_Alerts, Ongoing_Alerts, Ended_Alerts, Flagged_Sensors, Send_Alerts
from modules import Daily_Updates # 0
from modules import Call_APIs # 1
from modules import Update_Sensor_Tables # 2
from modules import Update_Alert_Tables # 3
from modules import Update_POIs_and_Reports # 4
from modules.Database.Queries import Sensor as sensor_query # 7

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### The Workflow
def main(base_config, runtime, next_system_update):
    '''
    This is the main workflow of one iteration of the SpikeAlerts App
    
    It should return next_regular_update, next_system_update (both are datetimes)    
    '''
    
    # ~~~~~~~~~~~~~~~~~~~~~
    
    # 0) System Update? - Only sensors right now
    
    if runtime > next_system_update:
        #pass
        next_system_update = Daily_Updates.workflow(base_config, next_system_update)

    # ~~~~~~~~~~~~~~~~~~~~~
    
    # 1) Query APIs for current data

    sensors_df, sensor_types_updated = Call_APIs.workflow(runtime, base_config['TIMEZONE'])

    if len(sensors_df) > 0:
        # 2) Update our database tables "Sensors" and "Sensor Type Information"

        Update_Sensor_Tables.workflow(sensors_df, sensor_types_updated, runtime)

        # ~~~~~~~~~~~~~~~~~~~~~

        # 3) Workflow for updating our database tables "Active Alerts" and "Archived Alerts"

        sensor_id_dict, ended_alert_ids = Update_Alert_Tables.workflow(sensors_df, runtime)
        
        # ~~~~~~~~~~~~~~~~~~~~~

        # 4) Workflow for updating our database tables "Places of Interest" and "Reports Archive"

        Update_POIs_and_Reports.workflow(sensors_df, sensor_id_dict, ended_alert_ids, runtime, base_config['REPORT_LAG'], base_config['EPSG_CODE'])
        
    # 7) Get the next regular update time
        
    next_regular_update = sensor_query.Get_next_regular_update(base_config['TIMEZONE'])

    return next_regular_update, next_system_update
