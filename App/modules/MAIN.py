''' 
This function is the workflow of one regular update

# It Sequentially calls many other workflows within the same directory

# 0 = Daily_Updates - Less regular System-wide updates for Sensors, Users, POIs

# 1 = Call_APIs - Gets real-time sensors' information
                        Returns a pd.dataframe of this called sensors_df
                        
sensors_df fields are used to inform the next 2 steps:
                        
# 2 = Update_Sensor_Tables - Update last_seen, channel_flags, and values in "Sensors" and last_update in "Sensor Type Information"

# 3 = Update_Alerts_Tables - Create new alerts, update ongoing alerts, and end old alerts

                            Returns a dictionary of the sensor_ids with the following structure:
                            
                            sensor_id_dict = 
                              {'TRUE' : {'new' : set of sensor_ids,
                                             'ongoing' : set of sensor_ids,
                                             'ended' : set of sensor_ids}
                              'FALSE' : {'new' : set of sensor_ids,
                                           'ongoing' : set of sensor_ids,
                                           'ended' : set of sensor_ids}
                              }
                                              
                                 where TRUE = for sensitive populations
                                        FALSE = for all populations

sensor_id_dict is used to inform the next step:
 
# 4 = Update_POIs_and_Reports - Updates the POI & reports tables

                    This returns a dictionary with the following format:
                    
                            reports_dict =
                              {
                              'TRUE' : list of tuples of (poi_id, report_id)
                              'FALSE' : list of tuples of (poi_id, report_id)
                              }
                                              
                                 where TRUE = for sensitive populations
                                        FALSE = for all populations   
                                        
reports_dict is used to inform the next step:

If the environment variable 'USERS' is set to 'y' then we will do step 5
                     
# 5 = Notify_and_Update_Users - NOT DONE - Checks the Users table and compiles info for messaging. Then composes messages, sends them, and updates the "Users" table field, alerted

# 6 = Send_Reports_and_Archive - NOT DONE, may move to daily updates - Periodically send reports to manager/orgs & archive the alerts and reports to another database

# 7 = Calculate next update time
'''

# Import the modules listed above
# from modules import Daily_Updates, GetSort_Spikes, New_Alerts, Ongoing_Alerts, Ended_Alerts, Flagged_Sensors, Send_Alerts
from modules import Daily_Updates # 0
from modules import Call_APIs # 1
from modules import Update_Sensor_Tables # 2
from modules import Update_Alert_Tables # 3
from modules import Update_POIs_and_Reports # 4
from modules import Notify_and_Update_Users # 5
#from modules import Send_Reports_and_Archive # 6 <- probably will move to the daily updates
from modules.Database.Queries import Sensor as sensor_query # 7

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

### The Workflow
def main(base_config, runtime, next_system_update):
    '''
    This is the main workflow of one iteration of the SpikeAlerts App
    
    It should return next_regular_update, next_system_update (both are datetimes)    
    '''
    
    # ~~~~~~~~~~~~~~~~~~~~~
    
    # 0) System Update? - Only sensors/POIs right now
    
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

        reports_dict = Update_POIs_and_Reports.workflow(sensor_id_dict, ended_alert_ids, runtime, base_config)
        
        
        if base_config['USERS'] == 'y':
        
            # ~~~~~~~~~~~~~~~~~~~~~

            # 5) Workflow for updating our database table "Users" and Compose and send messages

            Notify_and_Update_Users.workflow(reports_dict, base_config)
            
    # ~~~~~~~~~~~~~~~~~~~~
       
    # ?) Probably moving this to daily updates - If it's time, send reports to manager/orgs and archive data somewhere
    
    # ~~~~~~~~~~~~~~~~~~~~
       
    # 6) Get the next regular update time
        
    next_regular_update = sensor_query.Get_next_regular_update(base_config['TIMEZONE'])

    return next_regular_update, next_system_update
