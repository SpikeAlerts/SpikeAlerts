''' 
This function is the workflow of one regular update

# It Sequentially calls many other workflows within the same directory

# 0 = Daily Updates - Less regular System-wide updates for Sensors and POIs
# 1a = GetSort_Spikes - Gets real-time sensors' information & begins interpretation. 
                        Returns a pd.dataframe of this called sensors_df
# 1b = Update_Sensors_Table - Update last_elevated, last_seen, channel_flags, and values

 sensors_df column 'sensor_status' is used to inform steps 2-4:
 
# 2 = New_Alerts - Update Database & Compose Messages
# 3 = Ongoing_Alerts - Update Database & Compose Messages
# 4 = Ended_Alerts - Update Database & Compose Messages
# Finally,
# 6 = Send_Messages - Sends all messages <- If .env has MESSAGING='Something'
'''

# Import the modules listed above
# from modules import Daily_Updates, GetSort_Spikes, New_Alerts, Ongoing_Alerts, Ended_Alerts, Flagged_Sensors, Send_Alerts
from modules import Daily_Updates # 0
from modules import GetSort_Spikes # 1.a
from modules import Update_Sensors_Table # 1.b
### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

import os

### The Workflow
def main(base_config, now, next_system_update):
    '''
    This is the main workflow of one iteration of the SpikeAlerts App
    
    It should return next_system_update (datetime)    
    '''
    
    # ~~~~~~~~~~~~~~~~~~~~~
    
    # 0) System Update? - Only sensors for now
    
    if now > next_system_update:
        #pass
        print('Running Daily Update')
        next_system_update = Daily_Updates.workflow(base_config, next_system_update)
        print('Completed Daily Update')

    # ~~~~~~~~~~~~~~~~~~~~~
    
    # 1a) Query APIs for current data & Sort Sensors into categories

    sensors_df, runtime = GetSort_Spikes.workflow(base_config)

    # 1b) Update our database table "Sensors"

    Update_Sensors_Table.workflow(sensors_df, runtime)
    
    
    # # Update last_elevated
    
    # if len(spikes_df) > 0:
        
    #     Update_last_elevated(spikes_df.sensor_index.to_list(), purpleAir_runtime, pg_connection_dict)
    
    # if len(flagged_sensor_ids) > 0:
    #     # Flag sensors in our database (set channel_flags = 4 for the list of sensor_index)

    #     flag_sensors(flagged_sensor_ids.to_list(), pg_connection_dict)    
    
    # sensors_dict = Sort_sensor_indices(spikes_df, flagged_sensor_ids, pg_connection_dict)
    
    return next_system_update
