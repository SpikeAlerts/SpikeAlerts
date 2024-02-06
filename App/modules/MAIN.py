''' 
This function is the workflow of one regular update

# It Sequentially calls many other workflows within the same directory

# 0 = Daily Updates - Less regular System-wide updates for Sensors and POIs
# 1a = GetSort_Spikes - Gets real-time sensors' information & begins interpretation. 
                        Returns a pd.dataframe of this called sensors_df
# 1b = Update_Sensors_Table - Update last_elevated, last_seen, channel_flags, and values

 sensors_df column 'sensor_status' is used to inform the next steps:

# 2 = Update_Alerts_Tables - Create new alerts, update ongoing alerts, and end old alerts
 
# 3 = Update_POI_Tables - Updates the POI tables <- if .env has POI_FORM='Something'
# Finally,
# 6 = Send_Messages - Sends all messages <- If .env has MESSAGING='Something'
'''

# Import the modules listed above
# from modules import Daily_Updates, GetSort_Spikes, New_Alerts, Ongoing_Alerts, Ended_Alerts, Flagged_Sensors, Send_Alerts
from modules import Daily_Updates # 0
from modules import GetSort_Spikes # 1.a
from modules import Update_Sensors_Table # 1.b
from modules import Update_Alert_Tables # 2

### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

    # ~~~~~~~~~~~~~~~~~~~~~

    # 2) Workflow for updating our database tables "Active Alerts" and "Archived Alerts"

    Update_Alert_Tables.workflow(sensors_df, runtime)

    
    
    return next_system_update
