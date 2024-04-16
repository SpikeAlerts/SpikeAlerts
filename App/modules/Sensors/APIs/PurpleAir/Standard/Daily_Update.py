# Functions to Get a daily update for Standard PurpleAir Monitors

# Libraries

# Time

import datetime as dt
import pytz # Timezones

# Data Manipulation

import numpy as np
import pandas as pd
import geopandas as gpd

# Database

import modules.Database.Basic_PSQL as psql
from modules.Database.Queries import General as query
from modules.Database.Queries import Sensor as sensor_queries

# Sensors
import modules.Sensors.Sensor_Functions as sensors
import modules.Sensors.APIs.PurpleAir.API_functions as purp

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Workflow(monitor_dict, timezone):
    '''
    This function performs a daily update for Standard PurpleAir PM2.5 Sensors in our database
    
    Parameters: monitor_dict is a dictionary with the following format:
    
    {sensor_type : {
        pollutant : abbreviated name for pollutant sensor reads
        metric : a unit to append to readings
        thresholds : [list of 5 floats corresponding to health benchmarks],
        radius_meters : integer representing a distance a sensor accurately represents (on an average day),
        api_fieldname : string to query api for this value
        }, ...
     }
        
    timezone - a timezone for pytz
    
    test with 
    
    UPDATE "Sensors"
    SET channel_state = 1, channel_flags = 4
    WHERE sensor_type = 'papm25';

    DELETE FROM "Sensors"
    WHERE sensor_id = somenumber;

    UPDATE "Sensors"
    SET name = 'wrong_name'
    WHERE sensor_id = somenumber;
    '''
    
    # Iterate through sensors on the monitor
    
    for sensor_type in monitor_dict:        
        
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Load information from our database to compare to API
        
        sensors_df = sensor_queries.Get_Sensor_Info(fields = ['sensor_id', 'api_id', 'name',
                                                           'last_seen', 'channel_flags',
                                                            'channel_state'], 
                                                           sensor_types = [sensor_type])
        sensors_df['api_id'] = sensors_df.api_id.astype(int) # Format api_id as integer for merging

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Load information from PurpleAir
        
        nwlng, selat, selng, nwlat = query.Get_extent() # Get bounds of our project

        fields = ['sensor_index', 'channel_flags', 'last_seen', 'name'] # The PurpleAir fields we want
        
        purpleAir_df, runtime = purp.Get_with_bounds(fields, nwlng, selat, selng, nwlat, timezone)
        
        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Merge the datasets
        merged_df = pd.merge(sensors_df,
                         purpleAir_df, 
                         on = 'api_id',
                         how = 'outer',
                         suffixes = ('_SpikeAlerts',
                                     '_api') 
                                     )
                                     
        # Clean up datatypes post merge
        merged_df['channel_state'] = merged_df.channel_state.astype("Int64")
        merged_df['channel_flags_api'] = merged_df.channel_flags_api.astype("Int64")
        merged_df['channel_flags_SpikeAlerts'] = merged_df.channel_flags_SpikeAlerts.astype("Int64")

        # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        # Sort the sensors
        sensors_dict = Sort_Sensors(merged_df, timezone, 30) # A dictionary of lists of sensor_indices - categories/keys: 'Same Names', 'New', 'Expired', 'Conflicting Names', 'New Flags'
        
        if len(sensors_dict['New']): # Add new sensors to our database (another PurpleAir api call)
        
            Add_new_PurpleAir_Stations(sensors_dict['New'], timezone)
            
        if len(sensors_dict['Expired']): # "Retire" old sensors

            print('\n~~~\n~~~\nWarning - expired sensors not tested. See PurpleAir/Standard for this print stmt\n~~~\n~~~\n')

            # Get the sensor_ids
            is_expired = sensors_df.api_id.isin(sensors_dict['Expired'])
            sensor_ids = sensors_df[is_expired].sensor_id.to_list()
            sensors.Flag_channel_states(sensor_ids)
            
        if len(sensors_dict['Conflicting Names']): # Update our name & other fields
            
            name_controversy_df = merged_df[merged_df.api_id.isin(sensors_dict['Conflicting Names'
                                                                  ])].copy()

            # Select columns
            database_cols = ['name', 'last_seen', 'channel_flags']
            cols_to_select = [col + '_api' for col in database_cols]
            select_df = name_controversy_df[['sensor_id'] + cols_to_select]

            # Rename
            select_df = select_df.rename(columns = dict(zip(cols_to_select,
                                                       database_cols)))
            
            # Reformat dates
            select_df['last_seen'] = select_df.last_seen.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
            
            sensors.Update_Sensors(select_df)
            
        if len(sensors_dict['New Flags']): # Email the City about these new issues

            new_issue_df = merged_df[merged_df.api_id.isin(sensors_dict['New Flags'
                                                                  ])].copy()

            Email_City_flagged_sensors(new_issue_df, timezone)
        
        if len(sensors_dict['Same Names']): # Update our database's last_seen, channel_flags, 

            regular_update_df = merged_df[merged_df.api_id.isin(sensors_dict['Same Names'
                                                                  ])].copy()

            # Select columns
            database_cols = ['last_seen', 'channel_flags']
            cols_to_select = [col + '_api' for col in database_cols]
            select_df = regular_update_df[['sensor_id'] + cols_to_select]

            # Rename
            select_df = select_df.rename(columns = dict(zip(cols_to_select,
                                                       database_cols)))
            
            # Reformat dates
            select_df['last_seen'] = select_df.last_seen.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
            
            sensors.Update_Sensors(select_df)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def Sort_Sensors(merged_df, timezone = 'America/Chicago', expiration_time=30):
    '''
    For daily updates of ONE sensor_type
    Sort the api_ids of a dataframe
    Created from outer merging our database sensors with an api pull on api_id

    Parameters:

    merged_df - pd.dataframe with the following specs
    
    Fields need in this dataframe are:
    
    'api_id', 'last_seen',
    'name', 'channel_state' (no suffix only from internal database), 'channel_flags'

    ************ NOT DONE: Describe channel_state/flags currently in PurpleAir code **********
    
    with suffixes _SpikeAlerts and _api

    timezone - str - a timezone for pytz

    expiration_time - int - number of days until expiration of a sensor
    
    Returns a dictionary
    
    Values are lists of api_ids (integers)
    Sorted into categories/keys:
    'Same Names', 'New', 'Expired', 'Conflicting Names', 'New Flags'
    '''
    
    # Conditions (Boolean Pandas Series)
    
    # Do the names match up?
    names_match = (merged_df.name_SpikeAlerts == merged_df.name_api)
    # Do we not have the name?
    no_name_SpikeAlerts = (merged_df.name_SpikeAlerts.isna())
    # Does PurpleAir not have the name?
    no_name_api = (merged_df.name_api.isna())
    # We haven't seen recently? - within 30 days??
    not_seen_recently = (merged_df.last_seen_SpikeAlerts <
                            np.datetime64((dt.datetime.now(pytz.timezone(timezone)
                            ) - dt.timedelta(days = expiration_time))))
    # Good channel State
    good_channel_state = (merged_df.channel_state != 0)
    # New Flags (within past day) - a 4 in our database
    is_new_issue = (merged_df.channel_flags_SpikeAlerts == 4)

    # Use the conditions to sort

    same_name_indices = merged_df[names_match].api_id.to_list()
    new_indices = merged_df[(~names_match) 
                            & (no_name_SpikeAlerts)].api_id.to_list()
    expired_indices = merged_df[(~names_match) 
                                & (no_name_api) 
                                & (not_seen_recently)
                                & (good_channel_state)].api_id.to_list()
    confilcting_name_indices = merged_df[(~names_match) 
                                        & (~no_name_api) 
                                        & (~no_name_SpikeAlerts)].api_id.to_list()
    new_flag_indices = merged_df[(is_new_issue)].api_id.to_list()
    
    # Create the dictionary
    
    sensors_dict = {'Same Names':same_name_indices,
                   'New':new_indices,
                   'Expired':expired_indices, 
                   'Conflicting Names':confilcting_name_indices, 
                   'New Flags':new_flag_indices
                    }
                    
    return sensors_dict

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
def Add_new_PurpleAir_Stations(sensor_indices, timezone):
    '''
    This function takes in a list of PurpleAir sensor_indices,
     queries PurpleAir for all of the fields,
    and adds them to our database.
    '''
    
    #Setting parameters for API
    fields = ['date_created', 'last_seen', 'name','channel_flags','altitude',
                  'latitude', 'longitude']          
    
    df, runtime = purp.Get_with_sensor_index(sensor_indices, fields, timezone)

    if len(df) > 0:

        # Spatializing
                                             
        gdf = gpd.GeoDataFrame(df, 
                                geometry = gpd.points_from_xy(
                                    df.longitude,
                                    df.latitude,
                                    crs = 'EPSG:4326')
                                   )

        # Format dataframe for database
        
        # Create sensor_type column
        gdf['sensor_type'] = 'papm25'
        # Dates to strings
        gdf['date_created'] = gdf.date_created.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
        gdf['last_seen'] = gdf.last_seen.apply(lambda x : x.strftime('%Y-%m-%d %H:%M:%S'))
        # Select columns
        cols_for_db = ['api_id', 'sensor_type', 'date_created', 'last_seen',
         'name', 'channel_flags', 'altitude']
         
        sorted_df = gdf.copy()[cols_for_db] 
        
        # Get Well Known Text of the geometry
                             
        sorted_df['geometry'] = gdf.geometry.apply(lambda x: x.wkt)

        # Insert into database
        psql.insert_into(sorted_df, "Sensors", is_spatial = True)    

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

def Email_City_flagged_sensors(new_issue_df, timezone):
    '''
    This function composes an email to the city about recently flagged sensors and prints it
    '''
    
    # Conditions

    conditions = ['wifi_down?', 'a_down', 'b_down', 'both_down'] # corresponds to 0, 1, 2, 3 from PurpleAir channel_flags

    # Initialize storage

    email = '''Hello City of Minneapolis Health Department,

    Writing today to inform you of some anomalies in the PurpleAir sensors that we discovered:

    name, last seen, channel issue

    '''

    for i, condition in enumerate(conditions):

        con_df = new_issue_df[new_issue_df.channel_flags_api == i]
        
        if i == 0: # These wifi issues are only important if older than 6 hours
            not_seen_recently_api = (con_df.last_seen_api < dt.datetime.now(pytz.timezone(timezone)) - dt.timedelta(hours = 6))
            
            con_df = con_df[not_seen_recently_api]
        
        for i, row in con_df.iterrows():
                
            email += f'\n{row.name_api}, {row.last_seen_api.strftime("%m/%d/%y - %H:%M")}, {condition}'

    email += '\n\nTake Care,\nSpikeAlerts'
    print(email)
        
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
