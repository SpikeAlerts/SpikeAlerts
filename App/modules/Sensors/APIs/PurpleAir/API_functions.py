# Functions to interface with PurpleAir

## Load modules

# File Manipulation
from dotenv import load_dotenv # Loading .env info

# Web

import requests
import os

# Time

import datetime as dt
import pytz # Timezones

# Data Manipulation

import numpy as np
import pandas as pd

## Load Env information

load_dotenv() # Load .env file) # Load .env file

# '.env.sensors.secret

# API Key

purpleAir_api = os.getenv('PURPLEAIR_API_TOKEN') # PurpleAir API Read Key
name_filter = os.getenv('PURPLEAIR_NAME_FILTER') # PurpleAir API Read Key

# Function to get Sensors Data from PurpleAir

def getSensorsData(query='', name_filter=''):
    # my_url is assigned the URL we are going to send our request to.
    url = 'https://api.purpleair.com/v1/sensors?' + query

    # my_headers is assigned the context of our request we want to make. In this case
    # we will pass through our API read key using the variable created above.
    my_headers = {'X-API-Key':purpleAir_api}

    # This line creates and sends the request and then assigns its response to the
    # variable, response.
    response = requests.get(url, headers=my_headers)
    
    # Checking the response
    
    if response.status_code != 200: 
        print('ERROR in PurpleAir API Call')
        print('HTTP Status: ' + str(response.status_code))
        print(response.text)
        
        df = pd.DataFrame() # Return an empty dataframe
        
    else:
        response_dict = response.json() # Read response as a json (dictionary)
        col_names = response_dict['fields']
        data = np.array(response_dict['data'])

        df = pd.DataFrame(data, columns = col_names) # Format as Pandas dataframe

    # Filter by name_filter if it exists
    
    if len(name_filter) > 0: # If specified
        is_name_filter = df.name.apply(lambda x: name_filter.upper() in x.upper())
        purpleAir_df =  df[is_name_filter].copy()
    else: # Else
        purpleAir_df = df.copy()

    # Reformat datatypes (see next function)

    if len(purpleAir_df) > 0:
        purpleAir_df = Reformat_PurpleAir_data(purpleAir_df) # Format
    
    return purpleAir_df
    
#### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
### The Function to format columns in PurpleAir dataframe

def Reformat_PurpleAir_data(df, timezone = 'America/Chicago'):
    '''
    This function formats the purpleAir_df into the following:
    
    integers
    sensor_index, channel_flags, channel_state, altitude
    
    datetimes in timezone
    last_seen, date_created
    
    
    Finally, it renames sensor_index to api_id
    '''
    
    cols = set(df.columns.to_list())
    
    # Integers
    
    int_cols = {'channel_flags', 'channel_state', 'altitude', 'sensor_index'}
    
    for col in cols.intersection(int_cols):
    
        df[col] = df[col].astype(int)
    
    # Datetimes
    
    datetime_cols = ['last_seen', 'date_created']
    
    for col in cols.intersection(datetime_cols):
    
        df[col] = pd.to_datetime(df[col].astype(int),
                                         utc = True,
                                         unit='s').dt.tz_convert(timezone)
    # Rename sensor_index to api_id
                                 
    formatted_df = df.rename(columns = {'sensor_index':'api_id'}
                                ).copy()
    return formatted_df
    
#### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
### The Function to get a dataframe from purpleair for select sensor_indices

def Get_with_sensor_index(sensor_indices, fields, timezone = 'America/Chicago'):
    
    ''' This function queries the PurpleAir API for sensors in the list of sensor_ids for readings over a spike threshold. 
    It will return an unformatted pandas dataframe with the specified fields as well as a runtime (datetime)
    
    Inputs:
    
    sensor_indices = list of integers of purpleair sensor ids to query
    fields - list of strings that line up with PurpleAir api
    
    Outputs:
    
    df = Pandas DataFrame with fields (datatypes formatted!)
    runtime = datetime object when query was run
    '''
    
    ### Setting parameters for API
    fields_string = 'fields=' + '%2C'.join(fields)
    sensor_string = 'show_only=' + '%2C'.join(pd.Series(sensor_indices).astype(str))

    query_string = '&'.join([fields_string, sensor_string])
    
    ### Call the api
    
    runtime = dt.datetime.now(pytz.timezone(timezone)) # When we call - datetime in our timezone    
    purpleAir_df = getSensorsData(query_string) # The response is a requests.response object
            
    return purpleAir_df, runtime
    
#### ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
   
### The Function to get a dataframe from purpleair for select sensor_ids

def Get_with_bounds(fields, nwlng, selat, selng, nwlat, timezone = 'America/Chicago'):
    
    '''
    This function gets Purple Air data for all sensors in the given boundary
    
    returns a dataframe purpleAir_df
    fields: 'sensor_index', 'channel_flags', 'last_seen', 'name'
    datatypes: int, int, datetime timezone 'America/ Chicago', str
    
    Inputs:
    
    fields - list of strings that line up with PurpleAir api    
    purpleAir_api = string of PurpleAir API api_read_key
    nwlng, selat, selng, nwlat = the bounding box in lat/lons
    
    Outputs:
    
    df = Pandas DataFrame with fields (datatypes not formatted!)
    runtime = datetime object when query was run
    '''
    
    # Bounding string
    bounds_strings = [f'nwlng={nwlng}',
                      f'nwlat={nwlat}',
                      f'selng={selng}',
                      f'selat={selat}']
    bounds_string = '&'.join(bounds_strings)  
    # Field string
    fields_string = 'fields=' + '%2C'.join(fields)

    query_string = '&'.join([fields_string, bounds_string])
    
    ### Call the api
    
    runtime = dt.datetime.now(pytz.timezone(timezone)) # When we call - datetime in our timezone
    purpleAir_df = getSensorsData(query_string, name_filter) # The response is a requests.response object
            
    return purpleAir_df, runtime
