-- Set the search path to desired schema

SET SEARCH_PATH = 'base';

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- The sensor type(s) information

INSERT INTO base."Sensor Type Information" (
    sensor_type, -- text, -- Sensor Type Identifier
    api_name, -- text, -- the keeper of the api for this sensor, eg. PurpleAir - These should be in App/modules/Sensor_APIs
    monitor_name, -- text, -- A name for the monitor that holds this sensor, - These should be in App/modules/Sensor_APIs/api_name
    api_fieldname, -- text, -- the fieldname to get regular readings from the api
    pollutant, -- text, -- A name of the pollutant measured
    metric, -- text, -- A unit to append to readings of this sensor for context
    thresholds, -- float [],  -- The health thresholds for this sensor (in the above metric)
    -- ^ In this order (lowest possible, moderate, Unhealth for Sensitive Groups, Unhealthy, Very Unhealthy, Hazardous, highest possible)
    radius_meters, -- float, -- The distance this sensor is relevant to (for POIs)
    update_frequency -- int, -- The frequency for regular updates in minutes
    )
VALUES (
    'papm25',
    'PurpleAir',
    'Standard',
    'pm2.5_10minute',
    'PM2.5',
    'ug/m^3',
    ARRAY[0, 12.1, 35.5, 55.5, 150.5, 250.5, 1000],
    800,
    10
);
