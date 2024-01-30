# Initializing the sensor types:

# An Example of SQL to do this for PurpleAir is:

## The sensor type(s) information

INSERT INTO base."Sensor Type Information" (
    sensor_type, -- text, -- Sensor Type Identifier
    api_name, -- text, -- the keeper of the api for this sensor, eg. PurpleAir - These should be in App/modules/Sensor_APIs
    monitor_name, -- text, -- A name for the monitor that holds this sensor, - These should be in App/modules/Sensor_APIs/api_name
    api_fieldname, -- text, -- the fieldname to get regular readings from the api
    pollutant, -- text, -- A name of the pollutant measured
    metric, -- text, -- A unit to append to readings of this sensor for context
    thresholds, -- float [],  -- The left inclusive health thresholds for this sensor (in the above metric)
    -- ^ In this order (moderate, Unhealth for Sensitive Groups, Unhealthy, Very Unhealthy, Hazardous)
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
    ARRAY[12.1, 35.5, 55.5, 150.5, 250.5],
    1000,
    10
);

## Sensors

CREATE TABLE "Sensors" -- Storage for all sensors
(
	sensor_id serial, -- Our Unique Identifier
	sensor_type text, -- Relates to above table
	api_id text, -- The unique identifier for the api
	name varchar(100), -- A name for the sensor (for humans)
	date_created timestamp DEFAULT CURRENT_TIMESTAMP,
	last_seen timestamp DEFAULT CURRENT_TIMESTAMP,
	last_elevated timestamp DEFAULT TIMESTAMP '2000-01-01 00:00:00',
	channel_state int, -- Indicates whether the sensor is active or not, 0 = not active, any other value = active
	channel_flags int, -- Indicates whether sensor is depricated, 0 = not degraded, 1 = channel a degraded, 2 = channel b degraded, 3 = degraded, 4 = newly flagged
	altitude int,
	current_reading float DEFAULT -1 -- The last value seen of the sensor
--	geometry geometry -- A Point
);
