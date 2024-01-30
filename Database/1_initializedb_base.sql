-- Run this file to initialize the database, schema, and tables for SpikeAlerts

-- To Do beforehand

--CREATE DATABASE "SpikeAlerts"; -- Create the database

-- CHANGE 'base' to your desired schema name 3 times in this script!!!
-- They occur in lines 22, 45, 120-156, and 168

-- Then you're good to run this script to initialize the database
-- You can run this by using a psql command like:
-- psql "host=postgres.cla.umn.edu user=<your_username> password=<your_password> " -f 1_initializedb_base.sql

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Prep

-- Drop/create schema and extensions
DO $$
DECLARE schemaname text; -- Initialize variable for the schema name
BEGIN
	schemaname := 'base'; -- Set the Schema Name you wish to use - CHANGE THIS!!! 
	
	-- Drop schema and extensions if they exist
	execute format(
	'
	DROP SCHEMA IF EXISTS %I CASCADE;
	DROP EXTENSION IF EXISTS postgis CASCADE;
	', schemaname
	);	
	
	-- Create schema and extensions
	execute format(
	'
	CREATE SCHEMA %I;
	CREATE EXTENSION postgis; -- Add spatial extensions
	CREATE EXTENSION postgis_topology;
    ', schemaname
	);

END$$;

-- Set the search path to desired schema

SET SEARCH_PATH = 'base'; -- CHANGE THIS!!!!

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

-- Initialize Tables

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


-- General 

CREATE TABLE "Daily Log" -- This is to store important daily metrics
    ("date" date DEFAULT CURRENT_DATE,
     new_POIs int DEFAULT 0,
     new_sensors int DEFAULT 0,
     retired_sensors int DEFAULT 0,
	 alerts_sent int DEFAULT 0
    );
    
CREATE TABLE "Extent" -- This is to define the bounding box of the project
    (minlng Double Precision,
    maxlng Double Precision,
    minlat Double Precision,
    maxlat Double Precision
    );
    
-- Sensors

CREATE TABLE "Sensor Type Information" -- This is to keep track of the different sensors in the project
    (sensor_type text, -- Sensor Type Identifier
    api_name text, -- the keeper of the api for this sensor, eg. PurpleAir - These should be in App/modules/Sensor_APIs
    monitor_name text, -- A name for the monitor that holds this sensor, - These should be in App/modules/Sensor_APIs/api_name
    api_fieldname text, -- the fieldname to get regular readings from the api
    pollutant text, -- An abbreviated name of the pollutant measured
    metric text, -- A unit to append to readings of this sensor for context
    thresholds float [],  -- The health thresholds for this sensor (in the above metric)
    -- ^ In this order (moderate, Unhealth for Sensitive Groups, Unhealthy, Very Unhealthy, Hazardous)
    radius_meters float, -- The max distance this sensor is relevant to (for POIs)
    last_update timestamp DEFAULT TIMESTAMP '2000-01-01 00:00:00', -- Last regular update time
    update_frequency int -- The frequency for regular updates in minutes
    );

CREATE TABLE "Sensors" -- Storage for all sensors
(
	sensor_id serial, -- Our Unique Identifier
	sensor_type text, -- Relates to above table
	api_id text, -- The unique identifier for the api
	name varchar(100), -- A name for the sensor (for humans)
	date_created timestamp DEFAULT CURRENT_TIMESTAMP,
	last_seen timestamp DEFAULT CURRENT_TIMESTAMP,
	last_elevated timestamp DEFAULT TIMESTAMP '2000-01-01 00:00:01',
	channel_state int DEFAULT 1, -- Indicates whether the sensor is active or not,
	channel_flags int DEFAULT 0, -- Indicates whether sensor is depricated
	altitude int,
	current_reading float DEFAULT -1 -- The last value seen of the sensor
--	geometry geometry -- A Point
);

-- Alerts

CREATE TABLE "Active Alerts" -- These are the SpikeAlerts that are currently out
	(alert_id bigserial, -- Unique identifier for a spike alert
	 sensor_id int, -- Sensor Unique Identifiers
	  start_time timestamp,
	  avg_reading float, -- Average value registered
	   max_reading float); -- Maximum value registered

CREATE TABLE "Archived Alerts" -- Archive of the Above table
	(alert_id bigint, -- Unique identifier for a spike alert
	 sensor_id int, -- Sensor Unique Identifiers
	  start_time timestamp,
	  duration_minutes integer, -- How long it lasted in minutes
	  avg_reading float, -- Average value registered
	   max_reading float); -- Maximum value registered
	    
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

-- Fill in essential Tables - CHANGE THIS!

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
	   
-- Extent of the project (in latitude/longitude)
	   
INSERT INTO base."Extent" (minlng, maxlng, minlat, maxlat)
VALUES (-93.3303753775222, -93.1930625073825, 44.8896883413448, 45.0521464662874
);

-- The sensor type(s) information

INSERT INTO base."Sensor Type Information" (
    sensor_type, -- text, -- Sensor Type Identifier
    api_name, -- text, -- the keeper of the api for this sensor, eg. PurpleAir - These should be in App/modules/Sensor_APIs
    monitor_name, -- text, -- A name for the monitor that holds this sensor, - These should be in App/modules/Sensor_APIs/api_name
    api_fieldname, -- text, -- the fieldname to get regular readings from the api
    pollutant, -- text, -- A name of the pollutant measured
    metric, -- text, -- A unit to append to readings of this sensor for context
    thresholds, -- float [],  -- The health thresholds for this sensor (in the above metric)
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
	   
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Spatial Stuff

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Reset Search path, add geometry columns, create spatial indices
DO $$
DECLARE schemaname text; -- Initialize variable for the schema name
BEGIN
schemaname := 'base'; -- Set the Schema Name you wish to use - CHANGE THIS!!!
execute format('SET SEARCH_PATH = "$user", public, topology;
			   
			   ALTER TABLE %I."Sensors"
			   ADD geometry geometry; -- A Point
			   CREATE INDEX sensor_gid ON %I."Sensors" USING GIST(geometry);  -- Create spatial index for stations
			   '
			   , schemaname, schemaname);
END$$;
