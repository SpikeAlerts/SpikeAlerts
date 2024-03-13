-- Run this file to initialize the database, schema, and tables for SpikeAlerts

-- To Do beforehand

--CREATE DATABASE "SpikeAlerts"; -- Create the database

-- Then you're good to run this script to initialize the tables
-- You can run this by using a psql command like:
-- psql "host=postgres.cla.umn.edu user=<your_username> password=<your_password> " -f 4_initialize_tables.sql

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Create Functions

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- This function maps a value to its health descriptor

CREATE or REPLACE function map_to_health(val float, thresholds float [])
returns text language sql immutable as $$
	SELECT CASE
		   WHEN val >= thresholds[7] THEN '7 ERROR (too high)'
		   WHEN val >= thresholds[6] THEN '6 hazardous'
		   WHEN val >= thresholds[5] THEN '5 very unhealthy'
		   WHEN val >= thresholds[4] THEN '4 unhealthy'
		   WHEN val >= thresholds[3] THEN '3 unhealthy for sensitive groups'
		   WHEN val >= thresholds[2] THEN '2 moderate'
		   WHEN val >= thresholds[1] THEN '1 good'
		   ELSE '0 ERROR (too low)'
		   END as health_descriptor
$$;

-- Array differences

CREATE or REPLACE function array_diff(array1 anyarray, array2 anyarray)
returns anyarray language sql immutable as $$
    select coalesce(array_agg(elem), '{}')
    from unnest(array1) elem
    where elem <> all(array2)
$$;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

-- Set 'base' schema to search path

SET SEARCH_PATH = 'base';

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`

-- Initialize Tables

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


-- General 

CREATE TABLE "Daily Log" -- This is to store important daily metrics
    ("date" date PRIMARY KEY DEFAULT CURRENT_DATE,
     new_POIs int DEFAULT 0,
     new_sensors int DEFAULT 0,
     retired_sensors int DEFAULT 0,
	 messages_sent int DEFAULT 0,
	 reports_for_day int DEFAULT 0
    );
    
CREATE TABLE "extent" -- This is to define the bounding box of the project
    (minlng Double Precision,
    maxlng Double Precision,
    minlat Double Precision,
    maxlat Double Precision
    );
    
-- Sensors

CREATE TABLE "Sensor Type Information" -- This is to keep track of the different sensors in the project
    (sensor_type text PRIMARY KEY, -- Sensor Type Identifier
    api_name text, -- the keeper of the api for this sensor, eg. PurpleAir - These should be in App/modules/Sensors/APIs
    monitor_name text, -- A name for the monitor that holds this sensor, - These should be in App/modules/Sensors/APIs/api_name
    api_fieldname text, -- the fieldname to get regular readings from the api
    pollutant text, -- An abbreviated name of the pollutant measured
    metric text, -- A unit to append to readings of this sensor for context
    thresholds float [],  -- The health thresholds for this sensor (in the above metric)
    -- ^ In this order (lowest possible, moderate, unhealth for sensitive groups, unhealthy, very unhealthy, hazardous, highest possible)
    radius_meters float, -- The max distance this sensor is relevant to (for POIs)
    last_update timestamp DEFAULT TIMESTAMP '2000-01-01 00:00:00', -- Last regular update time
    update_frequency int -- The frequency for regular updates in minutes, should relate to api_fieldname's time interval
    );

CREATE TABLE "Sensors" -- Storage for all sensors
(
	sensor_id serial PRIMARY KEY, -- Our Unique Identifier
	sensor_type text REFERENCES "Sensor Type Information" (sensor_type), -- Relates to above table
	api_id text, -- The unique identifier for the api
	name varchar(100), -- A name for the sensor (for humans)
	date_created timestamp DEFAULT CURRENT_TIMESTAMP,
	last_seen timestamp DEFAULT CURRENT_TIMESTAMP,
	channel_state int DEFAULT 1, -- Indicates whether the sensor is active (1) or not (0),
	channel_flags int DEFAULT 0, -- Indicates whether sensor is depricated, 0 = not degraded, 1 = channel a degraded, 2 = channel b degraded, 3 = degraded, 4 = newly flagged
	altitude int,
	current_reading float DEFAULT -1 -- The last value seen of the sensor
--	geometry geometry -- A Point, added later in this script
);

-- Alerts

CREATE TABLE "Active Alerts" -- These are the SpikeAlerts that are currently out
	(alert_id bigserial PRIMARY KEY, -- Unique identifier for a spike alert
	 sensor_id int REFERENCES "Sensors" (sensor_id), -- Sensor Unique Identifiers
	 sensitive boolean DEFAULT TRUE, -- Indicates whether this is an alert only for sensitive groups
	  start_time timestamp, -- The time when sensor values started reporting high
	  last_update timestamp, -- last time the alert was updated
	  avg_reading float, -- Average value registered
	   max_reading float, -- Maximum value registered
	   UNIQUE (sensor_id, sensitive)); -- Ensure that each alert has a unique sensor_id, sensitive combo

CREATE TABLE "Archived Alerts" -- Archive of the Above table
	(alert_id bigint PRIMARY KEY, -- Unique identifier for a spike alert
	 sensor_id int REFERENCES "Sensors" (sensor_id), -- Sensor Unique Identifiers
	 sensitive boolean DEFAULT TRUE, -- Indicates whether this is an alert only for sensitive groups
	  start_time timestamp,
	  duration_minutes integer, -- How long it lasted in minutes
	  avg_reading float, -- Average value registered
	   max_reading float -- Maximum value registered
	   );
	   
-- POIs

CREATE TABLE "Places of Interest"-- This is our internal record keeping for POIs (AKA users)
	(poi_id bigserial PRIMARY KEY, -- Unique Identifier
	name varchar(100) DEFAULT '', -- A name for the POI. Can be null for privacy
	alerts_sent int DEFAULT 0, -- Number of alerts sent
    active_alerts_sensitive bigint [] DEFAULT array[]::bigint [], -- List of Active Alert ids (for sensitive populations)
	cached_alerts_sensitive bigint [] DEFAULT array[]::bigint [], -- List of ended Alerts ids in same event as above
	active_alerts bigint [] DEFAULT array[]::bigint [], -- List of Active Alert ids (for all populations)
	cached_alerts bigint [] DEFAULT array[]::bigint [], -- List of ended Alerts ids in same event as above
	active boolean DEFAULT TRUE -- Are we monitoring this point?
	);

-- Reports

CREATE TABLE "Reports Archive"-- These are for keeping track of reports for each POI
	(report_id varchar(12) PRIMARY KEY, -- Unique Identifier with format #####-MMDDYY
	poi_name varchar(100), -- A name to attach to the POI. Can be null for privacy
	start_time timestamp,
	duration_minutes integer,
	sensitive boolean, -- Indicates whether this is a report only for sensitive groups
	alert_ids bigint [] -- List of alert_ids
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
			   CREATE INDEX sensor_gid ON %I."Sensors" USING GIST(geometry);  -- Create spatial index for sensors
			   
			   ALTER TABLE %I."Places of Interest"
			   ADD geometry geometry; -- A Point
			   CREATE INDEX poi_gid ON %I."Places of Interest" USING GIST(geometry);  -- Create spatial index for POIs
			   '
			   , schemaname, schemaname, schemaname, schemaname);
END$$;

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
-- Create Views

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- Sensors and Sensor Type Information merged

CREATE VIEW base.sensor_ids_w_info AS
(
SELECT s.sensor_id, i.sensor_type, i.api_name, i.monitor_name, i.api_fieldname, 
       i.pollutant, i.metric, i.thresholds, i.radius_meters, i.last_update, i.update_frequency
FROM base."Sensor Type Information" i
RIGHT JOIN base."Sensors" s ON (i.sensor_type = s.sensor_type)
);

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- Sensors with an active alert

CREATE VIEW base.alerted_sensors AS
(
SELECT s.sensor_id, s.name, s.sensor_type,
       a.alert_id, a.sensitive, a.start_time, s.last_seen,
       s.current_reading, a.avg_reading, a.max_reading,
	   s.geometry 
FROM base."Active Alerts" a
INNER JOIN base."Sensors" s ON (a.sensor_id = s.sensor_id)
);

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
-- Active Alerts, Sensors, and Sensor Type Information merged

CREATE VIEW base.alerts_w_info AS
(
SELECT s.sensor_id, s.name, s.alert_id, s.sensitive, s.start_time, s.last_seen,
	   i.pollutant, i.metric, i.thresholds, i.radius_meters,
       s.current_reading, s.avg_reading, s.max_reading,
       map_to_health(s.current_reading, i.thresholds) as health_descriptor,
	   s.geometry
FROM base.alerted_sensors s
INNER JOIN base.sensor_ids_w_info i ON (s.sensor_id = i.sensor_id)
);
 
-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

-- Create Public Views

-- ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CREATE VIEW public.alerted_sensors AS
(
SELECT s.name, a.start_time, s.last_seen,
       s.current_reading, a.avg_reading, a.max_reading,
	   s.geometry
FROM base."Active Alerts" a
INNER JOIN base."Sensors" s ON (a.sensor_id = s.sensor_id)
);
