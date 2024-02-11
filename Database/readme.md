# 1) Create a [Postgres](https://www.postgresql.org/) server and connect to it 

---
---
# 2) Create the "Manager" user (mgmt)

## PSQL:
```
CREATE ROLE mgmt with LOGIN CREATEDB CREATEROLE SUPERUSER;

ALTER ROLE mgmt WITH PASSWORD 'postgres'; -- Change this!
```

---
---
# 3) Create the database for the App

## PSQL: 
`CREATE DATABASE "SpikeAlerts"`

---
---
# 4) Connect to the "SpikeAlerts" database as mgmt

Command line: psql "host=IP_ADDRESS dbname='SpikeAlerts' user=mgmt password=<your_password>"

or psql -h XXX.XXX.XXX.XXXX -U mgmt -d 'SpikeAlerts'

and then type in your password

Or use pgAdmin or another database manager

---
---
# 3) Initialize the base schema and postgis extensions
Careful with this codeblock. It cascade deletes POSTGIS which will remove geometry columns of all tables **within your entire database**... You may want to remove that line.

## SQL
```
-- Drop/create schema and extensions
DO $$
DECLARE schemaname text; -- Initialize variable for the schema name
BEGIN
    schemaname := 'base'; -- Set the Schema Name you wish to use
    
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
```

---
---
# 4) Run the SQL in the file /4_initialize_tables.sql

---
---
# 4) Create the "App" user

## PSQL:
```
CREATE ROLE app with LOGIN;

ALTER ROLE app WITH PASSWORD 'postgres'; -- Change this!

GRANT USAGE ON SCHEMA base TO app;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA base TO app;

GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA base TO app;
```

---
---
# 5) Insert Extent

Fill in extent.csv with appropriate latitudes and longitudes and run this 

## PSQL:
`\copy base.extent(minlng, maxlng, minlat, maxlat) FROM 'SpikeAlerts/Database/extent.csv' with header CSV"`

Or use the following SQL (after changing the lat/lons)

## SQL:
```
-- Extent of the project (in latitude/longitude)

INSERT INTO base."extent" (minlng, maxlng, minlat, maxlat)
VALUES (-93.3303753775222, -93.1930625073825, 44.8896883413448, 45.0521464662874
);
```
---
---
# 6) Add Sensor Types

## An Example of SQL to do this for PurpleAir is:

```
INSERT INTO base."Sensor Type Information" ( 
    sensor_type, -- text, -- Sensor Type Identifier 
    api_name, -- text, -- the keeper of the api for this sensor, eg. PurpleAir - These should be in App/modules/Sensor_APIs 
    monitor_name, -- text, -- A name for the monitor that holds this sensor, - These should be in App/modules/Sensor_APIs/api_name 
    api_fieldname, -- text, -- the fieldname to get regular readings from the api 
    pollutant, -- text, -- A name of the pollutant measured 
    metric, -- text, -- A unit to append to readings of this sensor for context 
    thresholds, -- float [],  -- The left inclusive health thresholds for this sensor (in the above metric) 
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
    1000, 
    10 
); 
```
