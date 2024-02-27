# How to Initialize the Database

## 1) Create a [Postgres](https://www.postgresql.org/) server and connect to it 

---
---
## 2) Create the "Manager" user (mgmt)

### PSQL:
```
CREATE ROLE mgmt with LOGIN CREATEDB CREATEROLE SUPERUSER;

ALTER ROLE mgmt WITH PASSWORD 'postgres'; -- Change this!
```

---
---
## 3) Create the database for the App

### PSQL: 
`CREATE DATABASE "SpikeAlerts"`

---
---
## 4) Connect to the "SpikeAlerts" database as mgmt

Command line: psql "host=IP_ADDRESS dbname='SpikeAlerts' user=mgmt password=<your_password>"

or psql -h XXX.XXX.XXXX -U mgmt -d 'SpikeAlerts'

and then type in your password

Or use pgAdmin or another database manager

---
---
## 5) Initialize the base schema and postgis extensions
Careful with this codeblock. It cascade deletes POSTGIS which will remove geometry columns of all tables **within your entire database**... You may want to remove that line.

### SQL
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
## 6) Run the SQL in the file /initialize_tables.sql

Copy, paste, and execute the sql or use:

### PSQL: 
`-f initialize_tables.sql`

---
---
## 7) Create the "App" user

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
# 8) Insert Extent

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
# 9) Add Sensor Types

More examples are given in add_sensor_types.sql

## An Example of SQL to do this for PurpleAir is:

```
INSERT INTO base."Sensor Type Information" ( 
    sensor_type, -- text, -- Sensor Type Identifier 
    api_name, -- text, -- the keeper of the api for this sensor, eg. PurpleAir - These should be in App/modules/Sensors/APIs 
    monitor_name, -- text, -- A name for the monitor that holds this sensor, - These should be in App/modules/Sensors/APIs/api_name 
    api_fieldname, -- text, -- the fieldname to get regular readings from the api 
    pollutant, -- text, -- A name of the pollutant measured 
    metric, -- text, -- A unit to append to readings of this sensor for context 
    thresholds, -- float [],  -- The left inclusive health thresholds for this sensor (in the above metric) 
    -- ^ In this order (lowest possible, moderate, unhealth for sensitive groups, unhealthy, very unhealthy, hazardous, highest possible) 
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
```

---
---
# 10) Add POIs 
Use any geometry (Points, Multipoints, Lines, Polygons)

## SQL Examples

### Center(ish) Point of Smith Foundry

```
INSERT INTO base."Places of Interest" 
	(
	name, -- varchar(100), -- A name for the POI. Can be null.
	geometry -- Can be any geometry
	)
VALUES 
	(
	'Smith Foundry Point',
	ST_SetSRID(ST_MakePoint(-93.24553722585783, 44.95170035308462), 4326)
	);
```

### A point NW of HERC:

```
INSERT INTO base."Places of Interest" 
	(
	name, -- varchar(100), -- A name for the POI. Can be null.
	geometry -- Can be any geometry
	)
VALUES 
	(
	'Point NW of Hennepin Energy Recovery Center',
	ST_SetSRID(ST_MakePoint(-93.28983454197842, 44.99135801605877), 4326)
	);
```

### A polygon NW of Smith Foundry:

```
WITH geom as
(
SELECT ST_MakePolygon( ST_AddPoint(foo.open_line, ST_StartPoint(foo.open_line)) ) as poly
	FROM (
	SELECT ST_GeomFromText('LINESTRING(-93.24737530547333 44.95200401860498,
					   -93.25003219449039 44.951945865667824,
					   -93.24737530547333 44.95374857932035)
					   ', 4326
					   ) AS open_line
	     ) as foo
)
INSERT INTO base."Places of Interest" 
(
name, -- varchar(100), -- A name for the POI. Can be null.
geometry -- Can be any geometry
)
SELECT
'Area NW of Smith Foundry', poly 
FROM geom;
```
---
---
# Extension (Users)

If you want to have folks sign up for the alerts we'll have to add one more table.

## SQL:
```
CREATE TABLE base."Users" -- Storage for all sensors
(
	user_id serial PRIMARY KEY, -- Our Unique Identifier
	poi_id int REFERENCES base."Places of Interest" (poi_id), -- Aligns with a POI in the database, might change to an array one day
	alerted boolean DEFAULT FALSE, -- Is the user currently alerted?
	contact_method text, -- How will we get a hold of this user? Should be a script in App/modules/Users/Messaging/{contact_method}.py
	api_id text, -- This should be the identifier for wherever the contact info is stored (if not in this database)
	sensitive boolean, -- True = send alerts when "Unhealthy for sensitive populations"
	days_to_contact int [] DEFAULT array[0,1,2,3,4,5,6]::int[], -- 0 = Monday, 6 = Sunday
	start_time time, -- The earliest time to send the user a message
	end_time time, -- The latest time to send the user a message
	active boolean DEFAULT TRUE -- Is the user currently active?
);
```
