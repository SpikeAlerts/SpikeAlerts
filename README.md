# Hello!

We are a collective working to make a free and open source alert/reporting system for Air Quality ("SpikeAlerts").

## What is this Repository?

This is a repository to document and develop the base of SpikeAlerts. SpikeAlert instances can (and should) be set up by anyone, anywhere! 

A general overview of the repository: 

+ .env.example - The base environment variables needed for the system to run
+ .env.sensors.example - The environment variables needed for updating the Sensors from their APIs
+ requirements.txt - The Python libraries needed beyond base python 3.10
+ /Database - Files to initialize the database
+ /App - The SpikeAlerts Application. Run with python App/spikealerts.py

## How to Set Up

### 1) Create the Database

Please see the readme in the /Database directory

### 2) Fill in the .env's

`.env.example` and `.env.sensors.example` have some directions on how to create your own environment files. Presently, the extensions are not ready

### 3) Set up Python Environment

This can be done in a number of ways (eg. [miniconda](https://docs.anaconda.com/free/miniconda/index.html)). The Python requirements are in `requirements.txt`. If you use miniconda, below is an example of an `environment.yml` file that will set up an environment called SpikeAlerts. You can create this environment with `conda env create -f environment.yml`

environment.yml:
```
name: SpikeAlerts
channels:
  - conda-forge
  - defaults
dependencies:
  - python =3.10
  - pip
  - geopandas
  - psycopg2-binary
  - python-dotenv
```

### 4) Run the App!

Here's how you can run the app from a terminal:

```
cd home/Documents/GitHub/SpikeAlerts # Change directory to this Repository

conda activate SpikeAlerts # Activate the Python Environment

python App/spikealerts.py # Run the App
```

### 5) Check out the database

You should be able to see the "Sensors", "Active Alerts", "Archived Alerts", "Places of Interest" tables updating. 

## Foundational Developers

Rob Hendrickson

Priya Dalal-Whelan

Dan Raskin

Doug Carmody

Mateo Frumholtz

## Other Contributors (organizing, discussion, feedback, research, sensor installation & maintenance...)

Connor Stratton, Urszula Parfieniuk, Nazir Khan, Jenni Lansing, Lucy Shapiro, Alice Froehlich, Megan Greenberg, Mary Marek-Spartz, Kerry Wang, Daniel Furata, Eamonn Fetherston, Jake Ford

We also acknowledge the preliminary work of the [Quality Air, Quality Cities team](https://github.com/RTGS-Lab/QualityAirQualityCities).
