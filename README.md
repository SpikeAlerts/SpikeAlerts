# Hello!

We are a collective working to make a text alert and reporting system for Air Quality "Spikes" in Minneapolis.

## What is this Repository?

This is a repository to document and develop features for SpikeAlerts. Still a work in process!

Here's what's in the repository right now in order of what must be done to run your own instance of this application:

+ .env.example - The base environment variables needed for the system to run
+ .env.sensors.example - The environment variables needed for updating the Sensors from their APIs
+ requirements.txt - The Python libraries needed beyond base python 
+ /Database - Files to initialize the database
+ /App - The SpikeAlerts Application. Run with python App/spikealerts.py

## How to Set Up

### 1) Create the Database

Please see the readme in the Database folder

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

<!--+ Basic_Python_Requirements.txt - The python requirements for the VM application
+ Conda_Developer_Environment.yml - Conda environment for all scripts and exploring data
+ .env.example - An example of a .env file that is needed for full functionality

+ APP_Documentation/ - A directory that includes all scripts and Jupyter notebooks showing how they work
    + App/ - A copy of the app that is currently running on a Heroku Dyno
+ Database/ - A directory with the Database information and documentation-->

<!--## Installation Instructions

 For development or personal use

+ Clone this repository 
+ Install Pyton 3
+ Install Jupyter Lab/Notebook (optional for running notebooks)
+ Install  Python dependencies using your preferred environment manager ([Miniconda](https://docs.conda.io/projects/miniconda/en/latest/) works well!). View dependencies [here](https://github.com/RwHendrickson/AQ_SpikeAlerts/blob/main/Conda_Environment.yml)
+ Install prefered database manager (we use pgAdmin 4)
+ Configure database (If you are interested in working on our database, please reach out to us for credential information. If you'd like to create your own, create a PostgreSQL Database following instructions in initializedb.sql
    + If not using our database, create and connect to your own database, add PostGIS extensions, then run intializedb.sql
    + Add your database credentials to the .env file
    + Run everything in the 1_QAQC  folder.  
+ Configure a [.env](https://github.com/RwHendrickson/AQ_SpikeAlerts/blob/main/.env.example) file
	+ Reach out to PurpleAir to get an API read key
 	+ REDCap tokens will need to be generated for the sign-up and report surveys - we're working on supplying templates for these
 	+ To have the texting option, you will need to set up a [Twilio account](https://www.twilio.com/en-us/sms/pricing/us) or modify code to work with another SMS service -->


<!--## Outstanding To-Dos: 
+ Clustering Spikes - When multiple monitors close together spike at once it should be treated as one spike
+ Health Thresholds (pm2.5) may be adjusted
+ Allow users to specify message frequency, radius of sensor interest, spike threshold?
+ Perhaps include information on nearby roadways/facilities
+ Create our own webmap for selecting sensors/location of interest?
+ Test and community feedback -->

<!--## Potential use and next steps:  
+ Funding and deployment: Long term use an deployment requires an organizational sponsor or community fundraising. 
	+ Estimated costs (calculated by google cloud estimater) 
        + Google Cloud storage: $51.01- $100/month
        + Compute engineer $12-35/ month
        + Texts via Twillio  $.0079/ text to use Twillio. At a list of 500 users, texted twice a day, upper limit of $240/month. 
            + Alternatively, alerts could also be sent out via email or social media to keep costs down. 
+ Building out of subscriber list
+ Health training /response. Subscribers should be educated on how to respond to alert messages in a. For instance, teachers and parents could sign up for alert so they are aware when it is not safe to have students/children outside. 
+ Organizing: while protecting ourselves from dangerous air quality in the moment is useful, our ultimate goal is to shut down the facilities and infrastructure that creates these problems. We can imagine this tool being integrated with organizing efforts in a variety of ways –  ultimately this information is only as powerful as the communities that use it. -->

## Authors 

Priya Dalal-Whelan

Rob Hendrickson

### Contributors:

Mateo Frumholtz - Designing, building, and maintaining the REDCap Surveys

Jake Ford - Brainstorming

Connor - Organizing/Facilitating Zoom meetings

Doug - SMS Messaging development

Dan - Cloud development

We also acknowledge the preliminary work of the [Quality Air, Quality Cities team](https://github.com/RTGS-Lab/QualityAirQualityCities).
