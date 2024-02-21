# Hello!

We are a collective working to make a free and open source alert & reporting system for Air Quality called "SpikeAlerts".

## What is this Repository?

This code is for the Minneapolis Instance of a SpikeAlerts system.

## Functionality  

Users who want to receive alert messages can fill out our survey and have their phone number and location of interest stored in a secure [REDCap](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC5764586/) database hosted by the University of Minnesota.

The program queries the PurpleAir API every 10 minutes (soon we'll have more sensors) and checks for readings above a threshold (35.5 micrograms/meter^3 for sensitive groups, 55.5 for all) according to the current [24-hr EPA Standards for PM2.5](https://www.epa.gov/pm-pollution/national-ambient-air-quality-standards-naaqs-pm). The thresholds are variables that can easily be changed/adjusted. When the system detects a spike, it sends a text to all subscribers within 1 kilometer of the monitor if they don't already have an active alert. The text links to the sensor on the PurpleAir Webmap.

When all alerts end for a user, an end of spike alert message is sent to the subscriber, detailing the length and severity of the event, and a unique reporting option through REDCap. Alerts are archived for future reference (this does not include the user's location/phone number).

## How to Contribute?

We welcome collaboration! Please check out [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Foundational Developers

Rob Hendrickson

Priya Dalal-Whelan

Dan Raskin

Mateo Frumholtz

Doug Carmody

## Sensor Maintenance & Counsel

Jenni Lansing

Lucy Shapiro

## Other Contributors 

Connor Stratton, Urszula Parfieniuk, Michael Wilson, Nazir Khan, Alice Froehlich, Megan Greenberg, Mary Marek-Spartz, Kerry Wang, Daniel Furata, Eamonn Fetherston, Jake Ford

Thank you for organizing, discussion, feedback, research, and everything in between!

We also acknowledge the preliminary work of the [Quality Air, Quality Cities team](https://github.com/RTGS-Lab/QualityAirQualityCities).
