### Import Modules
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def new_alert_message(poi_name = '', webmap_link = '', verified_number = True):
    '''
    Get a message for a new alert
    # Composes and returns a single message
    
    parameters:
    poi_name = string describing the place of interest that is alerted
    webmap_link = string sending the user to the correct site to visualize the alert
    verified_number = Can we send URLs for their contact_method?
    '''
    
        
    # Short version (1 segment)
    
    message = '''Warning
Air quality may be unhealthy near '''

    if poi_name == '':
        message += 'your place of interest'
    else:
        message += poi_name
    
    # URLs cannot be sent until phone number is verified
    if verified_number:
        message = message + f'''
        
{webmap_link}'''
    else:
        message = message + '''
        Please see PurpleAir'''
        
    message = message + '''
    
Text STOP to unsubscribe'''
        
    return message


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def end_alert_message(duration, severity, report_url = ''):
    '''
    Get a list of messages to send when an alert is over

    inputs:
    duration = integer (number of minutes)
    severity = the alert interpretted by health benchmarks
    report_url = string sending the user to the correct site to report any observations
    
    Returns a message (string)
    '''
        
    message = f'''Alert Over
Duration: {int(duration)} minutes 
Severity: {severity}

Report here - '''
    
    # URLs cannot be sent until phone number is verified
    if report_url != '':
        message = message + f"{report_url}"
    else:
        message = message + f'Report option coming soon'
    # See https://help.redcap.ualberta.ca/help-and-faq/survey-parameters for filling in variable in url
        
    return message
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def morning_alert_message(poi_name, webmap_link = '', verified_number = True):
    '''
    Get a message for an ongoing alert for the morning update
    # Composes and returns a single message
    
    parameters:
    poi_name = string describing the place of interest that is alerted
    webmap_link = string sending the user to the correct site to visualize the alert
    verified_number = Can we send URLs for their contact_method?
    '''
    
        
    # Short version (1 segment)
    
    message = '''Ongoing Alert
Air quality may be unhealthy near '''

    if poi_name == '':
        message += 'your place of interest'
    else:
        message += poi_name
    
    # URLs cannot be sent until phone number is verified
    if verified_number:
        message = message + f'''
{webmap_link}'''
    else:
        message = message + '''
        Please see PurpleAir'''
        
    message = message + '''
    
Text STOP to unsubscribe'''
        
    return message
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    
def no_location(signUp_url):
    '''
    Composes a message informing a user of misentered locations 
    '''
    
    message = f'''Hello, this is SpikeAlerts. 

We're sorry, no location was entered in your form. Please resubmit.

{signUp_url}'''

    return message
    
def welcome_message():
    '''
    Composes a message welcoming a new user!
    '''

    message = '''Welcome to SpikeAlerts! 

We will text when air quality seems unhealthy (using 24 hour Standards) in your area.

Consider alerts as a caution and stay vigilant!

For questions see SpikeAlerts.github.io/Website

Reply STOP to end this service. Msg&Data Rates May Apply'''

    return message
