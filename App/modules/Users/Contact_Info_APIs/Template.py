### Import Packages

from dotenv import load_dotenv # Loading .env info
import os

load_dotenv('.env.secret')

# ~~~~~~~~~~~~~

def get_contacts(messaging_df):
    '''
    takes a dataframe with fields contact_method, api_id, message 
    
    Returns 3 lists: api_ids, contacts, messages with the same indexing!
    '''
    # Initialize return values
    
    api_ids = messaging_df.api_id.to_list()
    contacts = []
    messages = messaging_df.message.to_list()
    
    # Get the token to access external API with contact info
    
    contact_api_token = os.getenv('CONTACT_API_TOKEN')
    
    # Get the contacts somehow (CHANGE THIS!)
    
    contacts = ['000-000-0000'] * len(api_ids)
    
    return api_ids, contacts, messages

# ~~~~~~~~~~~~~

def get_new_users(max_api_id):
    '''
    This function gets the newest users' informations
    
    returns a pandas dataframe with fields:
    
    poi_id, -- int REFERENCES base."Places of Interest" (poi_id), -- Aligns with a POI in the database, might change to an array one day
	contact_method, -- text, -- How will we get a hold of this user? Should be a script in App/modules/Users/Contact_Methods/{contact_method}.py with a function send_messages()
	api_id, -- text, -- This should be the identifier for wherever the contact info is stored (if not in this database)
	sensitive, -- boolean, -- True = send alerts when "Unhealthy for sensitive populations"
	days_to_contact, -- int [] DEFAULT array[1,2,3,4,5,6,7]::int[], -- 1 = Monday, 7 = Sunday
	start_time, -- time, -- The earliest time to send the user a message
	end_time -- time, -- The latest time to send the user a message
    '''

    new_users_df = pd.DataFrame(columns = ['poi_id',
                                           'contact_method',
                                           'api_id',
                                           'sensitive',
                                           'days_to_contact',
                                           'start_time',
                                           'end_time'])
    
    
    return new_users_df
    
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
