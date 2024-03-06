### Import Packages

# File Manipulation

import os # For working with Operating System
#import sys # System arguments
from dotenv import load_dotenv # Loading .env info

# Database 

from modules.Database import Basic_PSQL as psql
import psycopg2
from psycopg2 import sql

# Data Manipulation

import numpy as np
import pandas as pd

# Importing Libraries
from importlib import import_module
  
# ~~~~~~~~~~~~~~ 
   
def workflow(messaging_df, contact_info_api, timezone):
    '''
    This function will send each message to the corresponding users

    parameters:
    
    messaging_df - a dataframe with fields: contact_method, api_id, message
    '''
    
    # Initialize count for number of messages sent
    
    messages_sent = len(messaging_df)
    
    # Get contact methods to iterate through
    
    contact_methods = messaging_df.contact_method.unique()
    
    # Iterate
    
    for method in contact_methods:
        
        temp_df = messaging_df[messaging_df.contact_method == method]
        
        # Get the contact information (from the more secure external storage)
        
        module = f'modules.Users.Contact_Info_APIs.{contact_info_api}'
        api_ids, contacts, messages = import_module(module).get_contacts(temp_df) # Make sure this returns these 3 lists with same indexing!!
        
        # Send Messages (Using this method)
        # Returns indices of the above tuples that have unsubscribed
        
        module = f'modules.Users.Contact_Methods.{method}'
        unsubscribed_indices = import_module(module).send_messages(contacts, messages) 
        
        # If any of these users have unsubscribed, change active in our database
        if len(unsubscribed_indices) > 0:
        
            api_ids_to_unsubscribe = list(np.array(api_ids)[unsubscribed_indices])
            Unsubscribe_users(method, api_ids_to_unsubscribe)
            
            messages_sent -= unsubscribed_indices # Didn't send these messages
    
    update_daily_log(messages_sent, timezone)
    
# ~~~~~~~~~~~~~

def update_daily_log(messages_sent, timezone):
    '''
    This function adds to the number of messages sent
    '''
    cmd = sql.SQL('''UPDATE "Daily Log"
                    SET messages_sent = messages_sent + {}
                    WHERE date = DATE(CURRENT_TIMESTAMP AT TIME ZONE {});
                   ''').format(sql.Literal(messages_sent),
                               sql.Literal(timezone)
                               )
                   
    psql.send_update(cmd)
    
# ~~~~~~~~~~~~~~~~~~~~~
    
def Unsubscribe_users(api_ids, contact_method):
    '''
    Change record_ids to subscribed = FALSE in our database
    '''
    
    cmd = sql.SQL('''UPDATE "Sign Up Information"
    SET active = FALSE
WHERE api_id = ANY ( {} )
AND contact_method = {};
    ''').format(sql.Literal(api_ids),
                sql.Literal(contact_method))
    
    psql.send_update(cmd)
    
# ~~~~~~~~~~~~~~~~~~~~~~~~

def Message_mgmt(message):
    '''
    This function should send the string argument to the manager of the app 
    '''
    
    load_dotenv('env.secret')
    
    method = os.getenv('MGMT_CONTACT_METHOD')
    contact = os.getenv('MGMT_CONTACT')
    
    module = f'modules.Users.Contact_Methods.{method}'
    
    import_module(module).send_messages([contact], [message]) 
