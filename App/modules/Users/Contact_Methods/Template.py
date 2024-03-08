import os
import time

def send_messages(contact_info_list, messages): 
    '''
    basic send function that takes in a list of contact informations + list of messages and sends them out
    
    returns unsubscribe_indices that correspond to indices of both above lists
    '''

    # Check Unsubscriptions
    
    unsubscribed_indices = check_unsubscriptions(contact_info_list)
    
    # pop() unsubscriptions from contact_info_list/messages list
        
    for unsubscribed_index in unsubscribed_indices:
        contact_info_list.pop(unsubscribed_index)
        messages.pop(unsubscribed_index)
        
    # CHANGE BELOW

    for contact_info, message in zip(contact_info_list, messages):

        print('contact info: ', contact_info, '\n\n message:\n', message)
        
        time.sleep(1) # Sleeping for 1 second between sending messages
        
    return unsubscribed_indices
    
def check_unsubscriptions(contact_info_list):
    '''Returns the indices of contact_info_list that have unsubscribed
    '''

    unsubscribed_indices = []
    
    return unsubscribed_indices
