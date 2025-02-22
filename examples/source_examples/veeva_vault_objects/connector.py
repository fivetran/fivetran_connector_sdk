# This is a simple example for how to work with the fivetran_connector_sdk module.
# This code is currently configured to Retrieve Details from All Object Types in Veeva Vault and then utilize VQL to retrieve all Object records, creating one table per object
# You will need to provide your own Veeva Vault credentials for this to work --> subdomain, username, and password variables in configuration.json 
# Retrieve Details from All Object Types endpoint: https://developer.veevavault.com/api/24.2/#retrieve-details-from-all-object-types
# VQL endpoint: https://developer.veevavault.com/api/24.2/#vault-query-language-vql
# You can also add code to extract from other endpoints as needed.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import requests
import json
import datetime

# Function to start a Veeva Session, inputs needed are: username, password, and subdomain -- all from the configuration.json file
# Auth endpoint: https://developer.veevavault.com/api/24.2/#user-name-and-password 
# The startVeevaSession function takes 1 parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.

def startVeevaSession(configuration: dict):
    username = configuration.get('username')
    password = configuration.get('password')
    auth_url = f"https://{configuration.get('subdomain')}.veevavault.com/api/v24.2/auth"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }
    data = {
        "username": username,
        "password": password
    }
    response = requests.post(auth_url, headers=headers, data=data)
    if response.status_code == 200:
        response_json = response.json()
        if response_json.get('responseStatus') == "SUCCESS":
            session_id = response_json.get('sessionId')
        else:
            log.severe(f"Failed to create session for user with username: {username}")
            session_id = None
    else:
        log.severe(f"Authentication failed: {auth_url} returned {response.status_code}. Expected 200.")
        session_id = None
    log.info(f"Started session for user: {username}")
    return session_id

# Function that ends a Veeva session, inputs needed are: active session_id and subdomain from configuration.json file
# End session endpoint: https://developer.veevavault.com/api/24.2/#end-session
# The endVeevaSession function takes 2 parameters:
# - configuration: a dictionary that holds the configuration settings for the connector.
# - session_id: a Veeva Vault session ID generated from the startVeevaSession function

def endVeevaSession(configuration: dict, session_id):
    deactivate_session_url = f"https://{configuration.get('subdomain')}.veevavault.com/api/v24.2/auth"
    headers = {
    "Authorization": session_id
    }
    response_deact = requests.delete(deactivate_session_url, headers=headers)
    log.info(f"Deactivation of session for user: {configuration.get('username')} responded with code: {response_deact.status_code}: {response_deact.reason}")
    
# Function to get list of available Veeva Vault objects
# Endpoint documentation: https://developer.veevavault.com/api/24.2/#retrieve-details-from-all-object-types
# The getVaultObjects function takes 2 parameters:
# - configuration: a dictionary that holds the configuration settings for the connector.
# - session_id: a Veeva Vault session ID generated from the startVeevaSession function

def getVaultObjects(configuration: dict, session_id):
    base_url = f"https://{configuration.get('subdomain')}.veevavault.com/api/v24.2/auth"
    headers = {'Authorization': session_id,
            'Accept': 'application/json'}
    get_objects_url = base_url + 'configuration/Objecttype'
    obj_response = requests.get(get_objects_url, headers = headers).json()
    objects = obj_response.get('data')
    return objects

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes 1 parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.

def schema(configuration: dict):

    # Dynamically define schema based on getVaultObjects response
    # Note: Defining all tables to have a PK of "id" right now - need to verify that this will hold true for all objects

    session_id = startVeevaSession(configuration)
    objects = getVaultObjects(configuration, session_id)
    schema_def = []
    for obj in objects:
        schema_def.append({"table": obj.get('label_plural'), "primary_key": ["id"]})
    endVeevaSession(configuration, session_id)
    return schema_def

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes 2 parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
# This function is designed to loop through and retrieve vault Object data using VQL endpoint: https://developer.veevavault.com/api/24.2/#submitting-a-query
    
def update(configuration: dict, state: dict):

    #Initialize the sync process
    
    base_url, sub_domain, page_size, current_time, cursor, session_id, objects = initialize_sync(configuration, state)

    #Loop through all objects to define fields and compose query

    for obj in objects:
        compose_query(base_url,sub_domain,obj, cursor, page_size,session_id,current_time)

    #End Veeva Session after all objects and pages are iterated through

    endVeevaSession(configuration, session_id)
    
    #Set cursor/filter time for next sync to be the start time of this current sync
    
    yield op.checkpoint({"cursor": current_time})

#Helper Functions

# Function to initialize the sync
# Initialize_sync function takes in 2 parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync

def initialize_sync(configuration: dict, state: dict):
    
    sub_domain = configuration.get('subdomain')
    base_url = f"https://{sub_domain}.veevavault.com/api/v24.2/auth"
    
    # Batch size to query, this can be updated and will be placed in every query's PAGESIZE VQL clause
    
    page_size = 50

    # Define current time (where to start subsequent sync) and extract current cursor from state

    current_time = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    cursor = state.get('cursor')

    # Start Veeva session and retrieve all object details for which to query
    
    session_id = startVeevaSession(configuration)
    objects = getVaultObjects(configuration, session_id)
    return base_url, sub_domain, page_size, current_time, cursor, session_id, objects

# Function to take the output of the initialization and apply operations to sync each object
# Compose Query takes in 6 parameters:
# - base_url
# - sub_domain
# - obj
# - cursor
# - page_size
# - session_id

def compose_query(base_url: str, sub_domain:str, obj: dict, cursor: str, page_size: int,session_id:str):

    # Get object name, label, and compose commma separated list of fields to query for

    object_name = obj.get('object')
    object_label = obj.get('label_plural')
    fields_dict = obj.get('type_fields')

    # May want to wrap some fields in TOLABEL() here depending on desired output

    fields = ', '.join(d.get('name') for d in fields_dict)

    # Query with date filter if incremental sync, without if initial sync

    if cursor:
        query = f"select {fields} from {object_name} WHERE modified_date__v > '{cursor}' PAGESIZE {page_size}"
    else:
        query = f"select {fields} from {object_name} PAGESIZE {page_size}"
        log.info(f"Next query is: {query}")

        # Define headers and %20 spaces for query payload

        vql_headers = {
            'Authorization': session_id,
            'Accept': 'application/json',
            'X-VaultAPI-DescribeQuery': 'true',
            'Content-Type': 'application/x-www-form-urlencoded'
            }
        payload = f"q={query.replace(' ', '%20')}"

        # Generate API url and send VQL endpoint
        # VQL endpoint: https://developer.veevavault.com/api/24.2/#submitting-a-query

        vql_url = base_url + 'query'
        obj_vql = requests.post(vql_url, headers = vql_headers, data=payload).json()

        # Get and analyze responseDetails, which includes: pagesize, pageoffset, size, total, next_page

        response_details = obj_vql.get('responseDetails')
        log.info(f"VQL query response details: {response_details}")

        # If records returned, turn them into dictionary and upsert to object_table

        if response_details.get('size') > 0:
            for record in obj_vql.get('data'):
                row = {k: v[0] if isinstance(v, list) else v for k,v in record.items()}
                yield op.upsert(object_label, row)

        # Until there is no next_page, keep calling next_page_url from API response to paginate through object

        while response_details.get('next_page'):
            next_page_url = f"https://{sub_domain}{response_details.get('next_page')}"
            obj_vql = requests.post(next_page_url, headers = vql_headers).json()
            response_details = obj_vql.get('responseDetails')
            log.info(f"VQL query response details: {response_details}")
            if response_details.get('size') > 0:
                for record in obj_vql.get('data'):
                    row = {k: v[0] if isinstance(v, list) else v for k,v in record.items()}

                    # Upsert row to object's table

                    yield op.upsert(object_label, row)

# This creates the connector object that will use the update function defined in this connector.py file.

connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.

if __name__ == "__main__":

    # Open the configuration.json file and load its contents into a dictionary.

    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    
    connector.debug(configuration=configuration)