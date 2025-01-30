# Hubspot OAuth2.0 with refresh token Sample API Connector for Fivetran

# This module implements a connector for syncing data from the Hubspot API.
# NOTE: Hubspot connector is already present in Fivetran, and can be directly created
#     from the dashboard. Please do not use this as an alternative
# It is an example of using OAuth 2.0 client credentials flow, and the refresh of Access token from the provided refresh token.
# NOTE: We do not support programmatically updating refresh token currently.
#     If the refresh token is updates, it has to be updated manually on the fivetran dashboard after deployment.
#     Check readme file for more information on generating refresh token.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# imported constants from same directory
from constants import *
# Import datetime for handling date and time conversions.
import time
# import json to infer .
import json
# Imported requests for hubspot api requests generation
import requests
import urllib
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

# Global variables for
ACCESS_TOKEN = ""
REFRESH_TIME = 0

def get_access_token(configuration: dict):
    global ACCESS_TOKEN
    global REFRESH_TIME

    param_dict = {
        "grant_type": "refresh_token",
        "client_id": configuration.get("client_id"),
        "refresh_token": configuration.get("refresh_token"),
        "client_secret": configuration.get("client_secret")
    }

    uri = AUTH_URL +  urllib.parse.urlencode(param_dict)
    headers = {'content-type': 'application/x-www-form-urlencoded'}

    response = requests.request("POST", uri, headers=headers)

    if response.ok:
        log.info("Access token obtained successfully")
        data = response.json()
        # Updating ACCESS TOKEN and REFRESH TIME which is the epoch time in seconds when the ACCESS TOKEN will expire.
        # Note: We currently do not support programmatically updating refresh token.
        #     if required, It has to be updated manually on the fivetran dashboard after deployment.
        ACCESS_TOKEN = data["access_token"]
        REFRESH_TIME = int(data["expires_in"]) + time.time()
        return
    else:
        log.severe(f"Failed to obtain access token: {response.text}")
        log.info(uri)
        raise Exception("Failed to obtain access token")


def sync_contacts(configuration, cursor, state):
    # this is a custom function, meant to process the contacts
    # this processed data is then sent to fivetran
    def process_record(raw_contact):
        contact = {}
        contact["vid"] = raw_contact["vid"]
        contact["firstname"] = raw_contact["properties"]["firstname"]["value"]
        contact["company"] = raw_contact["properties"].get("company", {"value" : ""})["value"]
        contact["lastmodifieddate"] = raw_contact["properties"]["lastmodifieddate"]["value"]
        contact["email"] = raw_contact["identity-profiles"][0]["identities"][0]["value"]
        return contact

    has_more = True
    params = {"count":CONTACTS_RESPONSE_LIMIT}
    while has_more:
        data = get_data("contacts", params, {}, configuration)
        # Checking if more data is available, setting the required offset for the next request
        if data["has-more"]:
            params["vidOffset"] = data["vid-offset"]
        else:
            has_more = False
        # sending contact details to fivetran connector
        for contact in data["contacts"]:
            if contact["properties"].get("firstname") and contact["identity-profiles"][0].get("identities"):
                yield op.upsert("contacts", process_record(contact))


def sync_companies(configuration, cursor, state):
    # this is a custom function, meant to process the company record
    def process_record(raw_company):
        company = {}
        company["companyId"] = raw_company["companyId"]
        company["name"] = raw_company["properties"]["name"]["value"]
        company["timestamp"] = raw_company["properties"]["name"]["timestamp"]
        return company

    has_more = True
    params = {"properties":"name", "limit":COMPANY_RESPONSE_LIMIT}
    while has_more:
        data = get_data("companies", params, {}, configuration)
        # Checking if more data is available, setting the required offset for the next request
        if data["has-more"]:
            params["offset"] = data["offset"]
        else:
            has_more = False
        # https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
        for company in data["companies"]:
            yield op.upsert("companies", process_record(company))

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    curr_time = time.time()

    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
    cursor = state['last_updated_at'] if 'last_updated_at' in state else '0001-01-01T00:00:00Z'
    log.info(f"Starting update process. Initial state: {cursor}")

    # Yeilds all the required data from individual methods, and pushes them into the connector upsert function
    yield from sync_contacts(configuration, cursor, state)
    yield from sync_companies(configuration, cursor, state)

    # Save the final checkpoint by updating the state with the current time
    state['last_updated_at'] = curr_time
    yield op.checkpoint(state)

    log.info(f"Completed the update process. Total duration of sync(in s): " + str(time.time()-curr_time))


def get_data(method, params, headers, configuration, body=None):
    global ACCESS_TOKEN
    global REFRESH_TIME
    # This checks the refresh time set while fetching the last access token
    # if REFRESH TIME is less than the current time, it means the ACCESS TOKEN is expired and need a refresh
    if REFRESH_TIME<time.time():
        get_access_token(configuration)

    headers["authorization"] = "Bearer " + ACCESS_TOKEN
    if method=="contacts":
        response = requests.get(CONTACTS_URL, params=params, headers=headers)
    elif method=="companies":
        response = requests.get(COMPANY_URL, params=params, headers=headers)
    else:
        log.severe(f"Failed to fetch data. Method: " + method)
        raise Exception("Unknown method")

    if response.ok:
        log.fine("Fetched data for method: " + method)
        data = response.json()
        return data
    else:
        log.severe(f"Failed to obtain access token: {response.text}")
        raise Exception("Failed to obtain access token")

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "contacts",
            "primary_key": ["vid"],
            "columns": {
                "vid": "LONG",
                "lastmodifieddate": "STRING",
                "firstname": "STRING",
                "company": "STRING",
                "email": "STRING"
            }
        },
        {
            "table": "companies",
            "primary_key": ["companyId"],
            "columns": {
                "companyId": "LONG",
                "name": "STRING",
                "timestamp": "LONG"
            }
        }
    ]

# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
