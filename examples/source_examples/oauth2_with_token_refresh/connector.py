"""
Hubspot OAuth2.0 Sample API Connector for Fivetran

This module implements a connector for syncing data from the Hubspot API.
NOTE: Hubspot connector is already present in Fivetran, and can be directly created
    from the dashboard. Please do not use this as an alternative
It is an example of using OAuth 2.0 client credentials flow, and
the refresh of Access token from the provided refresh token.

Date: 2025-01-29
"""
import json
import requests
import time
import urllib
from fivetran_connector_sdk import Connector, Logging as log, Operations as op

BASE_URL = "https://api.hubapi.com/"
AUTH_URL = BASE_URL + "oauth/v1/token?"
CONTACTS_URL = BASE_URL + "contacts/v1/lists/all/contacts/all"
COMPANY_URL = BASE_URL + "companies/v2/companies/paged"
ACCESS_TOKEN = ""
REFRESH_TIME = 0

def get_access_token(configuration: dict):
    global AUTH_URL
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

    if response.status_code == 200:
        log.info("Access token obtained successfully")
        data = response.json()
        # This sets the ACCESS TOKEN and the updated REFRESH TIME
        # REFRESH TIME is the epoch time in seconds when the ACCESS TOKEN will expire
        ACCESS_TOKEN = data["access_token"]
        REFRESH_TIME = int(data["expires_in"]) + time.time()
        return
    else:
        log.severe(f"Failed to obtain access token: {response.text}")
        raise Exception("Failed to obtain access token")


def sync_contacts(configuration, cursor, curr_time, state):
    # this is a custom function, meant to process the contacts
    # this processed data is then sent to fivetran
    def process_record(raw_contact):
        contact = {}
        contact["vid"] = raw_contact["vid"]
        contact["firstname"] = raw_contact["properties"]["firstname"]["value"]
        contact["company"] = raw_contact["properties"]["company"]["value"]
        contact["lastmodifieddate"] = raw_contact["properties"]["lastmodifieddate"]["value"]
        contact["email"] = raw_contact["identity-profiles"][0]["identities"][0]["value"]
        return contact

    has_more = True
    params = {}
    while has_more:
        data = get_data("contacts", params, {}, configuration)
        # Checking if more data is available, setting the required offset for the next request
        if data["has-more"]:
            params["vidOffset"] = data["vid-offset"]
        else:
            has_more = False
        # sending contact details to fivetran connector
        for contact in data["contacts"]:
            yield op.upsert("contacts", process_record(contact))


def sync_companies(configuration, cursor, curr_time, state):
    # this is a custom function, meant to process the company record
    def process_record(raw_company):
        company = {}
        company["companyId"] = raw_company["companyId"]
        company["name"] = raw_company["properties"]["name"]["value"]
        company["timestamp"] = raw_company["properties"]["name"]["timestamp"]
        return company

    has_more = True
    params = {"properties":"name"}
    while has_more:
        data = get_data("companies", params, {}, configuration)
        # Checking if more data is available, setting the required offset for the next request
        if data["has-more"]:
            params["offset"] = data["offset"]
        else:
            has_more = False
        # sending company details to fivetran connector
        for company in data["companies"]:
            yield op.upsert("companies", process_record(company))

def update(configuration: dict, state: dict):
    curr_time = time.time()

    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
    cursor = state['last_updated_at'] if 'last_updated_at' in state else '0001-01-01T00:00:00Z'
    log.info(f"Starting update process. Initial state: {cursor}")

    # Yeilds all the required data from individual methods, and pushes them into the connector upsert function
    yield from sync_contacts(configuration, cursor, curr_time, state)
    yield from sync_companies(configuration, cursor, curr_time, state)

    # Save the final checkpoint by updating the state with the current time
    state['last_updated_at'] = curr_time
    yield op.checkpoint(state)

    log.info(f"Completed the update process. Total duration of sync(in s): " + str(time.time()-curr_time))


def get_data(method, params, headers, configuration, body=None):
    global ACCESS_TOKEN
    global REFRESH_TIME
    global CONTACTS_URL
    global COMPANY_URL
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

    if 200 <= response.status_code < 300:
        log.info("Fetched data for method: " + method)
        data = response.json()
        return data
    else:
        log.severe(f"Failed to obtain access token: {response.text}")
        raise Exception("Failed to obtain access token")


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

connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("conf.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
