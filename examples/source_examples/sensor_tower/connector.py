# This example details how to pull data from SensorTower, which is a market intelligence and analytics platform 
# that provides insights into mobile apps, app store trends, and digital advertising
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# This SDK is pulling from the Sensor Tower Connector API for three specific tables sales_report_estimates, active_users and retention
# You can change the app IDs to those of your choosing to gather the necessary information


# Import requests to make HTTP calls to API

import requests
import pytz
import json  # Import the json module to handle JSON data.

# Import required classes from fivetran_connector_sdk

from datetime import datetime
from dateutil.relativedelta import relativedelta
from fivetran_connector_sdk import Connector  # Import the Connector class from the fivetran_connector_sdk module.
from fivetran_connector_sdk import Operations as op  # Import the Operations class from the fivetran_connector_sdk module, aliased as op.


# Mapping used later for the sales_report_estimates table to provide more helpful column names

key_mapping = {
  "sales_report_estimates_key": {
    "ios": {
      "aid": "App ID",
      "cc": "Country Code",
      "d": "Date",
      "iu": "iPhone Downloads",
      "ir": "iPhone Revenue",
      "au": "iPad Downloads",
      "ar": "iPad Revenue"
    },
    "android": {
      "aid": "App ID",
      "c": "Country Code",
      "d": "Date",
      "u": "Android Downloads",
      "r": "Android Revenue"
        }
    }
}

# App IDs that have unique values in Sensor Tower application - can be changed for specific applications of interest

ios_app_ids = "6448311069, 6737597349"
android_app_ids = "com.openai.chatgpt, com.deepseek.chat"

# Additional parameters for endpoint filters 

os = ["ios", "android"]
endpoints = ["active_users", "sales_report_estimates", "retention"]
time_period = ["day", "week", "month"]
country_codes = ["US", "AU", "FR", "DE", "GB", "IT", "CA", "KR", "JP", "BR", "IN", "ES"]

# The schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.

def schema(configuration: dict):
    return [
        {
            "table": "sales_report_estimates",  # Name of the table in the destination.
            "primary_key": ["App ID", "Date", "Country Code"],  # Primary key columns for the table.
            "columns": {"App ID": "STRING"} # Specify as a string due to data type mismatch issues
        },
        {
            "table": "active_users",  
            "primary_key": ["app_id", "date", "time_period", "country"],  
            "columns": {"app_id": "STRING"}

        },
        {
            "table": "retention",  
            "primary_key": ["app_id", "date", "country"],  
            "columns": {"app_id": "STRING"}

        }
    ]


# The get_data function, which is a helper function that run API calls to get the records from each endpoint
# The function takes 5 parameters
# - params: dictionary defining the standard params that will be used in all API call
# - os: the operating system (ios or Android) for the application
# - endpoint: each table has their own API endpoint we need to call to populate
# - time_period: aggregation period for the data (examples: "day", "week", "month")
# - country_code: a required parameter of the API calls is country_code, which you need to change to gather data for different locations

def get_data(params: dict, os, endpoint, time_period, country_code):
    
    f_params = params.copy()

    if os == "ios":
        f_params["app_ids"] = ios_app_ids
    else:
        f_params["app_ids"] = android_app_ids

    # Each endpoint has their own specific URL and parameters

    if endpoint == "sales_report_estimates": 
        url = "https://api.sensortower.com/v1/" + os + "/" + endpoint
        f_params["date_granularity"] = "daily"

       
    elif endpoint == "active_users":
        url = "https://api.sensortower.com/v1/" + os + "/usage/" + endpoint
        f_params["time_period"] = time_period

    else: 
        url = "https://api.sensortower.com/v1/" + os + "/usage/" + endpoint
        f_params["date_granularity"] = "all_time"
        f_params["country"] = country_code


    response = requests.get(url, params=f_params) 
    records = response.json()

    return records
    

# The update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync

def update(configuration: dict, state: dict):

    # Pull in the auth token from the configuration file in the project

    auth_token = configuration["auth_token"]

    # If no cursor exists, go through a historical sync

    if 'look_back' not in state:
        look_back = (datetime.now() - relativedelta(months=3)).strftime('%Y-%m-%d') # Change timeframe to suit requirements
    else:  
        # For incremental syncs, do a one week lookback

        look_back = (datetime.now() - relativedelta(weeks=1)).strftime('%Y-%m-%d') # Change timeframe to suit requirements

    current_date = (datetime.now()).strftime('%Y-%m-%d')
    
    # Set params that will be stable for all API calls

    params = {
        "start_date": look_back,
        "end_date": current_date,
        "auth_token": auth_token,
    }



    # Nested api calls going through the various parameters and using the get_data helper function with each configuration

    for endpoint in endpoints: 
        for system in os:
            if endpoint == "active_users":
                for period in time_period:
                    records = get_data(params, system, endpoint, period, None)

                    # Write to table

                    for record in records:
                        record["time_period"] = period
                        yield op.upsert(table=endpoint,
                                        data=record)

            elif endpoint == "sales_report_estimates":
                records = get_data(params, system, endpoint, None, None)
                for record in records:

                    if system == "android":
                        mapping = key_mapping["sales_report_estimates_key"]["android"]
                    else:
                        mapping = key_mapping["sales_report_estimates_key"]["ios"]     

                    # Mapping listed in key_mapping, used to get more useful column names

                    record = {mapping.get(k, k): v for k, v in record.items()}
                    yield op.upsert(table=endpoint,
                                            data=record)
            else:

                for country in country_codes:
                    raw_data = get_data(params, system, endpoint, None, country)
                    records = raw_data["app_data"]
                

                    for record in records:
                        # Need to convert from JSON

                        record["corrected_retention"] = json.dumps(record["corrected_retention"])
                        yield op.upsert(table=endpoint,
                                            data=record)


    # Set new cursor
    # Since we are doing the incremental pulls based on the current date, this is mostly used as a boolean here to see if a cursor exists or not

    yield op.checkpoint(state={
        "look_back": look_back
    })

  
            

# This creates the connector object that will use the update function defined in this connector.py file

connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.

    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.

    connector.debug(configuration=configuration)


# Running Fivetran debug on this --
# Mar 25, 2025 05:08:03 PM: INFO Fivetran-Tester-Process: SYNC PROGRESS:

# Operation       | Calls     
# ----------------+------------
# Upserts         | 5786      
# Updates         | 0         
# Deletes         | 0         
# Truncates       | 0         
# SchemaChanges   | 3         
# Checkpoints     | 1      