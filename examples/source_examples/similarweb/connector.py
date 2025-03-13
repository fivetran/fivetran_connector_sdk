# This example details how to pull data from Similarweb, which is a digital intelligence platform
# that provides data,insights and analytics about websites and apps
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
# Refer to SimilarWeb's API for more information (https://developers.similarweb.com/docs/similarweb-web-traffic-api)

# Import requests to make HTTP calls to API

import requests
import pytz
import json  
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Import required classes from fivetran_connector_sdk

from fivetran_connector_sdk import Connector  # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code

#Define the domains,countries and metrics you need

domain_list = [ "chat.mistral.ai", 
               "chat.deepseek.com",
               "grok.com", "meta.ai",
               "chatgpt.com",
               "gemini.google.com",
               "character.ai",
               "claude.ai",
               "copilot.microsoft.com",
               "perplexity.ai",
               "poe.com"]
country_list = ["WW", "US"] # Can specify specific countries of interest. WW is 'Worldwide'
metric_list = ["all_traffic_visits"], # Necessary metric can be changed per requirements 

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.

def schema(configuration: dict):
    return [
        {
            "table": "all_traffic_visits",  # Name of the table in the destination.
            "primary_key": ["domain", "country", "date"],  # Primary key columns for the table.
        }
    ]


# The request_report function is a helper function called within the required update function
# The functon takes two parameters:
# - start_date: beginning date for the report
# - end_date: end date for the report

def request_report(start_date, end_date, api_key):

    current_date = datetime.today().strftime("%Y-%m-%d")

    request_url = "https://api.similarweb.com/batch/v4/request-report"
    headers = {
        "api-key": api_key,
        "Content-Type": "application/json"
    }

    # Payload can be altered per the required requests

    payload = {
        "delivery_information": {
            "response_format": "csv",
            "delivery_method_params": {
                "retention_days": 60
            }
        },
        "report_query": {
            "tables": [
                {
                    "vtable": "traffic_and_engagement",
                    "granularity": "daily",
                    "filters": {
                        "domains": domain_list,
                        "countries": country_list,
                        "include_subdomains": True
                    },
                    "metrics": metric_list, # Necessary metric can be changed per requirements 
                    "start_date": start_date,
                    "end_date": end_date
                }
            ]
        },
        "report_name": f"Fivetran_Sync_{current_date}" # Name of the report generated directly within SimilarWeb
    }

    response = requests.post(request_url, headers=headers, json=payload)
    report_data = response.json()
    report_id = report_data.get("report_id") # After requesting a report, gather the report ID 


    if not report_id:
        log.info("Failed to create report:", report_data)
        raise RuntimeError(report_id)

    log.info(f"Report requested. Report ID: {report_id}")
    return report_id


# Check_report_status function is a helper function called within the required update function
# The functon takes two parameters:
# - report_id: ID for the generated report
# - api_key: Key for the API calls 
# This returns the current status -- once the report is ready it will return the report URL

def check_report_status(report_id, api_key):
    status_url = f"https://api.similarweb.com/v3/batch/request-status/{report_id}"

    count_status_checks = 0
    maximum_status_checks = 200

    # check the status 200 times, or for 20 minutes
    while count_status_checks < maximum_status_checks:

        count_status_checks += 1 
        status_response = requests.get(status_url, headers={"api-key": api_key})
        status_data = status_response.json()
        log.info(status_data)

        status = status_data.get("status")
        if status == "completed":
            download_url = status_data.get("download_url")
            log.info(f"Report is ready! Download it here: {download_url}")
            return download_url
        elif status == "bad_request":
            log.severe(f"Error -- {status_data}")
            raise RuntimeError(status_data)
        elif status == "failed":
            log.severe("Report generation failed.")
            raise RuntimeError(status_data)

        log.info("Report is still processing... Waiting 6 seconds.")

        time.sleep(6)
    # if the report isn't generated in 20 minutes, return an error
    raise RuntimeError(status_data)    

# The download_report is a helper function called within the required update function
# The functon takes one parameters:
# - download_url: the url used to download the report -- this is typically passed along from the check_report_status function
# Returns the report information as a JSON object (list of dictionaries).

def download_report(download_url):
    if not download_url:
        log.info("Invalid download URL. Exiting.")
        return None

    report_response = requests.get(download_url)

    try:
        # Decode CSV content into a list of dictionaries 
        csv_content = report_response.text
        lines = csv_content.splitlines()
        headers = lines[0].split(",")  # Extract headers from the first row
        data = [dict(zip(headers, line.split(","))) for line in lines[1:]]

        log.info("Report downloaded successfully.")
        return data  # Returning as a Python list of dictionaries

    except Exception as e:
        log.info("Failed to process the report:", e)
        raise RuntimeError(e)

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync

def update(configuration: dict, state: dict):

    api_key = configuration["api_key"]

    # Historical sync
    if 'look_back' not in state:
        look_back = (datetime.now() - relativedelta(days=5)).strftime('%Y-%m-%d') # for historical sync, do a 2 month lookback
    else:  
        look_back = (datetime.now() - relativedelta(days=5)).strftime("%Y-%m-%d")   # for incremental syncs, one week lookback from 11 days ago to 4 days ago

    n = 4 # Set variable for end date on report data range
    n_days_ago = (datetime.now() - relativedelta(days=n)).strftime("%Y-%m-%d") # SimilarWeb reports are delayed by 3-4 days, so you can't get data up to current day
    report_id = request_report(look_back, n_days_ago, api_key) # will create a report and return the ID associated with it

    if report_id:
        download_url = check_report_status(report_id, api_key) # takes report_id and returns the download URL. This will take about ~1 min for incremental syncs, ~5 minutes for historical
        if download_url:
            records = download_report(download_url)       
            for record in records:
                # write data into table
                yield op.upsert(table="all_traffic_visits",
                                            data=record)
        else:
            log.warning("No download URL")
    else:
        log.warning("No report found")
    
    #set state so Fivetran will know to do an incremental sync next time            
    yield op.checkpoint(state={
        "look_back": look_back
    })
                

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
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)