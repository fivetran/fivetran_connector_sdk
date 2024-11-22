# This is a simple example for how to work with the fivetran_connector_sdk module.
# Note: This code is currently configured to hit the getSheet endpoint and get data from 1 pre-defined sheet in Smartsheets that does not need pagination
# You will need to provide your own smartsheet credentials for this to work --> api_key variable in configuration.json together with the smartsheet sheet id
# getSheet endpoint: https://smartsheet.redoc.ly/tag/sheets#operation/getSheet
# Add Additional code in the update function to handle multiple sheets and/or pagination through sheets
# Can also add code to extract from other endpoints as needed
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log
import requests
import json
from datetime import datetime, timezone

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "smartsheet_table_name",
            "primary_key": ['id'],
            "columns": {"id": "STRING"}
        }
    ]

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    # Smartsheets sheets API endpoint
    sheets_url = 'https://api.smartsheet.com/2.0/sheets/' 
    # Define api_key and sheet_id from configuration.json
    # If needing multiple sheets, define the sheet_id variable as a list and loop through API call until yield op.upsert 
    api_token = configuration.get('smartsheet_api_token')
    sheet_id = configuration.get('smartsheet_sheet_id')

    # Define sync_cursor and sync_start time
    # sync_cursor will be date filter for current sync, sync_start will become date filter for the next sync
    # Note: on the first sync of the connection, sync_cursor will be NULL
    # Note: You will need to change the timezone.utc in the sync_start definition if your data uses a different timezone
    sync_cursor = state.get('sync_cursor')
    sync_start = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    # Generate full URL and Retrieve data for given sheet 
    # Note: This example uses the Python requests module - you can alternatively use Python Smartsheet Client
    # If sync_cursor exists (not first sync), filter the API call with it -- If not, call the API with no filter
    if sync_cursor:
        api_url = f"{sheets_url}{sheet_id}?rowsModifiedSince={sync_cursor}"
    else:
        api_url = f"{sheets_url}{sheet_id}"
    response = requests.get(api_url, headers={'Authorization': f'Bearer {api_token}'})
    data=response.json()

    # dictionary for mapping column ID : Column Name 
    column_mapping = {col['id']: col['title'] for col in data.get('columns')}
    # Loop through all rows in response to get cell values
    for row in data.get('rows'):
        row_data = {}
        cells = row.get('cells')
        # keep the following 5 lines if you want non-cell metadata about each row in the response -- comment out or remove them if not
        row_data['id'] = row.get('id')
        row_data['row_number'] = row.get('rowNumber')
        row_data['expanded'] = row.get('expanded')
        row_data['created_at'] = row.get('createdAt')
        row_data['modified_at'] = row.get('modifiedAt')
        # Loop through each cell of a row and map column_name to get cell/column values
        for cell in cells:
            column_name = column_mapping.get(cell.get('columnId'))
            row_data[column_name] = cell.get('value')
        # Upsert row to given table
        yield op.upsert("smartsheet_table_name", row_data)

    # Set cursor/filter time for next sync to be the start time of this current sync
    yield op.checkpoint(state = {"sync_cursor": sync_start})

# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(schema=schema, update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open('configuration.json', 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration)
