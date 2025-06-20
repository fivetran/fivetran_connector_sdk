# This is a simple example for how to work with the fivetran_connector_sdk module.
# The code will retrieve data for specific endpoints from Talon.one and create one table per endpoint
# Tables and endpoints defined at the top of the script in table_endpoint_list (can)
# You will need to provide your own Talon.one credentials for this to work --> "base_url" and "api_key" in configuration.json
# You can also define how far back in history to go on initial sync using the "initial_sync_start_date" in configuration.json
# Relevant Talon.one API documentation: https://docs.talon.one/management-api#tag/Customer-data/operation/getApplicationEventsWithoutTotalCount
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
from fivetran_connector_sdk import Connector, Logging as log, Operations as op
import json
import requests
from datetime import datetime, timezone
import time

application_id = <your_application_id>
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

## Add dictionaries to this list to sync new endpoints
# table: The name of the table that will be created downstream in warehouse/lake
# primary_key: PK of the table to Merge and de-duplicate using
# endpoint: API endpoint to append to base_url for calling Talon.one API
table_endpoint_list = [
    {
        "table": "event",
        "primary_key": ["id"],
        "endpoint": f"applications/{application_id}/events/no_total"
    }
]

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {"table": d.get("table"), "primary_key": d.get("primary_key")}
        for d in table_endpoint_list
    ]

# The get_api_response function sends an HTTP GET request to the provided URL with the specified parameters.
# It performs the following tasks:
# 1. Logs the URL and query parameters used for the API call for debugging and tracking purposes.
# 2. Makes the API request using the 'requests' library, passing the URL and parameters.
# 3. Parses the JSON response from the API, sleeps to avoid 3 calls per second rate limit, and returns it as a dictionary.
#
# The function takes three parameters:
# - url: The URL to which the API request is made.
# - params: A dictionary of query parameters to be included in the API request.
# - headers: API headers (api key included)
#
# Returns:
# - response_page: A dictionary containing the parsed JSON response from the API.
def get_api_response(url, params, headers):

    log.info(f"Making API call to url: {url} with params: {params}")
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()
    time.sleep(0.3)
    return response_page

# Function to check if there are more pages of data
# Takes in two parameters
# - response_page: most recent response page from the API
# - params: most recent params sent to the API
# And returns two objects
# - has_more: Boolean to indicate whether there are more pages or not
# - params: new params for next API call if needed
def should_continue_pagination(response_page, params):
    has_more = response_page.get("hasMore")  
    if has_more:
        params['skip'] += params.get("pageSize")
    return has_more, params

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):

    # Extract constants from configuration.json and compose headers
    base_url = configuration.get("base_url")
    api_key = configuration.get("api_key")
    page_size = int(configuration.get("page_size", "100"))
    initial_start_time = configuration.get("initial_sync_start_time", "2020-01-01T00:00:00.00Z")

    headers = {
        "Authorization": f"ManagementKey-v1 {api_key}",
        "Accept": "application/json"
    }

    # Initialize state dictionary
    checkpoint_state = {}

    # Retrieve and sync data
    for table in table_endpoint_list:
        table_name = table.get("table")
        endpoint = table.get("endpoint")
        
        table_cursor = table_name + "_cursor"
        table_cursor_value = state.get(table_cursor, initial_start_time)
        table_start_time = datetime.now(timezone.utc).strftime(TIMESTAMP_FORMAT)

        table_url = f"{base_url}{endpoint}"
        skip = 0

        params = {
                "pageSize": page_size,
                "skip": skip,
                "sort": "created",
                "createdAfter": table_cursor_value
            }
        
        yield from sync_items(table_name, table_url, params, headers)
        # Checkpoint
        checkpoint_state[table_cursor] = table_start_time
        yield op.checkpoint(state=checkpoint_state)


# The sync_items function handles the retrieval and processing of paginated API data.
# It performs the following tasks:
# 1. Sends an API request to the specified URL with the provided parameters.
# 2. Processes the items returned in the API response by yielding upsert operations to Fivetran.
# 3. Continues fetching and processing data from the API until all pages are processed.
#
# The function takes four parameters:
# - table_name: Doubles as the endpoint and the name of the table to write to in the destination
# - api_url: The URL to the API endpoint.
# - params: A dictionary of query parameters to be sent with the API request.
# - headers: API headers including API key
def sync_items(table_name, api_url, params, headers):
    more_data = True

    while more_data:
        # Get response from API call.
        response_page = get_api_response(api_url, params, headers)

        # Process the items.
        items = response_page.get("data", [])
        if not items:
            break  # End pagination if there are no records in response.

        # Iterate over each user in the 'items' list and yield an upsert operation.
        for item in items:

            # Convert any column values that are Lists to json and upsert record
            yield op.upsert(table=table_name, data=list_to_json(item))

        # Determine if we should continue pagination based on the total items and the current offset.
        more_data, params = should_continue_pagination(response_page, params)

# Converts any list values in the input dictionary to JSON strings.
# The function takes one parameter
# - data: Dictionary to process
#
# Returns
# - New dictionary with lists converted to JSON strings
def list_to_json(data: dict):
    return {key: json.dumps(value) if isinstance(value, list) else value for key, value in data.items()}

# Initialize the connector with the defined update and schema functions
connector = Connector(update=update, schema=schema)

# Entry point for running the script
if __name__ == "__main__":
    with open("/configuration.json", 'r') as f:
        configuration = json.load(f)
    connector.debug(configuration=configuration)


## Fivetran debug results
# Jun 12, 2025 02:39:45 PM: INFO Fivetran-Tester-Process: Checkpoint: {"event_cursor": "2025-06-12T21:33:01Z"} 
# Jun 12, 2025 02:39:45 PM: INFO Fivetran-Tester-Process: SYNC PROGRESS:
# Operation       | Calls     
# ----------------+------------
# Upserts         | 10364     
# Updates         | 0         
# Deletes         | 0         
# Truncates       | 0         
# SchemaChanges   | 1         
# Checkpoints     | 1         
 
# Jun 12, 2025 02:39:45 PM: INFO Fivetran-Tester-Process: Sync SUCCEEDED
