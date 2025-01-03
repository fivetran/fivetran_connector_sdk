# This is a simple example of how to work with key-based pagination for a REST API.
# It defines a simple `update` method, which upserts retrieved data to a table named "item".
# THIS EXAMPLE IS TO HELP YOU UNDERSTAND CONCEPTS USING DUMMY DATA. IT REQUIRES THE FIVETRAN-API-PLAYGROUND PACKAGE
# (https://pypi.org/project/fivetran-api-playground/) TO RUN.
# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import requests to make HTTP calls to API.
import requests as rq
# Import required classes from fivetran_connector_sdk.
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "user",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "address": "STRING",
                "company": "STRING",
                "job": "STRING",
                "updatedAt": "UTC_DATETIME",
                "createdAt": "UTC_DATETIME",
            },
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector.
# - state: a dictionary that contains whatever state you have chosen to checkpoint during the prior sync.
# The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    log.warning("Example: Common Patterns For Connectors - Pagination - Keyset")

    print("RECOMMENDATION: Please ensure the base url is properly set, you can also use "
          "https://pypi.org/project/fivetran-api-playground/ to start mock API on your local machine.")
    base_url = "http://127.0.0.1:5001/pagination/keyset"

    # Retrieve the cursor from the state to determine the current position in the data sync.
    # If the cursor is not present in the state, start from the beginning of time ('0001-01-01T00:00:00Z').
    cursor = state['last_updated_at'] if 'last_updated_at' in state else '0001-01-01T00:00:00Z'

    params = {
        "updated_since": cursor
    }

    yield from sync_items(base_url, params, state)


# The sync_items function handles the retrieval and processing of paginated API data.
# It performs the following tasks:
# 1. Sends an API request to the specified URL with the provided parameters.
# 2. Processes the items returned in the API response by yielding upsert operations to Fivetran.
# 3. Updates the state with the 'updatedAt' timestamp of the last processed item.
# 4. Saves the state periodically to ensure the sync can resume from the correct point.
# 5. Continues fetching and processing data from the API until all pages are processed.
#
# The function takes three parameters:
# - base_url: The URL to the API endpoint.
# - params: A dictionary of query parameters to be sent with the API request.
# - state: A dictionary representing the current state of the sync, including the last retrieved key.
def sync_items(base_url, params, state):
    more_data = True

    while more_data:
        # Get response from API call.
        response_page = get_api_response(base_url, params)

        # Process the items.
        items = response_page.get("data", [])
        if not items:
            break  # End pagination if there are no records in response.

        # Iterate over each user in the 'items' list and yield an upsert operation.
        # The 'upsert' operation inserts the data into the destination.
        # Update the state with the 'updatedAt' timestamp of the current item.
        summary_first_item = {'id': items[0]['id'], 'name': items[0]['name']}
        log.info(f"processing page of items. First item starts: {summary_first_item}, Total items: {len(items)}")
        for user in items:
            yield op.upsert(table="user", data=user)
            state["last_updated_at"] = user["updatedAt"] # Assuming the API returns the data in ascending order

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state)

        more_data, params = should_continue_pagination(response_page)


# The should_continue_pagination function determines whether pagination should continue based on the presence of a
# 'scroll_param' in the API response.
# It performs the following tasks:
# 1. Retrieves the 'scroll_param' value from the API response, which is used to fetch the next page of data.
# 2. If a 'next_key' is found, updates the parameters with this key to be used in the next API request.
# 3. If no 'next_key' is present, sets the flag to end the pagination process.
#
# Parameters:
# - response_page: A dictionary representing the parsed JSON response from the API.
#
# Returns:
# - has_more_pages: A boolean indicating whether there are more pages to retrieve.
# - params: The updated query parameters for the next API request, including the 'scroll_param' if present.
#
# API response will look like the following structure:
# {
#   "data": [
#     {"id": "c8fda876-6869-4aae-b989-b514a8e45dc6", "name": "Mark Taylor", ... },
#     {"id": "3910cbb0-27d4-47f5-9003-a401338eff6e", "name": "Alan Taylor", ... }
#   ],
#   "scroll_param": "n3h3nbj23n-213f-wef2-344s-9n3idn34if"
# }
#
# For real API example you can refer Intercom's Companies Scroll API:
# https://developers.intercom.com/docs/references/rest-api/api.intercom.io/companies/scrolloverallcompanies
def should_continue_pagination(response_page):
    params = {}
    has_more_pages = True

    next_key = response_page.get("scroll_param")
    if next_key:
        params = {"scroll_param": next_key}
    else:
        has_more_pages = False  # End pagination if no next_key is provided.
    return has_more_pages, params


# The get_api_response function sends an HTTP GET request to the provided URL with the specified parameters.
# It performs the following tasks:
# 1. Logs the URL and query parameters used for the API call for debugging and tracking purposes.
# 2. Makes the API request using the 'requests' library, passing the URL and parameters.
# 3. Parses the JSON response from the API and returns it as a dictionary.
#
# The function takes two parameters:
# - base_url: The URL to which the API request is made.
# - params: A dictionary of query parameters to be included in the API request.
#
# Returns:
# - response_page: A dictionary containing the parsed JSON response from the API.
def get_api_response(base_url, params):
    log.info(f"Making API call to url: {base_url} with params: {params}")
    response = rq.get(base_url, params=params)
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()
    return response_page


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
# be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production. Please test using the
# Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from
    # your IDE.
    connector.debug()

# Resulting table:
# ┌───────────────────────────────────────┬───────────────┬────────────────────────┬──────────────────────────┬───────────────────────────┐
# │                 id                    │      name     │         job            │        updatedAt         │        createdAt          │
# │               string                  │     string    │       string           │      timestamp with UTC  │      timestamp with UTC   │
# ├───────────────────────────────────────┼───────────────┼────────────────────────┼──────────────────────────┼───────────────────────────┤
# │ c8fda876-6869-4aae-b989-b514a8e45dc6  │ Mark Taylor   │   Pilot, airline       │ 2024-09-22T19:35:41Z     │ 2024-09-22T18:50:06Z      │
# │ 3910cbb0-27d4-47f5-9003-a401338eff6e  │ Alan Taylor   │   Dispensing optician  │ 2024-09-22T20:28:11Z     │ 2024-09-22T19:58:38Z      │
# ├───────────────────────────────────────┴───────────────┴────────────────────────┴──────────────────────────┴───────────────────────────┤
# │  2 rows                                                                                                                     5 columns │
# └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
