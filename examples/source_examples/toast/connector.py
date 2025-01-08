
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import requests to make HTTP calls to API
import requests as rq
import traceback
import datetime
import json
import copy

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):

    return [
        {"table": "restaurant","primary_key": ["restaurantGuid"]},
        {"table": "orders", "primary_key":["guid"]},
        {"table": "time_entry", "primary_key": ["guid"]},
        {"table": "job", "primary_key": ["guid"]},
        {"table": "employee", "primary_key": ["guid"]},
        {"table": "menu_item", "primary_key": ["guid"]},
        {"table": "menu", "primary_key": ["guid"]}
    ]

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):

    try:
        conf = configuration
        domain = conf["domain"]
        base_url = f"https://{domain}"
        to_ts = datetime.datetime.now(datetime.timezone.utc).isoformat("T", "milliseconds")
        if 'to_ts' in state:
            from_ts = state['to_ts']
        else:
            from_ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=10)
            from_ts = from_ts.isoformat("T", "milliseconds")

        payload = {"clientId": conf["clientId"],
            "clientSecret": conf["clientSecret"],
            "userAccessType": conf["userAccessType"]}

        auth_response = rq.post(base_url + "/authentication/v1/authentication/login", json=payload)
        auth_page = auth_response.json()
        auth_token = auth_page["token"]["accessToken"]

        headers = {"Authorization": "Bearer " + auth_token, "accept": "application/json"}

        # Update the state with the new cursor position, incremented by 1.
        new_state = {"to_ts": to_ts}
        log.fine(f"state updated, new state: {repr(new_state)}")

        # Yield a checkpoint operation to save the new state.
        yield op.checkpoint(state=new_state)
        yield from sync_items(base_url, headers, from_ts, to_ts, state)

    except Exception as e:
        # Return error response
        exception_message = str(e)
        stack_trace = traceback.format_exc()
        detailed_message = f"Error Message: {exception_message}\nStack Trace:\n{stack_trace}"
        raise RuntimeError(detailed_message)


# The function takes five parameters:
# - base_url: The URL to the API endpoint.
# - headers: Authentication headers
# - ts_from: starting timestamp
# - ts_to: ending timestamp
# - state: State dictionary
def sync_items(base_url, headers, ts_from, ts_to, state):

    timerange_params = {"startDate": ts_from, "endDate": ts_to}
    config_params = {"lastModified": ts_from}

    # Get response from API call.
    response_page, next_token = get_api_response(base_url+"/partners/v1/restaurants", headers, {})

    # Iterate over each restaurant in the response and yield.
    restaurant_count = len(response_page)
    for index, r in enumerate(response_page):
        guid = r["restaurantGuid"]
        log.info(f"***** starting restaurant {guid}, {index + 1} of {restaurant_count} ***** ")
        yield op.upsert(table="restaurant", data=r)

        # config_params
        yield from process_endpoint(base_url, headers, config_params, "/config/v2/menus", "menu", guid)
        yield from process_endpoint(base_url, headers, config_params, "/config/v2/menuItems", "menu_item", guid)

        # with timerange_params
        yield from process_endpoint(base_url, headers, timerange_params, "/orders/v2/ordersBulk", "orders", guid)
        yield from process_endpoint(base_url, headers, timerange_params, "/labor/v1/timeEntries", "time_entry", guid)

        # no timerange_params
        yield from process_endpoint(base_url, headers, {}, "/labor/v1/jobs", "job", guid)
        yield from process_endpoint(base_url, headers, {}, "/labor/v1/employees", "employee", guid)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of interruptions.
    yield op.checkpoint(state)

# The function takes six parameters:
# - base_url: The URL to the API endpoint.
# - headers: Authentication headers
# - params: Dictionary of parameters required for endpoint
# - endpoint: endpoint to add to base_url for API call
# - table: table to be written to in destination
# - rst_guid: guid for restaurant
def process_endpoint(base_url, headers, params, endpoint, tbl, rst_guid):
    # Update headers for current restaurant
    headers["Toast-Restaurant-External-ID"] = rst_guid
    more_data = True
    # copy parameters because they may change in this function
    params_copy = copy.deepcopy(params)

    while more_data:
        next_token = None
        response_page, next_token = get_api_response(base_url + endpoint, headers, params_copy)
        for o in response_page:
            o = stringify_lists(o)
            yield op.upsert(table=tbl, data=o)
        if next_token:
            params_copy["pageToken"] = next_token
        else:
            more_data = False

# The get_api_response function sends an HTTP GET request to the provided URL with the specified parameters.
# It performs the following tasks:
# 1. Logs the URL and query parameters used for the API call for debugging and tracking purposes.
# 2. Makes the API request using the 'requests' library, passing the URL and parameters.
# 3. Parses the JSON response from the API and returns it as a dictionary.
#
# The function takes two parameters:
# - base_url: The URL to which the API request is made.
# - headers: A dictionary of headers to be included in the API request.
# - params: A dictionary of query parameters to be included in the API request.
#
# Returns:
# - response_page: A dictionary containing the parsed JSON response from the API.
def get_api_response(endpoint_path, headers, params):
    # copy parameters since they may change
    params_copy = copy.deepcopy(params)
    # get response
    response = rq.get(endpoint_path, headers=headers, params=params_copy)

    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()

    # set next_page_token if one is returned
    response_headers = response.headers
    next_page_token = response_headers["Toast-Next-Page-Token"] if "Toast-Next-Page-Token" in response_headers else None
    return response_page, next_page_token

# The stringify_lists function changes lists to strings
#
# The function takes one parameter:
# - d: a dictionary
#
# Returns:
# - new_dict: A dictionary without any values that are lists
def stringify_lists(d):
    new_dict = {}
    for key, value in d.items():
        if isinstance(value, list):
            new_dict[key] = str(value)
        else:
            new_dict[key] = value

    return new_dict


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition. 
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "main":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

