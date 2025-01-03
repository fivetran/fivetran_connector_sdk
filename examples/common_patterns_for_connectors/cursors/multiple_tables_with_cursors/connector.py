# This is an example for how to work with parent-child relationship between tables from incremental API endpoints.
# It defines a simple `update` method, which upserts retrieved data to respective company and department tables.
# THIS EXAMPLE IS THE JUST FOR UNDERSTANDING AND SIMULATES API CALL WITH STATIC HARD CODED DATA.
# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import requests to make HTTP calls to API
import requests as rq
# Import required classes from fivetran_connector_sdk
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
            "table": "company",
            "primary_key": ["company_id"],
            "columns": {
                "company_id": "STRING",
                "company_name": "STRING",
                "updated_at": "UTC_DATETIME"
            }
        },
        {
            "table": "department",
            "primary_key": ["department_id"],
            "columns": {
                "department_id": "STRING",
                "company_id": "STRING",
                "department_name": "STRING",
                "updated_at": "UTC_DATETIME"
            }
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
    log.warning("Example: Common Patterns For Connectors - Cursors - Multiple Tables With Cursors")

    company_cursor = state["company_cursor"] if "company_cursor" in state else '0001-01-01T00:00:00Z'
    department_cursor = state["department_cursor"] if "department_cursor" in state else {}

    # Fetch and process the companies
    for company in fetch_companies(company_cursor):
        yield op.upsert(table="company", data=company)
        company_id = company["company_id"]

        # Update Cursor for Companies
        state["company_cursor"] = company["updated_at"]

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state)

        # Fetch departments for a company
        department_data = fetch_departments_for_company(department_cursor, company_id)
        for department in department_data:
            yield op.upsert(table="department", data=department)

            # Update Cursor for a Department in department_cursor
            department_cursor[company_id] = department["updated_at"]
            state["department_cursor"] = department_cursor

            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            yield op.checkpoint(state)

    # State at the end of sync for simulated records
    # {"company_cursor": "2024-08-14T02:01:00Z",
    # "department_cursor": {"1": "2024-08-14T03:00:00Z", "2": "2024-08-14T03:00:00Z"}}


# The fetch_companies function makes API call to fetch recently updated companies
#
# The function takes one parameter:
# - company_cursor: The cursor for companies which is a UTC_DATETIME
#
# Returns:
# - data: A dictionary containing the companies received from the API.
def fetch_companies(company_cursor):
    # Actual data fetch from API
    # params = {
    #     "order_by": "updatedAt",
    #     "order": "asc",
    #     "since": company_cursor
    # }
    # response_page = get_api_response("https://example.com/api/companies", params)
    # data = response_page.get("companies", [])

    # Simulate data via API call to url:
    # https://example.com/api/companies
    # with params: {'order_by': 'updatedAt', 'order': 'asc', 'since': '0001-01-01T00:00:00Z'}
    data = [
        {"company_id": "1", "company_name": "ABC Inc", "updated_at": "2024-08-14T01:00:00Z"},
        {"company_id": "2", "company_name": "XYZ Inc", "updated_at": "2024-08-14T02:01:00Z"}
    ]
    return data


# The fetch_departments_for_company function makes API call to fetch recently updated departments for a specified
# company id and cursor
#
# The function takes two parameters:
# - department_cursor: The department_cursor dict which looks like
#   {"company_id 1": "UTC_DATETIME", "company_id 2": "UTC_DATETIME", ... }
# - company_id: The id of the company for which departments are to be fetched
#
# Returns:
# - data: A dictionary containing the departments received from the API.
def fetch_departments_for_company(department_cursor, company_id):
    # Fetch each company_id's cursor in department_cursor dict, if key is not present use default start value
    cursor = department_cursor[company_id] if company_id in department_cursor else '0001-01-01T00:00:00Z'

    # Actual data fetch from API
    # params = {
    #     "order_by": "updatedAt",
    #     "order": "asc",
    #     "since": cursor
    # }
    # response_page = get_api_response(f"https://example.com/api/{company_id}/departments", params)
    # data = response_page.get("departments", [])

    # Simulate data via API call to url:
    # https://example.com/api/1/departments
    # with params: {'order_by': 'updatedAt', 'order': 'asc', 'since': '0001-01-01T00:00:00Z'}
    if company_id == "1":
        data = [
            {"department_id": "1", "company_id": "1", "department_name": "Engineering",
             "updated_at": "2024-08-14T01:00:00Z"},
            {"department_id": "2", "company_id": "1", "department_name": "R&D",
             "updated_at": "2024-08-14T02:00:00Z"},
            {"department_id": "3", "company_id": "1", "department_name": "Marketing",
             "updated_at": "2024-08-14T03:00:00Z"},
        ]

    # Simulate data via API call to url:
    # https://example.com/api/2/departments
    # with params: {'order_by': 'updatedAt', 'order': 'asc', 'since': '0001-01-01T00:00:00Z'}
    elif company_id == "2":
        data = [
            {"department_id": "1", "company_id": "2", "department_name": "Sales",
             "updated_at": "2024-08-14T01:00:00Z"},
            {"department_id": "2", "company_id": "2", "department_name": "Support",
             "updated_at": "2024-08-14T02:00:00Z"},
            {"department_id": "3", "company_id": "2", "department_name": "Design",
             "updated_at": "2024-08-14T03:00:00Z"},
        ]
    else:
        data = []

    return data


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
def get_api_response(endpoint_path, params):
    log.info(f"Making API call to url: {endpoint_path} with params: {params}")
    response = rq.get(endpoint_path, params=params)
    response.raise_for_status()  # Ensure we raise an exception for HTTP errors.
    response_page = response.json()
    return response_page


# Create the connector object using the schema and update functions.
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Test your connector by running your file directly from your IDE.
    connector.debug()

# Resulting tables:
# Table "company":
# ┌─────────────┬──────────────┬─────────────────────────┐
# │ company_id  │ company_name │      updated_at         │
# ├─────────────┼──────────────┼─────────────────────────┤
# │      1      │   ABC Inc    │  2024-08-14T01:00:00Z   │
# │      2      │   XYZ Inc    │  2024-08-14T02:01:00Z   │
# └─────────────┴──────────────┴─────────────────────────┘
#
# Table "department":
# ┌───────────────┬────────────┬─────────────────┬─────────────────────────┐
# │ department_id │ company_id │ department_name │      updated_at         │
# ├───────────────┼────────────┼─────────────────┼─────────────────────────┤
# │      1        │      2     │      Sales      │    2024-08-14T01:00:00Z │
# │      2        │      2     │     Support     │    2024-08-14T02:00:00Z │
# │      3        │      2     │      Design     │    2024-08-14T03:00:00Z │
# └───────────────┴────────────┴─────────────────┴─────────────────────────┘
