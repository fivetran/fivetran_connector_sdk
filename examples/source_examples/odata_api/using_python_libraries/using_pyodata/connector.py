# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to fetch data from an OData API version 2 and sync it to a destination using pyodata.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting operations like checkpoint

from datetime import datetime, timezone
import requests
# pyodata is a Python library for consuming OData v2 services.
# See the documentation for more details (https://pyodata.readthedocs.io/en/latest/)
import pyodata

# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "Customers",  # Name of the table in the destination, required.
            "primary_key": ["CustomerID"],  # Primary key column(s) for the table, optional.
            "columns": {  # Definition of columns and their types, optional.
                "Orders" : "JSON",
                "CustomerDemographics" : "JSON",
            },  # For any columns whose names are not provided here, e.g. id, their data types will be inferred
        },
        {
            "table": "Filtered_Customers",
            "primary_key": ["CustomerID"],
            "columns": {
                "Orders": "JSON",
                "CustomerDemographics": "JSON",
            },
        },
        {
            "table": "Orders",
            "primary_key": ["OrderID"],
            "columns": {
            },
        },
    ]


# Helper function to parse ISO date strings
def parse_iso_date(date_string):
    """Parse ISO format date string to datetime object."""
    dateformat = '%Y-%m-%dT%H:%M:%S'
    try:
        if date_string.endswith('Z'):
            date_string = date_string[:-1]  # Remove Z
        return datetime.strptime(date_string, dateformat).replace(tzinfo=timezone.utc)
    except ValueError as e:
        raise RuntimeError(f"Invalid date format: {e}")


# Initializing the OData service client for Northwind OData v2
def setup_odata_service():
    service_url = 'https://services.odata.org/V2/Northwind/Northwind.svc/'
    # Initialize the session object to be used by the pyOdata
    # This maintains connection state, headers, and authentication across requests
    # For more information on requests session object refer to the documentation
    # https://requests.readthedocs.io/en/latest/user/advanced
    session = requests.Session()

    # Initialize the pyodata client
    client = pyodata.Client(service_url, session)
    log.info("OData v2 service connection established")

    return client


# Fetch all customers from the service and yield upsert operations.
# This function fetches all customers from the service and yields upsert operations for each customer.
# The service parameter is the pyodata client object and the table parameter is the destination table name.
def upsert_all_customers(service, table):
    log.info("Fetching all customers")

    # Query all customers using pyodata
    customers = service.entity_sets.Customers.get_entities().execute()

    # Fetching the next page of results
    while True:
        for customer in customers:
            # Convert the entity to a dictionary
            data = customer._cache
            # upsert the data to the destination
            yield op.upsert(table=table, data=data)

        # If there are no more pages, break the loop
        if customers.next_url is None:
            break

        # Fetch the next page of results
        customers = service.entity_sets.Customers.get_entities().next_url(customers.next_url).execute()

        log.info(f"Upserted {len(customers)} customers to the destination")


# Fetch customers whose CompanyName starts with 'S'
# The service parameter is the pyodata client object and the table parameter is the destination table name.
def upsert_customer_name_starting_with_s(service, table):
    log.info("Fetching customers with name starting with 'S'")

    # Query filtered customers using pyodata
    filtered_customers = service.entity_sets.Customers.get_entities().filter(
        "startswith(CompanyName, 'S') eq true"
    ).execute()

    for customer in filtered_customers:
        # Convert the entity to a dictionary
        data = customer._cache
        yield op.upsert(table=table, data=data)

    log.info(f"Upserted {len(filtered_customers)} filtered customers to the destination")


# Query orders after the given date
# The service parameter is the pyodata client object and the initial_date parameter is the date after which orders are to be fetched.
def query_orders(service, initial_date):
    # Format date into iso format for OData v2 filter
    formatted_date = initial_date.strftime('%Y-%m-%dT%H:%M:%S')

    # Query orders with filter and selected properties
    orders = service.entity_sets.Orders.get_entities().select(
            'OrderID,CustomerID,EmployeeID,OrderDate,RequiredDate,ShippedDate'
        ).filter(f"OrderDate gt datetime'{formatted_date}'").execute()

    # Fetching the next page of results adn yielding the fetched data
    while True:
        yield orders
        if orders.next_url is None:
            break

        orders = service.entity_sets.Orders.get_entities().next_url(orders.next_url).execute()


# Process order results and track the latest date
# The results parameter is the fetched order results, the table parameter is the destination table name,
# the track_column parameter is the column to track the latest date, and the initial_max_date parameter is the initial max date.
def process_order_results(results, table, track_column, initial_max_date):
    max_date = initial_max_date
    count = 0

    for entity in results:
        # Convert the entity to a dictionary
        data = entity._cache
        count += 1
        yield op.upsert(table=table, data=data)

        # Track the latest date
        current_date = data.get(track_column)
        if isinstance(current_date, datetime):
            if current_date > max_date:
                max_date = current_date
        elif isinstance(current_date, str):
            parsed_date = parse_iso_date(current_date)
            if parsed_date and parsed_date > max_date:
                max_date = parsed_date

    log.info(f"Upserted {count} records to the destination")
    return max_date


# Fetch orders after the initial state value and upsert them.
# The service parameter is the pyodata client object, the table parameter is the destination table name,
# the initial_state_value parameter is the initial state value, and the track_column parameter is the column to track the latest date.
def upsert_all_orders(service, table, initial_state_value, track_column):
    log.info(f"Fetching all the orders after {initial_state_value}")

    # parse the date into iso format
    initial_date = parse_iso_date(initial_state_value)
    max_date = initial_date
    for results in query_orders(service, initial_date):
        max_date = yield from process_order_results(results, table, track_column, initial_date)

    # Return datetime as string in ISO format for updating the state
    dateformat = '%Y-%m-%dT%H:%M:%S'
    if isinstance(max_date, datetime):
        return max_date.strftime(dateformat)
    return max_date


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: OData Data fetching and syncing using python-odata and pyodata library")

    service_v4 = setup_odata_service()

    # Upserting all the customer data to the destination
    yield from upsert_all_customers(service_v4, table="Customers")
    yield op.checkpoint(state)

    # Upserting selected data from customer to the destination
    yield from upsert_customer_name_starting_with_s(service_v4, table="Filtered_Customers")
    yield op.checkpoint(state)

    # Upserting all the orders data to the destination while maintaining the state
    last_order_date = state.get("latestOrderDate", '1990-01-01T00:00:00')
    final_state_value = yield from upsert_all_orders(service_v4, table="Orders", initial_state_value=last_order_date, track_column="OrderDate")
    # update the state with the latest order date
    state["latestOrderDate"] = final_state_value
    yield op.checkpoint(state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()