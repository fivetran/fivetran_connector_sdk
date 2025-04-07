# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to fetch data from an OData API version 2 and sync it to a destination.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting operations like checkpoint

import requests
from ODataClient import ODataClient


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "Products", # Name of the table in the destination, required.
            "primary_key": ["ProductID"], # Primary key column(s) for the table, optional.
            "columns": { # Definition of columns and their types, optional.
                "ProductID": "INT",
                "CategoryID": "INT",
            }, # For any columns whose names are not provided here, e.g. id, their data types will be inferred
        },
        {
            "table": "Orders",
            "primary_key": ["OrderID"],
            "columns": {
                "Order_Details": "JSON", # For nested entities, use JSON type
            },
        },
        {
            "table": "Orders_Inc",
            "primary_key": ["OrderID"],
        },
        {
            "table": "Customers_Multiple",
            "primary_key": ["CustomerID"],
        },
        {
            "table": "Products_Multiple",
            "primary_key": ["ProductID"],
            "columns": {
                "Category": "JSON",
            },
        },
        {
            "table": "Orders_batch",
            "primary_key": ["OrderID"],
        },
        {
            "table": "Customers_batch",
            "primary_key": ["CustomerID"],
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
    log.warning("Example: Syncing data from OData Version 2")

    # Initialize the session object to be used by the ODataClient
    # This maintains connection state, headers, and authentication across requests
    # Authentication can be done by setting the authentication parameters in the session object
    # For more information on requests session object refer to the documentation
    # https://requests.readthedocs.io/en/latest/user/advanced
    session = requests.Session()

    # Initialize the ODataClient object required to interact with the OData service
    # The ODataClient object requires the base URL of the OData service, session object and state dictionary
    northwind_client = ODataClient(service_url="https://services.odata.org/V2/Northwind/Northwind.svc/",session=session, state=state)


    # EXAMPLE 1 : Fetch basic product information with specific field selection
    log.info("Example 1 : Fetching Products data from northwind odata service")

    # query_options allow you to customize how data is retrieved from an OData service. They are passed as a dictionary to the ODataClient methods.
    # The various keys that can be used in query_options are:
    # - select: List of fields to be fetched. The value should be a list of exact field names to be fetched from the entity.
    # - filter: Filter the data to be fetched. The value should be a string that represents the filter condition. Multiple conditions can be combined using logical operators.
    # - expand: Expand the related entities. Refer to EXAMPLE 2 for more details about expand.
    # - top: Limit the number of records to be fetched.
    # - orderby: Sort the data fetched. The value should be a string that represents the exact field name to sort by.
    query_options = {
        'select': ['ProductID', 'ProductName', 'UnitPrice', 'CategoryID'],
    }

    # entity dictionary contains the entity_set name, query_options and table name
    # Additionally, it can also contain update_state dictionary for incremental sync (Refer EXAMPLE 3)
    # The entity is passed a parameter to the upsert_entity() method to fetch data from the entity set and upsert it to the destination table.
    # If the key "table" is not provided, the destination table name will be the same as the entity_set name.
    entity = {
        "entity_set": "Products",
        "query_options": query_options,
        "table": "Products"
    }

    # The upsert_entity() methods retrieves data from an OData entity set and upserts it to the destination table.
    # The method requires the entity dictionary as a parameter which contains the entity_set name, query_options and table name.
    # It can also take an optional parameter state which is used to maintain the state of the connector. Passing the state parameter sets the state dictionary for the ODataClient instance to the value passed. (Refer EXAMPLE 3)
    # The method returns the updated state dictionary after the upsert operation.
    # The method handles the pagination of the data and fetches all the data from the entity set.
    # To know more about the upsert_entity() method refer to the upsert_entity() method of ODataClient class
    # The method returns the state dictionary after the completion of the upsert operation.
    modified_state = yield from northwind_client.upsert_entity(entity=entity)
    # The modified state dictionary is empty as no modifications were done to the state dictionary in EXAMPLE 1


    # EXAMPLE 2 : Fetching orders data from northwind odata service with expand
    log.info("Example 2 : Fetching Orders data from northwind odata service with expand")

    # expand key in query_options allows you to fetch related entities along with the main entity. It is used to fetch data from the related entities of the main entity.
    # The value of the expand key should be a dictionary where the key is the name of the related entity and the value is the query_options for the related entity.
    # Structure:
    # {
    #     'expand': {
    #         'NavigationProperty1': {  # Name of the relationship to expand
    #             'select': ['Field1', 'Field2'],  # Fields to include from related entity
    #         },
    #         'NavigationProperty2': {}  # Expand without specific options
    #     }
    # }
    query_options = {
        'select': ['OrderID', 'CustomerID', 'OrderDate'],
        'expand': {
            'Order_Details': {
                'select': ['ProductID', 'UnitPrice', 'Quantity']
            }
        },
    }

    # entity dictionary contains the entity_set name, query_options and table name
    entity = {
        "entity_set": "Orders",
        "query_options": query_options,
        "table": "Orders"
    }

    # The modified state from the EXAMPLE 1 is passed as the state parameter
    # This ensures that any changes done to state in EXAMPLE 1 are maintained in EXAMPLE 2
    # The upsert_entity() method will read the state key-value pairs from the modified state passed to the method.
    yield from northwind_client.upsert_entity(entity=entity, state=modified_state)


    # EXAMPLE 3 : Fetching orders data from northwind odata service with Incremental Sync
    log.info("Example 3 : Fetching Orders data from northwind odata service with Incremental Sync")

    # Incremental Sync allows you to fetch only the data that has changed since the last sync.
    # read the last order date fetched in the previous sync from the state dictionary
    last_order_date = state.get("lastOrderDate", '1990-07-08T00:00:00')

    # For incremental syncs, it is necessary to filter the data based on the last fetched data.
    # IMPORTANT : Make sure to add a filter condition to fetch only the data that has changed since the last sync.
    incremental_query = {
        'select': ['OrderID', 'CustomerID', 'OrderDate'],
        "filter": f"OrderDate gt datetime'{last_order_date}'" # This will fetch orders after the last order date fetched in the previous sync
    }

    # update_state is used to map the state variable with the field in the fetched data to update it correctly
    # This dictionary allows the ODataClient to update the exact key in the state dictionary with the last value fetched from the source data.
    # The update dictionary can contain multiple key value pairs mapping the state variable with the exact field name in the fetched data.
    update_state = {
       "lastOrderDate":"OrderDate"
    }

    # The update_state dictionary is passed as the value of "update_state" key in the entity dictionary
    # This ensures that the upsert_entity() method updates the state dictionary with the last fetched data
    # Not passing the update_state dictionary will lead to a complete sync of the data
    entity = {
        "entity_set": "Orders",
        "query_options": incremental_query,
        "table": "Orders_Inc",
        "update_state": update_state
    }

    modified_state = yield from northwind_client.upsert_entity(entity=entity, state=modified_state)
    # The modified_state dictionary contains {'lastOrderDate': '1998-05-06T00:00:00+00:00'}


    # EXAMPLE 4 : Fetching Multiple entities
    log.info("Example 4 : Fetching data from multiple entities")

    # for fetching data from multiple entities, we need to create a list of entity dictionaries.
    # Each entity dictionary should contain the entity_set name, query_options and table name.
    # If it is an incremental sync entity, it can also contain the update_state dictionary with key as "update_state"
    entity_list  = []

    # customer_entity dictionary contains the entity_set name, query_options and table name for the Customers entity
    customer_entity = {
        "entity_set": "Customers",
        "query_options": {
            "select": ["CustomerID", "CompanyName", "ContactName", "Country"],
            "filter": "Country eq 'Germany' or Country eq 'France'",
            "orderby": "CompanyName"
        },
        "table": "Customers_Multiple"
    }
    # Append the customer_entity dictionary to the entity_list
    entity_list.append(customer_entity)

    # product_entity dictionary contains the entity_set name, query_options and table name for the Products entity
    product_entity = {
        "entity_set": "Products",
        "query_options": {
            "select": ["ProductID", "ProductName", "UnitPrice", "CategoryID"],
            "expand": {
                "Category": {
                    "select": ["CategoryName", "Description"]
                }
            },
            "filter": "UnitPrice gt 50"
        },
        "table": "Products_Multiple"
    }
    # Append the product_entity dictionary to the entity_list
    entity_list.append(product_entity)

    # The upsert_multiple_entity() method is used to fetch data from multiple entities.
    # This method queries the OData service for each entity in the entity_list and upserts the data to the destination table.
    # The method requires the entity_list and state dictionary as parameters.
    # The state parameter is passed as {'lastOrderDate': '1998-05-06T00:00:00+00:00'} as it was the modified state returned after the EXAMPLE 3 upsert operation
    modified_state = yield from northwind_client.upsert_multiple_entity(entity_list=entity_list, state=modified_state)


    # EXAMPLE 5 : Batch Operations to fetch multiple data in a single request
    log.info("Example 5 : Batch Operations to fetch multiple data in a single request")

    # entity dictionary contains the entity_set name, query_options and table name
    # Additionally, it can also contain update_state dictionary for incremental sync (Refer EXAMPLE 3)
    entity_1 = {
        "entity_set": "Customers",
        "query_options": {
            'select': ['CustomerID', 'CompanyName', 'ContactName', 'ContactTitle', 'Address', 'PostalCode', 'Country', 'Phone'],
        },
        "table": "Customers_batch"
    }


    # entity dictionary contains the entity_set name, query_options and table name
    entity_2 = {
        "entity_set": "Orders",
        "query_options": {
            'select': ['OrderID', 'CustomerID', 'OrderDate'],
        },
        "table": "Orders_batch",
    }

    # OData version 2 supports batch requests to reduce the number of calls made to the service using the $batch endpoint.
    # The add_batch() method is used to add a batch operations to the ODataClient instance.
    # It takes the entity dictionary as parameter which contains the entity_set, query_options, table and update_state.
    # The add_batch() method supports method chaining
    northwind_client.add_batch(
        entity=entity_1
    ).add_batch(
        entity=entity_2
    )

    # The upsert_batch() method is used to execute the batch operations added to the ODataClient instance.
    # It takes the state dictionary as a parameter and returns the updated state dictionary.
    state = yield from northwind_client.upsert_batch(state=modified_state)

    log.severe(f"Modified_state example 5: {state}")

    yield op.checkpoint(state) # optional checkpoint as the checkpoint is called in the upsert_batch function during the sync


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()

