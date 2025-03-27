## Syncing data from OData version 4

The OData version 4 API is the latest version of the OData protocol. It provides a more flexible and powerful way to interact with OData services. This document provides a reference guide for using the ODataClient class in your Fivetran connector:

### Initialization
```python
from ODataClient import ODataClient
import requests
session = requests.Session()

odata_client = ODataClient(
    service_url="https://services.odata.org/V2/Northwind/Northwind.svc/", 
    session=session,
    state=state  # Pass the state from update function parameter
)
```
Parameters:
- `service_url`: URL endpoint of the OData service
- `session`: requests.Session object for maintaining connection state
- `state`: Dictionary containing checkpoint state (optional)
> NOTE : If your OData service requires authentication, you can set the authentication in the session object. Refer to the [requests documentation](https://requests.readthedocs.io/en/latest/user/advanced) for more information.

### Fetching data

```python
# Define entity and query configuration
entity = {
    "entity_set": "Products",
    "query_options": {
        'select': ['ProductID', 'ProductName', 'UnitPrice'],
        'filter': "UnitPrice gt 20.0"
    },
    "table": "Products"
}

# Execute the query and upsert data
state = yield from odata_client.upsert_entity(entity=entity)
```

Query Options:

The query_options dictionary supports these parameters:
- `select`: List of fields to retrieve
- `filter`: OData filter expression as string
- `orderby`: Field(s) to order results by
- `top`: Maximum number of records to retrieve
- `skip`: Number of records to skip from beginning
- `expand`: Dictionary defining related entities to expand

### Advanced Usage

#### Expanded Relationships

Fetch related entities along with the main entity:

```python
query_options = {
    'select': ['OrderID', 'CustomerID', 'OrderDate'],
    'expand': {
        'Order_Details': {
            'select': ['ProductID', 'UnitPrice', 'Quantity']
        }
    }
}

entity = {
    "entity_set": "Orders",
    "query_options": query_options,
    "table": "Orders"
}

yield from odata_client.upsert_entity(entity=entity)
```

#### incremental sync

For efficient syncs that only fetch new or changed data:

```python
# Get last synced value from state
last_order_date = state.get("lastOrderDate", '1990-01-01T00:00:00')

# Define query with filter using the state value
entity = {
    "entity_set": "Orders",
    "query_options": {
        'select': ['OrderID', 'CustomerID', 'OrderDate'],
        'filter': f"OrderDate gt datetime'{last_order_date}'"
    },
    "table": "Orders_Inc",
    "update_state": {
        "lastOrderDate": "OrderDate"  # Maps state variable to record field
    }
}

# Execute incremental sync
state = yield from odata_client.upsert_entity(entity=entity)
```

#### Multiple Entity Operations
Sync data from multiple entity sets:
```python
entity_list = [
    {
        "entity_set": "Customers",
        "query_options": {"select": ["CustomerID", "CompanyName", "Country"]},
        "table": "Customers_Multiple"
    },
    {
        "entity_set": "Products",
        "query_options": {"select": ["ProductID", "ProductName", "UnitPrice"]},
        "table": "Products_Multiple"
    }
]

state = yield from odata_client.upsert_multiple_entity(entity_list=entity_list, state=state)
```

#### Batch Operations

For more efficient data retrieval, use batch operations to combine multiple requests:

```python
# Add requests to batch (supports method chaining)
odata_client.add_batch(
    entity={
        "entity_set": "Customers",
        "query_options": {'select': ['CustomerID', 'CompanyName']},
        "table": "Customers_batch"
    }
).add_batch(
    entity={
        "entity_set": "Orders",
        "query_options": {'select': ['OrderID', 'OrderDate']},
        "table": "Orders_batch"
    }
)

# Execute batch and process results
state = yield from odata_client.upsert_batch(state=state)
```

### State Management
The state management to sync the data from the last stored checkpoint requires:

- Storing the last synced value in state
- Using that value in a filter expression
- Defining which field to track with `update_state`

The client handles state management automatically when you provide the update_state parameter:
```python
update_state = {
    "lastOrderDate": "OrderDate"  # Maps state variable to record field
}
```

This configuration:  
- Updates the lastOrderDate state variable with the value from the OrderDate field
- Uses the highest value found during the current sync
- Automatically checkpoints the state after processing

```python
# Get last synced date from state
last_order_date = state.get("lastOrderDate", '1990-01-01T00:00:00')

entity = {
    "entity_set": "Orders",
    "query_options": {
        'select': ['OrderID', 'CustomerID', 'OrderDate'],
        'filter': f"OrderDate gt datetime'{last_order_date}'"
    },
    "table": "Orders_Inc",
    "update_state": update_state # Maps state variable to record field
    }
}

# Execute incremental sync
state = yield from odata_client.upsert_entity(entity=entity)
```
If no `update_state` is provided, a full sync will be performed without tracking progress.

> NOTE: The client automatically filters out OData metadata fields (containing `@odata`) from results. You can customize this behavior by modifying the `clean_odata_fields` method in the `ODataClient` class
>
>State is automatically updated and checkpointed during sync operations.