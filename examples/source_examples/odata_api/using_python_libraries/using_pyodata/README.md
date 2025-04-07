# OData API Connector using pyodata

This connector demonstrates how to fetch data from an OData v2 API and sync it to the destination using the pyodata python library.

### Overview

This connector:
- Connects to the Northwind OData v2 sample service
- Fetches customers and orders data
- Supports filtering and incremental syncs


### Schema Definition

The connector defines three tables:
- `Customers`: All customer data with orders as nested JSON
- `Filtered_Customers`: Customers whose company name starts with 'S'
- `Orders`: Order data with incremental sync support

You can customize the schema to match your OData service and destination tables. For more reference about the schema method, see the [Fivetran Connector SDK documentation](https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema).

### Key Functions

```python
def setup_odata_service():
    # Establishes connection to the OData service
    # Uses request.session() to maintain the connection state, headers and authentication
    # Addition headers needed for the request can be added here
    # The authentication method can be set here
```

```python
def upsert_all_customers(service, table):
    # Fetches all customers from the OData service and upserts them into the destination
    # For your use case, this method can be modified to include additional logic.
```
Similarly, Other methods like `upsert_all_orders` and `upsert_customer_name_starting_with_s` use pyodata library to query the odata api and sync the data to the destination.

### Customizing for Your OData API

To adapt this connector for your own OData service:

1. **Update the service URL**:
   ```python
   service_url = 'https://your-odata-service-url/'
   ```

2. **Add authentication** if required:
   ```python
   session = requests.Session()
   session.auth = ('username', 'password')  # Basic auth
   # OR
   session.headers.update({'Authorization': 'Bearer your_token'})
   ```
   For more details on authentication, refer to the [requests library documentation](https://docs.python-requests.org/en/latest/user/advanced/#session-objects).


3. **Modify the schema function** to match your data model:
   ```python
   def schema(configuration: dict):
       return [
           {
               "table": "YourEntityName",
               "primary_key": ["YourPrimaryKeyField"],
               "columns": {
                   "YourRelationshipField": "FieldDataType",
               },
           },
       ]
   ```

4. **Update the entity set names** in the functions:
   ```python
   entities = service.entity_sets.YourEntityName.get_entities().execute()
   ```
   Replace `YourEntityName` with the actual entity set name from your OData service.


5. **Adjust filters** for your specific requirements:
   ```python
   filtered = service.entity_sets.YourEntityName.get_entities().filter(
       "YourField eq 'YourValue'"
   ).execute()
   ```


### Additional Resources

- [PyOData Documentation](https://pyodata.readthedocs.io/en/latest/)
- [OData Protocol Documentation](https://www.odata.org/documentation/)
- [Fivetran Connector SDK Technical Reference](https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
- [Fivetran Connector SDK Best Practices](https://fivetran.com/docs/connectors/connector-sdk/best-practices)
