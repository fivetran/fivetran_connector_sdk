# This is an example for how to fetch the schema from a database and use it in your connector.
# It defines a `schema` method, which retrieves the schema from a Snowflake database and returns it in a format that Fivetran can use.
# It defines a simple `update` method, which upserts retrieved data to respective orders and products tables.
# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Import to connect to Snowflake
import snowflake.connector
import datetime
import json


# This function connects to the Snowflake database using the provided configuration.
# It checks if all required keys are present in the configuration dictionary.
# If any key is missing, it raises a ValueError with a message indicating which key is missing.
# This method returns a connection object that can be used to interact with the Snowflake database.
def get_snowflake_connection(configuration):
    # Check if all required keys are present in the configuration
    for key in ["user", "password", "account", "database", "schema", "tables"]:
        if key not in configuration:
            raise ValueError(f"Missing required configuration key: {key}")

    # Create a connection to Snowflake using the provided configuration
    conn = snowflake.connector.connect(
        user=configuration["user"],
        password=configuration["password"],
        account=configuration["account"],
        database=configuration["database"],
        schema=configuration["schema"]
    )
    log.info("Connected to Snowflake database")

    # return the connection object
    return conn


# This function fetches primary keys of the table from the Snowflake database using a cursor.
# It executes a SQL command to retrieve the primary keys of the specified table.
# The function takes two parameters:
# - cursor: a cursor object to execute SQL commands
# - table_name: the name of the table for which to fetch primary keys
# The function returns a list of primary keys for the specified table.
def get_table_primary_key(cursor, table_name):
    cursor.execute(f"SHOW PRIMARY KEYS IN TABLE {table_name}")
    primary_keys = cursor.fetchall()
    # Extract the primary key names from the result set and return them in lowercase
    return [pk[4].lower() for pk in primary_keys]


# This function retrieves the column details of the specified tables from the Snowflake database.
# It executes a SQL command to fetch the table name, column name, and data type of each column.
# The function takes three parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - cursor: a cursor object to execute SQL commands
# - table_list: a list of table names for which to fetch column details
# The function returns a dictionary where the keys are table names and the values are dictionaries of column names and their data types.
def get_column_details(configuration, cursor, table_list):
    # Execute a SQL command to fetch the table name, column name, and data type of each column
    cursor.execute(
        f"SELECT table_name, column_name, data_type FROM {configuration['database']}.information_schema.columns WHERE table_schema = '{configuration['schema']}';")
    columns = cursor.fetchall()

    table_columns = {}

    # Iterate through the result set and populate the table_columns dictionary
    for column in columns:
        table_name = column[0].lower()
        column_name = column[1].lower()
        # Extract the data type and get the fivetran supported corresponding type
        data_type = get_fivetran_datatype(column[2]).upper()
        if table_name in table_list:
            # If the table name is in the list of selected tables, add it to the dictionary
            if table_name not in table_columns:
                table_columns[table_name] = {}
            table_columns[table_name].update({column_name: data_type})

    return table_columns


# This function maps Snowflake data types to Fivetran data types.
# You can modify this function to add more mappings as needed.
# see the technical reference documentation for more details on the supported data types:
# https://fivetran.com/docs/connector-sdk/technical-reference#supporteddatatypes
def get_fivetran_datatype(snowflake_type):
    TYPE_MAPPING = {
        'STRING': 'STRING',
        'NUMBER': 'DOUBLE',
        'FLOAT': 'FLOAT',
        'INTEGER': 'INTEGER',
        'BOOLEAN': 'BOOLEAN',
        'VARCHAR': 'STRING',
        'CHAR': 'STRING',
        'TEXT': 'STRING',
        'DATE': 'NAIVE_DATE',
        'DECIMAL': 'DECIMAL'
        # Add more mappings as needed
    }
    # Return the mapped type or default to STRING if not found
    return TYPE_MAPPING.get(snowflake_type, "STRING")


# This function builds the schema by fetching the table and column details from the Snowflake database.
# It uses the get_column_details and get_table_primary_key functions to retrieve the necessary information.
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - cursor: a cursor object to execute SQL commands
# The function returns a list of dictionaries, each representing a table with its name, primary key, and columns as accepted by the schema method.
def build_schema(configuration, cursor):
    # The tables key in configuration should contain a comma-separated string of table names
    # These tables should be present in the Snowflake database
    # These tables will be used to fetch the schema from the Snowflake database
    # Split the tables string into a list of table names
    table_list = configuration["tables"].split(",")
    # Convert the table names to lowercase and strip any leading/trailing whitespace
    table_list = [table.strip().lower() for table in table_list]
    # fetch the column details for the specified tables
    table_columns = get_column_details(configuration, cursor, table_list)
    schema_list = []

    for table_name, columns in table_columns.items():
        # Get the primary key for the table
        primary_key = get_table_primary_key(cursor, table_name)
        # Add the table schema to the schema_list
        schema_list.append({
            "table": table_name,
            "primary_key": primary_key,
            "columns": columns

        })

    return schema_list


# Define the schema function which lets you configure the schema your connector delivers.
# This method extracts the schema from the Snowflake database and returns it in a format that Fivetran can use.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
def schema(configuration: dict):
    # connect to Snowflake using the provided configuration
    connection = get_snowflake_connection(configuration)
    # create a cursor to execute SQL commands
    cursor = connection.cursor()
    # fetch the schema from the Snowflake database in a format that Fivetran can use
    schema_list = build_schema(configuration, cursor)

    # close the cursor and connection
    cursor.close()
    connection.close()

    # return the schema list
    return schema_list


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: Common Patterns For Connectors - Fetching Schema From Database")

    # connect to Snowflake using the provided configuration to fetch the data
    connection = get_snowflake_connection(configuration)
    cursor = connection.cursor()

    # fetch the data from the products table using a SQL command
    query = "SELECT * FROM products"
    cursor.execute(query)
    # Get the column names from the cursor description
    columns = [col[0].lower() for col in cursor.description]

    # The cursor.fetchall() method returns a list of tuples, where each tuple represents a row in the result set
    for row in cursor.fetchall():
        # This is needed to map the column names to the data returned by the query
        # The zip function pairs each column name with its corresponding value in the row tuple
        # The dict function creates a dictionary from these pairs
        upsert_data = dict(zip(columns, row))
        yield op.upsert(table="products", data=upsert_data)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)

    # Fetch the data from the orders table using a SQL command
    # The query fetches all orders that were placed after the last order date stored in the state dictionary.
    last_order = state.get("lastOrder", "2020-08-10") # fetch the last order date from the state dictionary
    query = f"SELECT * FROM orders WHERE order_date > '{last_order}'"
    cursor.execute(query)
    # Get the column names from the cursor description
    columns = [col[0].lower() for col in cursor.description]

    for row in cursor.fetchall():
        # The cursor.fetchall() method returns a list of tuples, where each tuple represents a row in the result set
        # Tuples can be converted to lists for easier manipulation
        row = list(row)

        # If the fetched row needs to be modified, you can do it here.
        for index,data in enumerate(row):
            # Check if the data is a datetime object.
            # It needs to be converted to a string in ISO format
            if isinstance(data, datetime.datetime) or isinstance(data, datetime.date):
                # Convert datetime to string in ISO format
                row[index] = data.isoformat()

        # This is needed to map the column names to the data returned by the query
        # The zip function pairs each column name with its corresponding value in the row tuple
        # The dict function creates a dictionary from these pairs
        upsert_data = dict(zip(columns, row))
        yield op.upsert(table="orders", data=upsert_data)

        # Update the last order date in the state dictionary
        last_order = upsert_data["order_date"] if upsert_data["order_date"] > last_order else last_order

    # Update the state dictionary with the last order date
    state["lastOrder"] = last_order
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    yield op.checkpoint(state)

    # close the cursor and connection
    cursor.close()
    connection.close()


# Create the connector object using the schema and update functions.
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)

# Resulting tables:
# Table "orders":
# ┌────────────┬──────────────┬────────────────┬──────────┬──────────┬──────────────────────────────┬────────┐
# | product_id | product_code | product_name   | price    | in_stock | description                  | weight |
# |────────────|──────────────|────────────────|──────────|──────────|──────────────────────────────|────────|
# | 101        | LP01         | Laptop Pro     | 1299.99  | True     | High-performance laptop      | 2.5    |
# | 102        | SW01         | Smart Watch    | 249.50   | True     | Fitness tracking smart watch | 0.3    |
# | 103        | OC01         | Office Chair   | 189.95   | False    | Ergonomic office chair       | 12.8   |
# └────────────┴──────────────┴────────────────┴──────────┴──────────┴──────────────────────────────┴────────┘
#
# Table "products":
# ┌───────────────┬─────────────┬─────────────┬────────────┬──────────┬────────────┬──────────┬────────────────┬────────────┬─────────────────┬───────────┬───────┬───────┬──────────────────┐
# | order_id      | customer_id | order_date  | product_id | quantity | unit_price | amount   | payment_method | status     | street_address  | city      | state | zip   | discount_applied |
# |───────────────|─────────────|─────────────|────────────|──────────|────────────|──────────|────────────────|────────────|─────────────────|───────────|───────|───────|──────────────────|
# | ord-45678-a   | 1           | 2023-08-15  | 101        | 1        | 1299.99    | 1299.99  | Credit Card    | Completed  | 123 Main St     | Austin    | TX    | 78701 | 10.00            |
# | ord-45678-b   | 1           | 2023-08-15  | 103        | 1        | 189.95     | 189.95   | Credit Card    | Completed  | 123 Main St     | Austin    | TX    | 78701 | 0.00             |
# | ord-98765     | 2           | 2023-09-03  | 102        | 1        | 249.50     | 249.50   | PayPal         | Processing | 456 Park Ave    | New York  | NY    | 10022 | 0.00             |
# | ord-12345-a   | 3           | 2023-09-10  | 101        | 1        | 1299.99    | 1299.99  | Debit Card     | Shipped    | 789 Beach Rd    | Miami     | FL    | 33139 | 15.50            |
# | ord-12345-b   | 3           | 2023-09-10  | 102        | 1        | 249.50     | 249.50   | Debit Card     | Shipped    | 789 Beach Rd    | Miami     | FL    | 33139 | 0.00             |
# └───────────────┴─────────────┴─────────────┴────────────┴──────────┴────────────┴──────────┴────────────────┴────────────┴─────────────────┴───────────┴───────┴───────┴──────────────────┘