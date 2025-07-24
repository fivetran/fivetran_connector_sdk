# This is an example for how to work with the fivetran_connector_sdk module.
# This shows how to fetch data from Sybase IQ and upsert it to destination using FreeTDS driver and pyodbc.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import required libraries
import pyodbc  # For connecting to Sybase IQ using FreeTDS
import json
import datetime


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary for required fields.
    Args:
        configuration (dict): A dictionary containing the connection parameters.
    Raises:
        ValueError: If any required field is missing or invalid.
    """
    required_fields = ["server", "port", "database", "user_id", "password"]
    for field in required_fields:
        if field not in configuration or not configuration[field]:
            raise ValueError(f"Missing required configuration field: {field}")
    log.info("Configuration validation passed.")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "customers",  # Name of the table in the destination, required.
            "primary_key": ["customer_id"],  # Primary key column(s) for the table, optional.
            "columns": {  # Definition of columns and their types, optional.
                "customer_id": "INT",  # Contains a dictionary of column names and data types
                "created_date": "NAIVE_DATE",
            },  # For any columns whose names are not provided here, e.g. id, their data types will be inferred
        }
    ]


def create_sybase_connection(configuration: dict):
    """
    Create a connection to the Sybase IQ database using FreeTDS.
    This function reads the connection parameters from the provided configuration dictionary.
    Args:
        configuration (dict): A dictionary containing the connection parameters.
    Returns:
        connection: A connection object to the Sybase IQ database.
    """
    server = configuration.get("server")
    port = int(configuration.get("port"))
    database = configuration.get("database")
    user_id = configuration.get("user_id")
    password = configuration.get("password")

    try:
        connection_str = (
            f"DRIVER=FreeTDS;"
            f"SERVER={server};"
            f"PORT={port};"
            f"DATABASE={database};"
            f"UID={user_id};"
            f"PWD={password};"
            "TDS_Version=5.0"
        )
        connection = pyodbc.connect(connection_str)
        log.info("Connection to Sybase IQ established successfully.")
        return connection
    except Exception as e:
        raise RuntimeError("Connection to Sybase IQ failed") from e


def close_sybase_connection(connection, cursor):
    """
    Close the connection to the Sybase IQ database.
    Args:
        connection: A connection object to the Sybase IQ database.
    """
    if cursor:
        cursor.close()
        log.info("Cursor closed successfully.")
    if connection:
        connection.close()
        log.info("Connection to Sybase IQ closed successfully.")


def fetch_and_upsert(cursor, query, table_name: str, state: dict, batch_size: int = 1000):
    """
    Fetch data from the Sybase IQ database and upsert it into the destination table.
    This function executes the provided SQL query, fetches data in batches, and performs upsert operations.
    It also updates the state with the last processed row based on the `created_date` timestamp.
    Args:
        cursor: A cursor object to the Sybase IQ database.
        query (str): The SQL query to execute for fetching data.
        table_name (str): The name of the destination table for upserting data.
        state (dict): A dictionary containing state information from previous runs.
        batch_size (int): The number of rows to fetch in each batch.
    """
    # last_created is used to track the last processed row based on the created_date
    last_created = state.get("last_created", "1990-01-01T00:00:00")

    # Execute the SQL query to fetch data from the Sybase IQ database
    cursor.execute(query)
    # Fetch the column names from the cursor description
    # This is necessary to map the data to the correct columns in the upsert operation
    column_names = [col[0] for col in cursor.description]

    while True:
        # Fetch data in batches to handle large datasets efficiently
        # This ensures that entire data is not loaded into memory at once and makes it memory efficient
        results = cursor.fetchmany(batch_size)
        if not results:
            # No more data to fetch, exit the loop
            break

        for row in results:
            # Convert the row tuple to a dictionary using the column names
            row_data = dict(zip(column_names, row))
            # Ensure created_date is in ISO format if it exists
            if row_data["created_date"] and isinstance(row_data["created_date"], datetime.date):
                row_data["created_date"] = row_data["created_date"].isoformat()

            # The 'upsert' operation is used to insert or update data in the destination table.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted
            op.upsert(table=table_name, data=row_data)

            # Update the last_created timestamp if the current row's created_date is more recent
            if row_data["created_date"] and row_data["created_date"] > last_created:
                last_created = row_data["created_date"]

        # Update the state with the last_created timestamp after processing each batch
        state["last_created"] = last_created
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        op.checkpoint(state)

    # After processing all rows, update the state with the last_created timestamp and checkpoint it.
    # this ensures that the next run will start from the correct position
    state["last_created"] = last_created
    op.checkpoint(state)


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Examples: Source Example: Sybase IQ")

    # Validate the configuration
    validate_configuration(configuration=configuration)

    # last_created is used to track the last processed row based on the created_date
    last_created = state.get("last_created", "1990-01-01T00:00:00")

    # Define the table name to fetch data from
    table_name = "customers"
    # SQL query to fetch data from the Sybase IQ database
    # Adjust the query to match your table structure and requirements
    # The order by clause ensures that the data is processed in the order of creation. This is important for incremental updates.
    query = (
        f"SELECT * FROM {table_name} WHERE created_date > '{last_created}' ORDER BY created_date"
    )

    # Create a connection to the Sybase IQ database using the provided configuration
    connection = create_sybase_connection(configuration=configuration)
    # Create a cursor object to execute SQL queries
    cursor = connection.cursor()

    # Fetch data from the Sybase IQ database and upsert it into the destination table
    fetch_and_upsert(
        cursor=cursor, query=query, table_name=table_name, state=state, batch_size=1000
    )

    # Close the cursor and connection to the Sybase IQ database
    close_sybase_connection(connection=connection, cursor=cursor)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        # Open the configuration.json file and load its contents
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        log.info("Using empty configuration!")
        configuration = {}

    # Test the connector locally
    connector.debug(configuration=configuration)
