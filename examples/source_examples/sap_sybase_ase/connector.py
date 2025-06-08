# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to create a connector that connects to a SAP Sybase ASE instance and syncs data from it.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Import necessary libraries
import datetime
import time
import json
# Import pyodbc to communicate with Sybase ASE
import pyodbc


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    # Validate required configuration parameters
    required_configs = ["server", "user", "password", "database", "table_name", "port"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


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
            "table": "customers",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "customer_name": "STRING",
                "customer_email": "STRING",
                "customer_address": "STRING",
                "customer_phone": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME"
            }
        }
    ]


def connect_to_sybase(configuration: dict):
    """
    Connect to a Sybase ASE database using pyodbc.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        cursor: A pyodbc cursor object to the Sybase ASE database.
    """

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    try:
        # Extract configuration values
        server = configuration.get("server")
        database = configuration.get("database")
        user = configuration.get("user")
        password = configuration.get("password")
        port = int(configuration.get("port"))

        # Establish a connection to the Sybase ASE database
        conn = pyodbc.connect(
            "DRIVER={FreeTDS};"
            f"SERVER={server};"
            f"PORT={port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            "TDS_VERSION=5.0;"
        )
        log.info("Connection to Sybase ASE established successfully")
        return conn

    except Exception as e:
        raise RuntimeError("Error connecting to Sybase ASE: " + str(e))


def update(configuration: dict, state: dict):
    """
     Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    log.warning("Example: Source Examples : SAP Sybase ASE Connector")

    table_name = configuration.get("table_name")

    # Fetch and upsert records
    yield from read_sybase_and_upsert(configuration=configuration, table_name=table_name)

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


def read_sybase_and_upsert(configuration, table_name, batch_size=1000):
    """
    Connects to a Sybase ASE database, reads rows from a specified table,
    and upserts them into destination.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
        table_name (str): The name of the table to read from.
        batch_size (int): The number of rows to fetch in each batch. Default is 1000.
    """
    try:
        # Connect to Sybase ASE and get a cursor object
        conn = connect_to_sybase(configuration)
        cursor = conn.cursor()

        # Prepare the SQL query to fetch data from the specified table
        # You can modify this query to match your specific requirements
        query = f"SELECT * FROM {table_name}"
        columns = [column[0] for column in cursor.description]

        # Execute the query
        cursor.execute(query)

        # Fetch and process rows
        record_count = 0
        rows = cursor.fetchmany(batch_size)  # Fetch rows in batches for efficiency

        while rows:
            for row in cursor:
                upsert_row = {}
                for i, column in enumerate(columns):
                    value = row[i]
                    # Convert datetime objects to strings
                    if isinstance(value, datetime.datetime) or isinstance(value, datetime.date):
                        value = value.isoformat()
                    upsert_row[column.lower()] = value

                # The yield statement returns a generator object.
                # This generator will yield an upsert operation to the Fivetran connector.
                # The op.upsert method is called with two arguments:
                # - The first argument is the name of the table to upsert the data into.
                # - The second argument is a dictionary containing the data to be upserted.
                yield op.upsert(table="customers", data=upsert_row)
                record_count += 1

            # Get next batch
            rows = cursor.fetchmany(batch_size)

        log.info(f"{record_count} records upserted")

        # Close the cursor and connection after processing
        close_connection(connection=conn, cursor=cursor)

    except Exception as e:
        raise RuntimeError(f"Failed to read from Sybase ASE: {str(e)}")


def close_connection(connection, cursor):
    """
    Close the connection to the Sybase ASE database.
    Args:
        connection: The connection object to be closed.
        cursor: The cursor object to be closed.
    """
    try:
        if cursor:
            cursor.close()
            log.info("Cursor closed successfully")
        if connection:
            connection.close()
            log.info("Connection closed successfully")
    except Exception as e:
        raise RuntimeError(f"Error closing connection: {str(e)}")


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)