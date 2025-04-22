# This is a simple example for how to work with the fivetran_connector_sdk module.
# It defines an `update` method, which upserts data from an IBM Informix database.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log  # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Import the ibm_db module for connecting to IBM Informix
import ibm_db
import json


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    # Check if the configuration dictionary has all the required keys
    required_keys = ["hostname", "port", "database", "user_id", "password", "protocol"]
    for key in required_keys:
        if key not in configuration:
            if key == "protocol":
                log.warning("Protocol is not set in configuration, defaulting to TCPIP")
            else:
                raise ValueError(f"Missing required configuration key: {key}")

    return [
        {
            "table": "sample_table", # Name of the table in the destination.
            "primary_key": ["id"], # Primary key column(s) for the table.
            # No columns are defined, meaning the types will be inferred.
        }
    ]


# This method is used to create a connection string for IBM Informix database.
# This takes the configuration dictionary as an argument and extracts the necessary parameters to create the connection string.
# The connection string is used to establish a connection to the database.
def get_connection_string(configuration: dict):
    # Extract the necessary parameters from the configuration dictionary
    hostname = configuration.get("hostname")
    port = configuration.get("port")
    database = configuration.get("database")
    user_id = configuration.get("user_id")
    password = configuration.get("password")
    # The protocol is set to "TCPIP" by default, but can be changed in the configuration.
    protocol = configuration.get("protocol", "TCPIP")

    # return the connection string
    return (
        f"DATABASE={database};"
        f"HOSTNAME={hostname};"
        f"PORT={port};"
        f"PROTOCOL={protocol};"
        f"UID={user_id};"
        f"PWD={password};"
    )


# This method is used to establish a connection to IBM Informix database.
# It takes the configuration dictionary as an argument and uses the get_connection_string method to create the connection string.
# It then attempts to connect to the database using the ibm_db module.
# If the connection is successful, it returns the connection object.
# If the connection fails, it raises a RuntimeError with the error message.
def connect_to_db(configuration: dict):
    # Get the connection string
    conn_str = get_connection_string(configuration)

    # Connect to the database
    try:
        conn = ibm_db.connect(conn_str, "", "")
        log.info("Connected to Informix database successfully!")
        return conn
    except Exception as e:
        log.severe(f"Connection failed: {e}")
        raise RuntimeError("Connection failed") from e


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: Source Examples - IBM Informix")

    # Connect to the IBM Informix database
    conn = connect_to_db(configuration)

    # The date format of the created_at column in the database is "YYYY-MM-DD HH:MM:SS"
    # Please ensure that while handling the datetime, you are using the correct format for the columns.
    last_created = state.get("last_created","1990-01-01 00:00:00")

    # The SQL query to select all records from the sample person table
    sql = f"SELECT * FROM person WHERE created_at > '{last_created}'"
    # Execute the SQL query
    stmt = ibm_db.exec_immediate(conn, sql)
    # Fetch the first record from the result set
    # The ibm_db.fetch_assoc method fetches the next row from the result set as a dictionary
    data = ibm_db.fetch_assoc(stmt)
    # Iterate over the result set and upsert each record until there are no more records
    while data:
        # The yield statement returns a generator object.
        # This generator will yield an upsert operation to the Fivetran connector.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into, in this case, "hello".
        # - The second argument is a dictionary containing the data to be upserted,
        yield op.upsert(table="products", data=data)

        # Update the last_created variable with the created_at value of the current record
        last_created_from_data = data["created_at"].strftime("%Y-%m-%d %H:%M:%S")
        if last_created_from_data > last_created:
            last_created = last_created_from_data
        data = ibm_db.fetch_assoc(stmt)

    log.info("upserted all records from the products table")

    # Close the database connection after the operation is complete
    if 'conn' in locals() and conn:
        ibm_db.close(conn)
        log.info("Connection closed")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    state["last_created"] = last_created
    yield op.checkpoint(state)


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)