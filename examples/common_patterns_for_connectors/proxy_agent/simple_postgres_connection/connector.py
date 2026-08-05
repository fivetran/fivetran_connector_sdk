# This example demonstrates how to connect to a PostgreSQL instance using the Fivetran Connector SDK
# when running behind a Fivetran Proxy Agent.
# The connector reads a single `host` entry in the format `hostname:port` from `configuration.json`
# along with the database credentials, and establishes a direct PostgreSQL connection using `psycopg2`.
# When the connector is deployed behind a Fivetran Proxy Agent, the proxy agent transparently forwards
# traffic from Fivetran to your on-prem PostgreSQL instance; no code changes are required in the connector
# beyond pointing `host` to the reachable address exposed by the proxy agent.
# See the Technical Reference documentation
# (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For interacting with PostgreSQL
import psycopg2
import psycopg2.extras

__CHECKPOINT_INTERVAL = 1000  # Number of records to process before checkpointing state
__DEFAULT_POSTGRES_PORT = 5432  # Default PostgreSQL port
__MAX_PORT_NUMBER = 65535  # Highest valid TCP port number
__TABLE_NAME = "TEST"  # Name of the source table to sync


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary
    configuration values before attempting a connection.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or `host` is malformed.
    """
    required_configs = ["host", "db_user", "db_password", "db_name"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    hostname, _ = split_host_port(configuration["host"])
    if not hostname:
        raise ValueError("Configuration value 'host' must be in the format 'hostname:port'.")


def split_host_port(host_entry: str):
    """
    Split a `hostname:port` string into its hostname and port components.
    If no port is specified, the default PostgreSQL port is returned.
    Args:
        host_entry: A string in the form `hostname` or `hostname:port`.
    Returns:
        A tuple of (hostname, port_int).
    """
    host_entry = (host_entry or "").strip()
    if not host_entry:
        return "", __DEFAULT_POSTGRES_PORT
    if ":" in host_entry:
        hostname, port_str = host_entry.rsplit(":", 1)
        return hostname.strip(), parse_port(port_str, host_entry)
    return host_entry, __DEFAULT_POSTGRES_PORT


def parse_port(port_str: str, host_entry: str):
    """
    Parse and validate a TCP port from a host entry.
    Args:
        port_str: The port portion extracted from the host entry.
        host_entry: The original host entry for error reporting.
    Returns:
        The validated port as an integer.
    Raises:
        ValueError: if the port is missing, non-numeric, or outside the valid TCP range.
    """
    port_str = (port_str or "").strip()
    if not port_str:
        raise ValueError(f"Host entry '{host_entry}' must include digits after ':'.")
    if not port_str.isdigit():
        raise ValueError(f"Host entry '{host_entry}' contains an invalid port: '{port_str}'.")

    port = int(port_str)
    if not 1 <= port <= __MAX_PORT_NUMBER:
        raise ValueError(
            f"Host entry '{host_entry}' contains port {port}, but valid ports are 1-{__MAX_PORT_NUMBER}."
        )
    return port


def get_database_connection(configuration: dict):
    """
    Create a PostgreSQL connection using the parameters provided in the configuration.
    When the connector is deployed behind a Fivetran Proxy Agent, the `host` value in the configuration
    should point to the address that is reachable through the proxy agent.
    Args:
        configuration: a dictionary that holds the connector configuration.
    Returns:
        A psycopg2 connection object.
    """
    hostname, port = split_host_port(configuration.get("host", ""))
    db_user = configuration.get("db_user")
    db_secret = configuration.get("db_password")
    db_name = configuration.get("db_name")

    log.info(f"Connecting to PostgreSQL at {hostname}:{port}, database={db_name}")

    connect_kwargs = {
        "host": hostname,
        "port": port,
        "user": db_user,
        "password": db_secret,
        "dbname": db_name,
        "connect_timeout": 60,
        "sslmode": "disable",
    }

    try:
        return psycopg2.connect(**connect_kwargs)
    except psycopg2.Error as e:
        log.error("Failed to connect to PostgreSQL database", e)
        raise


def fetch_and_upsert_data(database_connection, state):
    """
    Fetch all rows from the source table using a server-side named cursor and upsert into the destination.
    Uses fetchmany() to stream data in batches, avoiding loading all rows into memory at once.
    Args:
        database_connection: A psycopg2 connection object.
        state: A dictionary containing state information from previous runs.
    """
    log.info(f"Starting extraction from table: {__TABLE_NAME}")

    # Use a named server-side cursor to stream results in batches, avoiding loading all rows into memory.
    database_cursor = database_connection.cursor(
        name="server_side_cursor", cursor_factory=psycopg2.extras.RealDictCursor
    )
    database_cursor.execute(f"SELECT * FROM {__TABLE_NAME};")

    total_rows = 0
    while True:
        rows = database_cursor.fetchmany(__CHECKPOINT_INTERVAL)
        if not rows:
            break

        for row in rows:
            # The 'upsert' operation is used to insert or update data in the destination table.
            # The first argument is the name of the destination table.
            # The second argument is a dictionary containing the record to be upserted.
            op.upsert(table=__TABLE_NAME, data=dict(row))

        total_rows += len(rows)
        state["total_rows"] = total_rows
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
        # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
        op.checkpoint(state)
        log.info(f"Upserted {total_rows} rows so far from {__TABLE_NAME}")

    database_cursor.close()
    database_connection.close()
    log.info(f"Completed extraction from {__TABLE_NAME}. Total rows: {total_rows}")


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": __TABLE_NAME,
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
            The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.warning(
        "Example: Common Pattern For Connectors - Proxy Agent : Simple Postgres Connection"
    )

    validate_configuration(configuration=configuration)

    connection = get_database_connection(configuration=configuration)
    fetch_and_upsert_data(database_connection=connection, state=state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module. This is Python's standard entry method allowing your script to
# be run directly from the command line or IDE 'run' button. This is useful for debugging while you write your code.
# Note this method is not called by Fivetran when executing your connector in production. Please test using the
# Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    try:
        with open("configuration.json", "r") as f:
            configuration = json.load(f)
    except FileNotFoundError:
        configuration = {}
    connector.debug(configuration=configuration)
