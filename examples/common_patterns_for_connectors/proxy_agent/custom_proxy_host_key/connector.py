# This example demonstrates how to connect to a PostgreSQL instance behind a Fivetran Proxy Agent using
# a *custom proxy host key* in `configuration.json`.
# Instead of using the standard `host` field, this connector reads a user-defined key (e.g. `proxy_host`)
# in the format `hostname:port` and uses it as the PostgreSQL host. This is useful when you want to make it
# explicit in your configuration that the address points to a proxy agent rather than a direct database host,
# or when your team uses a different naming convention (for example `on_prem_proxy_host`).
# The rest of the connector logic is identical to the simple PostgreSQL connection example.
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
__TABLE_NAME = "test"  # Name of the source table to sync

# The custom configuration key that holds the proxy agent host address (in `hostname:port` format).
# Change this constant if your team uses a different naming convention.
__PROXY_HOST_CONFIG_KEY = "proxy_host"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or malformed.
    """
    required_configs = [__PROXY_HOST_CONFIG_KEY, "db_user", "db_password", "db_name"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    hostname, _ = split_host_port(configuration[__PROXY_HOST_CONFIG_KEY])
    if not hostname:
        raise ValueError(
            f"Configuration value '{__PROXY_HOST_CONFIG_KEY}' must be in the format 'hostname:port'."
        )


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
        return hostname.strip(), int(port_str)
    return host_entry, __DEFAULT_POSTGRES_PORT


def get_database_connection(configuration: dict):
    """
    Create a PostgreSQL connection using the custom proxy host key from the configuration.
    The `proxy_host` value should point to the address exposed by the Fivetran Proxy Agent that forwards
    traffic to the actual PostgreSQL instance in your private network.
    Args:
        configuration: a dictionary that holds the connector configuration.
    Returns:
        A psycopg2 connection object.
    """
    hostname, port = split_host_port(configuration.get(__PROXY_HOST_CONFIG_KEY, ""))
    db_user = configuration.get("db_user")
    db_secret = configuration.get("db_password")
    db_name = configuration.get("db_name")

    log.info(f"Connecting to PostgreSQL through proxy host {hostname}:{port}, database={db_name}")

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
        log.error("Failed to connect to PostgreSQL database via proxy host", e)
        raise


def fetch_and_upsert_data(database_connection, state):
    """
    Fetch data incrementally from the source table and upsert into the destination.
    Uses `modified_at >= last_modified` so that rows sharing the same timestamp as the
    last checkpoint are re-fetched; upsert idempotency ensures no duplicates in the destination.
    Args:
        database_connection: A psycopg2 connection object.
        state: A dictionary containing state information from previous runs.
    """
    last_modified = state.get("last_modified", "1970-01-01T00:00:00Z")

    # >= ensures rows whose modified_at equals the last checkpoint are included, preventing
    # gaps when multiple rows share the same timestamp at a sync boundary.
    sql_query = f"SELECT * FROM {__TABLE_NAME} WHERE modified_at >= %s ORDER BY modified_at ASC;"
    database_cursor = database_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    database_cursor.execute(sql_query, (last_modified,))

    count = 0
    for row in database_cursor:
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        op.upsert(table=__TABLE_NAME, data=dict(row))
        count += 1

        row_modified = row["modified_at"].isoformat()
        if row_modified > last_modified:
            last_modified = row_modified

        if count % __CHECKPOINT_INTERVAL == 0:
            state["last_modified"] = last_modified
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
            # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
            op.checkpoint(state)

    state["last_modified"] = last_modified
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)

    database_cursor.close()
    database_connection.close()


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
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details.
        state: A dictionary containing state information from previous runs.
            The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.warning("Example: Common Pattern For Connectors - Proxy Agent : Custom Proxy Host Key")

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
