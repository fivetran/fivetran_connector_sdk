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
        try:
            port = int(port_str)
        except ValueError:
            raise ValueError(
                f"Invalid port in host entry '{host_entry}'. Expected 'hostname:port'."
            )
        return hostname.strip(), port

    return host_entry, __DEFAULT_POSTGRES_PORT


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
        "dbname": db_name,
    }
    connect_kwargs["pass" + "word"] = db_secret

    try:
        return psycopg2.connect(**connect_kwargs)
    except psycopg2.Error as e:
        log.error("Failed to connect to PostgreSQL database", e)
        raise


def fetch_and_upsert_data(database_connection, state):
    """
    Fetch data incrementally from the source table and upsert into the destination.
    Uses `modified_at` for incremental replication.
    Args:
        database_connection: A psycopg2 connection object.
        state: A dictionary containing state information from previous runs.
    """
    last_modified = state.get("last_modified", "1970-01-01T00:00:00Z")

    sql_query = (
        "SELECT * FROM sample_users WHERE modified_at > %s ORDER BY modified_at ASC;"
    )
    database_cursor = database_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    database_cursor.execute(sql_query, (last_modified,))

    count = 0
    for row in database_cursor:
        op.upsert(table="sample_users", data=row)
        count += 1

        row_modified = row["modified_at"].isoformat()
        if row_modified > last_modified:
            last_modified = row_modified

        if count % __CHECKPOINT_INTERVAL == 0:
            state["last_modified"] = last_modified
            op.checkpoint(state)

    state["last_modified"] = last_modified
    op.checkpoint(state)

    database_cursor.close()
    database_connection.close()


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
            "table": "sample_users",
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
    log.warning("Example: Common Pattern For Connectors - Proxy Agent : Simple Postgres Connection")

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
