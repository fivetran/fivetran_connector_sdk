# This example demonstrates how to connect to a PostgreSQL instance behind a Fivetran Proxy Agent when
# multiple candidate hosts are configured.
# The connector reads a list of hosts from `configuration.json` (`hosts`, a JSON array where each entry is
# in `hostname:port` format) and attempts to connect to each in order until one succeeds. This is useful when:
#   - Multiple proxy agents front the same database for high availability.
#   - You have a primary and one or more read replicas.
#   - Region-specific hosts share the same credentials.
# The rest of the sync logic (incremental read on `modified_at`, checkpointing, upsert) is unchanged
# from the simple example.
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
__CONNECT_TIMEOUT_SECONDS = 10  # Per-host connection timeout to fail fast on unreachable hosts
__TABLE_NAME = "test"  # Name of the source table to sync


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing or `hosts` is empty/malformed.
    """
    required_configs = ["hosts", "db_user", "db_password", "db_name"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    if not isinstance(configuration["hosts"], list):
        raise ValueError(
            "Configuration value 'hosts' must be a JSON array of 'hostname:port' entries."
        )

    parsed = parse_hosts(configuration["hosts"])
    if not parsed:
        raise ValueError(
            "Configuration value 'hosts' must contain at least one entry in 'hostname:port' format."
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


def parse_hosts(hosts_value):
    """
    Parse the `hosts` configuration value into an ordered list of (hostname, port) tuples.
    The value is expected to be a JSON array where each entry is `hostname:port`
    (e.g. ["host1:5432", "host2:5433"]).
    Args:
        hosts_value: list of host entries from configuration.
    Returns:
        A list of (hostname, port) tuples preserving the configured order. Empty entries are skipped.
    """
    if not isinstance(hosts_value, list):
        return []

    parsed = []
    for entry in hosts_value:
        if not isinstance(entry, str):
            continue
        entry = entry.strip()
        if not entry:
            continue
        hostname, port = split_host_port(entry)
        if hostname:
            parsed.append((hostname, port))
    return parsed


def get_database_connection(configuration: dict):
    """
    Attempt to connect to PostgreSQL by iterating over the configured list of hosts.
    The first host that accepts the connection is used for the sync. Subsequent hosts act as failover
    targets when running behind multiple Fivetran Proxy Agents or when using read replicas.
    Args:
        configuration: a dictionary that holds the connector configuration.
    Returns:
        A psycopg2 connection object connected to the first reachable host.
    Raises:
        ConnectionError: if all configured hosts fail to connect.
    """
    hosts = parse_hosts(configuration.get("hosts", []))
    db_user = configuration.get("db_user")
    db_secret = configuration.get("db_password")
    db_name = configuration.get("db_name")

    last_error = None
    for hostname, port in hosts:
        log.info(f"Attempting to connect to PostgreSQL host {hostname}:{port}, database={db_name}")
        connect_kwargs = {
            "host": hostname,
            "port": port,
            "user": db_user,
            "password": db_secret,
            "dbname": db_name,
            "connect_timeout": __CONNECT_TIMEOUT_SECONDS,
            "sslmode": "disable",
        }

        try:
            connection = psycopg2.connect(**connect_kwargs)
            log.info(f"Successfully connected to PostgreSQL host {hostname}:{port}")
            return connection
        except psycopg2.Error as e:
            log.warning(f"Failed to connect to PostgreSQL host {hostname}:{port}: {e}")
            last_error = e

    log.error("Failed to connect to any configured PostgreSQL host", last_error)
    raise ConnectionError(f"Unable to connect to any of the configured hosts: {hosts}")


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
    log.warning("Example: Common Pattern For Connectors - Proxy Agent : Multiple Hosts")

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
