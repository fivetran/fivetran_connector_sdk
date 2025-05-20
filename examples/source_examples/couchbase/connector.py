# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to work with a Couchbase Capella.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import the required classes from the connector SDK
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# Import the required libraries
from datetime import timedelta
import random
import json
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import (ClusterOptions, ClusterTimeoutOptions,QueryOptions)


def create_couchbase_client(configuration: dict):
    """
    Create a connection to Couchbase
    This function creates a couchbase client using the couchbase library.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        cluster_bucket: a couchbase bucket reference object
    """
    username = configuration.get("username")
    password = configuration.get("password")
    endpoint = configuration.get("endpoint")
    bucket_name = configuration.get("bucket_name")

    try:
        # Authenticate using the provided username and password
        auth = PasswordAuthenticator(username, password)
        options = ClusterOptions(auth)

        # Sets a pre-configured profile called "wan_development" to help avoid latency issues
        # when accessing Capella from a different Wide Area Network or Availability Zone(e.g. your laptop).
        options.apply_profile('wan_development')
        cluster = Cluster('couchbases://{}'.format(endpoint), options)

        # Wait until the cluster is ready for use.
        cluster.wait_until_ready(timedelta(seconds=10))
        # get a reference to our bucket
        cluster_bucket = cluster.bucket(bucket_name)
        log.info(f"Connected to bucket {bucket_name} successfully.")
        # client = cb.scope(scope).collection(collection)
        return cluster_bucket

    except Exception as e:
        raise RuntimeError(f"Failed to connect to couchbase: {e}")


def execute_query_and_upsert(client, query, table_name, state):
    """
    This function executes a query and upserts the results into the couchbase database.
    The data is fetched in a streaming manner to handle large datasets efficiently.
    Args:
        client: a couchbase client object
        query: the SQL query to execute
        table_name: the name of the table to upsert data into
        state: a dictionary containing state information from previous runs
    """
    # This query fetches the column names from the couchbase table
    # The column_names is not supported in couchbase streaming queries
    # So we need to run a separate query to get the column names
    # This is needed to map the data to the correct columns in the upsert operation
    column_names = client.query(query + " LIMIT 0").column_names

    with client.query_rows_stream(query) as stream:
        for row in stream:
            # Convert the row tuple to a dictionary using the column names
            row = dict(zip(column_names, row))
            # Upsert the data into the destination table
            yield op.upsert(table=table_name, data=row)

            # Update the state with the last created_at timestamp
            if row['created_at']:
                last_created = row.get("created_at").isoformat()
                if last_created > state.get("last_created", "01-01-1800T00:00:00"):
                    state["last_created"] = last_created

    log.info(f"Upserted data into {table_name} successfully.")
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    # check if required configuration values are present in configuration
    for key in ['endpoint', 'username', 'password', 'bucket_name', "scope", "collection"]:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value : {key}")

    return [
        {
            "table": "test_table",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "value": "FLOAT",
                "created_at": "UTC_DATETIME",
            },
        }
    ]

def lookup_by_callsign(cs, client):
    print("\nLookup Result: ")
    try:
        inventory_scope = client.scope('tenant_agent_00')
        sql_query = 'SELECT VALUE name FROM airline WHERE callsign = $1'
        row_iter = inventory_scope.query(
            sql_query,
            QueryOptions(positional_parameters=[cs]))
        for row in row_iter:
            print(row)
    except Exception as e:
        print(e)


def update(configuration, state):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    # Create a couchbase client
    client = create_couchbase_client(configuration)
    scope = configuration.get("scope")
    collection = configuration.get("collection")
    table_name = "sample_table"

    # client = cluster_bucket.scope(scope).collection(collection)

    last_created = state.get("last_created", "1990-01-01T00:00:00")
    lookup_by_callsign("CBS", client)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)