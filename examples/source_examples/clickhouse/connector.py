# This is an example for how to work with the fivetran_connector_sdk module.
# It defines a simple 'update' method, which upserts data from Clickhouse database.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import the required classes from the connector SDK
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# Import the required libraries
import datetime
import random
import json
import clickhouse_connect # This is used to connect to ClickHouse

# Import custom function to create dummy data
from clickhouse_dummy_data_generator import insert_dummy_data


def create_clickhouse_client(configuration: dict):
    """
    Create and test a connection to ClickHouse
    This function creates a ClickHouse client using the clickhouse_connect library.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        client: a ClickHouse client object
    """
    client = clickhouse_connect.get_client(
        host= configuration.get("hostname"),
        username=configuration.get("username"),
        password=configuration.get("password"),
        secure=True
    )

    try:
        test_connection = client.query("SELECT 1").result_rows[0][0]
        log.info(f"Connection to ClickHouse successful: {test_connection}")
        return client
    except Exception as e:
        raise RuntimeError(f"Failed to connect to ClickHouse: {e}")


def execute_query_and_upsert(client, query, table_name, state):
    """
    This function executes a query and upserts the results into the ClickHouse database.
    The data is fetched in a streaming manner to handle large datasets efficiently.
    Args:
        client: a ClickHouse client object
        query: the SQL query to execute
        table_name: the name of the table to upsert data into
        state: a dictionary containing state information from previous runs
    """
    # This query fetches the column names from the ClickHouse table
    # The column_names is not supported in ClickHouse streaming queries
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
    for key in ['hostname', 'username', 'password', 'database']:
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
    # Create a ClickHouse client
    client = create_clickhouse_client(configuration)
    database_name = configuration.get("database")
    table_name = "sample_table"

    # Insert dummy data for testing purposes
    # This method is used only for testing purposes and will not be used in production.
    insert_dummy_data(client=client, database_name=database_name, table_name=table_name)

    last_created = state.get("last_created", "1990-01-01T00:00:00")

    # Execute the query to fetch data from the ClickHouse table and upsert it
    clickhouse_query = f"SELECT * FROM {database_name}.{table_name} WHERE created_at > '{last_created}'"
    # The name of the table to upsert data into
    destination_table = "destination_table"
    yield from execute_query_and_upsert(client=client, query=clickhouse_query, table_name=destination_table, state=state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)