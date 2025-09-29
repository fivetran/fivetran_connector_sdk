# This is a simple example for how to work with the fivetran_connector_sdk module.
# The example demonstrates how to fetch data from GreenPlum database using psycopg2 and upsert it into a destination table.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import required libraries.
import json
from helper import GreenplumClient
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

# Name of the destination table to upsert data into.
DESTINATION_TABLE = "sample_table"


def schema(configuration: dict):
    return [
        {
            "table": DESTINATION_TABLE,  # Name of the table in the destination.
            "primary_key": ["datid"],  # Primary key column(s) for the table.
            "columns": {
                "datid": "INT",
                "datname": "STRING",
                "sess_id": "INT",
                "usename": "STRING",
                "query_start": "UTC_DATETIME",
            },
            # columns not defined will be inferred.
        }
    ]


def update(configuration: dict, state: dict):
    log.warning("Example: Source Examples: Greenplum Database")

    # Get the last query from the state dictionary.
    last_query_timestamp = state.get("last_query_timestamp", "1990-01-01T00:00:00Z")

    # create a GreenplumClient object to handle database operations.
    greenplum_client = GreenplumClient(configuration)
    log.info("Connected to Greenplum database.")

    # SQL query to fetch data from the Greenplum database.
    # you can modify this query to fetch data from any table in your Greenplum database as per your need.
    # The query used here is for demonstration purposes.
    # This query fetches all rows from the pg_stat_activity system view.
    query = f"SELECT * FROM pg_stat_activity WHERE query_start > '{last_query_timestamp}'"

    # Call the upsert_data method to fetch data from the Greenplum database and upsert it into the destination table.
    greenplum_client.upsert_data(query, DESTINATION_TABLE, state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)
