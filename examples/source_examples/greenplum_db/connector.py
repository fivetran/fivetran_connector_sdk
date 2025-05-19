# This is a simple example for how to work with the fivetran_connector_sdk module.
# The example demonstrates how to fetch data from GreenPlum database using psycopg2 and upsert it into a destination table.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import required libraries.
import json
import psycopg2
import psycopg2.extras
from datetime import datetime

# Import the Fivetran Connector SDK.
from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()


# Define the Greenplum client class to handle database operations.
class GreenplumClient:
    def __init__(self, configuration):
        self.connection = GreenplumClient.connect(configuration)
        # Create a cursor object to execute SQL queries.
        # The cursor is created with RealDictCursor factory to return results as dictionaries.
        # The cursor is name "my_cursor" to allow server-side cursors. This enables streaming of results.
        # This is useful for large result sets as it does not load entire data in memory and prevents memory overflow errors.
        self.cursor = self.connection.cursor(name="my_cursor", cursor_factory=psycopg2.extras.RealDictCursor)

    @staticmethod
    def connect(configuration):
        """
        This method establishes a connection to the Greenplum database using psycopg2.
        It uses the connection parameters provided in the configuration.
        If the connection fails, it raises a ConnectionError with a message.
        """
        host = configuration.get("HOST")
        port = int(configuration.get("PORT"))
        database = configuration.get("DATABASE")
        user = configuration.get("USERNAME")
        password = configuration.get("PASSWORD")

        try:
            return psycopg2.connect(
                host = host,
                port = port,
                database = database,
                user = user,
                password = password,
            )
        except Exception as e:
            raise ConnectionError(f"Error connecting to Greenplum database: {e}")

    def disconnect(self):
        # This method closes the connection to the Greenplum database.
        # It checks if the connection is open before attempting to close it.
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        log.info("Database connection closed.")


    def upsert_data(self, query, table_name, state):
        """
        This method executes a SQL query to fetch data from the Greenplum database and upserts it into the destination table.
        Args:
            query: Query to fetch data from the Greenplum database.
            table_name: name of the destination table.
            state: a dictionary containing the state checkpointed during the prior sync.
        """

        # Get the last query from the state dictionary.
        last_query = state.get("last_query", "1990-01-01T00:00:00Z")

        # Execute the SQL query using the cursor.
        self.cursor.execute(query)

        # Iterate through the cursor to fetch and upsert data.
        # The cursor is a named cursor, which allows for server-side cursors.
        # This means that the cursor will fetch rows from the database server in streaming fashion, rather than loading all rows into memory at once.
        for row in self.cursor:
            # Converting the row to a dictionary
            upsert_row = dict(row)
            # Converting datetime objects to ISO format.
            upsert_row = GreenplumClient.convert_datetime_to_iso(upsert_row)
            # The yield statement returns a generator object.
            # This generator will yield an upsert operation to the Fivetran connector.
            # The op.upsert method is called with two arguments:
            # - The first argument is the name of the table to upsert the data into.
            # - The second argument is a dictionary containing the data to be upserted.
            yield op.upsert(table=table_name, data=upsert_row)

            # update the last_query variable if the current row's query_start is greater than the last_query
            if upsert_row["query_start"] > last_query:
                last_query = upsert_row["query_start"]

        state = {
            "last_query": last_query
        }
        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state)


    @staticmethod
    def convert_datetime_to_iso(data):
        # This method converts datetime objects in the data dictionary to ISO format.
        # This is a helper method to ensure that datetime values are serialized correctly.
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # Check if the credentials for connecting to database is present in the configuration.
    cred = ["HOST", "DATABASE", "USERNAME", "PASSWORD","PORT"]
    for key in cred:
        if key not in configuration:
            raise ValueError(f"Missing required configuration: {key}")

    return [
        {
            "table": "sample_table",  # Name of the table in the destination.
            "primary_key": ["datid"],  # Primary key column(s) for the table.
            "columns":{
                "datid": "INT",
                "datname": "STRING",
                "sess_id": "INT",
                "usename": "STRING",
                "query_start": "UTC_DATETIME",
            }
            # columns not defined will be inferred.
        }
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
        state:  a dictionary containing the state checkpointed during the prior sync.
        The state dictionary is empty for the first sync or for any full re-sync.
    """
    log.warning("Example: Source Examples: Greenplum Database")

    # create a GreenplumClient object to handle database operations.
    greenplum_client = GreenplumClient(configuration)
    log.info("Connected to Greenplum database.")

    # Name of the destination table to upsert data into.
    destination_table = "sample_table"
    # SQL query to fetch data from the Greenplum database.
    # you can modify this query to fetch data from any table in your Greenplum database as per your need.
    # The query used here is for demonstration purposes.
    # This query fetches all rows from the pg_stat_activity system view.
    query = "SELECT * FROM pg_stat_activity"

    # Call the upsert_data method to fetch data from the Greenplum database and upsert it into the destination table.
    yield from greenplum_client.upsert_data(query, destination_table, state)


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)