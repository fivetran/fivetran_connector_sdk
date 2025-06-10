import psycopg2
import psycopg2.extras
from datetime import datetime

from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code


# Define the Greenplum client class to handle database operations.
class GreenplumClient:
    def __init__(self, configuration):
        # Check if the credentials for connecting to database is present in the configuration.
        cred = ["HOST", "DATABASE", "USERNAME", "PASSWORD", "PORT"]
        for key in cred:
            if key not in configuration:
                raise ValueError(f"Missing required configuration: {key}")

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
        last_query_timestamp = state.get("last_query_timestamp", "1990-01-01T00:00:00Z")

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

            # update the last_query_timestamp variable if the current row's query_start is greater than the last_query_timestamp
            if upsert_row["query_start"] > last_query_timestamp:
                last_query_timestamp = upsert_row["query_start"]

        state = {
            "last_query_timestamp": last_query_timestamp
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
