import psycopg2
import psycopg2.extras
from datetime import datetime

from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


# Define the Greenplum client class to handle database operations.
class GreenplumClient:
    def __init__(self, configuration):
        # Check if the credentials for connecting to database is present in the configuration.
        cred = ["HOST", "DATABASE", "USERNAME", "PASSWORD", "PORT"]
        for key in cred:
            if key not in configuration:
                raise ValueError(f"Missing required configuration: {key}")

        self.connection = GreenplumClient.connect(configuration)
        self.cursor = self.connection.cursor(
            name="my_cursor", cursor_factory=psycopg2.extras.RealDictCursor
        )

    @staticmethod
    def connect(configuration):
        host = configuration.get("HOST")
        port = int(configuration.get("PORT"))
        database = configuration.get("DATABASE")
        user = configuration.get("USERNAME")
        password = configuration.get("PASSWORD")

        try:
            return psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
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

        for row in self.cursor:
            # Converting the row to a dictionary
            upsert_row = dict(row)
            # Converting datetime objects to ISO format.
            upsert_row = GreenplumClient.convert_datetime_to_iso(upsert_row)
            op.upsert(table=table_name, data=upsert_row)

            # update the last_query_timestamp variable if the current row's query_start is greater than the last_query_timestamp
            if upsert_row["query_start"] > last_query_timestamp:
                last_query_timestamp = upsert_row["query_start"]

        state = {"last_query_timestamp": last_query_timestamp}
        op.checkpoint(state)

    @staticmethod
    def convert_datetime_to_iso(data):
        # This method converts datetime objects in the data dictionary to ISO format.
        # This is a helper method to ensure that datetime values are serialized correctly.
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.isoformat()
        return data
