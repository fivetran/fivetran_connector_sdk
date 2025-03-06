# Importing External Drivers using installation.sh script file.

# This module implements a connector that requires external driver installation using installation.sh file
# It is an example of using libpq5 to communicate with posgress db and iterate over the data to update the connector
# NOTE: Do check with Customer Support to implement this or similar installation.sh script file.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import datetime for handling date and time conversions.
import time
import json
import csv
# Import psycopg2 to communicate with db
import psycopg2
from fivetran_connector_sdk import Connector, Logging as log, Operations as op


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration : dict):
    return [
        {
            "table": "orders",
            "primary_key": ["id"],
            "columns": {
                "id": "LONG",
                "title": "STRING",
                "name": "STRING",
                "city": "STRING",
                "mobile": "STRING",
            }
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration : dict, state : dict):
    try:
        host = configuration.get("host")
        database = configuration.get("database")
        user = configuration.get("user")
        password = configuration.get("password")
        port = configuration.get("port")
        table_name = configuration.get("table_name")
        log.info("Fetching the CSV from desired location.")
        yield from read_postgres_and_upsert(host, database, user, password, port, table_name)

        state["timestamp"] = time.time()
        yield op.checkpoint(state)
    except Exception as e:
        log.severe(f"An error occurred: {e}")






def read_postgres_and_upsert(host, database, user, password, port, table_name):
    """
    Connects to a PostgreSQL database, reads all rows from a specified table,
    and prints them.

    Args:
        host (str): The PostgreSQL server hostname or IP address.
        database (str): The database name.
        user (str): The database username.
        password (str): The database password.
        port (int): The database port.
        table_name (str): The name of the table to read from.
    """
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port
        )
        cursor = conn.cursor()

        # Execute a SELECT query to retrieve all rows from the table
        cursor.execute(f"SELECT * FROM {table_name}")

        # Fetch all rows from the result set
        rows = cursor.fetchall()

        # Print the rows
        for row in rows:
            print(row)

        headers = {"id", "title", "name", "city", "mobile"}
        log.info("Iterating over the downloaded data.")
        for row in rows:
            upsert_line = {}
            for col_index, value in enumerate(row):
                upsert_line[headers[col_index]] = value
            yield op.upsert("orders", upsert_line)

        # Close the cursor and connection
        cursor.close()
        conn.close()

    except psycopg2.Error as e:
        print(f"Error: {e}")


connector = Connector(update=update, schema=schema)

# required inputs docs https://fivetran.com/docs/connectors/connector-sdk/technical-reference#technicaldetailsrequiredobjectconnector
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE.
    connector.debug(configuration=configuration)