# This is a simple example for how to work with the fivetran_connector_sdk module.
# This is an example to show how we can sync records from redshift DB via Connector SDK.
# You would need to provide your redshift credentials for this example to work.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

import json  # Import the json module to handle JSON data.
# Import datetime for handling date and time conversions.
from datetime import datetime
# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
import redshift_connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
# This column is the replication key which helps determine which records are updated
REPLICATION_KEY = "updated_at"


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "customers",  # Name of the table in the destination.
            "primary_key": ["customer_id"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "customer_id": "INT",  # Integer column for the customer_id.
                "first_name": "STRING",  # String column for the first name.
                "last_name": "STRING",  # String column for the last name.
                "email": "STRING",  # String column for the email.
                "updated_at": "UTC_DATETIME",  # UTC date-time column for the updated_at.
                # In this example we are using `updated_at` as the replication key
            },
        }
    ]


def dt2str(incoming: datetime) -> str:
    return incoming.strftime(TIMESTAMP_FORMAT)


def connect_to_redshift(configuration):
    return redshift_connector.connect(
        host=configuration['host'],
        database=configuration['database'],
        port=configuration['port'],
        user=configuration['user'],
        password=configuration['password']
    )


def setup_db(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS testers;")
    cursor.execute("DROP TABLE IF EXISTS testers.customers;")

    cursor.execute("CREATE TABLE IF NOT EXISTS testers.customers "
                   "(customer_id INTEGER PRIMARY KEY, "
                   "first_name VARCHAR, "
                   "last_name VARCHAR, "
                   "email VARCHAR, "
                   "city VARCHAR, "
                   "state VARCHAR, "
                   "country VARCHAR, "
                   "updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP) "
                   "SORTKEY (updated_at)")

    cursor.execute("INSERT INTO testers.customers "
                   "(customer_id, first_name, last_name, email, city, state, country, updated_at) "
                   "VALUES "
                   "(1, 'Mathew', 'Perry', 'mathew@fivetran.com', 'New York City', 'New York', 'United States', '2023-12-31T23:59:59Z'), "
                   "(2, 'Joe', 'Doe', 'joe@fivetran.com', 'Paris', 'Île-de-France', 'France', '2024-01-31T23:04:39Z'), "
                   "(3, 'Jake', 'Anderson', 'jake@fivetran.com', 'Tokyo', 'Tokyo Prefecture', 'Japan', '2023-11-01T23:59:59Z'), "
                   "(4, 'John', 'William', 'john@fivetran.com', 'Sydney', 'New South Wales', 'Australia', '2024-02-14T22:59:59Z'), "
                   "(5, 'Ricky', 'Roma', 'ricky@fivetran.com', 'Rio de Janeiro', 'Rio de Janeiro', 'Brazil', '2024-03-16T16:40:29Z')")


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: Source Examples - Redshift")

    # The yield statement returns a generator object.
    # This generator will yield an upsert operation to the Fivetran connector.
    # The op.upsert method is called with two arguments:
    # - The first argument is the name of the table to upsert the data into, in this case, "hello".
    # - The second argument is a dictionary containing the data to be upserted,

    # Connect to your database instance instance.
    conn = connect_to_redshift(configuration)
    cursor = conn.cursor()
    # This is not required. This is just for example illustration purposes.
    setup_db(cursor)

    # If the cursor is not present in the state, starting from ('2024-01-01T00:00:00Z') to represent incremental syncs.
    last_updated_at = state["last_updated_at"] if "last_updated_at" in state else '2024-01-01T00:00:00Z'

    # Fetch records from DB sorted in ascending order.
    # REPLICATION_KEY is updated_at column.
    query = (f"SELECT customer_id, first_name, last_name, email, updated_at FROM testers.customers WHERE {REPLICATION_KEY} > "
             f"'{last_updated_at}' ORDER BY {REPLICATION_KEY}")

    # This log message will only show while debugging.
    log.fine(f"fetching records from `customer` table modified after {last_updated_at}")
    cursor.execute(query)

    while True:
        # Fetching 2 rows at a time so that we keep checkpointing in intervals.
        result = cursor.fetchmany(2)
        if len(result) == 0:
            break
        # Yield an upsert operation to insert/update the row in the "customers" table.
        for row in result:
            yield op.upsert(table="customers",
                            data={
                                "customer_id": row[0],  # Customer id.
                                "first_name": row[1],  # First Name.
                                "last_name": row[2],  # Last name.
                                "email": row[3],  # Email id.
                                "updated_at": dt2str(row[4])  # record updated at.
                            })
            # Storing `updated_at` of last fetched record
            last_updated_at = dt2str(row[4])
        # Update the state to the updated_at of the last record.
        state["last_updated_at"] = last_updated_at

        # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
        # from the correct position in case of next sync or interruptions.
        # Learn more about how and where to checkpoint by reading our best practices documentation
        # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
        yield op.checkpoint(state)


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)

# Source table:
# ┌───────────────────┬───────────────────┬───────────────────────────────────────────┬────────────────────────────┐
# │     customer_id   │   first_name      │     last_name     │       email           │       updated_at           │
# │         int16     │      varchar      │     varchar       │       varchar         │   timestamp with time zone │
# ├───────────────────┼───────────────────┼───────────────────┼───────────────────────┤────────────────────────────│
# │         1         │       Mathew      │     Perry         │ mathew@fivetran.com   │    2023-12-31 23:59:59.000 │
# │         2         │       Joe         │     Doe           │ joe@fivetran.com      │    2024-01-31 23:04:39.000 │
# │         3         │       Jake        │     Anderson      │ jake@fivetran.com     │    2023-11-01 23:59:59.000 │
# │         4         │       John        │     William       │ john@fivetran.com     │    2024-02-14 22:59:59.000 │
# │         5         │       Ricky       │     Roma          │ ricky@fivetran.com    │    2024-03-16 16:40:29.000 │
# ├───────────────────┴───────────────────┴───────────────────┴────────────────────────────────────────────────────┤
# │ 5 rows                                                                                               5 columns │
# └────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘


# Resulting table:
# ┌─────────────────────┬───────────────────┬────────────────────────────────────────┬────────────────────────────┐
# │     customer_id     │   first_name      │     last_name     │       email        │       updated_at           │
# │         int16       │      varchar      │     varchar       │       varchar      │   timestamp with time zone │
# ├─────────────────────┼───────────────────┼───────────────────┼────────────────────┤────────────────────────────│
# │         2           │       Joe         │     Doe           │ joe@fivetran.com   │    2024-01-31T23:04:39Z    │
# │         4           │       John        │     William       │ john@fivetran.com  │    2024-02-14T22:59:59Z    │
# │         5           │       Ricky       │     Roma          │ ricky@fivetran.com │    2024-03-16T16:40:29Z    │
# ├─────────────────────┴───────────────────┴───────────────────┴─────────────────────────────────────────────────┤
# │ 3 rows                                                                                              5 columns │
# └───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
