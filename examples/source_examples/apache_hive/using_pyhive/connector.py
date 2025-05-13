# This is an example for how to work with the fivetran_connector_sdk module.
# It defines a simple 'update' method, which upserts data from Apache Hive.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import the required classes from the connector SDK
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# Import the required libraries
import json
from datetime import datetime
import random
# Import the Apache hive modules
from pyhive import hive


def create_hive_connection(configuration: dict):
    """
    Create a connection to Apache Hive
    This function creates an Apache Hive connection using the PyHive library.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        connection: an Apache Hive connection object
    """
    host= configuration.get("hostname")
    port= int(configuration.get("port"))
    username=configuration.get("username")
    password=configuration.get("password")

    try:
        connection = hive.Connection(host=host, port=port, username=username, password=password)
        return connection
    except Exception:
        raise RuntimeError("Failed to connect to Apache Hive")


def insert_dummy_data(cursor, table_name, record_count=10):
    """
    Inserts dummy data into the specified table in Apache Hive.
    This is a test function and should not be used in production.
    Args:
        cursor: an Apache Hive cursor object
        table_name: name of the table to insert data into
        record_count: number of records to insert
    """
    cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INT,
            name STRING,
            age INT,
            created_at TIMESTAMP CURRENT_TIMESTAMP
        )
        ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
    """)

    for count in range(record_count):
        # Generate random data and insert
        index = count
        name = f"Name_{count}"
        age = random.randint(18, 70)
        created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(f"INSERT INTO TABLE people VALUES ({index}, '{name}', {age}, '{created_at}')")

    log.info(f"Inserted {record_count} dummy records into {table_name}")


def fetch_and_upsert_data(cursor, table_name: str, state: dict, batch_size: int = 1000):
    """
    Fetches records from Apache Hive, upserts them, and updates state.
    Args:
        cursor: an Apache Hive cursor object
        table_name: name of the table to fetch data from
        state: a dictionary that holds the state of the connector
        batch_size: number of records to fetch in each batch
    """
    # Execute the query to fetch data.
    # You can modify the query to suit your needs.
    hive_query = "SELECT * FROM people"
    cursor.execute(hive_query)

    # Get column names from the cursor description
    columns = [desc[0] for desc in cursor.description]

    # upsert the data into the destination table in batches
    # The batch size can be adjusted based on your requirements
    # The batch sizes ensures that the entire data is not loaded into the memory
    # This is important for large datasets as it prevents memory overflow errors.
    while True:
        # Fetch a batch of rows
        rows = cursor.fetchmany(batch_size)
        if not rows:
            # No more rows to fetch, exit the loop
            break
        for row in rows:
            # Process each row and upsert it into the destination table
            # Each record should be in the form of a dictionary where the keys are the column names
            row_data = dict(zip(columns, row))
            # Upsert the row data into the destination table
            yield op.upsert(table=table_name, data=row_data)

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
    for key in ['hostname', 'username', 'password','port']:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value : {key}")

    return [
        {
            "table": "people",
            "primary_key": ["id"],
            "columns": {
                "id": "INT",
                "name": "STRING",
                "age": "INT",
                "created_at": "UTC_DATETIME"
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
    # Create a Apache Hive session
    connection = create_hive_connection(configuration)
    cursor = connection.cursor()
    table_name = "people"

    # Insert dummy data for testing purposes
    # This method is used only for testing purposes and will not be used in production.
    insert_dummy_data(cursor=cursor, table_name=table_name, record_count=10)

    # Fetch new rows from Apache Hive and upsert them
    yield from fetch_and_upsert_data(cursor=cursor, table_name=table_name, state=state, batch_size=1000)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", 'r') as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)