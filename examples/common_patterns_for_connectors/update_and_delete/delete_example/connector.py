# This is a simple example for how to work with the fivetran_connector_sdk module.
# It shows the delete operations you can use when the primary key is composite.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

import json
from typing import Dict, List, Any
import psycopg2
import psycopg2.extras

from fivetran_connector_sdk import Connector # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()


# Define the PostgresClient class to handle database operations.
class PostgresClient:
    def __init__(self, config):
        self.host = config.get("HOST")
        self.port = config.get("PORT")
        self.database = config.get("DATABASE")
        self.user = config.get("USERNAME")
        self.password = config.get("PASSWORD")
        self.connection = self.connect() # Connect to the database and return the connection object.
        self.push_sample_data() # Push sample data to the database.

    def connect(self):
        try:
            return psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
        except Exception as e:
            raise ConnectionError(f"Error connecting to PostgreSQL database: {e}")

    def disconnect(self) -> None:
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute("DROP TABLE IF EXISTS sample_table")
            self.connection.commit()
            log.info("Dropped sample table from database.")

            self.connection.close()
            log.info("Database connection closed.")

    def push_sample_data(self) -> None:
        try:
            if not self.connection:
                self.connect()

            cursor = self.connection.cursor()
            cursor.execute("""CREATE TABLE IF NOT EXISTS sample_table (id INT, name VARCHAR(255), department_id INT, address VARCHAR(255), _fivetran_deleted BOOLEAN DEFAULT FALSE)""")
            log.info("sample_table created in PostgreSQL.")

            cursor.execute("INSERT INTO sample_table (id, name, department_id, address) VALUES (1, 'John', 1, '123 Main St')")
            cursor.execute("INSERT INTO sample_table (id, name, department_id, address) VALUES (1, 'John', 2, '123 Main St')")
            cursor.execute("INSERT INTO sample_table (id, name, department_id, address) VALUES (2, 'Jane', 3, '456 Elm St')")
            cursor.execute("INSERT INTO sample_table (id, name, department_id, address) VALUES (3, 'Alice', 3, '789 Oak St')")
            cursor.execute("INSERT INTO sample_table (id, name, department_id, address) VALUES (3, 'Alice', 1, '789 Oak St')")
            cursor.execute("INSERT INTO sample_table (id, name, department_id, address) VALUES (4, 'Bob', 3, '1012 Pine St')")

            self.connection.commit()
            cursor.close()
            log.info("Sample data inserted into sample_table.")
        except Exception as e:
            raise ValueError(f"Error pushing sample data to PostgreSQL: {e}")

    def fetch_data(self, query: str) -> List[Dict[str, Any]]:
        try:
            if not self.connection:
                self.connect()

            cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()

            return [dict(row) for row in result]
        except Exception as e:
            raise ValueError(f"Error fetching data from PostgreSQL: {e}")


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    # Check if the credentials for connecting to database is present in the configuration.
    cred = ["HOST", "DATABASE", "USERNAME", "PASSWORD","PORT"]
    for key in cred:
        if key not in configuration:
            raise ValueError(f"Missing required configuration: {key}")

    return [
        {
            "table": "sample_table",  # Name of the table in the destination.
            "primary_key": ["id","department_id"],  # Primary key column(s) for the table.
            # The primary key is a composite key consisting of two columns: id and department_id.
            # No columns are defined, meaning the types will be inferred.
        }
    ]


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary containing any secrets or payloads you configure when deploying the connector.
# - state: a dictionary containing the state checkpointed during the prior sync.
#   The state dictionary is empty for the first sync or for any full re-sync.
def update(configuration: dict, state: dict):
    log.warning("Example: Delete Example with composite primary key")

    conn = PostgresClient(configuration)
    log.info("Connected to PostgreSQL database.")

    try:
        query = "SELECT * FROM sample_table"
        log.info(f"Executing query: {query}")
        records = conn.fetch_data(query)
        log.info(f"Retrieved {len(records)} records from sample_table")

        # upsert each record into the destination table
        for record in records:
            yield op.upsert("sample_table", record)
        yield op.checkpoint(state)

        # CASE 1: Delete using only the primary key 'id'
        log.info("CASE 1: Deleting with only primary key 'id'")
        yield op.delete("sample_table", {"id": 1})
        # This will delete ALL rows with id=1, regardless of department_id
        # In this case, rows (1,1) and (1,2) will both be deleted where (x, y) is (id, department_id)

        # CASE 2: Delete using only the secondary key 'department_id'
        log.info("CASE 2: Deleting with only secondary key 'department_id'")
        yield op.delete("sample_table", {"department_id": 3})
        # This will delete ALL rows with department_id=2, regardless of id
        # In this case, rows (2,3), (3,3), and (4,3) will be deleted where (x, y) is (id, department_id)

        # CASE 3: Delete using the complete composite primary key
        log.info("CASE 3: Deleting with complete composite primary key")
        yield op.delete("sample_table", {"id": 3, "department_id": 1})
        # This will delete ONLY the specific row that matches both conditions
        # In this case, only row (3,1) will be deleted, not (3,3) where (x, y) is (id, department_id)

        yield op.checkpoint(state)

    except Exception as e:
        raise ValueError(f"Error updating records: {e}")

    finally:
        conn.disconnect()


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


# Resulting table after each operation:
# Original table before any deletes:
# ┌─────────────────────────────────────────────────────────────┐
# │ id │ name  │ department_id│ address      │ _fivetran_deleted│
# ├─────────────────────────────────────────────────────────────┤
# │ 1  │ John  │ 1            │ 123 Main St  │ false            │
# │ 1  │ John  │ 2            │ 123 Main St  │ false            │
# │ 2  │ Jane  │ 3            │ 456 Elm St   │ false            │
# │ 3  │ Alice │ 3            │ 789 Oak St   │ false            │
# │ 3  │ Alice │ 1            │ 789 Oak St   │ false            │
# │ 4  │ Bob   │ 3            │ 1012 Pine St │ false            │
# └─────────────────────────────────────────────────────────────┘

# CASE 1: After deleting rows with id=1
# ┌─────────────────────────────────────────────────────────────┐
# │ id │ name  │ department_id│ address      │ _fivetran_deleted│
# ├─────────────────────────────────────────────────────────────┤
# │ 1  │ John  │ 1            │ 123 Main St  │ true             │ <- Marked as deleted
# │ 1  │ John  │ 2            │ 123 Main St  │ true             │ <- Marked as deleted
# │ 2  │ Jane  │ 3            │ 456 Elm St   │ false            │
# │ 3  │ Alice │ 3            │ 789 Oak St   │ false            │
# │ 3  │ Alice │ 1            │ 789 Oak St   │ false            │
# │ 4  │ Bob   │ 3            │ 1012 Pine St │ false            │
# └─────────────────────────────────────────────────────────────┘
# Using only id=1 marks ALL ROWS with id=1 as deleted, regardless of department_id

# CASE 2: After deleting rows with department_id=3
# ┌─────────────────────────────────────────────────────────────┐
# │ id │ name  │ department_id│ address      │ _fivetran_deleted│
# ├─────────────────────────────────────────────────────────────┤
# │ 1  │ John  │ 1            │ 123 Main St  │ true             │
# │ 1  │ John  │ 2            │ 123 Main St  │ true             │
# │ 2  │ Jane  │ 3            │ 456 Elm St   │ true             │ <- Marked as deleted
# │ 3  │ Alice │ 3            │ 789 Oak St   │ true             │ <- Marked as deleted
# │ 3  │ Alice │ 1            │ 789 Oak St   │ false            │
# │ 4  │ Bob   │ 3            │ 1012 Pine St │ true             │ <- Marked as deleted
# └─────────────────────────────────────────────────────────────┘
# Using only department_id=3 marks ALL ROWS with department_id=3 as deleted, regardless of id

# CASE 3: After deleting row with composite key (id=3, department_id=1)
# ┌─────────────────────────────────────────────────────────────┐
# │ id │ name  │ department_id│ address      │ _fivetran_deleted│
# ├─────────────────────────────────────────────────────────────┤
# │ 1  │ John  │ 1            │ 123 Main St  │ true             │
# │ 1  │ John  │ 2            │ 123 Main St  │ true             │
# │ 2  │ Jane  │ 3            │ 456 Elm St   │ true             │
# │ 3  │ Alice │ 3            │ 789 Oak St   │ true             │
# │ 3  │ Alice │ 1            │ 789 Oak St   │ true             │ <- Marked as deleted
# │ 4  │ Bob   │ 3            │ 1012 Pine St │ true             │
# └─────────────────────────────────────────────────────────────┘
# Using both id=3 and department_id=1 marks ONLY SPECIFIC ROW with id=3 and department_id=1 as deleted