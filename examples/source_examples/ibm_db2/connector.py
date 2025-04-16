# This is a simple example for how to work with the fivetran_connector_sdk module.
# It defines an `update` method, which upserts data from an IBM DB2 database.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector  # For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Logging as log  # For enabling Logs in your connector code
from fivetran_connector_sdk import Operations as op  # For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()

# Import the ibm_db module for connecting to DB2
import ibm_db
import json


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    # Check if the configuration dictionary has all the required keys
    required_keys = ["hostname", "port", "database", "user_id", "password"]
    for key in required_keys:
        if key not in configuration:
            raise ValueError(f"Missing required configuration key: {key}")

    return [
        {
            "table": "sample_table",
            "primary_key": ["product_id"]
        }
    ]


# This method is used to create a connection string for the IBM DB2 database.
# This takes the configuration dictionary as an argument and extracts the necessary parameters to create the connection string.
# The connection string is used to establish a connection to the database.
def get_connection_string(configuration: dict):
    # Extract the necessary parameters from the configuration dictionary
    hostname = configuration.get("hostname")
    port = configuration.get("port")
    database = configuration.get("database")
    user_id = configuration.get("user_id")
    password = configuration.get("password")

    # return the connection string
    return (
        f"DATABASE={database};"
        f"HOSTNAME={hostname};"
        f"PORT={port};"
        f"PROTOCOL=TCPIP;"
        f"UID={user_id};"
        f"PWD={password};"
    )


# This method is used to establish a connection to the IBM DB2 database.
# It takes the configuration dictionary as an argument and uses the get_connection_string method to create the connection string.
# It then attempts to connect to the database using the ibm_db module.
# If the connection is successful, it returns the connection object.
# If the connection fails, it raises a RuntimeError with the error message.
def connect_to_db(configuration: dict):
    # Get the connection string
    conn_str = get_connection_string(configuration)

    # Connect to the database
    try:
        conn = ibm_db.connect(conn_str, "", "")
        log.info("Connected to database successfully!")
        return conn
    except Exception as e:
        log.severe(f"Connection failed: {e}")
        raise RuntimeError("Connection failed", e)


# This method is used to create a sample data set to be inserted into the database.
# It returns a list of dictionaries, where each dictionary represents a product
# This is a test data to successfully run the connector.
# This method should be removed in production code.
def sample_data_to_insert():
    products = [
        {
            "id": 1,
            "name": "Smartphone XS",
            "desc": "Latest smartphone with advanced camera and long battery life. Features include 5G connectivity, water resistance, and a powerful processor.",
            "price": 799.99,
            "stock": 120,
            "release": '2024-09-15',
            "updated": '2025-04-01 10:30:00',
            "featured": True
        },
        {
            "id": 2,
            "name": "Laptop Pro",
            "desc": "High-performance laptop for professionals and gamers.",
            "price": 1299.99,
            "stock": 45,
            "release": '2024-07-22',
            "updated": '2025-03-15 14:45:22',
            "featured": True
        },
        {
            "id": 3,
            "name": "Wireless Earbuds",
            "desc": "Noise-cancelling earbuds with crystal clear sound quality.",
            "price": 149.50,
            "stock": 200,
            "release": '2024-12-01',
            "updated": '2025-04-10 09:15:30',
            "featured": False
        },
        {
            "id": 4,
            "name": "Smart Watch",
            "desc": "Health monitoring smartwatch with GPS and fitness tracking capabilities.",
            "price": 249.99,
            "stock": 75,
            "release": '2025-01-10',
            "updated": '2025-04-05 16:20:15',
            "featured": True
        }
    ]

    return products


# This method is used to create a sample table in the database.
# It checks if the table already exists in the database.
# If it does, it skips the creation process.
# If it doesn't, it creates a new table named "products" with various data types.
# This is a test method to successfully run the connector.
# This method should be removed in production code.
# This is because, in production code, the connector should be able to work with any existing table in the database.
def create_sample_table_into_db(conn):
    # Check if the table already exists
    check_table_sql = """
        SELECT 1 FROM SYSCAT.TABLES 
        WHERE TABSCHEMA = CURRENT_SCHEMA 
        AND TABNAME = 'PRODUCTS'
        """

    stmt = ibm_db.exec_immediate(conn, check_table_sql)
    result = ibm_db.fetch_tuple(stmt)

    if result:
        log.info("Table 'products' already exists, skipping creation")
    else:
        # Create a table with multiple data types if it doesn't exist
        create_table_sql = """
            CREATE TABLE products (
                product_id INTEGER NOT NULL PRIMARY KEY,
                product_name VARCHAR(100) NOT NULL,
                description CLOB,
                price DECIMAL(10,2) NOT NULL,
                in_stock SMALLINT NOT NULL,
                release_date DATE,
                last_updated TIMESTAMP,
                is_featured BOOLEAN
            )
            """

        # Create the table and commit the changes
        ibm_db.exec_immediate(conn, create_table_sql)
        ibm_db.commit(conn)
        log.info("Table 'products' created successfully")


# This method is used to insert sample data into the "products" table in the database.
# It first creates the sample table using the create_sample_table_into_db method.
# Then it prepares an SQL statement to insert data into the table.
# It iterates over the sample data and binds the parameters to the SQL statement.
# Finally, it executes the statement for each product and commits the changes to the database.
# This is a test method to successfully run the connector.
# This method should be removed in production code.
# This is because, in production code, the connector should be able to work with any existing table in the database.
def insert_sample_data_into_table(conn):
    create_sample_table_into_db(conn)
    insert_sql = """
            INSERT INTO products (product_id, product_name, description, price, in_stock, 
                                release_date, last_updated, is_featured)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """

    stmt = ibm_db.prepare(conn, insert_sql)
    products = sample_data_to_insert()
    for product in products:
        ibm_db.bind_param(stmt, 1, product["id"])
        ibm_db.bind_param(stmt, 2, product["name"])
        ibm_db.bind_param(stmt, 3, product["desc"])
        ibm_db.bind_param(stmt, 4, product["price"])
        ibm_db.bind_param(stmt, 5, product["stock"])
        ibm_db.bind_param(stmt, 6, product["release"])
        ibm_db.bind_param(stmt, 7, product["updated"])
        ibm_db.bind_param(stmt, 8, product["featured"])
        ibm_db.execute(stmt)

    ibm_db.commit(conn)
    log.info(f"Inserted {len(products)} rows into 'products' table successfully")


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: Source Examples - IBM DB2")

    # Connect to the IBM DB2 database
    conn = connect_to_db(configuration)

    # Insert sample data into the products table
    # This is a test method to successfully run the connector.
    # This method creates a sample table and inserts sample data into it.
    # This data will be fetched from the table and upserted using the fivetran_connector_sdk module.
    # This method should be removed in production code.
    insert_sample_data_into_table(conn)

    # Fetch data from the products table and upsert
    count = 0

    # Load the state from the state dictionary
    last_updated = state.get("last_updated", "1990-01-01")

    # The SQL query to select all records from the products table
    sql = f"SELECT * FROM products WHERE last_updated > '{last_updated}'"
    # Execute the SQL query
    stmt = ibm_db.exec_immediate(conn, sql)
    # Fetch the first record from the result set
    # The ibm_db.fetch_assoc method fetches the next row from the result set as a dictionary
    dictionary = ibm_db.fetch_assoc(stmt)
    # Iterate over the result set and upsert each record until there are no more records
    while dictionary:
        # The yield statement returns a generator object.
        # This generator will yield an upsert operation to the Fivetran connector.
        # The op.upsert method is called with two arguments:
        # - The first argument is the name of the table to upsert the data into, in this case, "products".
        # - The second argument is a dictionary containing the data to be upserted,
        yield op.upsert(table="products", data=dictionary)
        # Update the state with the last updated timestamp
        # This is important for ensuring that the sync process can resume from the correct position in case of next sync or interruptions
        last_updated = dictionary.get("last_updated", last_updated)
        count += 1
        dictionary = ibm_db.fetch_assoc(stmt)

    # commit the changes to the database, if any
    ibm_db.commit(conn)
    log.info(f"upserted {count} records from the products table")

    # Close the database connection after the operation is complete
    if 'conn' in locals() and conn:
        ibm_db.close(conn)
        log.info("Connection closed")

    # update the state with the last updated timestamp
    # This state is used to keep track of the last updated timestamp for the next sync
    # This will be checkpointed using op.checkpoint() method
    state = {
        "last_updated": last_updated
    }

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    yield op.checkpoint(state)


# This creates the connector object that will use the update function defined in this connector.py file.
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
    connector.debug()
