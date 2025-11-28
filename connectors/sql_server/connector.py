# This is a simple example for how to work with the fivetran_connector_sdk module.
# This is an example to show how we can sync records from SQL Server Db via Connector SDK.
# You would need to provide your SQL Server Db credentials for this example to work.
# Also you need the driver locally installed on your machine to make 'fivetran debug' work.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import the json module to handle JSON data.
import json

# Import datetime for handling date and time conversions.
from datetime import datetime

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import pyodbc which is used to connect with SQL Server Db
import pyodbc

TIMESTAMP_FORMAT = "%Y-%m-%d"


# Define the schema function which lets you configure the schema your connector delivers.
# See the technical reference documentation for more details on the schema function:
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
# The schema function takes one parameter:
# - configuration: a dictionary that holds the configuration settings for the connector.
def schema(configuration: dict):
    return [
        {
            "table": "employee_details",  # Name of the table in the destination.
            "primary_key": ["employee_id"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "employee_id": "INT",  # String column for the first name.
                "hire_date": "NAIVE_DATE",  # NAIVE_DATE column for the hire_date.
                "salary": "LONG",
                "updated_time": "NAIVE_DATETIME",  # Datetime of row update
            },
        }
    ]


# Establishes a connection to a database using pyodbc.
def connect_to_database(configuration):
    # Define connection parameters
    connection_string = (
        "DRIVER=" + configuration.get("driver") + ";"
        "SERVER=" + configuration.get("server") + ";"
        "DATABASE=" + configuration.get("database") + ";"
        "UID=" + configuration.get("user") + ";"
        "PWD=" + configuration.get("password") + ";"
    )

    try:
        # Establish connection
        conn = pyodbc.connect(connection_string)
        print("Connection established!")
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")
        return None


# Add mock data to your database, for illustration purposes
def setup_db(configuration):
    create_table_sql = """
        CREATE TABLE employee_details (
            employee_id INT IDENTITY(1,1) PRIMARY KEY, -- Auto-incrementing primary key
            first_name NVARCHAR(50) NOT NULL,          -- Employee's first name
            last_name NVARCHAR(50) NOT NULL,           -- Employee's last name
            hire_date DATE NOT NULL,                   -- Date of hire
            salary INT NOT NULL,                       -- Salary
            updated_time DATETIME                      -- Datetime of last update
        );
        """

    insert_data_sql = """
        INSERT INTO employee_details (first_name, last_name, hire_date, salary)
        VALUES
            ('John', 'Doe', '2020-05-15', 55000, '2020-05-15T20:10:00'),
            ('Jane', 'Smith', '2018-03-22', 62000, '2020-05-16T20:10:00'),
            ('Alice', 'Johnson', '2019-07-30', 58000, '2020-05-17T20:10:00'),
            ('Bob', 'Brown', '2021-11-01', 54000, '2020-05-18T20:10:00'),
            ('Charlie', 'Taylor', '2017-06-10', 67000, '2020-05-19T20:10:00'),
            ('Diana', 'Wilson', '2022-01-20', 51000, '2020-05-20T20:10:00'),
            ('Eve', 'Martin', '2015-12-15', 75000, '2020-05-21T20:10:00'),
            ('Frank', 'Moore', '2023-04-05', 52000, '2020-05-22T20:10:00'),
            ('Grace', 'Hall', '2020-09-14', 60000, '2020-05-23T20:10:00'),
            ('Hank', 'Lee', '2021-03-18', 53000, '2020-05-24T20:10:00');
        """

    # Initialize variables
    conn = None
    cursor = None

    try:
        # Establish connection
        conn = connect_to_database(configuration)
        cursor = conn.cursor()

        # Execute the SQL statements
        print("Creating the table...")
        cursor.execute(create_table_sql)
        conn.commit()
        print("Table created successfully.")

        print("Inserting data into the table...")
        cursor.execute(insert_data_sql)
        conn.commit()
        print("Data inserted successfully.")

    except pyodbc.Error as e:
        print(f"Database error: {e}")

    finally:
        # Clean up and close the connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        print("Connection closed.")


# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    log.warning("Example: Source Examples - SQL Server")

    # The 'upsert' operation is used to insert or update data in a table.
    # The op.upsert method is called with two arguments:
    # - The first argument is the name of the table to upsert the data into, in this case, "employee_details".
    # - The second argument is a dictionary containing the data to be upserted,

    # This is not required. This is just for example illustration purposes.
    setup_db(configuration)

    # Fetch records from DB
    last_query = state.get("employee_details", "1970-01-01T00:00:00")
    last_query_dt = datetime.fromisoformat(last_query)
    query = f"SELECT * FROM employee_details WHERE updated_time > {last_query}"  # Replace with your table name

    # Initialize variables
    conn = None
    cursor = None

    try:
        # Connect to your database instance instance.
        conn = connect_to_database(configuration)
        # Create a cursor from the connection
        cursor = conn.cursor()
        cursor.execute(query)

        # Fetch and display results
        print("Query executed successfully. Results:")
        batch_size = 2  # Fetch only few records, process them and then fetch more, ensuring we don't load all the records in memory
        while True:
            # Fetch a batch of records.
            rows = cursor.fetchmany(batch_size)
            if not rows:
                # Exit the loop when there are no more records.
                break

            for row in rows:
                op.upsert(
                    table="employee_details",
                    data={
                        "employee_id": row[0],  # Employee Id.
                        "first_name": row[1],  # First Name.
                        "last_name": row[2],  # Last Name.
                        "hire_date": row[3],  # Hire Date.
                        "salary": row[4],  # Salary.
                        "updated_time": row[5],  # Updated time.
                    },
                )
                if row[5] > last_query_dt:
                    last_query_dt = row[5]
    except pyodbc.Error as e:
        print(f"Error executing query: {e}")
    finally:
        # Clean up and close the connection
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()
        print("Connection closed.")

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
    state["employee_details"] = last_query_dt.isoformat()
    op.checkpoint(state)


# This creates the connector object that will use the update function defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents into a dictionary.
    with open("configuration.json", "r") as f:
        configuration = json.load(f)
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug(configuration=configuration)

# Resulting table:
# ┌───────────────┬─────────────┬─────────────┬─────────────┬───────────────┐
# │ employee_id   │ first_name  │ last_name   │ hire_date   │   salary      │
# │      int      │   varchar   │   varchar   │    date     │     int       │
# ├───────────────┼─────────────┼─────────────┼─────────────┼───────────────┤
# │       1       │    John     │    Doe      │ 2020-05-15  │    55000      │
# │       2       │    Jane     │   Smith     │ 2018-03-22  │    62000      │
# │       3       │    Alice    │  Johnson    │ 2019-07-30  │    58000      │
# │       4       │     Bob     │   Brown     │ 2021-11-01  │    54000      │
# │       5       │   Charlie   │  Taylor     │ 2017-06-10  │    67000      │
# │       6       │    Diana    │  Wilson     │ 2022-01-20  │    51000      │
# │       7       │     Eve     │   Martin    │ 2015-12-15  │    75000      │
# │       8       │    Frank    │   Moore     │ 2023-04-05  │    52000      │
# │       9       │    Grace    │    Hall     │ 2020-09-14  │    60000      │
# │      10       │     Hank    │     Lee     │ 2021-03-18  │    53000      │
# ├───────────────┴─────────────┴─────────────┴─────────────┴───────────────┤
# │ 10 rows                                                   5 columns     │
# └─────────────────────────────────────────────────────────────────────────┘
