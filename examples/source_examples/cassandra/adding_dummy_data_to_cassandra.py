# Import the required libraries
from fivetran_connector_sdk import Logging as log
import uuid
from datetime import datetime, timezone


def create_dummy_keyspace_and_table(session, keyspace, table_name):
    """
    Create a dummy database and dummy table in Cassandra
    This method is used for testing purposes.
    In production, you would typically not create a database or table.
    Args:
        session: a Cassandra session object
        keyspace: the name of the keyspace to create
        table_name: the name of the table to create
    """
    # Create a sample keyspace
    create_query = f"""CREATE KEYSPACE IF NOT EXISTS {keyspace}"""
    create_configs = """replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"""
    session.execute(f"""{create_query} WITH {create_configs}""")
    session.set_keyspace(keyspace)
    log.info("Keyspace created successfully")

    # Create a sample table
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id UUID PRIMARY KEY,
            name text,
            created_at timestamp
            )"""
        )

    log.info("Created table successfully")


def insert_dummy_data(session, keyspace, table_name, record_count=10):
    """
    Insert dummy data into the specified table
    This method is used for testing purposes.
    In production, you would typically not insert dummy data.
    Args:
        session: a Cassandra session object
        keyspace: the name of the keyspace
        table_name: the name of the table
        record_count: the number of records to insert
    """
    # Create the dummy keyspace and table
    # This method is used for testing purposes and will not be used in production.
    create_dummy_keyspace_and_table(session, keyspace, table_name)

    # Set the keyspace for the session
    session.set_keyspace(keyspace)

    # Prepare the insert statement
    insert_stmt = session.prepare(
        f"INSERT INTO {table_name} (id, name, created_at) VALUES (?, ?, ?)"
    )

    # Insert dummy data into the table
    for index in range(record_count):
        user_id = uuid.uuid4()
        name = f"Users_{index}"
        created_at = datetime.now(timezone.utc)
        session.execute(insert_stmt, (user_id, name, created_at))

    log.info(f"Inserted {record_count} records in table {table_name} and keyspace {keyspace}")

    # Create index on created_at column for efficient querying
    session.execute(f"CREATE INDEX IF NOT EXISTS idx_created_at ON {table_name}(created_at)")
    log.info(f"Created or confirmed index on {table_name}(created_at)")