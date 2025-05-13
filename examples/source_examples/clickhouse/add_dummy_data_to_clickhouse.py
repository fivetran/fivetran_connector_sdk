# Import necessary libraries
import datetime
import random
from fivetran_connector_sdk import Logging as log


def create_dummy_table(client, database_name, table_name):
    """
    Create a dummy database and dummy table in ClickHouse
    This method is used for testing purposes.
    In production, you would typically not create a database or table.
    Args:
        client: a ClickHouse client object
        database_name: the name of the database to create
        table_name: the name of the table to create
    Returns:
        database_name: the name of the created database
        table_name: the name of the created table
    """
    # Create a dummy database
    client.command(f"CREATE DATABASE IF NOT EXISTS {database_name}")
    log.info(f"Database '{database_name}' created or already exists")

    # Create a table
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
        id UInt32,
        name String,
        value Float64,
        created_at DateTime
    ) ENGINE = MergeTree() ORDER BY id
    """
    client.command(create_table_query)
    log.info(f"Table '{database_name}.{table_name}' created")


def insert_dummy_data(client, database_name, table_name):
    """
    Insert dummy data into the specified table
    This method is used for testing purposes.
    In production, you would typically not insert dummy data.
    Args:
        client: a ClickHouse client object
        database_name: the name of the database
        table_name: the name of the table
    """
    # Create the dummy database and table
    create_dummy_table(client, database_name, table_name)

    data = []
    # Inserting 1000 rows of data in dummy table
    for index in range(1000):
        data.append([
            index,
            f"item_{index}",
            round(random.uniform(1.0, 1000.0), 2),
            (datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 30)))
        ])

    client.insert(f"{database_name}.{table_name}", data, column_names=["id", "name", "value", "created_at"])
    log.info(f"Inserted {len(data)} records in {database_name}.{table_name} successfully.")