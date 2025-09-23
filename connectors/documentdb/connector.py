# This is an example for how to work with the fivetran_connector_sdk module.
# This example demonstrates how to connect to AWS DocumentDB and upsert data queried from DocumentDB collections.
#
# NOTE: Fivetran already provides a native Java-based DocumentDB connector that supports SaaS deployment models.
# This Python-based connector is specifically intended for Hybrid Deployment (HD) scenarios, as the native connector
# does not support Hybrid Deployment. For SaaS deployments, we recommend using the native DocumentDB connector
# available at https://fivetran.com/docs/connectors/databases/documentdb.
#
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import the required classes from the connector SDK
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Operations as op
from fivetran_connector_sdk import Logging as log

# Required to load configuration in debug
import json

# Required for data transformations before upsert
from datetime import timezone
from dateutil import parser

# Import the MongoDB driver for DocumentDB (DocumentDB is MongoDB-compatible)
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


def create_documentdb_connection(configuration: dict):
    """
    Create a connection to AWS DocumentDB
    This function creates a MongoDB client connection to DocumentDB using the pymongo library.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Returns:
        client: a MongoDB client object connected to DocumentDB
    """
    host = configuration.get("hostname")
    port = int(configuration.get("port", 27017))
    username = configuration.get("username")
    password = configuration.get("password")

    # DocumentDB connection string with proper SSL configuration
    connection_string = f"mongodb://{username}:{password}@{host}:{port}/?tls=true&retryWrites=false&DirectConnection=true"

    try:
        # Create MongoDB client without SSL configuration
        client = MongoClient(
            connection_string,
            tls=True,
            tlsAllowInvalidCertificates=True,
            tlsAllowInvalidHostnames=True,
        )

        # Test the connection
        client.admin.command("ping")
        log.info("Connection to DocumentDB successful")
        return client

    except ConnectionFailure as e:
        raise RuntimeError(f"Failed to connect to DocumentDB: {e}")
    except ServerSelectionTimeoutError as e:
        raise RuntimeError(f"DocumentDB server selection timeout: {e}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error connecting to DocumentDB: {e}")


def fetch_documents_with_pagination(collection, last_updated_at: str, batch_size: int = 100):
    """
    Fetches documents from DocumentDB collection using pagination.
    Args:
        collection: MongoDB collection object
        last_updated_at: ISO timestamp string for incremental sync
        batch_size: Number of documents to fetch per batch
    Returns:
        Generator of fetched documents
    """
    try:
        # Convert timestamp string to datetime object
        timestamp_obj = parser.parse(last_updated_at)
        if timestamp_obj.tzinfo is None:
            # Add UTC timezone if missing
            timestamp_obj = timestamp_obj.replace(tzinfo=timezone.utc)

        # Query to fetch documents updated after the last sync time
        # You can modify this query to suit your specific needs
        query = {"updated_at": {"$gt": timestamp_obj}}

        # Sort by updated_at to ensure consistent ordering
        sort_criteria = [("updated_at", 1)]

        # Use find with cursor for efficient pagination
        cursor = collection.find(query).sort(sort_criteria).batch_size(batch_size)

        # Iterate through the cursor and yield each document
        # This approach is memory-efficient for large datasets
        for document in cursor:
            yield document

    except Exception as e:
        raise RuntimeError(f"Error fetching documents from DocumentDB: {e}")


def upsert_documents_from_collection(
    client, database_name: str, collection_name: str, state: dict
):
    """
    Fetches documents from DocumentDB collection, upserts them, and updates state.
    Args:
        client: MongoDB client object
        database_name: Name of the DocumentDB database
        collection_name: Name of the collection to sync
        state: A dictionary containing state information from previous runs
    """
    # Get the database and collection
    db = client[database_name]
    collection = db[collection_name]

    # Get the last sync time from the state
    last_sync = state.get("last_updated_at", "1990-01-01T00:00:00Z")
    max_updated_at = parser.parse(last_sync)
    if max_updated_at.tzinfo is None:
        max_updated_at = max_updated_at.replace(tzinfo=timezone.utc)

    record_count = 0

    # Fetch documents from DocumentDB, prepare them and upsert them
    # This is done in a paginated manner to avoid loading all documents into memory at once
    for document in fetch_documents_with_pagination(collection, last_sync):
        # Ensure updated_at has timezone info before comparison
        doc_updated_at = document.get("updated_at")
        if doc_updated_at and doc_updated_at.tzinfo is None:
            doc_updated_at = doc_updated_at.replace(tzinfo=timezone.utc)

        # Prepare the record for upsert
        # Convert MongoDB ObjectId to string and handle other data transformations
        record = {
            "_id": str(document.get("_id")),
            "name": document.get("name"),
            "email": document.get("email"),
            "status": document.get("status"),
            "created_at": document.get("created_at").replace(tzinfo=timezone.utc),
            "updated_at": doc_updated_at.replace(tzinfo=timezone.utc),
            "metadata": document.get("metadata", {}),
        }

        # Upsert the record into destination and increment the record count
        op.upsert(table=collection_name, data=record)
        record_count += 1

        # Update the max_updated_at with doc_updated_at,
        # as the current document's doc_updated_at is always greater than the last, due to sorting by updated_at
        max_updated_at = doc_updated_at

        # Checkpoint every 1000 records
        # This is useful for large datasets to avoid losing progress in case of failure
        # You can modify this number based on your requirements
        if record_count % 1000 == 0:
            state["last_updated_at"] = max_updated_at.isoformat()
            # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
            # from the correct position in case of next sync or interruptions.
            # Learn more about how and where to checkpoint by reading our best practices documentation
            # (https://fivetran.com/docs/connectors/connector-sdk/best-practices#largedatasetrecommendation).
            op.checkpoint(state)
            log.info(f"{record_count} records processed")

    log.info(f"Total records processed: {record_count}")

    # Checkpoint the last updated_at timestamp
    state["last_updated_at"] = max_updated_at.isoformat()
    op.checkpoint(state)


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    # Check if required configuration values are present in configuration
    for key in ["hostname", "username", "password", "database", "port"]:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")

    return [
        {
            "table": "users",
            "primary_key": ["_id"],
            "columns": {
                "_id": "STRING",
                "name": "STRING",
                "email": "STRING",
                "status": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "metadata": "JSON",
            },
        },
        {
            "table": "orders",
            "primary_key": ["_id"],
            "columns": {
                "_id": "STRING",
                "user_id": "STRING",
                "order_number": "STRING",
                "total_amount": "FLOAT",
                "status": "STRING",
                "created_at": "UTC_DATETIME",
                "updated_at": "UTC_DATETIME",
                "items": "JSON",
            },
        },
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
    # Create a DocumentDB connection
    client = create_documentdb_connection(configuration)
    database_name = configuration.get("database")

    try:
        # IMPORTANT: This connector requires that the following prerequisites exist in your DocumentDB instance:
        # 1. A database named as specified in your configuration
        # 2. Collections named 'users' and 'orders' with documents containing the following fields:
        #    For users collection:
        #    - _id (ObjectId): Primary key
        #    - name (string): User name
        #    - email (string): User email
        #    - status (string): User status
        #    - created_at (datetime): Document creation timestamp
        #    - updated_at (datetime): Document update timestamp
        #    - metadata (object): Additional user metadata
        #
        #    For orders collection:
        #    - _id (ObjectId): Primary key
        #    - user_id (string): Reference to user
        #    - order_number (string): Order identifier
        #    - total_amount (number): Order total
        #    - status (string): Order status
        #    - created_at (datetime): Order creation timestamp
        #    - updated_at (datetime): Order update timestamp
        #    - items (array): Order items
        #
        # Example documents:
        # Users: {"_id": ObjectId(), "name": "John Doe", "email": "john@example.com", "status": "active", "created_at": ISODate(), "updated_at": ISODate(), "metadata": {"source": "web"}}
        # Orders: {"_id": ObjectId(), "user_id": "user123", "order_number": "ORD-001", "total_amount": 99.99, "status": "completed", "created_at": ISODate(), "updated_at": ISODate(), "items": [{"product": "Widget", "quantity": 2}]}
        #
        # If these prerequisites are not met, the connector will not function correctly.

        # Sync users collection
        log.info("Starting sync of users collection")
        upsert_documents_from_collection(
            client=client, database_name=database_name, collection_name="users", state=state
        )

        # Sync orders collection
        log.info("Starting sync of orders collection")
        upsert_documents_from_collection(
            client=client, database_name=database_name, collection_name="orders", state=state
        )

    finally:
        # Close the client connection
        client.close()


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
