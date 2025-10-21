"""RavenDB Documents Connector for Fivetran - fetches document data from RavenDB collections.
This connector demonstrates how to fetch document data from RavenDB NoSQL database and sync it to Fivetran using the RavenDB Python client.
See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details
"""

# For reading configuration from a JSON file
import json

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# Import the RavenDB Python SDK for connecting to RavenDB database
from ravendb import DocumentStore

# For handling dates and type hints
from datetime import datetime
from typing import Optional, List, Dict, Any

# For handling the deployment-safe base64-encoded certificate
import base64
import tempfile
import os

# Checkpoint every 1000 records
__CHECKPOINT_INTERVAL = 1000
__DEFAULT_BATCH_SIZE = 100
__DEFAULT_COLLECTION_NAME = "Orders"
__EARLIEST_TIMESTAMP = "1990-01-01T00:00:00.0000000Z"


def validate_configuration(configuration: dict):
    """
    Validate the configuration dictionary to ensure it contains all required parameters.
    This function is called at the start of the update method to ensure that the connector has all necessary configuration values.
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    Raises:
        ValueError: if any required configuration parameter is missing.
    """

    # Validate required configuration parameters
    required_configs = ["ravendb_urls", "database_name", "certificate_base64"]
    for key in required_configs:
        if key not in configuration:
            raise ValueError(f"Missing required configuration value: {key}")


def flatten_dict(data: dict, prefix: str = "", separator: str = "_") -> dict:
    """
    Flatten a nested dictionary by concatenating keys with a separator.
    This is used to convert nested JSON documents into flat table structures.

    Args:
        data: The dictionary to flatten
        prefix: Prefix to add to keys (used for recursion)
        separator: Separator to use between nested keys

    Returns:
        A flattened dictionary
    """
    flattened = {}

    for key, value in data.items():
        # Skip RavenDB metadata fields that start with @
        if key.startswith("@"):
            continue

        new_key = f"{prefix}{separator}{key}" if prefix else key

        if isinstance(value, dict):
            # Recursively flatten nested dictionaries
            flattened.update(flatten_dict(value, new_key, separator))
        elif isinstance(value, list):
            # Convert lists to JSON strings for storage
            flattened[new_key] = json.dumps(value)
        else:
            flattened[new_key] = value

    return flattened


def schema(configuration: dict):
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """

    # Get collection name from configuration or use default
    collection_name = configuration.get("collection_name", "Orders")

    return [
        {
            "table": collection_name.lower(),  # Name of the table in the destination, required.
            "primary_key": ["Id"],  # Primary key column(s) for the table, optional.
            "columns": {  # Definition of columns and their types, optional.
                "Id": "STRING",  # RavenDB document ID is a string
            },  # For any columns whose names are not provided here, their data types will be inferred
        },
    ]


def create_document_store(configuration: dict) -> DocumentStore:
    """Create and initialize a RavenDB DocumentStore using a base64-encoded PEM certificate.

    Deployment note:
    In the Fivetran runtime you cannot rely on local file paths being present. The
    certificate is therefore supplied as a base64 string in configuration, decoded
    at runtime, written to a transient temp PEM file, and referenced by the RavenDB
    client. We never persist the certificate beyond process lifetime.

    Args:
        configuration: Connector configuration dict containing: ravendb_urls, database_name, certificate_base64.
    Returns:
        An initialized DocumentStore instance ready for queries.
    """
    ravendb_urls = configuration.get("ravendb_urls")
    database_name = configuration.get("database_name")
    certificate_base64 = configuration.get("certificate_base64")

    temp_cert_path: Optional[str] = None

    try:
        if not ravendb_urls:
            raise ValueError("ravendb_urls is required")
        if not database_name:
            raise ValueError("database_name is required")
        if not certificate_base64:
            raise ValueError("certificate_base64 is required for RavenDB Cloud connection")

        # Support comma-separated URLs for cluster compatibility
        urls = [u.strip() for u in ravendb_urls.split(",") if u.strip()]
        if not urls:
            raise ValueError("No valid RavenDB URLs provided")

        log.info("Decoding base64 certificate")
        try:
            cert_bytes = base64.b64decode(certificate_base64)
        except Exception as decode_err:
            raise ValueError(f"Failed to decode base64 certificate: {decode_err}")

        # Write certificate bytes directly to temp PEM file
        temp_file = tempfile.NamedTemporaryFile(mode="wb", suffix=".pem", delete=False)
        temp_file.write(cert_bytes)
        temp_file.flush()
        temp_file.close()
        temp_cert_path = temp_file.name
        log.info(f"Temporary PEM certificate written to {temp_cert_path}")

        store = DocumentStore(urls=urls, database=database_name)
        store.certificate_pem_path = temp_cert_path
        store.initialize()
        log.info(f"DocumentStore initialized for database '{database_name}'")
        return store

    except Exception as e:
        if temp_cert_path and os.path.exists(temp_cert_path):
            try:
                os.unlink(temp_cert_path)
                log.info("Cleaned up temporary certificate after failure")
            except Exception as cleanup_err:
                log.warning(f"Failed to remove temporary certificate file: {cleanup_err}")
        log.severe(f"Failed to create RavenDB DocumentStore: {e}")
        raise RuntimeError(f"Failed to create RavenDB DocumentStore: {e}")


def fetch_documents_batch(
    store: DocumentStore,
    collection_name: str,
    last_modified: Optional[str] = None,
    skip: int = 0,
    take: int = __DEFAULT_BATCH_SIZE,
) -> tuple[List[Dict[str, Any]], bool]:
    """
    Fetch a batch of documents from RavenDB collection where LastModified is greater than the last sync time.
    Uses skip/take pagination to handle large collections efficiently.
    Data is explicitly ordered by LastModified to ensure consistent incremental sync.

    Args:
        store: The RavenDB DocumentStore instance.
        collection_name: The name of the collection to fetch data from.
        last_modified: The last modified timestamp from the previous sync.
        skip: Number of records to skip (for pagination).
        take: Number of records to fetch in this batch.
    Returns:
        A tuple containing (list of documents sorted by LastModified, has_more_data boolean).
    """
    try:
        with store.open_session() as session:
            # Build RQL query to get documents from specific collection
            # RavenDB groups documents into collections based on ID prefix (e.g., "Orders/1-A")
            rql = f"""
                from '{collection_name}'
            """

            # Add filter for incremental sync if last_modified provided
            if last_modified:
                rql += f" where @metadata.'@last-modified' > '{last_modified}'"

            # Add ordering for consistent results
            rql += " order by @metadata.'@last-modified'"

            # Add pagination
            rql += f" limit {skip}, {take}"

            # Execute raw RQL query
            results = list(session.advanced.raw_query(rql, object_type=dict))

            # Extract documents with their metadata
            documents = []
            for doc in results:
                # Get metadata from session
                metadata = session.advanced.get_metadata_for(doc)

                # Add Id and LastModified from metadata to document
                if metadata:
                    doc["Id"] = metadata.get("@id", "")
                    doc["LastModified"] = metadata.get("@last-modified", "")

                documents.append(doc)

            log.info(
                f"Fetched batch: {len(documents)} documents from collection: {collection_name}, "
                f"skip: {skip}, take: {take}"
            )

            # Check if there might be more data
            has_more_data = len(documents) == take

            return documents, has_more_data

    except Exception as e:
        log.severe(f"Failed to fetch batch from collection {collection_name} at skip {skip}: {e}")
        raise RuntimeError(
            f"Failed to fetch batch from collection {collection_name} at skip {skip}: {str(e)}"
        )


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """

    log.warning("Example: Database Examples: RavenDB Document Sync")

    # Validate the configuration to ensure it contains all required values.
    validate_configuration(configuration=configuration)

    # Create RavenDB DocumentStore
    store = create_document_store(configuration)

    try:
        # Get configuration parameters
        collection_name = configuration.get("collection_name", __DEFAULT_COLLECTION_NAME)
        batch_size = int(configuration.get("batch_size", __DEFAULT_BATCH_SIZE))

        # Get the state variable for the sync
        last_modified = state.get("last_modified", __EARLIEST_TIMESTAMP)
        new_last_modified = last_modified

        row_count = 0
        batch_count = 0
        skip = 0
        has_more_data = True

        log.info(f"Starting batch processing with batch size: {batch_size}")

        # Process data in batches using skip/take pagination
        while has_more_data:
            batch_count += 1
            log.info(f"Processing batch {batch_count}, skip: {skip}")

            # Fetch document batch from RavenDB
            documents, has_more_data = fetch_documents_batch(
                store, collection_name, last_modified, skip, batch_size
            )

            if not documents:
                log.info("No more documents to process")
                break

            # Process each document in the current batch
            batch_row_count = 0
            for document in documents:
                # Flatten nested document structure
                flattened_doc = flatten_dict(document)

                # The 'upsert' operation is used to insert or update data in the destination table.
                op.upsert(table=collection_name.lower(), data=flattened_doc)
                row_count += 1
                batch_row_count += 1

                # Update new_last_modified with the current document's LastModified
                doc_last_modified = document.get("LastModified")
                if doc_last_modified:
                    new_last_modified = doc_last_modified

            # Checkpoint periodically
            if row_count % __CHECKPOINT_INTERVAL == 0:
                save_state(new_last_modified)

            log.info(
                f"Completed batch {batch_count}: processed {batch_row_count} documents, "
                f"total processed: {row_count}, last modified: {new_last_modified}"
            )

            # Update skip for next batch
            skip += batch_size

        # Final checkpoint
        save_state(new_last_modified)

        log.info(f"Successfully synced {row_count} documents across {batch_count} batches")

    except Exception as e:
        # In case of an exception, raise a runtime error
        raise RuntimeError(f"Failed to sync data: {str(e)}")
    finally:
        # Always close the document store
        if store:
            store.close()


def save_state(new_last_modified):
    """
    Save the current sync state by checkpointing.
    Args:
        new_last_modified: The latest LastModified timestamp processed.
    """
    new_state = {"last_modified": new_last_modified}
    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    op.checkpoint(new_state)


# Create the connector object using the schema and update functions
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Open the configuration.json file and load its contents
    with open("configuration.json", "r") as f:
        configuration = json.load(f)

    # Test the connector locally
    connector.debug(configuration=configuration)
