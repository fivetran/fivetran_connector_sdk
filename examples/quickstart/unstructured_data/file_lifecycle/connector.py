# This is a simple example for how to use FileUpload with the fivetran_connector_sdk
# module to upload unstructured files (PDFs, images, etc.) alongside metadata rows.
# It demonstrates the complete file lifecycle: upload, update, and delete operations.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert(), Update(), Delete() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For uploading files alongside metadata rows
from fivetran_connector_sdk import FileUpload

# For working with in-memory file streams
import io

# For timestamp generation
from datetime import datetime, timezone

# For making HTTP requests to fetch files from public URLs
import requests


def schema(configuration: dict) -> list:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema
    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    # When you upload files using FileUpload, Fivetran automatically adds a
    # _fivetran_file_path column that stores the path from FileUpload.path.
    # You DON'T need to define _fivetran_file_path in columns - it's automatic!
    return [
        {
            "table": "documents",
            "primary_key": ["doc_id"],
            "columns": {
                "doc_id": "STRING",
                "doc_name": "STRING",
                "doc_version": "STRING",
            },
        }
    ]


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#update
    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: QuickStart Examples - File Lifecycle")

    # This example demonstrates file upload, update, and delete lifecycle operations.

    # ==========================================================================
    # PHASE 1: UPLOAD (Create new file)
    # ==========================================================================
    log.info("PHASE 1: UPLOAD - Creating new file")
    log.info("-" * 80)

    try:
        # Fetch a simple PDF from a public URL
        pdf_url = (
            "https://raw.githubusercontent.com/mozilla/pdf.js/master/test/pdfs/tracemonkey.pdf"
        )

        log.info(f"Fetching file from: {pdf_url}")
        response = requests.get(pdf_url, stream=True, timeout=30)
        response.raise_for_status()
        response.raw.decode_content = True

        file_size = int(response.headers.get("content-length", 0))
        log.info(f"File size: {file_size} bytes")

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        # The third argument (file) is the FileUpload object with file details associated with the metadata row.
        op.upsert(
            table="documents",
            data={
                "doc_id": "demo_doc_1",
                "doc_name": "tracemonkey_v1.pdf",
                "doc_version": "v1",
            },
            file=FileUpload(path="documents/v1/tracemonkey.pdf", stream=response.raw),
        )

        log.info("✓ File 1 uploaded successfully")

        log.info("Uploading second document...")
        csv_url_live = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv"
        response_live = requests.get(csv_url_live, stream=True, timeout=30)
        response_live.raise_for_status()
        response_live.raw.decode_content = True

        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        # The third argument (file) is the FileUpload object with file details associated with the metadata row.
        op.upsert(
            table="documents",
            data={
                "doc_id": "demo_doc_2",
                "doc_name": "penguins.csv",
                "doc_version": "v1",
            },
            file=FileUpload(path="documents/v1/penguins.csv", stream=response_live.raw),
        )

        log.info("✓ File 2 uploaded successfully")

    except Exception as e:
        log.error(f"Upload failed: {e}")
        raise

    # ==========================================================================
    # PHASE 2: UPDATE (Replace existing file)
    # ==========================================================================
    log.info("PHASE 2: UPDATE - Replacing existing file")
    log.info("-" * 80)

    try:
        # Fetch a different file (simulating an update)
        csv_url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"

        log.info(f"Fetching new file from: {csv_url}")
        response = requests.get(csv_url, timeout=30)
        response.raise_for_status()

        file_content = response.content
        file_size = len(file_content)
        log.info(f"New file size: {file_size} bytes")

        # Approach 1: Using op.upsert() - Replaces ALL columns
        log.info("Approach 1: Using op.upsert()")
        # The 'upsert' operation is used to insert or update data in the destination table.
        # The first argument is the name of the destination table.
        # The second argument is a dictionary containing the record to be upserted.
        # The third argument (file) is the FileUpload object with file details associated with the metadata row.
        op.upsert(
            table="documents",
            data={
                "doc_id": "demo_doc_1",  # Same primary key triggers update
                "doc_name": "tracemonkey_v2.csv",
                "doc_version": "v2",  # ALL columns must be provided
            },
            file=FileUpload(path="documents/v2/tracemonkey.csv", stream=io.BytesIO(file_content)),
        )

        log.info("✓ File updated with upsert()")

        # Approach 2: Using op.update() - Updates only specified columns (RECOMMENDED)
        log.info("Approach 2: Using op.update() - RECOMMENDED")

        # Fetch another file for the second update
        csv_url2 = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/titanic.csv"
        response2 = requests.get(csv_url2, timeout=30)
        response2.raise_for_status()
        file_content2 = response2.content

        # update() only requires primary key + columns you want to change
        op.update(
            table="documents",
            modified={
                "doc_id": "demo_doc_1",
                "doc_version": "v3",  # Only update version, name stays as v2
            },
            file=FileUpload(path="documents/v3/tracemonkey.csv", stream=io.BytesIO(file_content2)),
        )

        log.info("✓ File updated with update() - only specified columns changed")

    except Exception as e:
        log.error(f"Update failed: {e}")
        raise

    # ==========================================================================
    # PHASE 3: DELETE (Soft-delete)
    # ==========================================================================
    log.info("PHASE 3: DELETE - Soft-delete behavior")
    log.info("-" * 80)

    try:
        # Soft-delete marks the row with _fivetran_deleted = True
        op.delete(table="documents", keys={"doc_id": "demo_doc_1"})

        log.info("✓ Row soft-deleted (_fivetran_deleted = True)")
        log.info("Note: Both row and file remain in the destination")
        log.info("Result: 1 live document (demo_doc_2) + 1 soft-deleted document (demo_doc_1)")

    except Exception as e:
        log.error(f"Delete failed: {e}")
        raise

    # ==========================================================================
    # STATE MANAGEMENT
    # ==========================================================================
    log.info("STATE MANAGEMENT")
    log.info("-" * 80)

    # Update state for incremental syncs
    state["last_updated"] = datetime.now(timezone.utc).isoformat()

    # Save the progress by checkpointing the state. This is important for ensuring that the sync process can resume
    # from the correct position in case of next sync or interruptions.
    # You should checkpoint even if you are not using incremental sync, as it tells Fivetran it is safe to write to destination.
    # For large datasets, checkpoint regularly (e.g., every N records) not only at the end.
    # Learn more about how and where to checkpoint by reading our best practices documentation
    # (https://fivetran.com/docs/connector-sdk/best-practices#optimizingperformancewhenhandlinglargedatasets).
    op.checkpoint(state)

    log.info("✓ Checkpoint saved - file lifecycle demonstration complete")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()
