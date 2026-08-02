# This is an example showing different file streaming approaches for the fivetran_connector_sdk.
# It demonstrates when to use HTTP response streams, BytesIO, and custom readers based on file characteristics.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details.

# Import required classes from fivetran_connector_sdk
# For supporting Connector operations like Update() and Schema()
from fivetran_connector_sdk import Connector

# For enabling Logs in your connector code
from fivetran_connector_sdk import Logging as log

# For supporting Data operations like Upsert() and checkpoint()
from fivetran_connector_sdk import Operations as op

# For uploading files alongside metadata rows
from fivetran_connector_sdk import FileUpload

# For working with in-memory file streams
import io

# For timestamp generation
from datetime import datetime, timezone

# For making HTTP requests to fetch files from public URLs
import requests

# For generating unique file IDs
import hashlib

REQUEST_TIMEOUT = 30


def schema(configuration: dict) -> list:
    """
    Define the schema function which lets you configure the schema your connector delivers.
    See the technical reference documentation for more details on the schema function:
    https://fivetran.com/docs/connector-sdk/technical-reference/connector-sdk-code/connector-sdk-methods#schema

    When you upload files using FileUpload, Fivetran automatically adds a
    _fivetran_file_path column that stores the path from FileUpload.path.
    You DON'T need to define _fivetran_file_path in columns - it's automatic!

    Args:
        configuration: a dictionary that holds the configuration settings for the connector.
    """
    return [
        {
            "table": "files",
            "primary_key": ["file_id"],
            "columns": {
                "file_id": "STRING",
                "file_name": "STRING",
                "file_type": "STRING",
                "file_size_bytes": "INT",
            },
        }
    ]


def compute_file_id(file_name: str) -> str:
    """
    Generate unique file ID from filename.

    Args:
        file_name: Filename

    Returns:
        SHA256 hash of the filename (first 12 chars)
    """
    return hashlib.sha256(file_name.encode()).hexdigest()[:12]


class CustomBufferedReader:
    """
    Custom buffered reader implementing read(size) method.

    Demonstrates how to create a custom stream class that implements
    the required read(size) -> bytes method for FileUpload.
    """

    def __init__(self, data: bytes, chunk_size: int = 16384):
        self.data = data
        self.chunk_size = chunk_size
        self.position = 0

    def read(self, size: int = -1) -> bytes:
        """
        Read up to size bytes from the stream.

        Args:
            size: Number of bytes to read (-1 means read all remaining)

        Returns:
            Bytes read from current position
        """
        if size == -1:
            # Read all remaining data
            result = self.data[self.position :]
            self.position = len(self.data)
            return result
        else:
            # Read up to size bytes
            result = self.data[self.position : self.position + size]
            self.position += len(result)
            return result


def update(configuration: dict, state: dict):
    """
    Define the update function, which is a required function, and is called by Fivetran during each sync.
    See the technical reference documentation for more details on the update function
    https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update

    Demonstrates different file streaming approaches (HTTP response.raw, BytesIO, custom reader).

    Args:
        configuration: A dictionary containing connection details
        state: A dictionary containing state information from previous runs
        The state dictionary is empty for the first sync or for any full re-sync
    """
    log.warning("Example: QuickStart Examples - Stream Examples")

    # ==========================================================================
    # APPROACH 1: HTTP Response Streaming (response.raw)
    # ==========================================================================
    log.info("\nAPPROACH 1: HTTP Response Streaming (response.raw)")
    log.info("-" * 80)

    try:
        # Fetch a PDF file from Mozilla's PDF.js test suite
        pdf_url = (
            "https://raw.githubusercontent.com/mozilla/pdf.js/master/test/pdfs/tracemonkey.pdf"
        )

        log.info(f"Fetching PDF file from: {pdf_url}")
        response = requests.get(pdf_url, stream=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        # IMPORTANT: Set decode_content=True to handle compressed responses
        response.raw.decode_content = True

        file_size = int(response.headers.get("content-length", 0))
        log.info(f"File size: {file_size} bytes")

        # Upload using response.raw directly (implements read(size) -> bytes)
        op.upsert(
            table="files",
            data={
                "file_id": compute_file_id("tracemonkey.pdf"),
                "file_name": "tracemonkey.pdf",
                "file_type": "pdf",
                "file_size_bytes": file_size,
            },
            file=FileUpload(path="streams/pdf/tracemonkey.pdf", stream=response.raw),
        )

        log.info("✓ PDF file uploaded using response.raw streaming")

    except Exception as e:
        log.error(f"Approach 1 failed: {e}")
        raise

    # ==========================================================================
    # APPROACH 2: BytesIO Streaming
    # ==========================================================================
    log.info("\nAPPROACH 2: BytesIO Streaming")
    log.info("-" * 80)

    try:
        # Fetch a CSV file from Seaborn datasets
        csv_url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv"

        log.info(f"Fetching CSV file from: {csv_url}")
        response = requests.get(csv_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        # Wrap response content in BytesIO
        file_bytes = io.BytesIO(response.content)
        file_size = len(response.content)
        log.info(f"File size: {file_size} bytes")

        # Upload using BytesIO with expected_bytes for validation
        op.upsert(
            table="files",
            data={
                "file_id": compute_file_id("iris.csv"),
                "file_name": "iris.csv",
                "file_type": "csv",
                "file_size_bytes": file_size,
            },
            file=FileUpload(
                path="streams/csv/iris.csv",
                stream=file_bytes,
                expected_bytes=file_size,  # Optional: Fivetran validates the file size
            ),
        )

        log.info("✓ CSV file uploaded using BytesIO streaming")

    except Exception as e:
        log.error(f"Approach 2 failed: {e}")
        raise

    # ==========================================================================
    # APPROACH 3: Custom Reader Streaming
    # ==========================================================================
    log.info("\nAPPROACH 3: Custom Reader Streaming")
    log.info("-" * 80)

    try:
        # Fetch a JSON file from Vega datasets
        json_url = "https://raw.githubusercontent.com/vega/vega-datasets/master/data/cars.json"

        log.info(f"Fetching JSON file from: {json_url}")
        response = requests.get(json_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()

        file_content = response.content
        file_size = len(file_content)
        log.info(f"File size: {file_size} bytes")

        # Create custom reader with 16KB chunk size
        buffered_reader = CustomBufferedReader(file_content, chunk_size=16384)

        # Upload using custom reader (implements read(size) -> bytes)
        op.upsert(
            table="files",
            data={
                "file_id": compute_file_id("cars.json"),
                "file_name": "cars.json",
                "file_type": "json",
                "file_size_bytes": file_size,
            },
            file=FileUpload(
                path="streams/json/cars.json", stream=buffered_reader, expected_bytes=file_size
            ),
        )

        log.info("✓ JSON file uploaded using custom reader streaming")

    except Exception as e:
        log.error(f"Approach 3 failed: {e}")
        raise

    # ==========================================================================
    # STATE MANAGEMENT
    # ==========================================================================
    log.info("\nSTATE MANAGEMENT")
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

    log.info("✓ Checkpoint saved - streaming approaches demonstration complete")


# This creates the connector object that will use the update and schema functions defined in this connector.py file.
connector = Connector(update=update, schema=schema)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()
