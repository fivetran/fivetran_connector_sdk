# This is an example showing different file streaming approaches for the fivetran_connector_sdk.
# It demonstrates when to use HTTP response streams, BytesIO, and custom readers based on file characteristics.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference)
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
    Define schema for file tracking.

    When you upload files using FileUpload, Fivetran automatically adds a
    _fivetran_file_path column that stores the path from FileUpload.path.

    You DON'T need to define _fivetran_file_path in columns - it's automatic!

    Returns:
        List with one table schema
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
    Demonstrate different file streaming approaches.

    This shows three main streaming approaches:
    1. HTTP response.raw - Direct streaming from HTTP responses
    2. BytesIO - In-memory file handling
    3. Custom reader - Fine-grained control over streaming

    IMPORTANT: The file types (PDF, CSV, JSON) are just examples!
    - You can use response.raw for ANY file type (PDF, CSV, JSON, images, etc.)
    - You can use BytesIO for ANY file type
    - You can use CustomReader for ANY file type

    The choice depends on your needs, not the file format:
    - Use response.raw when streaming directly from HTTP
    - Use BytesIO when working with in-memory content
    - Use CustomReader when you need fine-grained control

    Args:
        configuration: Connector configuration
        state: Previous sync state (empty for first run)
    """

    log.info("=" * 80)
    log.info("STREAM EXAMPLES: Different File Streaming Approaches")
    log.info("=" * 80)
    log.info("")
    log.info("NOTE: We use PDF/CSV/JSON as examples, but each streaming approach")
    log.info("      works with ANY file type. Choose based on your use case!")
    log.info("=" * 80)

    # ==========================================================================
    # APPROACH 1: HTTP Response Streaming (response.raw)
    # ==========================================================================
    log.info("\n📄 APPROACH 1: HTTP Response Streaming (response.raw)")
    log.info("   Example: PDF file (but works for ANY file type from HTTP)")
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

        # Upload using response.raw directly
        # response.raw implements the required read(size) -> bytes method
        #
        # After this operation, this will be the state in the destination:
        # | file_id | file_name        | file_type | file_size_bytes | _fivetran_file_path      |
        # |---------|------------------|-----------|-----------------|--------------------------|
        # | abc123  | tracemonkey.pdf  | pdf       | 1016315         | streams/pdf/tracemonkey.pdf |
        #
        # Note: _fivetran_file_path stores the relative path from FileUpload.path
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
    log.info("\n📊 APPROACH 2: BytesIO Streaming")
    log.info("   Example: CSV file (but works for ANY file type in memory)")
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
        # BytesIO implements the required read(size) -> bytes method
        #
        # After this operation, this will be the state in the destination:
        # | file_id | file_name | file_type | file_size_bytes | _fivetran_file_path   |
        # |---------|-----------|-----------|-----------------|----------------------|
        # | def456  | iris.csv  | csv       | 4821            | streams/csv/iris.csv |
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
    log.info("\n🔧 APPROACH 3: Custom Reader Streaming")
    log.info("   Example: JSON file (but works for ANY file type with custom logic)")
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

        # Upload using custom reader
        # CustomBufferedReader implements the required read(size) -> bytes method
        #
        # After this operation, this will be the state in the destination:
        # | file_id | file_name | file_type | file_size_bytes | _fivetran_file_path     |
        # |---------|-----------|-----------|-----------------|-------------------------|
        # | ghi789  | cars.json | json      | 47831           | streams/json/cars.json  |
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
    log.info("\n💾 STATE MANAGEMENT")
    log.info("-" * 80)

    # Update state for incremental syncs
    state["last_updated"] = datetime.now(timezone.utc).isoformat()

    # Checkpoint saves the state for the next sync
    op.checkpoint(state)

    log.info("Checkpoint saved")

    # ==========================================================================
    # SUMMARY
    # ==========================================================================
    log.info("\n" + "=" * 80)
    log.info("STREAMING APPROACHES SUMMARY")
    log.info("=" * 80)
    log.info("")
    log.info("Approaches Demonstrated:")
    log.info("  1. response.raw     → Direct HTTP streaming (example: PDF)")
    log.info("  2. BytesIO          → In-memory handling (example: CSV)")
    log.info("  3. Custom Reader    → Fine-grained control (example: JSON)")
    log.info("")
    log.info("Key Takeaways:")
    log.info("  ✓ All streams MUST implement read(size) -> bytes method")
    log.info("  ✓ File type doesn't dictate the approach - choose by use case:")
    log.info("    • response.raw works for PDF, CSV, JSON, images, any HTTP download")
    log.info("    • BytesIO works for any file you have in memory")
    log.info("    • CustomReader works for any file needing custom buffering")
    log.info("  ✓ _fivetran_file_path stores relative path from FileUpload.path")
    log.info("=" * 80)

    return state


# Create the connector instance
connector = Connector(update=update, schema=schema)


if __name__ == "__main__":
    # Test locally with: fivetran debug
    connector.debug()
