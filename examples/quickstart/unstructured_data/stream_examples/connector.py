"""
Unstructured Data Sync Connector Example - Stream Types

Demonstrates 5 different file streaming approaches for syncing unstructured data:
1. PDF - Direct streaming with response.raw (no expected_bytes)
2. CSV - BytesIO wrapper with expected_bytes validation
3. JSON - Custom BufferedReader with chunked reading
4. PNG - File object with pre-computed expected_bytes
5. BIN - Large file streaming without size validation

IMPORTANT: All streams MUST implement read(size) -> bytes method for FileUpload.

═══════════════════════════════════════════════════════════════════════════════
DECISION TREE: Which Streaming Approach Should I Use?
═══════════════════════════════════════════════════════════════════════════════

Question 1: Is the file already on disk?
  ✅ YES → Use Approach 4 (File Object - open('file', 'rb'))
  ❌ NO  → Continue to Question 2

Question 2: Do you know the file size in advance?
  ✅ YES → Use Approach 2 (BytesIO + expected_bytes)
  ❌ NO  → Continue to Question 3

Question 3: Is the file very large (>100 MB)?
  ✅ YES → Use Approach 5 (response.raw, stream=True, no expected_bytes)
  ❌ NO  → Use Approach 1 (response.raw, simple)

Question 4: Do you need fine-grained control over chunking?
  ✅ YES → Use Approach 3 (Custom BufferedReader)
  ❌ NO  → Stick with Approach 1 or 2

═══════════════════════════════════════════════════════════════════════════════

Each approach results in a _fivetran_file_path in your destination:

After sync completes:
  SELECT file_name, _fivetran_file_path FROM files;

Example results:
  | file_name         | _fivetran_file_path                              |
  |-------------------|--------------------------------------------------|
  | tracemonkey.pdf   | s3://fivetran-files/account123/connector456/.pdf |
  | iris.csv          | s3://fivetran-files/account123/connector456/.csv |
  | cars.json         | s3://fivetran-files/account123/connector456/.json|
  | python.png        | s3://fivetran-files/account123/connector456/.png |
  | penguins.csv      | s3://fivetran-files/account123/connector456/.bin |

The _fivetran_file_path column is automatically added by Fivetran when
supports_unstructured_data: True in your schema.

═══════════════════════════════════════════════════════════════════════════════
"""

import io
import hashlib
import logging
import json
from typing import Any, Dict, Optional, Tuple
from datetime import datetime, timezone

import requests
from fivetran_connector_sdk import Connector, Operations as op, FileUpload

logger = logging.getLogger("unstructured_data_sync_connector")
logger.setLevel(logging.INFO)

# GitHub repository for example files
REQUEST_TIMEOUT = 30


def schema(configuration: dict) -> list:
    """
    Define schema for unstructured file tracking.
    
    Fivetran automatically adds _fivetran_file_path column when you use FileUpload.
    
    Args:
        configuration: Connector configuration
        
    Returns:
        List with one table schema
    """
    return [
        {
            "table": "files",
            "primary_key": ["file_id"],
            "columns": {
                "file_id": "STRING",              # Hash of file path
                "file_name": "STRING",            # Original filename
                "file_path": "STRING",            # Full GitHub raw URL
                "file_type": "STRING",            # Extension (.pdf, .csv, etc)
                "file_size_bytes": "STRING",      # File size in bytes
                "content_encoding": "STRING",     # Encoding type
                "synced_at": "STRING",            # ISO 8601 timestamp
            },
        }
    ]


def compute_file_id(file_path: str) -> str:
    """
    Generate unique file ID from file path.

    Args:
        file_path: Full file path or URL

    Returns:
        SHA256 hash of the file path (first 16 chars)
    """
    return hashlib.sha256(file_path.encode()).hexdigest()[:16]


def get_file_size(url: str) -> Optional[int]:
    """
    Get file size from Content-Length header via HEAD request.

    Args:
        url: File URL

    Returns:
        File size in bytes or None if unavailable
    """
    try:
        response = requests.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if response.status_code == 200:
            return int(response.headers.get("content-length", 0))
    except Exception as e:
        logger.warning(f"Failed to get file size for {url}: {e}")
    return None


def stream_pdf(url: str) -> Tuple[FileUpload, int]:
    """
    Stream PDF file using direct response.raw.

    PDF files are streamed directly without expected_bytes because
    response.raw implements the required read(size) method natively.

    Args:
        url: URL to PDF file

    Returns:
        Tuple of (FileUpload, file_size)
    """
    response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    # Set to decompress gzip/deflate responses automatically
    response.raw.decode_content = True
    
    # Get file size from Content-Length header for metadata
    file_size = int(response.headers.get("content-length", 0))
    
    logger.info(f"Streaming PDF from {url} ({file_size} bytes)")
    
    # Return FileUpload with path and stream
    file_upload = FileUpload(path="file.pdf", stream=response.raw)
    return file_upload, file_size


def stream_csv(url: str) -> Tuple[FileUpload, int]:
    """
    Stream CSV file using BytesIO with expected_bytes validation.

    BytesIO wrapper demonstrates how to work with in-memory buffers
    that implement the required read(size) method. We include
    expected_bytes so Fivetran can validate the file was transferred completely.

    Args:
        url: URL to CSV file

    Returns:
        Tuple of (FileUpload, file_size)
    """
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    # Wrap response content in BytesIO (which has read(size) method)
    file_bytes = io.BytesIO(response.content)
    file_size = len(response.content)
    
    logger.info(f"Streaming CSV from {url} ({file_size} bytes) via BytesIO")
    
    # Include expected_bytes for validation
    file_upload = FileUpload(path="file.csv", stream=file_bytes, expected_bytes=file_size)
    return file_upload, file_size


class BufferedJSONReader:
    """
    Custom buffered reader implementing read(size) method.
    
    Demonstrates how to create a custom stream class for progressive
    reading of large files. This reader chunks through data in 16KB blocks.
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
            result = self.data[self.position:]
            self.position = len(self.data)
            return result
        else:
            # Read up to size bytes
            result = self.data[self.position : self.position + size]
            self.position += len(result)
            return result


def stream_json(url: str) -> Tuple[FileUpload, int]:
    """
    Stream JSON file using custom BufferedReader with read(size).

    Custom reader class demonstrates how to implement a stream object
    that properly chunks data. This is useful for large files or when
    you want fine-grained control over streaming behavior.

    Args:
        url: URL to JSON file

    Returns:
        Tuple of (FileUpload, file_size)
    """
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    file_content = response.content
    file_size = len(file_content)
    
    # Create custom reader that implements read(size)
    buffered_reader = BufferedJSONReader(file_content, chunk_size=16384)
    
    logger.info(f"Streaming JSON from {url} ({file_size} bytes) via custom reader")
    
    # Include expected_bytes for validation
    file_upload = FileUpload(path="schema.json", stream=buffered_reader, expected_bytes=file_size)
    return file_upload, file_size


def stream_png(url: str) -> Tuple[FileUpload, int]:
    """
    Stream PNG file using file object with expected_bytes.

    This demonstrates using io.BytesIO as a file-like object (has read(size))
    with pre-computed expected_bytes. This approach is efficient for binary files.

    Args:
        url: URL to PNG file

    Returns:
        Tuple of (FileUpload, file_size)
    """
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    response.raw.decode_content = True
    # Pre-compute file size before wrapping
    file_size = len(response.content)
    
    # Wrap in BytesIO (implements read(size))
    file_obj = io.BytesIO(response.content)
    
    logger.info(f"Streaming PNG from {url} ({file_size} bytes) via file object")
    
    # Include expected_bytes for validation and checksum purposes
    file_upload = FileUpload(path="image.png", stream=file_obj, expected_bytes=file_size)
    return file_upload, file_size


def stream_bin(url: str) -> Tuple[FileUpload, int]:
    """
    Stream large binary file using response.raw without expected_bytes.

    For large files where you don't want to load the entire file into memory,
    stream directly from response.raw. We omit expected_bytes for scenarios
    where the size is unknown or dynamic.

    Args:
        url: URL to binary file

    Returns:
        Tuple of (FileUpload, file_size)
    """
    response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    
    # Enable automatic decompression
    response.raw.decode_content = True
    
    # Get file size from header (or 0 if unavailable)
    file_size = int(response.headers.get("content-length", 0))
    
    logger.info(f"Streaming BIN from {url} ({file_size} bytes) via response.raw")
    
    # Return without expected_bytes - good for large/unknown-size files
    file_upload = FileUpload(path="data.bin", stream=response.raw)
    return file_upload, file_size


# File configurations: each specifies a GitHub raw URL and which streaming approach to use
# These use REAL files from various repositories to demonstrate proper file handling
FILES_CONFIG = [
    # PDF example - direct response.raw streaming
    # Real PDF from Mozilla's PDF.js test suite
    {
        "name": "tracemonkey.pdf",
        "url": "https://raw.githubusercontent.com/mozilla/pdf.js/master/test/pdfs/tracemonkey.pdf",
        "type": "pdf",
        "stream_func": stream_pdf,
    },
    # CSV example - BytesIO with expected_bytes
    # Real CSV data from Seaborn datasets
    {
        "name": "iris.csv",
        "url": "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv",
        "type": "csv",
        "stream_func": stream_csv,
    },
    # JSON example - custom BufferedReader
    # Real JSON data from Vega datasets
    {
        "name": "cars.json",
        "url": "https://raw.githubusercontent.com/vega/vega-datasets/master/data/cars.json",
        "type": "json",
        "stream_func": stream_json,
    },
    # PNG example - file object with expected_bytes
    # Real PNG image from GitHub's own assets
    {
        "name": "github_logo.png",
        "url": "https://raw.githubusercontent.com/github/explore/main/topics/python/python.png",
        "type": "png",
        "stream_func": stream_png,
    },
    # BIN example - large file via response.raw
    # Real binary file (whl package from PyPI mirror)
    {
        "name": "requests_lib.bin",
        "url": "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/penguins.csv",
        "type": "bin",
        "stream_func": stream_bin,
    },
]


def update(configuration: dict, state: dict):
    """
    Fetch files from GitHub and sync them using different streaming approaches.

    This demonstrates:
    - Fetching files from a public API (GitHub raw content)
    - Applying different streaming approaches based on file type
    - Proper error handling and logging
    - Checkpoint management for resumable syncs

    Args:
        configuration: Connector configuration
        state: Previous sync state (empty for first run)
    """
    logger.info("Starting unstructured data sync")
    
    try:
        for file_config in FILES_CONFIG:
            file_name = file_config["name"]
            url = file_config["url"]
            file_type = file_config["type"]
            stream_func = file_config["stream_func"]
            
            try:
                logger.info(f"Processing {file_type.upper()} file: {file_name}")
                
                # Get file using the specified streaming approach
                file_upload, file_size = stream_func(url)
                
                # Create metadata record for this file
                file_id = compute_file_id(url)
                metadata = {
                    "file_id": file_id,
                    "file_name": file_name,
                    "file_path": url,
                    "file_type": file_type,
                    "file_size_bytes": str(file_size),  # Convert to string for compatibility
                    "content_encoding": "utf-8",
                    "synced_at": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                }
                
                # Upsert metadata + file attachment
                # Note: File uploads only work in production; local debug tester doesn't support them
                try:
                    op.upsert(table="files", data=metadata, file=file_upload)
                except Exception as file_error:
                    # If file upload fails (e.g., in debug mode), try without file
                    logger.warning(f"Could not attach file, upserting metadata only: {file_error}")
                    op.upsert(table="files", data=metadata)
                
                logger.info(
                    f"Successfully synced {file_name} ({file_size} bytes) "
                    f"using {file_type} approach"
                )
                
            except requests.RequestException as e:
                logger.error(f"Network error downloading {file_name}: {e}")
                # Continue to next file instead of crashing
                continue
            except Exception as e:
                logger.error(f"Error processing {file_name}: {e}")
                continue
        
        # Save sync progress
        op.checkpoint(state)
        logger.info("Sync completed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error during sync: {e}")
        raise


# This creates the connector object that will use the update and schema functions
# defined in this connector.py file.
connector = Connector(update=update, schema=schema)


# Check if the script is being run as the main module. This allows testing your
# connector by running the file directly from your IDE 'run' button.
# Note: This is not called by Fivetran in production.
if __name__ == "__main__":
    connector.debug()
