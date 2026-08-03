# Stream examples

## Connector overview

This example demonstrates three different approaches to file streaming with the Fivetran Connector SDK. It shows when to use HTTP response streams, BytesIO, and custom readers. 

> Important: The file types (PDF, CSV, JSON) are just examples — each streaming approach works with any file type. Choose your approach based on your use case, not the file format.

The example shows:
- HTTP response.raw - Direct streaming from HTTP responses
- BytesIO - In-memory file handling
- CustomReader - Fine-grained control over streaming
- How all streams must implement the `read(size) -> bytes` method

Refer to `def update()` in `connector.py` for the complete implementation.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

For available CLI commands, refer to the [Connector SDK Commands](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands) reference.

### Using this example

You can either run this example directly or initialize a new connector project based on it.

#### Option 1: Run this example directly

Run the connector locally in debug mode:

```bash
fivetran debug
```

#### Option 2: Initialize a new project from this example

Create a new connector project based on this example:

```bash
fivetran init my-stream-connector --example unstructured_data/stream_examples
```

This creates a new directory `my-stream-connector` with all example files copied. You can then modify the connector code for your specific use case.

Navigate to your new project:

```bash
cd my-stream-connector
```

Run the connector:

```bash
fivetran debug
```

## Features

- Demonstrates three main streaming approaches for file uploads
- Clarifies that streaming approach is independent of file type (choose based on use case, not file format)
- Shows the `read(size) -> bytes` method requirement that all streams must implement
- Demonstrates `expected_bytes` parameter for integrity checking (optional)
- Shows how `_fivetran_file_path` is automatically added to store the relative path from `FileUpload.path`
- Demonstrates state management with `op.checkpoint()` for incremental syncs

## Requirements file

This connector has no third-party dependencies and does not include a `requirements.txt` file.

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Tables created

This connector creates one table in your destination:

### files

Stores file metadata and references. Fivetran automatically adds the `_fivetran_file_path` column.

| Column Name           | Data Type | Description                                                    |
|-----------------------|-----------|----------------------------------------------------------------|
| `file_id`             | STRING    | Primary key - unique file identifier (SHA-256 hash)            |
| `file_name`           | STRING    | Filename (sample.pdf, data.csv, etc.)                          |
| `file_type`           | STRING    | File type (pdf, csv, json, etc.)                               |
| `file_size_bytes`     | INT       | File size in bytes                                             |
| `_fivetran_file_path` | STRING    | Auto-generated - stores the relative path from FileUpload.path |

## Data handling

The connector demonstrates three streaming approaches using the FileUpload API:

### 1. HTTP response.raw streaming

Use when: Streaming directly from HTTP responses

```python
response = requests.get(url, stream=True)
response.raw.decode_content = True  # Handle compressed responses
FileUpload(path="streams/pdf/file.pdf", stream=response.raw)
```

Good for: Any file type downloaded via HTTP (PDF, CSV, JSON, images, etc.)

### 2. BytesIO streaming

Use when: Working with in-memory file content

```python
file_bytes = io.BytesIO(response.content)
FileUpload(path="streams/csv/file.csv", stream=file_bytes, expected_bytes=len(response.content))
```

Good for: Any file type you have in memory (PDF, CSV, JSON, images, etc.)

### 3. Custom reader streaming

Use when: You need fine-grained control over buffering/chunking

```python
class CustomBufferedReader:
    def read(self, size: int = -1) -> bytes:
        # Your custom logic here
        ...

reader = CustomBufferedReader(file_content)
FileUpload(path="streams/json/file.json", stream=reader, expected_bytes=len(file_content))
```

Good for: Any file type needing custom streaming logic (PDF, CSV, JSON, images, etc.)

### Key concepts

**All streams must implement `read(size) -> bytes`:**

The SDK requires this method to read data from your stream. Compatible types include `requests.Response.raw`, `io.BytesIO`, and any custom class implementing the method.

**File type doesn't dictate the approach:**

The examples use PDF/CSV/JSON, but this is arbitrary. Choose your approach based on where the file comes from and whether you need custom logic, not the file type.

Refer to `def update()` in `connector.py` for the complete implementation with detailed comments.

## Error handling

The connector includes basic error handling:
- HTTP errors when fetching files are logged and raised
- All file operations include try-except blocks with logging
- Failed operations are logged with clear error messages

For production connectors, consider adding:
- Retry logic for transient HTTP errors
- Validation of file sizes before upload
- Graceful handling of API rate limits
- More detailed error context for debugging

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
