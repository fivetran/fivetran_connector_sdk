# Stream examples

## Connector overview

This example demonstrates three different file streaming approaches for the Fivetran Connector SDK. It shows when to use HTTP response streams, BytesIO, and custom readers. **Important:** The file types (PDF, CSV, JSON) are just examples — each streaming approach works with ANY file type. Choose your approach based on your use case, not the file format.

The connector fetches files from public URLs and demonstrates:
1. **HTTP response.raw** - Direct streaming from HTTP (example: PDF)
2. **BytesIO** - In-memory file handling (example: CSV)
3. **CustomReader** - Fine-grained control over streaming (example: JSON)

All streams must implement the `read(size) -> bytes` method.

Refer to `def update()` in `connector.py` for implementation details.

## Requirements

- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started

Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide) to get started.

For available CLI commands, refer to the [Connector SDK Commands](https://fivetran.com/docs/connector-sdk/connector-development-and-configuration/connector-sdk-commands) reference.

1. Run the connector locally:

   ```bash
   fivetran debug
   ```

The connector demonstrates three streaming approaches:
- **Approach 1**: HTTP response.raw (PDF example)
- **Approach 2**: BytesIO (CSV example)
- **Approach 3**: Custom BufferedReader (JSON example)

Each approach logs which method was used and the file details.

## Features

- Demonstrates three main streaming approaches for file uploads
- Clarifies that streaming approach is independent of file type:
  - response.raw works for PDF, CSV, JSON, images, any HTTP download
  - BytesIO works for any file you have in memory
  - CustomReader works for any file needing custom buffering
- Shows the `read(size) -> bytes` method requirement that all streams must implement
- Demonstrates `expected_bytes` parameter for integrity checking (optional)
- Shows `_fivetran_file_path` column (auto-generated, stores relative path)
- Uses real files from public URLs (Mozilla PDF.js, Seaborn datasets, Vega datasets)

## Requirements file

```txt
requests>=2.31.0
```

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt` when deploying.

## Data handling

The example demonstrates three streaming approaches using the FileUpload API. The connector fetches files from public URLs and uploads them using `Operations.upsert()` with the `file` parameter. Different streaming techniques are shown:

### 1. HTTP response.raw streaming

**Use when:** Streaming directly from HTTP responses

```python
response = requests.get(url, stream=True)
response.raw.decode_content = True  # Handle compressed responses
FileUpload(path="streams/pdf/file.pdf", stream=response.raw)
```

**Good for:** Any file type downloaded via HTTP (PDF, CSV, JSON, images, etc.)

### 2. BytesIO streaming

**Use when:** Working with in-memory file content

```python
file_bytes = io.BytesIO(response.content)
FileUpload(path="streams/csv/file.csv", stream=file_bytes, expected_bytes=len(response.content))
```

**Good for:** Any file type you have in memory (PDF, CSV, JSON, images, etc.)

### 3. Custom reader streaming

**Use when:** You need fine-grained control over buffering/chunking

```python
class CustomBufferedReader:
    def read(self, size: int = -1) -> bytes:
        # Your custom logic here
        ...

reader = CustomBufferedReader(file_content)
FileUpload(path="streams/json/file.json", stream=reader, expected_bytes=len(file_content))
```

**Good for:** Any file type needing custom streaming logic (PDF, CSV, JSON, images, etc.)

### Key concepts

**All streams must implement `read(size) -> bytes`:**

The SDK requires this method to read data from your stream. Compatible types include:
- `requests.Response.raw` (from `requests.get(url, stream=True)`)
- `io.BytesIO(content)`
- Any custom class implementing `read(size) -> bytes`

**File type doesn't dictate the approach:**

The examples use PDF/CSV/JSON, but this is arbitrary. You can use any approach with any file type. Choose based on:
- Where the file comes from (HTTP → response.raw)
- Whether it's in memory (memory → BytesIO)
- Whether you need custom logic (custom needs → CustomReader)

**`_fivetran_file_path` stores relative paths:**

When you provide `FileUpload(path="streams/pdf/file.pdf", ...)`, the `_fivetran_file_path` column will store `"streams/pdf/file.pdf"` — the same relative path you provided.

Refer to `def update()` for implementation details showing all three approaches with detailed comments.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
