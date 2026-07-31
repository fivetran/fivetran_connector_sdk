# Stream examples

## Connector overview

This example demonstrates different file streaming approaches for syncing unstructured data with the Fivetran Connector SDK. It shows when to use each approach based on file characteristics such as size, location, and whether the size is known in advance.

The connector fetches files from public repositories (Mozilla PDF.js, Seaborn datasets, Vega datasets) and uploads them using different streaming techniques: direct response.raw streaming, BytesIO with size validation, and file objects for local files. Each approach demonstrates the `read(size) -> bytes` method requirement and how to use the `expected_bytes` parameter for integrity checking.

Refer to `def update()` in `connector.py` for the streaming implementations.

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

The connector uploads files using different streaming approaches and logs which approach was used for each file type.

## Features

- Demonstrates HTTP response streaming (requests.raw) for direct API-to-destination uploads
- Shows BytesIO streaming for in-memory file handling with size validation  
- Illustrates file handle streaming for local file uploads
- Uses `expected_bytes` parameter for integrity checking when file size is known
- Omits `expected_bytes` when file size is unknown or for large files
- Shows the `read(size) -> bytes` method requirement that all streams must implement
- Uses real files from public URLs (Mozilla PDF.js test suite, Seaborn datasets, Vega datasets)

## Requirements file

```txt
requests>=2.28.0
```

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt` when deploying.

## Data handling

The example demonstrates three streaming approaches using the FileUpload API. The connector fetches files from public URLs and uploads them using `Operations.upsert()` with the `file` parameter. Different streaming techniques are used based on the file source and whether size validation is needed: `response.raw` for HTTP responses, `BytesIO` for in-memory handling, and file handles for local files.

Refer to `def update()` for details.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
- Uses real files from public repositories (PDF, CSV, JSON, PNG) for realistic examples
- Explains `_fivetran_file_path` behavior and how it appears in the destination
- Includes comparison table showing memory usage, validation, and best use cases for each approach

## Requirements file

```txt
requests>=2.28.0
fivetran_connector_sdk>=1.0.0
```

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt` when deploying.

## The 5 Approaches

### Approach 1: PDF - Direct response.raw (Simple)

**Use when:** Downloading from HTTP, don't need size validation

```python
response = requests.get(pdf_url, stream=True)
response.raw.decode_content = True

file_upload = FileUpload(path="file.pdf", stream=response.raw)
```

**Pros:**
- ✅ Simple and straightforward
- ✅ Memory efficient (streaming)
- ✅ Works with any HTTP source

**Cons:**
- ❌ No size validation
- ❌ No progress tracking

---

### Approach 2: CSV - BytesIO + expected_bytes

**Use when:** You know the file size and want validation

```python
response = requests.get(csv_url)
file_bytes = io.BytesIO(response.content)
file_size = len(response.content)

file_upload = FileUpload(
    path="file.csv",
    stream=file_bytes,
    expected_bytes=file_size  # ← Fivetran validates this
)
```

**Pros:**
- ✅ Size validation (Fivetran verifies expected_bytes)
- ✅ Good for small-medium files (<50 MB)
- ✅ Full file loaded (can rewind stream)

**Cons:**
- ❌ Loads entire file into memory
- ❌ Not ideal for large files

---

### Approach 3: JSON - Custom BufferedReader

**Use when:** You need control over chunking behavior

```python
class BufferedJSONReader:
    def __init__(self, data: bytes, chunk_size: int = 16384):
        self.data = data
        self.position = 0
        self.chunk_size = chunk_size
    
    def read(self, size: int = -1) -> bytes:
        if size == -1:
            result = self.data[self.position:]
            self.position = len(self.data)
            return result
        else:
            result = self.data[self.position : self.position + size]
            self.position += len(result)
            return result

file_upload = FileUpload(
    path="file.json",
    stream=BufferedJSONReader(file_content),
    expected_bytes=len(file_content)
)
```

**Pros:**
- ✅ Full control over chunking
- ✅ Custom chunk size
- ✅ Can add logging/progress tracking

**Cons:**
- ❌ More code to maintain
- ❌ Overkill for simple use cases

---

### Approach 4: PNG - File Object (Local Files)

**Use when:** File is already on disk

```python
with open('image.png', 'rb') as f:
    file_size = os.path.getsize('image.png')
    
    file_upload = FileUpload(
        path="image.png",
        stream=f,
        expected_bytes=file_size
    )
```

**Pros:**
- ✅ Most efficient for local files
- ✅ Direct file handle (no extra copy)
- ✅ Size validation

**Cons:**
- ❌ Only works for files on disk
- ❌ File must exist locally

---

### Approach 5: BIN - Large File Streaming

**Use when:** File is very large (>100 MB) and you want memory efficiency

```python
response = requests.get(large_file_url, stream=True)
response.raw.decode_content = True

file_upload = FileUpload(
    path="large_file.bin",
    stream=response.raw
    # Note: No expected_bytes for large files
)
```

**Pros:**
- ✅ Memory efficient (only ~2 MB buffer)
- ✅ Works for GB-scale files
- ✅ Streaming all the way

**Cons:**
- ❌ No size validation
- ❌ Can't rewind stream

---

## Comparison Table

| Approach | Stream Type | Memory Usage | Size Validation | Best For |
|----------|-------------|--------------|-----------------|----------|
| 1. PDF | response.raw | Low | ❌ No | Simple HTTP downloads |
| 2. CSV | BytesIO | Medium | ✅ Yes | Small-medium files with known size |
| 3. JSON | Custom Reader | Low-Medium | ✅ Yes | Fine-grained control needed |
| 4. PNG | File object | Low | ✅ Yes | Files already on disk |
| 5. BIN | response.raw (stream) | Very Low | ❌ No | Large files (GB-scale) |

---

## The `_fivetran_file_path` Column

All approaches result in a `_fivetran_file_path` column in your destination.

**After sync completes, query your destination:**

```sql
SELECT file_id, file_name, file_type, _fivetran_file_path 
FROM files
ORDER BY synced_at DESC;
```

**Example results:**

| file_id | file_name | file_type | _fivetran_file_path |
|---------|-----------|-----------|---------------------|
| abc123  | tracemonkey.pdf | pdf | s3://fivetran-files/account123/connector456/files/abc123/tracemonkey.pdf |
| def456  | iris.csv | csv | s3://fivetran-files/account123/connector456/files/def456/iris.csv |
| ghi789  | cars.json | json | s3://fivetran-files/account123/connector456/files/ghi789/cars.json |
| jkl012  | python.png | png | s3://fivetran-files/account123/connector456/files/jkl012/python.png |
| mno345  | penguins.csv | bin | s3://fivetran-files/account123/connector456/files/mno345/penguins.csv |

**Key insights:**

1. **`_fivetran_file_path` is auto-generated** - you don't create it
2. **Path structure:** `{protocol}://{bucket}/{account}/{connector}/{table}/{file_id}/{filename}`
3. **Different per file** - each file gets a unique path
4. **Approach doesn't matter** - all 5 approaches produce the same result format

---

## Stream Requirements

**ALL streams MUST implement `read(size)` method:**

```python
def read(self, size: int = -1) -> bytes:
    """
    Read up to size bytes from the stream.
    
    Args:
        size: Number of bytes to read (-1 means read all)
        
    Returns:
        Bytes read from current position
    """
    pass
```

**Common stream objects with `read(size)`:**
- ✅ `requests.Response.raw` (from `requests.get(url, stream=True)`)
- ✅ `io.BytesIO(content)`
- ✅ File objects from `open(path, 'rb')`
- ✅ Custom classes implementing `read(size)`

**Common mistakes:**
- ❌ `requests.Response` directly (doesn't have `read()`)
- ❌ `response.content` (bytes object, not a stream)
- ❌ `response.iter_content()` (iterator, not a stream)

---

## Running This Example

```bash
# Install dependencies
pip install requests fivetran_connector_sdk

# Run debug mode
fivetran debug connector.py
```

**Expected output:**
```
Processing PDF file: tracemonkey.pdf
✓ Successfully synced tracemonkey.pdf (1016315 bytes) using pdf approach

Processing CSV file: iris.csv
✓ Successfully synced iris.csv (4821 bytes) using csv approach

Processing JSON file: cars.json
✓ Successfully synced cars.json (47831 bytes) using json approach

Processing PNG file: github_logo.png
✓ Successfully synced github_logo.png (5437 bytes) using png approach

Processing BIN file: requests_lib.bin
✓ Successfully synced requests_lib.bin (14290 bytes) using bin approach

✓ Checkpoint saved
Sync completed successfully
```

---

## Troubleshooting

### "Stream has no `read()` method"
- ❌ Check that your stream object implements `read(size)`
- ✅ Use `response.raw`, not `response` or `response.content`

### "expected_bytes mismatch"
- ❌ File size changed during upload
- ✅ Verify `expected_bytes` matches actual file size

### "Memory error with large files"
- ❌ Using Approach 2 (BytesIO) for GB-scale files
- ✅ Switch to Approach 5 (streaming) for large files

### "`_fivetran_file_path` not appearing"
- ❌ `supports_unstructured_data: True` not set
- ✅ Check schema definition

---

## Data handling

The example fetches real files from various public repositories to demonstrate each streaming approach. The decision tree at the top of `connector.py` helps you choose which approach fits your use case based on whether the file is on disk, whether you know the size, how large it is, and whether you need fine-grained chunking control. Each streaming function shows proper use of `response.raw.decode_content = True` for handling compressed responses and how to construct FileUpload objects.

Refer to `def stream_pdf()` through `def stream_bin()` for implementation details.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
