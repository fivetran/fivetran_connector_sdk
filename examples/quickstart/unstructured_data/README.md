# Unstructured Data Examples

Learn how to sync files (PDFs, images, binaries, etc.) using the Fivetran Connector SDK.

> **Note:** Unstructured file support is currently available for Snowflake, BigQuery, and Databricks destinations only. See [Unstructured File Replication](https://fivetran.com/docs/core-concepts/features/unstructured-file-replication) for more details.

---

## Quick start

**New to unstructured data?** Start here:

1. **[file_lifecycle](file_lifecycle/)** - Upload your first file + understand upload/update/delete lifecycle (5 minutes)
2. **[stream_examples](stream_examples/)** - Learn different streaming approaches and when to use expected_bytes

---

## What is unstructured data?

**Unstructured data** = Files that don't fit neatly into rows and columns:
- PDFs (reports, invoices, contracts)
- Images (photos, screenshots, diagrams)
- Archives (ZIP, TAR)
- Any binary content

**Structured data** = Traditional tabular data (rows/columns) that fits into database tables.

---

## How it works

```
Your Connector
  ↓
  Downloads file from source API
  ↓
  Creates FileUpload(path="file.pdf", stream=...)
  ↓
  Calls Operations.upsert(table, data, file=...)
  ↓
Fivetran Platform
  ↓
  Loads the file to destination storage
  ↓
  Writes row to destination with _fivetran_file_path
  ↓
Destination (Snowflake/BigQuery/Databricks)
  ↓
  Table contains metadata columns + _fivetran_file_path
  ↓
  You can query/download files using the path
```

---

## The `_fivetran_file_path` column

When you upload a file, Fivetran **automatically adds** a `_fivetran_file_path` column to your table to track the file location.

**You don't define this column** - it's added by the platform automatically.

### Example:

**Your schema:**
```python
def schema(configuration):
    return [{
        "table": "documents",
        "primary_key": ["doc_id"],
        "columns": {
            "doc_id": "STRING",
            "doc_name": "STRING",
        },
    }]
```

**After sync, query your destination:**
```sql
SELECT doc_id, doc_name, _fivetran_file_path FROM documents;
```

**Result:**
```
| doc_id  | doc_name    | _fivetran_file_path     |
|---------|-------------|-------------------------|
| 1       | report.pdf  | documents/report.pdf    |
```

### Understanding file paths:

| What | Who Controls It | Example |
|------|-----------------|---------|
| `FileUpload.path` | **You** (connector code) | `"documents/report.pdf"` |
| `_fivetran_file_path` | **Fivetran** (stores the same value) | `"documents/report.pdf"` |

**Key insight:** The path you provide in `FileUpload.path` is stored in `_fivetran_file_path`. They contain the same value.

---

## Choose your example

| I want to... | Go to |
|-------------|-------|
| Upload my first file | [file_lifecycle](file_lifecycle/) |
| Understand upload/update/delete lifecycle | [file_lifecycle](file_lifecycle/) |
| Learn `_fivetran_file_path` behavior | [file_lifecycle](file_lifecycle/) |
| Learn different streaming approaches | [stream_examples](stream_examples/) |
| Understand when to use expected_bytes | [stream_examples](stream_examples/) |

---

## Key concepts covered

### [file_lifecycle/](file_lifecycle/)
- ✅ How to upload a file
- ✅ How to update a file (upsert vs update)
- ✅ How to delete a file
- ✅ `_fivetran_file_path` lifecycle
- ✅ State management with checkpoint

### [stream_examples/](stream_examples/)
- ✅ HTTP response streams (requests.raw)
- ✅ In-memory bytes (BytesIO)
- ✅ File handles (local files)
- ✅ With expected_bytes (size validation)
- ✅ Without expected_bytes (unknown size)

---

## Common troubleshooting

### `_fivetran_file_path` column is missing
- ❌ Not passing `file=FileUpload(...)` parameter
- ✅ Check your upsert() or update() call includes `file` parameter

### "FileUpload object has no attribute 'read'"
- ❌ Your stream object doesn't have a `read(size)` method
- ✅ Use `response.raw`, `BytesIO`, or file object

### File not appearing in destination
- ❌ Not passing `file=FileUpload(...)` to `Operations.upsert()`
- ✅ Check your upsert() call includes `file` parameter

### "expected_bytes mismatch"
- ❌ File size changed during upload
- ✅ Verify expected_bytes matches actual file size

---

## Key requirements

### 1. FileUpload with Stream
```python
FileUpload(
    path="documents/filename.pdf",  # Path within table namespace
    stream=file_stream               # Stream with read(size) method
)
```

### 2. Operations.upsert or update with file Parameter
```python
Operations.upsert(
    table="documents",
    data={"doc_id": "1", "doc_name": "report.pdf"},
    file=FileUpload(...)  # ← Pass file here
)
```

---

## Getting started

```bash
# 1. Pick an example
cd file_lifecycle

# 2. Run locally
fivetran debug
```

---

## Pro tips

1. **Start with file_lifecycle/** to understand fundamentals
2. **Use stream_examples/** to learn different streaming approaches
3. **Test locally first** with `fivetran debug` before deploying
4. **Check destination** after sync to verify `_fivetran_file_path` is populated

---

## Example workflow

```python
# 1. Download file with streaming
response = requests.get(file_url, stream=True)
response.raw.decode_content = True

# 2. Upload with FileUpload
Operations.upsert(
    table="files",
    data={"file_id": "123", "file_name": "report.pdf"},
    file=FileUpload(path="files/report.pdf", stream=response.raw)
)

# 3. Query destination to verify
# SELECT file_id, file_name, _fivetran_file_path FROM files;
```

---

**Ready to get started?** Head to [file_lifecycle](file_lifecycle/) →
