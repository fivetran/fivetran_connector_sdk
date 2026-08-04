# Unstructured Data Examples

## Overview

Learn how to upload unstructured data such as PDFs, images, and binaries to a destination using your Connector SDK code.

Unlike structured data, which has traditional tabular data (rows/columns) that fits into database tables, unstructured data includes files that don't fit neatly into rows and columns:
- PDFs (reports, invoices, contracts)
- Images (photos, screenshots, diagrams)
- Archives (ZIP, TAR)
- Any binary content

> Note: Unstructured file support is currently available for Snowflake, BigQuery, and Databricks destinations only. See [Unstructured File Replication](https://fivetran.com/docs/core-concepts/features/unstructured-file-replication) for more details.

---

## Getting started

**New to unstructured data?** Start here:

-  [file_lifecycle](file_lifecycle/): Upload your first file and understand the upload, update, and delete lifecycle (5 minutes).
-  [stream_examples](stream_examples/): Learn different streaming approaches and when to use `expected_bytes`.

To run an example:

```bash
# 1. Pick an example
cd file_lifecycle

# 2. Run locally
fivetran debug
```
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

### The `_fivetran_file_path` column

When you upload a file, Fivetran automatically adds a `_fivetran_file_path` column to your table to track the file location. You don't define this column; the platform adds it automatically.

#### Example:

Your schema:

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

After sync, query your destination:

```sql
SELECT doc_id, doc_name, _fivetran_file_path FROM documents;
```

Result:

```
| doc_id  | doc_name    | _fivetran_file_path     |
|---------|-------------|-------------------------|
| 1       | report.pdf  | documents/report.pdf    |
```

#### Understanding file paths:

| What | Who Controls It | Example |
|------|-----------------|---------|
| `FileUpload.path` | You (connector code) | `"documents/report.pdf"` |
| `_fivetran_file_path` | Fivetran (stores the same value) | `"documents/report.pdf"` |

> Note: The path you provide in `FileUpload.path` is stored in `_fivetran_file_path`. They contain the same value.

---

## Examples

| I want to | Go to |
|-------------|-------|
| Upload my first file | [file_lifecycle](file_lifecycle/) |
| Understand the upload, update, and delete lifecycle | [file_lifecycle](file_lifecycle/) |
| Learn  the `_fivetran_file_path` behavior | [file_lifecycle](file_lifecycle/) |
| Learn different streaming approaches | [stream_examples](stream_examples/) |
| Understand when to use expected_bytes | [stream_examples](stream_examples/) |

---

## Common troubleshooting

### `_fivetran_file_path` column is missing
- ❌ Not passing `file=FileUpload(...)` parameter
- ✅ Check whether your `upsert()` or `update()` call  includes the `file` parameter

### "FileUpload object has no attribute 'read'"
- ❌ Your stream object doesn't have a `read(size)` method
- ✅ Use `response.raw`, `BytesIO`, or file object

### File not appearing in destination
- ❌ Not passing `file=FileUpload(...)` to `Operations.upsert()`
- ✅ Check whether your `upsert()` call includes the `file` parameter

### "expected_bytes mismatch"
- ❌ File size changed during upload
- ✅ Verify `expected_bytes` matches the actual file size

---

**Ready to get started?** Head to [file_lifecycle](file_lifecycle/) →
