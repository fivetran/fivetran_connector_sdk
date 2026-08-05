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

New to unstructured data? Start here:

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

![File Upload Flow](assets/unstructured-data-sync-flow-diagram.svg)

Uploading a file involves the following steps:

1. Your connector fetches the file from the source (API, cloud storage).
2. Create a `FileUpload` object with the path for the file in your destination and a stream to read from (`FileUpload(path="file.pdf", stream=...)`).
3. Call `upsert()` with the file parameter to upload both the file and metadata (`op.upsert(table, data, file=...)`).
4. The Fivetran platform receives the file and metadata from your connector.
5. The Fivetran platform stores the file in the destination's file storage (Snowflake stages, BigQuery Cloud Storage, or Databricks volumes).
6. The Fivetran platform writes a metadata row to the destination table, including the automatically added `_fivetran_file_path` column with the file path.

> To download the file from your destination, query the table to retrieve the `_fivetran_file_path` value, then use that value to download the file.

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

| Issue | Solution |
|-------|----------|
| `_fivetran_file_path` column is missing or file not appearing in destination | Check whether your `upsert()` or `update()` call includes the `file=FileUpload(...)` parameter |
| "FileUpload object has no attribute 'read'" | Ensure your stream object has a `read(size)` method. Use `response.raw`, `BytesIO`, or file object |
| "expected_bytes mismatch" | Verify `expected_bytes` matches the actual file size |

---

Ready to get started? Head to [file_lifecycle](file_lifecycle/).
