# File lifecycle

## Connector overview

This example demonstrates the complete lifecycle of file operations with the Fivetran Connector SDK: upload, update (using both `upsert()` and `update()`), and delete. It shows how the auto-generated `_fivetran_file_path` column stores the relative path from `FileUpload.path`, and clarifies the difference between `upsert()` (replaces all columns) and `update()` (updates only specified columns).

The connector fetches files from public URLs and demonstrates:
- Uploading a new file
- Updating with `upsert()` (requires all columns)
- Updating with `update()` (only columns you want to change)
- Soft-deleting a row (row and file both remain in destination)

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

The connector executes three phases:
- **Phase 1**: Upload a PDF file (documents/v1/tracemonkey.pdf)
- **Phase 2**: Update with two approaches - `upsert()` for v2, `update()` for v3
- **Phase 3**: Soft-delete the row

Each phase logs the state changes in the destination.

## Features

- Demonstrates complete file lifecycle: upload → update (two approaches) → delete
- Shows `_fivetran_file_path` column (auto-generated, stores relative path from FileUpload.path)
- Compares `upsert()` vs `update()` for file replacement:
  - `upsert()`: Replaces ALL columns (must provide all column values)
  - `update()`: Updates only specified columns (recommended for file updates)
- Clarifies soft-delete behavior: `delete()` sets `_fivetran_deleted=True`, row and file both remain
- Uses nested paths (documents/v1/, documents/v2/, documents/v3/) to show path organization
- State management with checkpoint pattern for incremental syncs
- Uses real files from public URLs (Mozilla PDF.js, Seaborn datasets)

## Requirements file

```txt
requests>=2.31.0
```

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt` when deploying.

## Data handling

The example uses the FileUpload API to demonstrate the file lifecycle. The connector fetches files from public URLs (Mozilla PDF.js for PDF, Seaborn datasets for CSV/CSV) and uploads them using `Operations.upsert()` and `Operations.update()` with the `file` parameter. The `_fivetran_file_path` column automatically tracks file paths throughout the lifecycle.

### Key concepts

**`_fivetran_file_path` stores relative paths:**

When you provide `FileUpload(path="documents/v1/file.pdf", ...)`, the `_fivetran_file_path` column will store `"documents/v1/file.pdf"` — not an absolute cloud URL like `s3://...`. Both `FileUpload.path` and `_fivetran_file_path` contain the same value.

**`upsert()` vs `update()` for file replacement:**

- `upsert()`: Replaces ALL columns. You must provide all column values, or unprovided columns become NULL.
- `update()`: Updates only specified columns. Other columns remain unchanged. Recommended for file updates.

**Soft-delete behavior:**

`Operations.delete()` marks the row with `_fivetran_deleted=True`. The row remains in the table, and the file remains in the destination. To query only active rows, filter by `WHERE _fivetran_deleted IS NULL OR _fivetran_deleted = False`.

**Old file deletion:**

Old files are automatically deleted only when you update the row with a different file path. There is no way to delete only the file while keeping the metadata row.

Refer to `def update()` for implementation details showing all three phases with detailed comments.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
