# File lifecycle example

## Connector overview

This example demonstrates how to upload unstructured files (PDFs, images, etc.) alongside metadata rows using the `FileUpload` class from the Fivetran Connector SDK. It covers the complete file lifecycle: upload, update (replace), and soft-delete operations.

The example shows:
- How to upload a new file with metadata
- How to update (replace) an existing file using both `op.upsert()` and `op.update()`
- How to soft-delete a row and file using `op.delete()`
- How the `_fivetran_file_path` column is automatically managed
- Basic state management with checkpoints

Refer to `def update()` in `connector.py` for the complete lifecycle implementation.

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
fivetran init my-file-connector --example unstructured_data/file_lifecycle
```

This creates a new directory `my-file-connector` with all example files copied. You can then modify the connector code for your specific use case.

Navigate to your new project:

```bash
cd my-file-connector
```

Run the connector:

```bash
fivetran debug
```

## Features

- Demonstrates `FileUpload` with `stream` parameter for uploading files from HTTP responses, in-memory content, or file handles
- Shows the difference between `op.upsert()` (replaces all columns) and `op.update()` (updates specific columns) when replacing files
- Explains how `_fivetran_file_path` is automatically added to the destination table and stores the relative path from `FileUpload.path`
- Shows soft-delete behavior: `op.delete()` marks the row as deleted (`_fivetran_deleted = True`) but the row and file remain in the destination
- Uses nested paths (like `documents/v1/`, `documents/v2/`) to organize files by version
- Demonstrates state management with `op.checkpoint()` for incremental syncs

## Requirements file

This connector has no third-party dependencies and does not include a `requirements.txt` file.

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Tables created

This connector creates one table in your destination:

### documents

Stores document metadata and file references. Fivetran automatically adds the `_fivetran_file_path` column.

| Column Name           | Data Type | Description                                                    |
|-----------------------|-----------|----------------------------------------------------------------|
| `doc_id`              | STRING    | Primary key - unique document identifier                       |
| `doc_name`            | STRING    | Document filename                                              |
| `doc_version`         | STRING    | Document version (v1, v2, v3, etc.)                            |
| `_fivetran_file_path` | STRING    | Auto-generated - stores the relative path from FileUpload.path |

## Data handling

The connector demonstrates three phases of the file lifecycle:

1. **Phase 1 - Upload**: Creates a new document with metadata and uploads a file (simulated PDF content). The file is stored at the path specified in `FileUpload.path` (such as `documents/v1/report.pdf`), and this same path is automatically stored in the `_fivetran_file_path` column.

2. **Phase 2 - Update/Replace**: Shows two approaches to update an existing file:
   - **Approach 1**: Using `op.upsert()` - Replaces ALL column values (you must provide all columns or they become NULL)
   - **Approach 2**: Using `op.update()` with `modified={...}` - Updates only specified columns (recommended for partial updates)

   Both approaches replace the file by providing a new `FileUpload` with a different path.

3. **Phase 3 - Soft-delete**: Uses `op.delete()` to mark the row as deleted by setting `_fivetran_deleted = True`. Important: The row remains in the table and the file remains in the destination storage. Files are only removed when you update the row with a different file path.

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
