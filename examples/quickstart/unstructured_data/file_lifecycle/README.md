# File lifecycle example

## Connector overview

This example demonstrates the complete lifecycle of unstructured file syncing with the Fivetran Connector SDK. It covers how to upload a new file, update an existing file by replacing it, and delete the metadata row. The example also explains the auto-generated `_fivetran_file_path` column that Fivetran adds to track the file path within your table's namespace.

The connector uploads a PDF file from GitHub, updates it with a CSV file, and then demonstrates row deletion — all while showing how `_fivetran_file_path` changes through each operation. It also shows state management using the checkpoint pattern for incremental syncs.

Refer to `def update()` in `connector.py` for the main upload/update/delete lifecycle implementation.

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

The connector executes three phases demonstrating the file lifecycle:
- **Phase 1**: Upload a PDF file (tracemonkey.pdf)
- **Phase 2**: Update it with a CSV file (iris.csv)  
- **Phase 3**: Delete the metadata row (soft-delete)

Each phase logs how `_fivetran_file_path` changes through the operations.

## Features

- Demonstrates the complete file lifecycle: upload → update → delete
- Shows how `_fivetran_file_path` is auto-generated and changes through operations
- Illustrates update behavior: Both `Operations.upsert()` and `Operations.update()` can replace files
- Clarifies `Operations.upsert()` replaces ALL columns (requires all column values), while `Operations.update()` updates only specified columns (recommended for file replacement)
- Demonstrates row deletion: `Operations.delete()` soft-deletes the row; file remains in destination stage
- Shows state management with checkpoint pattern for incremental syncs
- Uses real files from public URLs (Mozilla PDF.js test suite, Seaborn datasets)

## Requirements file

```txt
requests>=2.28.0
```

> Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt` when deploying.

## Data handling

The example uses the FileUpload API to demonstrate the file lifecycle. The connector fetches files from public URLs (Mozilla PDF.js for PDF, Seaborn datasets for CSV) and uploads them using `Operations.upsert()` with the `file` parameter. Configuration values control which phase runs, and the `_fivetran_file_path` column automatically tracks file locations throughout the lifecycle.

Refer to `def update()` for details.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
        "supports_unstructured_data": True,  # ← Enable unstructured data
    }]
```

When `supports_unstructured_data: True`:
- ✅ Fivetran automatically adds a `_fivetran_file_path` column
- ✅ You can pass `file=FileUpload(...)` to `Operations.upsert()`
- ✅ Files are uploaded to cloud storage (S3/GCS/Azure)

---

### 2. The `_fivetran_file_path` Column

**You DON'T define this column** - Fivetran adds it automatically.

After your sync completes, query the destination:

```sql
SELECT doc_id, doc_name, _fivetran_file_path FROM documents;
```

Result:
```
| doc_id      | doc_name           | _fivetran_file_path                                    |
|-------------|--------------------|--------------------------------------------------------|
| demo_doc_1  | tracemonkey_v1.pdf | s3://fivetran-files/account123/connector456/v1.pdf     |
```

**What is `_fivetran_file_path`?**
- Auto-generated cloud storage path (S3, GCS, or Azure Blob)
- Points to where Fivetran stored your uploaded file
- Used to download/access the file from your destination
- Format: `{protocol}://{bucket}/{account}/{connector}/{table}/{file}`

**Two paths to understand:**

| Path Type | Who Controls It | Example |
|-----------|-----------------|---------|
| `FileUpload.path` | **You** (connector code) | `"tracemonkey_v1.pdf"` |
| `_fivetran_file_path` | **Fivetran** (platform) | `"s3://fivetran-files/.../v1.pdf"` |

---

### 3. Upload, Update, Delete Lifecycle

#### Upload (Create New File)

```python
op.upsert(
    table="documents",
    data={"doc_id": "demo_doc_1", "doc_name": "v1.pdf"},
    file=FileUpload(path="v1.pdf", stream=response.raw)
)
```

**Result in destination:**
```
| doc_id      | doc_name | _fivetran_file_path          |
|-------------|----------|------------------------------|
| demo_doc_1  | v1.pdf   | s3://fivetran-files/.../v1.pdf |
```

---

#### Update (Replace Existing File)

You can use **either** `op.upsert()` or `op.update()` to replace a file:

**Option 1: Using `op.upsert()` (insert-or-update)**

```python
# Same doc_id, but NEW file
# Inserts if row doesn't exist, updates if it does
op.upsert(
    table="documents",
    data={"doc_id": "demo_doc_1", "doc_name": "v2.csv"},  # Same doc_id!
    file=FileUpload(path="v2.csv", stream=new_file_stream)
)
```

**Option 2: Using `op.update()` (update existing row)**

```python
# Same doc_id, but NEW file
# Assumes row already exists
op.update(
    table="documents",
    data={
        "doc_id": "demo_doc_1",  # Primary key required
        "doc_name": "v2.csv",    # Can provide all columns or just changed ones
    },
    file=FileUpload(path="v2.csv", stream=new_file_stream)
)
```

> **Note:** Both methods can accept full or partial column data. The difference is semantic:
> - `op.upsert()` = insert if not exists, update if exists
> - `op.update()` = update existing row only

**Result in destination (after update):**
```
| doc_id      | doc_name | _fivetran_file_path          |
|-------------|----------|------------------------------|
| demo_doc_1  | v2.csv   | s3://fivetran-files/.../v2.csv |  ← Path changed!
```

**What happened:**
- Row updated (same `doc_id`)
- Old file (`v1.pdf`) replaced with new file (`v2.csv`)
- `_fivetran_file_path` changed to point to new file
- Old `v1.pdf` is garbage collected from storage

**Key insight:** To update a file, use the **same primary key** with a **new FileUpload**. Both `op.upsert()` and `op.update()` work.

---

#### Delete Options

**Deleting the metadata row (file stays in destination stage):**

```python
op.delete(
    table="documents",
    data={"doc_id": "demo_doc_1"}
)
```

**Result in destination (after delete):**
```sql
SELECT * FROM documents WHERE doc_id = 'demo_doc_1';
-- Empty (0 rows) - but file still exists in destination stage!
```

**What happened:**
- Metadata row marked as deleted (soft delete)
- **File REMAINS in destination stage** (not automatically deleted)
- Use this when you want to remove the row but keep the file

{% note %}
**Note**: There is no direct way to delete only the file while keeping the metadata row. The old file is automatically deleted only when you update the row with a **different file path** (see "Update" section above).
{% /note %}

---

### 4. State Management

State is a dictionary that persists between syncs:

```python
def update(configuration, state):
    # Read previous state
    last_sync = state.get("last_sync_timestamp", "1970-01-01T00:00:00Z")
    
    # Fetch only NEW files since last_sync
    new_files = fetch_files_since(last_sync)
    
    # Upload new files
    for file in new_files:
        op.upsert(table="documents", data=..., file=...)
    
    # Update state with current timestamp
    state["last_sync_timestamp"] = datetime.now(timezone.utc).isoformat()
    
    # Save state for next sync
    op.checkpoint(state)
    
    return state
```

**State lifecycle:**

```
Sync 1:
  Input:  state = {}
  Output: state = {"last_sync_timestamp": "2024-01-15T10:00:00Z"}
  Checkpoint: ✓ Saved

Sync 2:
  Input:  state = {"last_sync_timestamp": "2024-01-15T10:00:00Z"}
  Output: state = {"last_sync_timestamp": "2024-01-15T11:00:00Z"}
  Checkpoint: ✓ Saved

Sync 3 (if error before checkpoint):
  Input:  state = {"last_sync_timestamp": "2024-01-15T11:00:00Z"}  ← Reverts to last checkpoint
```

**Key points:**
- `op.checkpoint(state)` saves state for next sync
- If sync fails before checkpoint, state rolls back
- Use state to track cursor/timestamp for incremental syncs

---

## Lifecycle Summary Table

| Operation | Primary Key | File | `_fivetran_file_path` | Row Exists | File Exists |
|-----------|-------------|------|-----------------------|------------|-------------|
| **Upload** | `demo_doc_1` | v1.pdf | `s3://.../v1.pdf` | ✅ Yes | ✅ Yes |
| **Update** | `demo_doc_1` | v2.csv | `s3://.../v2.csv` | ✅ Yes | ✅ Yes (new) |
| **Delete Row** | `demo_doc_1` | - | - | ❌ No | ✅ Yes (in stage) |

{% note %}
**Note**: Old files are automatically deleted when you update a row with a **different file path**. There is no separate "delete file only" operation.
{% /note %}

---

## Running This Example

### Prerequisites
```bash
pip install requests fivetran_connector_sdk
```

### Local Testing
```bash
fivetran debug connector.py
```

### Expected Output
```
================================================================================
BASIC UPLOAD EXAMPLE: File Lifecycle Demonstration
================================================================================

📤 PHASE 1: UPLOAD - Creating new file
--------------------------------------------------------------------------------
Fetching file from: https://raw.githubusercontent.com/mozilla/pdf.js/...
✓ File size: 1016315 bytes
✓ File uploaded successfully

After this sync completes, query your destination:
  SELECT doc_id, doc_name, _fivetran_file_path FROM documents;

Result will show:
  | doc_id       | doc_name            | _fivetran_file_path             |
  |--------------|---------------------|---------------------------------|
  | demo_doc_1   | tracemonkey_v1.pdf  | s3://fivetran-files/.../v1.pdf  |

🔄 PHASE 2: UPDATE - Replacing existing file
--------------------------------------------------------------------------------
Fetching new file from: https://raw.githubusercontent.com/mwaskom/seaborn-data/...
✓ New file size: 4821 bytes
✓ File updated successfully

After this sync completes, query your destination:
  SELECT doc_id, doc_name, doc_version, _fivetran_file_path FROM documents;

Result will now show:
  | doc_id       | doc_name            | doc_version | _fivetran_file_path             |
  |--------------|---------------------|-------------|---------------------------------|
  | demo_doc_1   | tracemonkey_v2.csv  | v2          | s3://fivetran-files/.../v2.csv  |

🗑️  PHASE 3: DELETE - Removing file
--------------------------------------------------------------------------------
✓ File deleted successfully
```

---

## Troubleshooting

### `_fivetran_file_path` column is missing
- ❌ Check that `supports_unstructured_data: True` is set in schema
- ✅ This column is ONLY added when the flag is enabled

### File not appearing in destination
- ❌ Check that you're passing `file=FileUpload(...)` to `op.upsert()`
- ✅ Verify `supports_unstructured_data: True` in schema

### Update not replacing file
- ❌ Check that you're using the **same primary key** value
- ✅ Update = upsert with same PK + new FileUpload

### Delete not removing file from storage

This is expected behavior:

- **Delete row** (`op.delete()`): Soft-deletes the row, **file stays in destination stage**
- Old files are automatically deleted only when you **update the row with a different file path**

There is no direct way to delete only the file while keeping the metadata row.

---

## Data handling

The example fetches real files from public repositories (Mozilla PDF.js for PDF, Seaborn datasets for CSV) to demonstrate proper file streaming. Configuration values are not used in this basic example, but the pattern shown — reading state, processing files, updating state, and checkpointing — is the foundation for incremental syncs where you would track `last_sync_timestamp` or similar cursors.

Refer to `def update()` for implementation details.

## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
