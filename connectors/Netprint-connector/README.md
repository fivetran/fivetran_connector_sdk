# NetPrint Connector (Custom Fivetran SDK)

## Connector overview

This connector extracts data from the **NetPrint Web API** (used by [printing.ne.jp](https://printing.ne.jp)) and loads it into a Fivetran destination using the **Fivetran Connector SDK (v2)**.

It performs:
- **Full refreshes** of small tables (`system_info`, `folder_usage`)
- **Incremental syncs** of files (`files`)
- **Soft deletes** for removed files using `_fivetran_deleted` flag

The connector is fully stateless for system metadata but maintains checkpoints for incremental syncs of files.

---

## Features

- Supports both **full** and **incremental** syncs  
- **Soft deletes** for missing files via `_fivetran_deleted` flag  
- Handles **429 rate limits** with automatic backoff  
- Converts timestamps to **UTC** consistently  
- Efficient **paging** using `fromCount` and `showCount` parameters  
- Designed for **idempotent** upserts and resumable checkpoints

---

## Requirements

* [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
* OS: macOS 13+, Windows 10+, or Linux (Ubuntu 20.04+, Amazon Linux 2+)
* Access to [NetPrint API](https://api-s.printing.ne.jp/usr/webservice/api/)
* API credentials: username and password

---

## Configuration file

Example `configuration.json`:

```json
{
  "username": "your_netprint_username",
  "password": "your_netprint_password",
  "BASE_URL": "https://api-s.printing.ne.jp/usr/webservice/api/",
  "PAGE_SIZE": "200"
}
```

| Key | Required | Description |
|-----|---------|-------------|
| `username` | Yes     | NetPrint username |
| `password` | Yes     | NetPrint password |
| `BASE_URL` | No      | Override for custom NetPrint API base URL |
| `PAGE_SIZE` | No      | Pagination size for `core/file` endpoint (default 200) |

> All configuration values are treated as **strings** per SDK requirements.

---

## Requirements file

Example `requirements.txt`:

```text
fivetran-connector-sdk
requests
```

---

## Authentication

NetPrint uses a **custom Base64 authentication scheme**:
```
X-NPS-Authorization: base64(username%password%4)
```
The connector automatically builds this header during initialization.

---

## Data model

### Tables created

#### `system_info`
| Field | Type | Description |
|--------|------|-------------|
| _dynamic_ | VARIES | Returns system-level info from `core/information` |
| `_fivetran_deleted` | BOOLEAN | Always `False` (no deletions expected) |

#### `folder_usage`
| Field | Type | Description |
|--------|------|-------------|
| _dynamic_ | VARIES | Returns folder usage from `core/folderSize` |
| `_fivetran_deleted` | BOOLEAN | Always `False` |

#### `files`
| Field | Type | Description |
|--------|------|-------------|
| `accessKey` | STRING | Unique file key (primary key) |
| `_fivetran_deleted` | BOOLEAN | Soft delete flag |
| _all other fields_ | VARIES | Metadata from `core/file` endpoint |

---

## Incremental sync logic

The connector stores state per table:

```json
"files": {
  "last_synced_at": "2025-10-21T09:00:00Z",
  "known_keys": ["ABCD123", "XYZ987", ...]
}
```

On each sync:
1. **Fetch active files** with paging (`core/file?fromCount=&showCount=`).
2. Track all current file `accessKey` values.
3. Compare against `known_keys` from previous state.
4. Insert/Upsert all current files with `_fivetran_deleted = False`.
5. Soft delete any missing keys by emitting `_fivetran_deleted = True`.
6. Save new state with updated `last_synced_at` and `known_keys`.

---

## Retry and rate limiting

When the API returns HTTP **429 Too Many Requests**, the connector:
- Reads `Retry-After` header (defaults to 60 seconds if absent)
- Sleeps for the indicated time
- Retries the request once

Example log:
```
429 from core/file. Sleeping 30s and retrying once.
```

---

## Checkpointing

State checkpoints ensure resumable syncs and incremental continuity.

```python
op.checkpoint(state)
```

A checkpoint is emitted after each sync cycle, storing the new `last_synced_at` and `known_keys`.

---

## Error handling

| Error | Description | Resolution |
|--------|-------------|-------------|
| 401 / 403 | Invalid credentials | Check username and password |
| 404 | Endpoint missing or invalid | Verify base URL |
| 429 | API throttled | Connector retries automatically |
| Network failure | Connection issue | Connector retries or logs |
| Invalid JSON | Malformed response | Skipped and logged |

---

## Example logs

```
Connected to NetPrint API.
Fetched 200 files from core/file.
files synced: 784, soft-deleted: 12
Checkpoint created.
```

---

## Local testing

Run the connector locally for debugging:

```bash
fivetran debug --configuration configuration.json
```

Example debug summary:

```
Operation       | Calls
----------------+------------
Upserts         | 996
Updates         | 0
Deletes         | 0
Truncates       | 0
SchemaChanges   | 3
Checkpoints     | 4
```

---

## Deployment

Deploy the connector in your Fivetran workspace:

```bash
fivetran deploy --destination <DESTINATION_NAME>                 --connection netprint_connector                 --configuration configuration.json
```

---

## Additional considerations

* The connector uses `_fivetran_deleted` for soft deletes instead of DELETE operations.
* Timestamps are converted to **UTC** via Python’s `datetime.fromisoformat()`.
* Missing or malformed datetimes default to epoch UTC (`1970-01-01T00:00:00Z`).
* For large datasets, increase `PAGE_SIZE` in configuration for fewer API calls.
* Each endpoint call (`core/information`, `core/folderSize`, `core/file`) is independent—failures in one do not halt the others.

---