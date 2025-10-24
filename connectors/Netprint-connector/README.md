# NetPrint Connector Example

## Connector overview
The NetPrint connector demonstrates how to use the Fivetran Connector SDK to extract data from the NetPrint Web API provided by [printing.ne.jp](https://printing.ne.jp). The connector retrieves information about user storage usage, account metadata, and uploaded files, then loads that data into a Fivetran destination.

It performs a full sync for small endpoints (`core/information`, `core/folderSize`) and an incremental sync for the `core/file` endpoint, which provides metadata about files stored in the user's account. The connector supports soft deletes for files that are removed between syncs.

This example is designed for developers who want to learn how to build custom connectors for APIs that use pagination, rate limits, and state-based incremental updates.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
    - Windows: 10 or later (64-bit only)
    - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
    - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started. This guide walks you through installing dependencies, configuring your connector locally, and running `fivetran debug` to test your implementation.

## Features
- Retrieves system information, folder usage, and file metadata from the NetPrint API.
- Performs a full refresh for small datasets and incremental syncs for large datasets using the `uploadDate` or `registrationDate` fields.
- Implements soft deletes using the `_fivetran_deleted` flag to mark removed files.
- Includes automatic handling of HTTP `429 Too Many Requests` responses with retry and backoff.
- Supports paging using the `fromCount` and `showCount` query parameters.
- Converts all timestamps to UTC for consistency.

## Configuration file
The connector reads configuration values from `configuration.json`. These settings are used to authenticate to the NetPrint API and to define pagination and base URL parameters.

```
{
  "username": "your_netprint_username",
  "password": "your_netprint_password",
  "BASE_URL": "https://api-s.printing.ne.jp/usr/webservice/api/",
  "PAGE_SIZE": "200"
}
```

| Key | Required | Description |
|-----|-----------|-------------|
| `username` | Yes | Your NetPrint account username. |
| `password` | Yes | Your NetPrint account password. |
| `BASE_URL` | No | The base URL for the NetPrint API (defaults to the production API). |
| `PAGE_SIZE` | No | Number of records retrieved per page from the API (default is 200). |

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

## Requirements file
The `requirements.txt` file specifies Python libraries required by the connector. For this example:

```
fivetran-connector-sdk
requests
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
The connector authenticates to the NetPrint API using a custom Base64-encoded header.

```
X-NPS-Authorization: base64(username%password%4)
```

This header is generated automatically when the connector is initialized. No OAuth or additional tokens are required.

## Pagination
Pagination is handled in the `NetPrintAPI.iter_files()` function. The connector uses the `fromCount` and `showCount` parameters to retrieve data in pages of a configurable size (`PAGE_SIZE`, default 200).

The connector continues fetching until an empty `fileList` response is received, ensuring all records are retrieved.

## Data handling
Data extraction and delivery are performed in the `update(configuration, state)` function:
- Fetches data from three endpoints:
    - `core/information` → inserted into `system_info`
    - `core/folderSize` → inserted into `folder_usage`
    - `core/file` → inserted into `files`
- Each endpoint response is emitted to Fivetran as `Upsert` operations.
- Missing files are marked with `_fivetran_deleted = True` to indicate soft deletion.
- The connector stores incremental state in the `state["files"]` dictionary, which includes `last_synced_at` and a list of `known_keys`.

Example state structure:
```
"files": {
  "last_synced_at": "2025-10-21T09:00:00Z",
  "known_keys": ["AK1", "AK2", "AK3"]
}
```

## Error handling
Refer to `NetPrintAPI._request()`.

The connector implements error handling for network and API-related issues:
- Retries once automatically for HTTP `429 Too Many Requests` responses, honoring the `Retry-After` header.
- Raises `PermissionError` for authentication failures (`401` or `403`).
- Logs a warning for `404` (resource not found).
- Logs and skips invalid JSON responses.
- All other HTTP errors raise a `RuntimeError`.

## Tables created
The connector creates the following destination tables.

### system_info
Contains general account and system-level information returned from the `core/information` endpoint.

| Column | Description |
|--------|-------------|
| *Dynamic fields* | Vary depending on API response. |
| `_fivetran_deleted` | Always false; indicates active record. |

### folder_usage
Contains details about total and used storage space returned from the `core/folderSize` endpoint.

| Column | Description |
|--------|-------------|
| *Dynamic fields* | Vary depending on API response. |
| `_fivetran_deleted` | Always false; indicates active record. |

### files
Contains metadata for each uploaded file returned from the `core/file` endpoint.

| Column | Description |
|--------|-------------|
| `accessKey` | Primary key identifying each file. |
| `_fivetran_deleted` | Boolean flag for soft deletes. |
| *Other fields* | Include file name, upload date, size, and related metadata. |

## Additional files
- `netprint_connector.py` – Contains all connector logic, including schema definition and update functions.
- `NetPrintAPI` – Internal helper class that wraps HTTP requests and implements retry logic.
- `_parse_dt()` – Converts timestamps from NetPrint API into UTC datetime objects.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we’ve tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
