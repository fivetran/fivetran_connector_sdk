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
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.

## Features
- Retrieves system information, folder usage, and file metadata from the NetPrint API.
- Performs a full refresh for small datasets and incremental syncs for large datasets using the `uploadDate` or `registrationDate` fields.
- Implements soft deletes using the `_fivetran_deleted` flag to mark removed files.
- Includes automatic handling of HTTP `429 Too Many Requests` responses with retry and backoff.
- Supports paging using the `fromCount` and `showCount` query parameters.
- Converts all timestamps to UTC for consistency.

## Configuration file
The connector reads configuration values from `configuration.json`. These settings are used to authenticate to the NetPrint API and to define pagination and base URL parameters.

```json
{
  "username": "<YOUR_NETPRINT_USERNAME>",
  "password": "<YOUR_NETPRINT_PASSWORD>",
  "BASE_URL": "<NETPRINT_API_BASE_URL>"
}
```

| Key | Required | Description |
| --- | -------- | ----------- |
| username | Yes | Your NetPrint account username. |
| password | Yes | Your NetPrint account password. |
| BASE_URL | No | The base URL for the NetPrint API (defaults to the production API). |

Note: Ensure that the configuration.json file is not checked into version control to protect sensitive information.

## Requirements file
The requirements.txt file lists only external dependencies that are not preinstalled in the Fivetran runtime.

In this example, the connector does not require any additional dependencies, so your requirements.txt should either be empty or omitted.

Do not include:
- `fivetran_connector_sdk`
- `requests`

These are already available in the environment.

## Authentication
The connector authenticates to the NetPrint API using a Base64-encoded header (`X-NPS-Authorization`) that contains the username, password, and a service identifier.

To set up authentication:
1. Obtain your NetPrint account credentials (username and password).
2. Add your credentials to the `configuration.json` file.
3. The connector will automatically encode these credentials in the required format for API authentication.

## Pagination
Pagination is handled in the NetPrintAPI.iter_files() function. The connector uses the fromCount and showCount parameters to retrieve data in pages of a configurable size (PAGE_SIZE, default 200).

The connector continues fetching until an empty fileList response is received.

## Data handling
Data extraction and delivery are performed in the update(configuration, state) function:

Fetches data from three endpoints:

- core/information → loaded into system_info

- core/folderSize → loaded into folder_usage

- core/file → loaded into files

- Each response is delivered using Upsert operations.

- Deleted files are marked using _fivetran_deleted = True.

- Incremental state is stored under state["files"], including:

  - last_synced_at (timestamp bookmark)
  - known_keys (list of previously seen accessKey values)

Example state:
```json
{
  "files": {
    "last_synced_at": "2025-10-21T09:00:00Z",
    "known_keys": ["AK1", "AK2", "AK3"]
  }
}
```

## Error handling
The connector implements error handling for network and API-related issues:

Retries for 429 responses based on the Retry-After header.

Raises PermissionError for 401 and 403 status codes.

Logs a warning for 404 responses.

Logs and skips invalid JSON.

Raises errors for other unexpected HTTP responses.

## Tables created

### system_info
| Column | Description |
| ------ | ----------- |
| Dynamic fields | Vary by API response. |
| _fivetran_deleted | Always false. |

### folder_usage
| Column | Description |
| ------ | ----------- |
| Dynamic fields | Vary by API response. |
| _fivetran_deleted | Always false. |

### files
| Column | Description |
| ------ | ----------- |
| accessKey | Primary key identifying each file. |
| _fivetran_deleted | Boolean flag for soft deletes. |
| Other fields | Include filename, upload timestamp, size, etc. |

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.