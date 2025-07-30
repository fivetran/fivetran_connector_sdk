# Key-Based Pagination Connector Example

## Connector overview
This connector demonstrates how to build a modular, testable data pipeline using the Fivetran Connector SDK. It simulates syncing synthetic user data from a mock REST API powered by the `Faker` library. The connector supports incremental syncing using a timestamp `updatedAt` and paginated retrieval using an offset mechanism.

This connector also supports Priority Sync Flow (PFS) - a Fivetran optimization designed to prioritize the most critical or frequently-changing tables during a sync. This helps minimize sync latency for key datasets while allowing less critical data to sync afterward.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started
Refer to the [Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- Uses Faker to generate diverse test `USER` data.
- Syncs from a mock API with offset-based pagination.
- Demonstrates modular connector design using `users_sync.py`.
- Implements incremental sync logic using a timestamp field `updatedAt`.


## Configuration file
This example does not require a configuration file.

In production, configuration.json might contain API tokens, initial cursors, or filters to narrow down API results.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
This connector requires the following Python dependencies:

```
Faker==30.3.0
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
This connector connects to a local mock server and does not require authentication. In a production scenario, use headers or token-based authentication in your request logic within `users_sync.py`.


## Pagination
Pagination is implemented in the `users_sync.py` module using offset-based pagination. Key behaviors:
- Starts with `offset=0` and fetches batches of users using a `limit`.
- Continues fetching until the API returns no further results.
- Updates the replication cursor using the `updatedAt` timestamp.
- Maintains pagination state in the connector’s `state` dictionary.


## Data handling
- User records are generated dynamically and returned as JSON.
- The connector sync each user record using `op.upsert()` into the `USER` table.
- After each batch, `op.checkpoint()` stores the updated sync state.
- Schema is explicitly defined via the `schema()` function in `connector.py`.


## Error handling
- Basic error handling is implemented via `response.raise_for_status()`.
- Invalid API responses or missing fields are skipped with log warnings.
- Sync will resume from the last checkpointed state if interrupted.


## Tables Created
The connector creates one table:

```
{
  "table": "user",
  "primary_key": ["id"],
  "columns": {
    "id": "STRING",
    "name": "STRING",
    "email": "STRING",
    "address": "STRING",
    "company": "STRING",
    "job": "STRING",
    "updatedAt": "UTC_DATETIME",
    "createdAt": "UTC_DATETIME"
  }
}
```


## Additional files
- `mock_api.py` – A FastAPI-based server that simulates a REST API for serving Faker-generated user data.
- `users_sync.py` – A utility module that contains the `sync_users()` function, which handles all API requests, pagination logic, and sync state management.


## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.