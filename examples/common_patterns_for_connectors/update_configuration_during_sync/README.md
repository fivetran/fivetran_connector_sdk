# Updating Configuration During Sync Connector Example

## Connector overview
This example demonstrates how to update the configuration values using [Fivetran REST API](https://fivetran.com/docs/rest-api/api-reference/connections/modify-connection?service=connector_sdk) during a sync. It simulates a login flow where credentials are exchanged for a temporary session token, which is then used to authorize subsequent API requests. This temporary session token is updated in the configuration using the Fivetran REST API during the sync. The connector retrieves user records and upserts them into a destination table named `USER`.

This example is intended for learning purposes and uses the [fivetran-api-playground](https://pypi.org/project/fivetran-api-playground/) package to mock API responses locally. It is not meant for production use.


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)   
- `fivetran-connector-sdk`: version `2.2.1` or later
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Distributions such as Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)


## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.


## Features
- Authenticates via a temporary session token obtained through a `/login` endpoint.
- Refreshes session tokens after expiration and updates the configuration accordingly.
- Handles configuration updates during runtime using Fivetran REST API.
- Separates credential handling and data retrieval in dedicated helper functions.
- Retrieves and syncs user records into a Fivetran-managed destination table.
- Demonstrates session-based authentication handling, error recovery, and state checkpointing.


## Configuration file
The connector requires the following configuration parameters: 

```json
{
  "username": "<YOUR_USERNAME>",
  "password": "<YOUR_PASSWORD>",
  "fivetran_api_key": "<YOUR_BASE64_ENCODED_FIVETRAN_API_KEY>"
}
```

The configuration also includes the `token` key. The value of the `token` key is updated during the sync process using Fivetran REST API.

> Ensure that the entire configuration needs to be passed as payload while calling the Fivetran REST API endpoint, and it will override all the keys present.

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.


## Requirements file
The connector does not require any additional packages beyond the pre-installed ones.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.


## Authentication
Authentication is done via a two-step session process:
- The connector calls a login endpoint `(/login)` with username and password.
- The response includes a session token, which is used in the `Authorization` header for subsequent data API calls.
    Example:
    `Authorization: Token <SESSION_TOKEN>`
- The session token expires after a set duration (`__TOKEN_EXPIRY_TIME`). The connector refreshes the token as needed and updates the configuration using Fivetran REST API.


## Pagination
This connector retrieves all data in a single request (no pagination). If extending to a paginated API, modify the `sync_items()` function to loop through pages using cursor tokens or offsets.


## Data handling
- Logs in to the mock API at `http://127.0.0.1:5001/auth/session_token/login` to retrieve a session token.
- Updates the configuration with the new token using Fivetran REST API.
- Retrieves user records from the `/data` endpoint using the token.
- Data is processed and passed to `op.upsert()` for syncing into Fivetran.
- `op.checkpoint()` is called to persist state after a successful sync.


## Error handling
- Missing credentials trigger a ValueError in `get_session_token()`.
- HTTP errors are caught via `raise_for_status()` in the `get_api_response()` function.
- Logging is done using the `fivetran_connector_sdk.Logging` module.


## Tables created
The connector creates the `USER` table:

```
{
  "table": "user",
  "primary_key": ["id"],
  "columns": {
    "id": "STRING",
    "name": "STRING",
    "updatedAt": "UTC_DATETIME",
    "createdAt": "UTC_DATETIME"
  }
}
```


## Additional considerations

The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
