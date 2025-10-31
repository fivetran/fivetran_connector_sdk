# People.ai Connector Example

## Connector overview
This example demonstrates how to extract activity and participant data from the People.ai API and load it into a destination using the Fivetran Connector SDK.  
The connector:
- Authenticates with OAuth2 using the client credentials grant type.
- Retrieves records from `/v0/public/activities` and `/v0/public/activities/{type}` endpoints.
- Supports token refresh and exponential backoff on transient errors.
- Upserts all records into their respective destination tables (`activity`, `participants`).

Related functions in `connector.py`:  
`schema`, `update`, `get_page`, `sync_base_activities`, `sync_activity_type`, `get_access_token`, `validate_configuration`.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04 or later, Debian 10 or later, or Amazon Linux 2 or later (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) for setup instructions.

For local testing, this example includes a `__main__` block that reads `configuration.json` and runs `connector.debug(...)`.

## Features
- **Activity ingestion:** Retrieves paginated activity data from `/v0/public/activities`.
- **Participants sync:** Retrieves participant details from `/v0/public/activities/participants`.
- **Authentication:** Automatically refreshes the access token when a 401 response is received.
- **Error handling:** Retries failed requests with exponential backoff for transient 5xx and connection errors.
- **Schema:** Defines two destination tables — `activity` and `participants`.

## Configuration file
The `configuration.json` file provides API credentials required for authentication.

```json
{
  "api_key": "<your_api_key>",
  "api_secret": "<your_api_secret>"
}
```
- `api_key`: Your People.ai API key (client ID).  
- `api_secret`: Your People.ai API secret (client secret).

### Notes
- Ensure that `configuration.json` is not committed to version control.  
- Both configuration values are required; the connector will raise an error if either is missing.

## Requirements file
The `requirements.txt` file lists external libraries needed for this connector.

Example content of `requirements.txt`:
python-dotenv==1.1.1

- `python-dotenv==1.1.1` is required to load environment variables from a `.env` file.

## Authentication
- Type: OAuth2 Client Credentials  
- Token URL: `https://api.people.ai/auth/v1/tokens`  
- Headers: `Content-Type: application/x-www-form-urlencoded`  
- Grant Type: `client_credentials`  
- Access Token Header: `Authorization: Bearer <access_token>`

Authentication is handled by `get_access_token`.  
The connector uses a reauthentication closure (`reauthenticate`) to refresh the token automatically when a `401` error occurs.

## Pagination
Both `/activities` and `/activities/{type}` endpoints use offset-based pagination.

- The connector fetches data in pages using `limit` and `offset` query parameters.  
- Pagination continues until fewer than `limit` records are returned.  
- Each page is upserted into the destination table using `op.upsert(...)`.

**Functions responsible for pagination:**
- `get_page`: Fetches a single page with retry and reauth logic.  
- `sync_base_activities`: Iterates through all pages for `/activities`.  
- `sync_activity_type`: Iterates through all pages for `/activities/{type}` (e.g., `participants`).

## Data handling
- Schema definition: `schema(configuration)` defines two tables:
  - `activity` (primary key: `uid`)
  - `participants` (primary key: `uid`, `email`)
- Renaming: The `subject` field (if present) is renamed to `api_subject` to avoid conflicts.  
- Upserts: All rows are written using `op.upsert(...)` to allow incremental updates.  
- Error resilience:
  - Retries up to five times for `5xx` and network errors with exponential backoff.  
  - Refreshes the access token once upon a `401` error.

## Error handling
- 401 Unauthorized: Triggers a single reauthentication attempt using `reauth_func`.  
- 502–599 Server Errors: Retries the request up to 5 times, with delays increasing exponentially (`2, 4, 8, 16, 32` seconds).  
- Connection errors: Retries similarly to `5xx` cases.  
- Configuration validation: Early failure if `api_key` or `api_secret` are missing.  
- Logging: Uses `fivetran_connector_sdk.Logging` for all status, error, and retry messages.

## Tables created
Summary of tables replicated.

### activity
- Primary key: `uid`  
- Selected columns (not exhaustive):  
  `uid`, `sub_type`, `created_at`, `activity_type`, `updated_at`

### participants
- Primary key: `uid`, `email`  
- Selected columns (not exhaustive):  
  `uid`, `email`, `status`, `name`, `ingested_at`, `phone_number`

## Additional files
- `connector.py` – Contains all core logic: `schema`, `update`, `get_page`, `sync_base_activities`, `sync_activity_type`, `get_access_token`, and `validate_configuration`.  
- `configuration.json` – Contains API credentials (`api_key`, `api_secret`).  
- `requirements.txt` – Lists any third-party Python libraries required (e.g., `requests`).

## Additional considerations
This connector provides a reference implementation for integrating with the People.ai API using the Fivetran Connector SDK.  
While this code has been tested for common cases, Fivetran cannot guarantee compatibility with all API configurations or rate limits.  

For troubleshooting or enhancements, consult the [Fivetran Connector SDK documentation](https://fivetran.com/docs/connectors/connector-sdk)  
or contact Fivetran Support.