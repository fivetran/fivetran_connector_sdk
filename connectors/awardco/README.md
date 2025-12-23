# Awardco Users Connector Example
AwardCo is an employee recognition platform. The AwardCo API typically exposes RESTful JSON endpoints to manage resources such as users and recognition-related entities. Authentication is performed via an API key supplied in request headers. Common behaviors include paginated responses, timestamp fields for change tracking, and conventional HTTP status codes. 

Refer to [AwardCo documentation](https://www.awardco.com/) for additional details and production configurations.

## Connector overview
This example connector uses the Fivetran Connector SDK to sync AwardCo user data into your destination. It performs incremental syncs based on a timestamp cursor, upserts user rows into a single `USER` table, and emits checkpoints for reliable resumption. It also supports paginated requests and can pass a timestamp filter to the AwardCo API so only changed records are returned (avoiding full table scans). There is a local mock mode for offline development and testing.

## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04+ / Debian 10+ / Amazon Linux 2+ (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) to get started.
## Features
- Incremental sync based on `updated_at` timestamp
- Pagination support for large datasets
- Automatic retry with exponential backoff for transient failures
- Configurable timestamp query parameter for API compatibility

## Configuration file
Configuration keys uploaded to Fivetran come from `configuration.json` (string values only):

```
{
  "api_key": "<YOUR_AWARDCO_API_KEY>",
  "base_url": "<YOUR_AWARDCO_BASE_URL>",
  "updated_since_param": "<OPTIONAL_QUERY_PARAM_NAME_DEFAULT_UPDATED_SINCE>"
}
```

Note: Ensure that the `configuration.json` file is not checked into version control to protect sensitive information.

### Validation rules
- `api_key` (required): AwardCo API key.
- `base_url` (required): AwardCo API base URL. Do not include a trailing `/api` because the connector appends the endpoint path (for example, `/api/users`).
- `updated_since_param` (optional): Name of the query parameter the AwardCo API expects for incremental filtering. Defaults to `updated_since`. Set this to `updated_after` or another name if your API requires it.

### Implementation reference
- Validation — `awardco-users-connector/connector.py:70-81`
- Main config usage in update — `awardco-users-connector/connector.py:198-199`

## Requirements file
This connector does not require any additional packages beyond those provided by the Fivetran environment.

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
This connector uses an API key. The request includes the key in the `apiKey` header when calling the AwardCo API. To obtain the API key, do the following:

### Steps
1. Obtain an API key from your AwardCo administrator or developer portal.
2. Set `api_key` in `configuration.json`.
3. Set `base_url` (for example, `https://api.awardco.com`).

### Reference
Refer to the `make_request_with_retry()` and `fetch_users()` functions in `connector.py` for implementation details. See also `awardco-users-connector/connector.py:58` and `awardco-users-connector/connector.py:72`.

## Pagination
This connector implements simple page-based pagination. The `update` loop requests pages from the users endpoint and processes each page in turn. If the AwardCo API supports a query parameter for returning only records updated since a timestamp, the connector will pass the checkpointed `last_sync_time` on every page request to avoid full-table scans.

## Data handling
- Table mapping: All user records are upserted into the `user` table.
- Primary key: `employeeId`.
- Incremental field: `updated_at` (the maximum value is stored as `last_sync_time`).
- State management: The connector writes a checkpoint with `last_sync_time` after processing each page.

## Error handling
The connector validates configuration and wraps network and processing steps in a try/except, surfacing failures with a clear error message. Logging uses the SDK logger for observability.

Recommendations
- Add `log.severe(...)` on critical failures where appropriate.
- Consider retries and rate-limit handling if the AwardCo API enforces limits.

## Tables created
- `USER` — Primary key: `employeeId`.

Example fields: `employeeId`, `firstName`, `lastName`, `email`, `balance`, `currencyCode`, `updated_at`, `created_at`.

After a debug run, validate the DuckDB output (default: `warehouse.db`) and check operation counts in the CLI summary.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
