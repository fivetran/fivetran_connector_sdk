# AwardCo Users Connector Example
AwardCo is an employee recognition platform. The AwardCo API typically exposes RESTful JSON endpoints to manage resources such as users and recognition-related entities. Authentication is performed via an API key supplied in request headers. Common behaviors include paginated responses, timestamp fields for change tracking, and conventional HTTP status codes. 

Refer to [AwardCo documentation](https://www.awardco.com/) for additional details and production configurations.

## Connector overview
This example connector uses the Fivetran Connector SDK to sync AwardCo user data into your destination. It performs incremental syncs based on a timestamp cursor, upserts user rows into a single `USER` table, and emits checkpoints for reliable resumption. There is also a local mock mode for offline development and testing.

Related code
- Configuration validation — `awardco-users-connector/connector.py:13`
- Schema definition — `awardco-users-connector/connector.py:26`
- Sync/update logic — `awardco-users-connector/connector.py:47`


## Requirements
- [Supported Python versions](https://github.com/fivetran/fivetran_connector_sdk/blob/main/README.md#requirements)
- Operating system:
  - Windows: 10 or later (64-bit only)
  - macOS: 13 (Ventura) or later (Apple Silicon [arm64] or Intel [x86_64])
  - Linux: Ubuntu 20.04+ / Debian 10+ / Amazon Linux 2+ (arm64 or x86_64)

## Getting started
Refer to the [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide).

Quick start
1) Copy and edit `awardco-users-connector/configuration.json`.
2) Run a local debug sync:

```
fivetran debug --configuration awardco-users-connector/configuration.json
```

Tip: Set `use_mock` to `"true"` to read from `awardco-users-connector/files/mock_users.json` and avoid network calls.

## Features
- Incremental sync using an ISO-8601 `updated_at` cursor (stored as `last_sync_time`).
- Idempotent upserts into the `user` table with a stable primary key.
- Local mock mode for offline development and deterministic testing.
- Simple state checkpointing at the end of each sync.

## Configuration file
Configuration keys uploaded to Fivetran come from `configuration.json` (string values only):

```
{
  "api_key": "<YOUR_AWARDCO_API_KEY>",
  "base_url": "<YOUR_AWARDCO_BASE_URL>",
  "rate_limit": "<YOUR_AWARDCO_RATE_LIMIT>",
  "use_mock": "<TRUE_OR_FALSE>"
}
```

Validation rules
- `api_key` (required): AwardCo API key.
- `base_url` (required): AwardCo API base URL. Do not include a trailing `/api` because the connector appends the endpoint path (for example, `/api/users`).
- `rate_limit` (optional): User-provided hint; not enforced by code.
- `use_mock` (optional): When `true`, reads `files/mock_users.json` instead of calling the API.

Implementation reference
- Validation — `awardco-users-connector/connector.py:13`
- Config usage — `awardco-users-connector/connector.py:58`

## Requirements file
*Explain the role of the `requirements.txt` file in specifying the Python libraries required by the connector.*

*Example content of `requirements.txt`:*

```
pandas
```

Note: The `fivetran_connector_sdk:latest` and `requests:latest` packages are pre-installed in the Fivetran environment. To avoid dependency conflicts, do not declare them in your `requirements.txt`.

## Authentication
This connector uses an API key. The request includes the key in the `apiKey` header when calling the AwardCo API. To obtain the API key, do the following:

Steps
1. Obtain an API key from your AwardCo administrator or developer portal.
2. Set `api_key` in `configuration.json`.
3. Set `base_url` (for example, `https://api.awardco.com`).

Reference — `awardco-users-connector/connector.py:58` and `awardco-users-connector/connector.py:72`

## Pagination
The sample implementation targets a single users endpoint and does not implement pagination. If the AwardCo API responds with paginated results, extend `update` to request subsequent pages and upsert each batch.

For inspiration, see the mock utilities that simulate a `has_more` flag:
- `awardco-users-connector/mock_responses.py:84`
- `awardco-users-connector/mock_api.py:33`

## Data handling
- Table mapping: all user records are upserted into the `user` table.
- Primary key: `employeeId`.
- Incremental field: `updated_at` (the maximum value is stored as `last_sync_time`).
- State management: the connector writes a checkpoint with `last_sync_time` after processing.

References
- Upserts — `awardco-users-connector/connector.py:76`
- Cursor tracking — `awardco-users-connector/connector.py:61` and `awardco-users-connector/connector.py:80`
- Checkpoint — `awardco-users-connector/connector.py:83`

## Error handling
The connector validates configuration and wraps network and processing steps in a try/except, surfacing failures with a clear error message. Logging uses the SDK logger for observability.

Examples
- Startup log — `awardco-users-connector/connector.py:54`
- Config validation — `awardco-users-connector/connector.py:13`
- Error propagation — `awardco-users-connector/connector.py:86`

Recommendations
- Add `log.severe(...)` on critical failures where appropriate.
- Consider retries and rate-limit handling if the AwardCo API enforces limits.

## Tables created
- `USER` — Primary key: `employeeId`.

Example fields: `employeeId`, `firstName`, `lastName`, `email`, `balance`, `currencyCode`, `updated_at`, `created_at`.

After a debug run, validate the DuckDB output (default: `warehouse.db`) and check operation counts in the CLI summary.

## Additional considerations
The examples provided are intended to help you effectively use Fivetran's Connector SDK. While we've tested the code, Fivetran cannot be held responsible for any unexpected or negative consequences that may arise from using these examples. For inquiries, please reach out to our Support team.
